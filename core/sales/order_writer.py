"""Kafka order events → PostgreSQL (see docs/ARCHITECTURE.md section 7, step 6)."""
import json
import logging
import uuid
from collections import defaultdict
from dataclasses import dataclass
from functools import partial

from django.db import IntegrityError, connection

from . import redis_client as rc
from .db import run_repeatable_read

log = logging.getLogger("sales.order_writer")


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    sale_id: int
    user_id: int


class BadEvent(ValueError):
    pass


class Oversold(Exception):
    """The DB has fewer units than Redis admitted for this batch."""


def parse(raw: bytes) -> OrderEvent:
    try:
        data = json.loads(raw)
        return OrderEvent(str(uuid.UUID(data["order_id"])), int(data["sale_id"]), int(data["user_id"]))
    except (ValueError, KeyError, TypeError) as exc:
        raise BadEvent(str(exc)) from exc


def persist(events: list[OrderEvent]) -> list[str]:
    """Write a batch. Returns the order_ids newly inserted as PENDING (to be fulfilled)."""
    by_sale: dict[int, list[OrderEvent]] = defaultdict(list)
    for event in events:
        by_sale[event.sale_id].append(event)

    new_pending: list[str] = []
    for sale_id, sale_events in by_sale.items():
        try:
            new_pending += run_repeatable_read(partial(_insert_batch, sale_id, sale_events))
        except (Oversold, IntegrityError) as exc:
            # Rare: Redis admitted more than the DB has, or a bad event (unknown sale/user).
            log.warning("sale %s: batch of %d fell back to one-by-one (%s)", sale_id, len(sale_events), type(exc).__name__)
            new_pending += _insert_one_by_one(sale_events)
    return new_pending


def _insert_batch(sale_id: int, events: list[OrderEvent]) -> list[str]:
    rows = ", ".join(["(%s, %s, %s, 'PENDING')"] * len(events))
    params = [value for e in events for value in (uuid.UUID(e.order_id), e.sale_id, e.user_id)]
    with connection.cursor() as cur:
        # A redelivered Kafka message has an order_id that already exists → skipped.
        cur.execute(
            f"INSERT INTO orders (id, sale_id, user_id, status) VALUES {rows} "
            "ON CONFLICT DO NOTHING RETURNING id",
            params,
        )
        inserted = [str(row[0]) for row in cur.fetchall()]
        if inserted:
            # Decrement only by the rows really inserted; never below zero.
            cur.execute(
                "UPDATE flash_sales SET remaining = remaining - %s WHERE id = %s AND remaining >= %s",
                [len(inserted), sale_id, len(inserted)],
            )
            if cur.rowcount == 0:
                raise Oversold(sale_id)  # rolls the whole batch back
    return inserted


def _insert_one_by_one(events: list[OrderEvent]) -> list[str]:
    new_pending = []
    for event in events:
        try:
            status = run_repeatable_read(partial(_insert_single, event))
        except IntegrityError as exc:
            # Unknown sale or user: a poison message. Retrying can never succeed, so drop it.
            log.error("dropping bad order event %s: %s", event, exc,
                      extra={"order_id": event.order_id, "sale_id": event.sale_id})
            continue
        if status == "PENDING":
            new_pending.append(event.order_id)
        elif status == "FAILED":
            # No stock change and no release.lua: this unit never existed in the DB.
            log.warning("sold out in DB, order FAILED", extra={"order_id": event.order_id, "sale_id": event.sale_id})
            rc.publish_order(event.user_id, event.order_id, event.sale_id, "FAILED")
    return new_pending


def _insert_single(event: OrderEvent) -> str | None:
    with connection.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (id, sale_id, user_id, status) VALUES (%s, %s, %s, 'PENDING') "
            "ON CONFLICT DO NOTHING RETURNING id",
            [uuid.UUID(event.order_id), event.sale_id, event.user_id],
        )
        if cur.fetchone() is None:
            return None  # duplicate
        cur.execute("UPDATE flash_sales SET remaining = remaining - 1 WHERE id = %s AND remaining >= 1", [event.sale_id])
        if cur.rowcount == 1:
            return "PENDING"
        cur.execute("UPDATE orders SET status = 'FAILED', updated_at = now() WHERE id = %s", [uuid.UUID(event.order_id)])
        return "FAILED"


def enqueue_fulfilment(order_id: str) -> None:
    from .tasks import fulfil_order

    fulfil_order.delay(order_id)


def on_orders_committed(order_ids: list[str]) -> None:
    """Called after the DB commit: every new PENDING order goes to Celery for fulfilment."""
    for order_id in order_ids:
        enqueue_fulfilment(order_id)
    if order_ids:
        log.info("committed %d new orders", len(order_ids))
