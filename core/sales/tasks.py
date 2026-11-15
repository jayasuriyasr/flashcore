import functools
import logging
import os
import random
import time
from datetime import timedelta
from functools import partial

import redis
from celery import shared_task
from django.conf import settings
from django.db import connection
from django.utils import timezone

from . import order_writer
from . import redis_client as rc
from .db import run_repeatable_read
from .locks import LockNotAcquired, RedisLock
from .models import FlashSale, Order

log = logging.getLogger("sales.tasks")


# The mock payment provider's side: one outcome per idempotency key, stored atomically.
# payments:{key} counts real charges, so tests can prove nobody was ever charged twice.
_PROVIDER_CHARGE_LUA = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then
  redis.call('INCR', KEYS[2])
  redis.call('EXPIRE', KEYS[2], ARGV[2])
  return ARGV[1]
end
return redis.call('GET', KEYS[1])
"""


@functools.cache
def _provider_charge():
    return rc.client().register_script(_PROVIDER_CHARGE_LUA)


def mock_payment(idempotency_key: str) -> bool:
    """Charge the card through a mock provider, using the order id as the idempotency key.

    Found by the chaos test: a worker killed after the provider charged the card but before
    our DB commit gets its task redelivered (acks_late), sees PENDING and pays again. Real
    providers (e.g. Stripe's Idempotency-Key) return the first result for a repeated key
    instead of charging twice; this mock does the same, atomically.
    """
    failure_rate = float(os.environ.get("PAYMENT_FAILURE_RATE", "0.05"))
    delay_ms = int(os.environ.get("MOCK_PAYMENT_DELAY_MS", "100"))
    time.sleep(delay_ms / 1000)  # provider round trip
    outcome = "declined" if random.random() < failure_rate else "approved"
    stored = _provider_charge()(
        keys=[f"payment:{idempotency_key}", f"payments:{idempotency_key}"], args=[outcome, 86400]
    )
    return stored == "approved"


@shared_task(bind=True, acks_late=True, max_retries=30)
def fulfil_order(self, order_id: str):
    # Celery delivers at least once (worker crash, visibility timeout, reconcile re-enqueue),
    # so the same order can be in flight twice. The lock + status check make it run once.
    try:
        with RedisLock(f"lock:order:{order_id}", ttl_ms=10_000, wait_s=2.0):
            return fulfil(order_id)
    except LockNotAcquired:
        raise self.retry(countdown=1) from None


def fulfil(order_id: str) -> str | None:
    order = Order.objects.filter(pk=order_id).values("status", "sale_id", "user_id").first()
    if order is None:
        log.warning("order not found", extra={"order_id": order_id})
        return None
    if order["status"] != Order.Status.PENDING:
        return order["status"]  # already handled by an earlier run

    paid = mock_payment(idempotency_key=order_id)

    def finish():
        with connection.cursor() as cur:
            if paid:
                cur.execute(
                    "UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id = %s AND status = 'PENDING'",
                    [order_id],
                )
                return "CONFIRMED" if cur.rowcount else None
            cur.execute(
                "UPDATE orders SET status = 'FAILED', updated_at = now() WHERE id = %s AND status = 'PENDING'",
                [order_id],
            )
            if not cur.rowcount:
                return None
            cur.execute("UPDATE flash_sales SET remaining = remaining + 1 WHERE id = %s", [order["sale_id"]])
            return "FAILED"

    status = run_repeatable_read(finish)
    if status is None:
        return None  # someone else moved it on; nothing to do
    # Side effects only after the DB commit.
    if status == "FAILED":
        rc.release(order["sale_id"], order["user_id"])  # unit back on sale, user may buy again
    rc.publish_order(order["user_id"], order_id, order["sale_id"], status)
    log.info("order %s", status.lower(), extra={"order_id": order_id, "sale_id": order["sale_id"]})
    return status


# --- Reconcile (Celery beat, every 60 s) -------------------------------------------------

STUCK_AFTER = timedelta(minutes=2)
REQUEUE_BATCH = 10_000  # per run (every 60 s): roughly what the workers finish in a minute
BUSY_QUEUE = 1_000      # more tasks than this already waiting: the workers are behind, nothing is lost


@functools.cache
def _broker() -> redis.Redis:
    return redis.Redis.from_url(settings.CELERY_BROKER_URL)


def fulfilment_queue_depth() -> int:
    """Tasks waiting in Celery's queue (with the Redis broker, a list named after the queue)."""
    return _broker().llen("celery")


@shared_task
def reconcile():
    results = [reconcile_sale(sale_id) for sale_id in FlashSale.objects.values_list("id", flat=True)]
    requeued = requeue_stuck_orders()
    return {"sales": len(results), "requeued": requeued}


def _snapshot(sale_id: int):
    # Two reads, one REPEATABLE READ snapshot: an order committed between them can't make
    # the numbers disagree (no phantom), so a reported violation is a real one.
    with connection.cursor() as cur:
        cur.execute("SELECT total_stock, remaining FROM flash_sales WHERE id = %s", [sale_id])
        total, remaining = cur.fetchone()
        cur.execute("SELECT count(*) FROM orders WHERE sale_id = %s AND status <> 'FAILED'", [sale_id])
        sold = cur.fetchone()[0]
    return total, remaining, sold


def reconcile_sale(sale_id: int) -> dict | None:
    """Check the DB invariant and correct Redis only in the safe direction. None = skipped (locked)."""
    try:
        with RedisLock(f"lock:reconcile:{sale_id}", ttl_ms=30_000):
            total, remaining, sold = run_repeatable_read(partial(_snapshot, sale_id))
            if remaining < 0 or remaining + sold != total:
                log.error("invariant violated: total=%s remaining=%s sold=%s", total, remaining, sold,
                          extra={"sale_id": sale_id})

            lowered = 0
            redis_stock = rc.get_stock(sale_id)
            if redis_stock is not None and redis_stock > remaining:
                # Unsafe: Redis could admit buyers the DB will reject. Lower it (never raises).
                lowered = rc.lower_stock(sale_id, remaining)
                log.warning("redis stock %s > db remaining %s: lowered by %s", redis_stock, remaining, lowered,
                            extra={"sale_id": sale_id})
            elif redis_stock is not None and redis_stock < remaining:
                # Safe (we undersell) and normal while events are in flight. End + Start the sale to resync.
                log.warning("redis stock %s < db remaining %s (safe direction, left as is)", redis_stock, remaining,
                            extra={"sale_id": sale_id})
            return {"sale_id": sale_id, "total": total, "remaining": remaining, "sold": sold,
                    "redis": redis_stock, "lowered": lowered}
    except LockNotAcquired:
        log.info("reconcile already running for this sale", extra={"sale_id": sale_id})
        return None


def requeue_stuck_orders(limit: int = REQUEUE_BATCH) -> int:
    """Orders PENDING for over 2 minutes with nothing waiting in the queue lost their Celery task
    (worker killed, broker restarted): send them again, oldest first.
    Safe because fulfil_order is idempotent (lock + status check).

    Measured: with a fixed 1,000 per run in no particular order, 17 orders that lost their task sat
    behind a 70,000-order backlog for over an hour. Now a run sends as much as the workers can clear
    before the next one, and does nothing while the queue is still busy (a long queue means "behind",
    not "lost", and re-sending would only add duplicates to it).
    """
    waiting = fulfilment_queue_depth()
    if waiting > BUSY_QUEUE:
        log.info("%d fulfilment tasks already queued; not re-enqueueing", waiting)
        return 0
    cutoff = timezone.now() - STUCK_AFTER
    ids = list(
        Order.objects.filter(status=Order.Status.PENDING, created_at__lt=cutoff)
        .order_by("created_at")
        .values_list("id", flat=True)[:limit]
    )
    for order_id in ids:
        order_writer.enqueue_fulfilment(str(order_id))
    if ids:
        log.warning("re-enqueued %d stuck PENDING orders", len(ids))
    return len(ids)
