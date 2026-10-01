import logging
import os
import random
import time

from celery import shared_task
from django.db import connection

from . import redis_client as rc
from .db import run_repeatable_read
from .locks import LockNotAcquired, RedisLock
from .models import Order

log = logging.getLogger("sales.tasks")


def mock_payment(order_id: str) -> bool:
    """Pretend to charge the card. Counts attempts so tests can prove nobody is charged twice."""
    failure_rate = float(os.environ.get("PAYMENT_FAILURE_RATE", "0.05"))
    delay_ms = int(os.environ.get("MOCK_PAYMENT_DELAY_MS", "100"))
    pipe = rc.client().pipeline()
    pipe.incr(f"payments:{order_id}")
    pipe.expire(f"payments:{order_id}", 3600)
    pipe.execute()
    time.sleep(delay_ms / 1000)
    return random.random() >= failure_rate


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

    paid = mock_payment(order_id)

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
