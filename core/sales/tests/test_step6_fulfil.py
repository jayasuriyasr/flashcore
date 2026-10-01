import json
import threading
import uuid

import pytest
from django.db import connection

from sales import order_writer, services
from sales import redis_client as rc
from sales.models import FlashSale, Order
from sales.order_writer import OrderEvent
from sales.tasks import fulfil, fulfil_order

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def pending_order(make_sale, make_users, redis_db):
    """A sale live in Redis, one user who reserved, and the order the writer stored."""
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    user, = make_users(1)
    reserve = rc._script("reserve")
    assert reserve(keys=[rc.stock_key(sale.id), rc.buyers_key(sale.id)], args=[user.id]) == 1
    order_id = str(uuid.uuid4())
    assert order_writer.persist([OrderEvent(order_id, sale.id, user.id)]) == [order_id]
    return sale, user, Order.objects.get(pk=order_id)


def payments(redis_db, order):
    return int(redis_db.get(f"payments:{order.id}") or 0)


def test_successful_payment_confirms(pending_order, redis_db):
    sale, user, order = pending_order
    assert fulfil(str(order.id)) == "CONFIRMED"
    order.refresh_from_db()
    assert order.status == "CONFIRMED"
    assert payments(redis_db, order) == 1
    assert FlashSale.objects.get(pk=sale.pk).remaining == 9
    assert redis_db.get(rc.stock_key(sale.id)) == "9"


def test_running_twice_charges_once(pending_order, redis_db):
    _, _, order = pending_order
    fulfil(str(order.id))
    assert fulfil(str(order.id)) == "CONFIRMED"  # second run sees CONFIRMED and stops
    assert payments(redis_db, order) == 1


def test_five_parallel_runs_charge_once(pending_order, redis_db):
    _, _, order = pending_order
    errors = []

    def run():
        try:
            fulfil_order.apply(args=[str(order.id)]).get()
        except Exception as exc:  # surface failures from threads
            errors.append(exc)
        finally:
            connection.close()  # each thread has its own DB connection

    threads = [threading.Thread(target=run) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert payments(redis_db, order) == 1
    order.refresh_from_db()
    assert order.status == "CONFIRMED"


def test_failed_payment_returns_the_unit(pending_order, redis_db, monkeypatch):
    monkeypatch.setenv("PAYMENT_FAILURE_RATE", "1")
    sale, user, order = pending_order

    assert fulfil(str(order.id)) == "FAILED"

    order.refresh_from_db()
    assert order.status == "FAILED"
    assert FlashSale.objects.get(pk=sale.pk).remaining == 10  # DB +1
    assert redis_db.get(rc.stock_key(sale.id)) == "10"  # Redis +1
    assert not redis_db.sismember(rc.buyers_key(sale.id), user.id)

    # The user can buy again and gets a new PENDING order (partial unique constraint).
    reserve = rc._script("reserve")
    assert reserve(keys=[rc.stock_key(sale.id), rc.buyers_key(sale.id)], args=[user.id]) == 1
    new_id = str(uuid.uuid4())
    assert order_writer.persist([OrderEvent(new_id, sale.id, user.id)]) == [new_id]
    assert Order.objects.get(pk=new_id).status == "PENDING"


def test_failed_order_is_not_released_twice(pending_order, redis_db, monkeypatch):
    monkeypatch.setenv("PAYMENT_FAILURE_RATE", "1")
    sale, _, order = pending_order
    fulfil(str(order.id))
    fulfil(str(order.id))  # duplicate delivery
    assert FlashSale.objects.get(pk=sale.pk).remaining == 10
    assert redis_db.get(rc.stock_key(sale.id)) == "10"


def test_publishes_exactly_one_update(pending_order, redis_db):
    _, user, order = pending_order
    pubsub = redis_db.pubsub()
    pubsub.subscribe(f"user:{user.id}")
    pubsub.get_message(timeout=1)  # subscription confirmation

    fulfil(str(order.id))
    fulfil(str(order.id))  # no second message for an already-finished order

    messages = []
    while (msg := pubsub.get_message(timeout=1)) is not None:
        if msg["type"] == "message":
            messages.append(json.loads(msg["data"]))
    pubsub.close()
    assert messages == [{"type": "order", "order_id": str(order.id), "sale_id": order.sale_id, "status": "CONFIRMED"}]


def test_order_writer_hands_new_orders_to_celery(make_sale, make_users, enqueued):
    sale = make_sale(total_stock=5)
    events = [OrderEvent(str(uuid.uuid4()), sale.id, u.id) for u in make_users(3)]
    new_ids = order_writer.persist(events)
    order_writer.on_orders_committed(new_ids)
    assert sorted(enqueued) == sorted(e.order_id for e in events)
