import logging
from datetime import timedelta

import pytest
from django.utils import timezone

from sales import redis_client as rc
from sales import services, tasks
from sales.locks import RedisLock
from sales.models import FlashSale, Order
from sales.tasks import fulfil, reconcile, reconcile_sale, requeue_stuck_orders

pytestmark = pytest.mark.django_db(transaction=True)


def logged(caplog, level, text):
    return any(r.levelname == level and text in r.getMessage() for r in caplog.records)


def test_redis_above_db_is_lowered(make_sale, redis_db, caplog):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    redis_db.set(rc.stock_key(sale.id), 15)  # e.g. DECRs lost in a Redis restart

    result = reconcile_sale(sale.id)

    assert result["lowered"] == 5
    assert redis_db.get(rc.stock_key(sale.id)) == "10"
    assert logged(caplog, "WARNING", "lowered by 5")


def test_redis_below_db_is_left_alone(make_sale, redis_db, caplog):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    redis_db.set(rc.stock_key(sale.id), 7)  # safe direction (e.g. events still in flight)

    result = reconcile_sale(sale.id)

    assert result["lowered"] == 0
    assert redis_db.get(rc.stock_key(sale.id)) == "7"
    assert logged(caplog, "WARNING", "safe direction")


def test_consistent_sale_logs_no_warnings(make_sale, redis_db, caplog):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    caplog.clear()
    reconcile_sale(sale.id)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_invariant_violation_is_logged_as_error(make_sale, make_users, redis_db, caplog):
    sale = make_sale(total_stock=10)
    user, = make_users(1)
    Order.objects.create(sale=sale, user=user)  # sold 1 but remaining still 10

    reconcile_sale(sale.id)

    assert logged(caplog, "ERROR", "invariant violated")


def test_sale_not_live_in_redis_is_only_checked(make_sale, redis_db):
    sale = make_sale(total_stock=10)
    result = reconcile_sale(sale.id)
    assert result["redis"] is None
    assert result["lowered"] == 0
    assert redis_db.get(rc.stock_key(sale.id)) is None  # not started by reconcile


def test_stuck_pending_orders_are_requeued(make_sale, make_users, enqueued):
    sale = make_sale(total_stock=10)
    old_user, new_user = make_users(2)
    stuck = Order.objects.create(sale=sale, user=old_user)
    Order.objects.create(sale=sale, user=new_user)  # recent: its task may still be queued
    Order.objects.filter(pk=stuck.pk).update(created_at=timezone.now() - timedelta(minutes=3))

    assert requeue_stuck_orders() == 1
    assert enqueued == [str(stuck.id)]


def test_stuck_orders_are_requeued_oldest_first_up_to_the_limit(make_sale, make_users, enqueued):
    sale = make_sale(total_stock=10)
    orders = [Order.objects.create(sale=sale, user=user) for user in make_users(3)]
    for minutes, order in zip((5, 9, 7), orders, strict=True):
        Order.objects.filter(pk=order.pk).update(created_at=timezone.now() - timedelta(minutes=minutes))

    assert requeue_stuck_orders(limit=2) == 2
    assert enqueued == [str(orders[1].id), str(orders[2].id)]  # 9 and 7 minutes old; the 5-minute one waits


def test_nothing_is_requeued_while_the_queue_is_busy(make_sale, make_users, enqueued, monkeypatch):
    # A long queue means the workers are behind, not that tasks were lost: re-sending would only
    # pile duplicates on top of it.
    sale = make_sale(total_stock=10)
    stuck = Order.objects.create(sale=sale, user=make_users(1)[0])
    Order.objects.filter(pk=stuck.pk).update(created_at=timezone.now() - timedelta(minutes=3))

    monkeypatch.setattr(tasks, "fulfilment_queue_depth", lambda: tasks.BUSY_QUEUE + 1)
    assert requeue_stuck_orders() == 0
    assert enqueued == []

    monkeypatch.setattr(tasks, "fulfilment_queue_depth", lambda: 0)
    assert requeue_stuck_orders() == 1


def test_requeued_order_is_finished(make_sale, make_users, redis_db, enqueued):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    user, = make_users(1)
    stuck = Order.objects.create(sale=sale, user=user)
    FlashSale.objects.filter(pk=sale.pk).update(remaining=9)
    Order.objects.filter(pk=stuck.pk).update(created_at=timezone.now() - timedelta(minutes=3))

    requeue_stuck_orders()
    assert fulfil(enqueued[0]) == "CONFIRMED"  # what the worker does with the re-sent task
    assert Order.objects.filter(status=Order.Status.PENDING).count() == 0


def test_two_reconcile_runs_at_once_only_one_works(make_sale, redis_db):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    redis_db.set(rc.stock_key(sale.id), 15)

    with RedisLock(f"lock:reconcile:{sale.id}"):  # another worker is reconciling this sale
        assert reconcile_sale(sale.id) is None
        assert redis_db.get(rc.stock_key(sale.id)) == "15"

    assert reconcile_sale(sale.id)["lowered"] == 5


def test_reconcile_task_covers_every_sale(make_sale, redis_db, enqueued):
    make_sale(total_stock=5)
    make_sale(total_stock=5)
    result = reconcile.apply().get()
    assert result == {"sales": 2, "requeued": 0}
