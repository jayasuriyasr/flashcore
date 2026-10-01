"""order_writer.persist() straight against PostgreSQL (no Kafka)."""
import json
import uuid
from io import StringIO

import pytest
from django.core.management import call_command

from sales import order_writer
from sales.models import FlashSale, Order
from sales.order_writer import OrderEvent

pytestmark = pytest.mark.django_db(transaction=True)


def events_for(sale, users):
    return [OrderEvent(str(uuid.uuid4()), sale.id, u.id) for u in users]


def test_batch_inserts_pending_and_decrements(make_sale, make_users):
    sale = make_sale(total_stock=100)
    events = events_for(sale, make_users(40))

    new_ids = order_writer.persist(events)

    assert sorted(new_ids) == sorted(e.order_id for e in events)
    assert Order.objects.filter(sale=sale, status="PENDING").count() == 40
    assert FlashSale.objects.get(pk=sale.pk).remaining == 60


def test_replayed_batch_is_a_no_op(make_sale, make_users):
    sale = make_sale(total_stock=100)
    events = events_for(sale, make_users(10))
    order_writer.persist(events)

    assert order_writer.persist(events) == []  # same order_ids again
    assert Order.objects.filter(sale=sale).count() == 10
    assert FlashSale.objects.get(pk=sale.pk).remaining == 90


def test_batch_with_duplicates_inside_counts_each_order_once(make_sale, make_users):
    sale = make_sale(total_stock=100)
    events = events_for(sale, make_users(5))
    order_writer.persist(events + events[:2])
    assert Order.objects.filter(sale=sale).count() == 5
    assert FlashSale.objects.get(pk=sale.pk).remaining == 95


def test_over_admission_marks_extras_failed(make_sale, make_users, redis_db):
    sale = make_sale(total_stock=10)
    events = events_for(sale, make_users(15))
    pubsub = redis_db.pubsub()
    pubsub.psubscribe("user:*")
    pubsub.get_message(timeout=1)  # subscription confirmation

    new_ids = order_writer.persist(events)

    assert len(new_ids) == 10
    assert Order.objects.filter(sale=sale, status="PENDING").count() == 10
    assert Order.objects.filter(sale=sale, status="FAILED").count() == 5
    assert FlashSale.objects.get(pk=sale.pk).remaining == 0
    call_command("check_invariants", stdout=StringIO())

    ours = {e.order_id for e in events}
    notified = []
    while (msg := pubsub.get_message(timeout=1)) is not None:
        if msg["type"] == "pmessage":
            body = json.loads(msg["data"])
            if body["order_id"] in ours:
                notified.append(body)
    assert len(notified) == 5
    assert {m["status"] for m in notified} == {"FAILED"}
    pubsub.close()


def test_unknown_sale_or_user_is_dropped_not_fatal(make_sale, make_users):
    sale = make_sale(total_stock=10)
    good_user, = make_users(1)
    events = [
        OrderEvent(str(uuid.uuid4()), sale.id, good_user.id),
        OrderEvent(str(uuid.uuid4()), sale.id, 987654321),   # no such user
        OrderEvent(str(uuid.uuid4()), 987654321, good_user.id),  # no such sale
    ]

    new_ids = order_writer.persist(events)

    assert new_ids == [events[0].order_id]
    assert FlashSale.objects.get(pk=sale.pk).remaining == 9


def test_two_sales_in_one_batch(make_sale, make_users):
    a, b = make_sale(total_stock=5), make_sale(total_stock=5)
    users = make_users(6)
    order_writer.persist(events_for(a, users[:3]) + events_for(b, users[3:]))
    assert FlashSale.objects.get(pk=a.pk).remaining == 2
    assert FlashSale.objects.get(pk=b.pk).remaining == 2


def test_parse_rejects_malformed_messages():
    with pytest.raises(order_writer.BadEvent):
        order_writer.parse(b"not json")
    with pytest.raises(order_writer.BadEvent):
        order_writer.parse(b'{"order_id": "nope", "sale_id": 1, "user_id": 1}')
    with pytest.raises(order_writer.BadEvent):
        order_writer.parse(b'{"sale_id": 1}')
    event = order_writer.parse(json.dumps({"order_id": str(uuid.uuid4()), "sale_id": "3", "user_id": 4}).encode())
    assert (event.sale_id, event.user_id) == (3, 4)
