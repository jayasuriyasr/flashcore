"""consume_orders end to end: real Kafka topic → command → PostgreSQL."""
from io import StringIO

import pytest
from django.core.management import call_command

from sales.models import FlashSale, Order

from .kafka_utils import create_topic, delete_topic, order_event, produce, run_writer

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def topic():
    name = create_topic()
    yield name
    delete_topic(name)


def test_500_events_become_500_pending_orders(topic, make_sale, make_users):
    sale = make_sale(total_stock=1000)
    produce(topic, [order_event(sale.id, u.id) for u in make_users(500)])

    run_writer(topic, max_messages=500)

    assert Order.objects.filter(sale=sale, status="PENDING").count() == 500
    assert FlashSale.objects.get(pk=sale.pk).remaining == 500
    call_command("check_invariants", stdout=StringIO())


def test_same_events_delivered_twice_are_written_once(topic, make_sale, make_users):
    sale = make_sale(total_stock=100)
    events = [order_event(sale.id, u.id) for u in make_users(50)]
    produce(topic, events)
    produce(topic, events)  # simulates redelivery after a crash

    run_writer(topic, max_messages=100)

    assert Order.objects.filter(sale=sale).count() == 50
    assert FlashSale.objects.get(pk=sale.pk).remaining == 50


def test_over_admission_10_units_15_events(topic, make_sale, make_users):
    sale = make_sale(total_stock=10)
    produce(topic, [order_event(sale.id, u.id) for u in make_users(15)])

    run_writer(topic, max_messages=15)

    assert Order.objects.filter(sale=sale, status="PENDING").count() == 10
    assert Order.objects.filter(sale=sale, status="FAILED").count() == 5
    assert FlashSale.objects.get(pk=sale.pk).remaining == 0
    call_command("check_invariants", stdout=StringIO())


def test_malformed_and_unknown_events_do_not_stop_the_writer(topic, make_sale, make_users):
    sale = make_sale(total_stock=10)
    user, = make_users(1)
    produce(topic, [
        b"not json at all",
        order_event(sale.id, user.id),
        order_event(sale.id, 987654321),       # unknown user
        order_event(987654321, user.id),       # unknown sale
    ])

    run_writer(topic, max_messages=4)

    assert Order.objects.filter(sale=sale).count() == 1
    assert FlashSale.objects.get(pk=sale.pk).remaining == 9


def test_offsets_are_committed_after_the_db_write(topic, make_sale, make_users):
    sale = make_sale(total_stock=10)
    produce(topic, [order_event(sale.id, u.id) for u in make_users(5)])

    run_writer(topic, max_messages=5, group_id="committed-group")
    output = run_writer(topic, max_messages=0, group_id="committed-group", idle_exit_seconds=8)

    assert "stopped after 0 messages" in output  # nothing re-read: offsets were committed
    assert Order.objects.filter(sale=sale).count() == 5
