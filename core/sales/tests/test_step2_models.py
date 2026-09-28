import pytest
from django.core.management import call_command
from django.db import IntegrityError, connection, transaction

from sales.models import FlashSale, Order

pytestmark = pytest.mark.django_db


def test_tables_exist():
    assert {"products", "flash_sales", "orders"} <= set(connection.introspection.table_names())


def test_no_missing_migrations():
    # Raises SystemExit(1) if a model changed without a migration.
    call_command("makemigrations", "--check", "--dry-run", verbosity=0)


def test_remaining_defaults_to_total_stock(make_sale):
    assert make_sale(total_stock=50).remaining == 50


def test_db_rejects_negative_stock(make_sale):
    sale = make_sale(total_stock=5)
    # .update() skips Python validation entirely, so only the DB CHECK can stop this.
    with pytest.raises(IntegrityError), transaction.atomic():
        FlashSale.objects.filter(pk=sale.pk).update(remaining=-1)


def test_two_pending_orders_same_user_rejected(make_sale, make_user):
    sale, user = make_sale(), make_user()
    Order.objects.create(sale=sale, user=user)
    with pytest.raises(IntegrityError), transaction.atomic():
        Order.objects.create(sale=sale, user=user)


def test_confirmed_then_pending_same_user_rejected(make_sale, make_user):
    sale, user = make_sale(), make_user()
    Order.objects.create(sale=sale, user=user, status=Order.Status.CONFIRMED)
    with pytest.raises(IntegrityError), transaction.atomic():
        Order.objects.create(sale=sale, user=user)


def test_failed_orders_do_not_block_a_new_order(make_sale, make_user):
    sale, user = make_sale(), make_user()
    Order.objects.create(sale=sale, user=user, status=Order.Status.FAILED)
    Order.objects.create(sale=sale, user=user, status=Order.Status.FAILED)
    Order.objects.create(sale=sale, user=user)  # PENDING is allowed after failures
    assert Order.objects.filter(sale=sale, user=user).count() == 3


def test_same_user_can_buy_in_different_sales(make_sale, make_user):
    user = make_user()
    Order.objects.create(sale=make_sale(), user=user)
    Order.objects.create(sale=make_sale(), user=user)
    assert Order.objects.filter(user=user).count() == 2


def test_timestamps_come_from_db_default(make_sale, make_user):
    order = Order.objects.create(sale=make_sale(), user=make_user())
    order.refresh_from_db()
    assert order.created_at is not None
    assert order.updated_at is not None
