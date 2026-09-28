from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.db import transaction

from sales.db import repeatable_read
from sales.models import FlashSale, Order

# check_invariants opens its own REPEATABLE READ transaction, so the test must not be
# wrapped in pytest-django's outer transaction.
pytestmark = pytest.mark.django_db(transaction=True)


def test_passes_on_consistent_data(make_sale, make_user):
    sale = make_sale(total_stock=10)
    for _ in range(3):
        Order.objects.create(sale=sale, user=make_user())
    Order.objects.create(sale=sale, user=make_user(), status=Order.Status.FAILED)  # not counted
    FlashSale.objects.filter(pk=sale.pk).update(remaining=7)

    out = StringIO()
    call_command("check_invariants", stdout=out)
    assert "OK: all 1 sale(s) consistent" in out.getvalue()


def test_fails_when_remaining_is_too_high(make_sale, make_user):
    sale = make_sale(total_stock=10)
    Order.objects.create(sale=sale, user=make_user())  # 1 sold but remaining still says 10
    with pytest.raises(CommandError, match="invariant violated"):
        call_command("check_invariants", stdout=StringIO())


def test_fails_when_remaining_is_too_low(make_sale):
    sale = make_sale(total_stock=10)
    FlashSale.objects.filter(pk=sale.pk).update(remaining=9)  # 1 unit vanished, no order
    with pytest.raises(CommandError):
        call_command("check_invariants", stdout=StringIO())


def test_repeatable_read_must_be_outermost():
    with transaction.atomic():
        with pytest.raises(RuntimeError):
            with repeatable_read():
                pass


def test_repeatable_read_sets_isolation_level():
    with repeatable_read() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_isolation")
            assert cur.fetchone()[0] == "repeatable read"
