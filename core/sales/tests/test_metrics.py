"""Business metrics served to Prometheus at /metrics/ (sales/metrics.py)."""
import re

import pytest

from sales import services
from sales.models import Order

pytestmark = pytest.mark.django_db


def samples(text: str) -> dict[str, float]:
    return {m.group(1): float(m.group(2)) for m in re.finditer(r"^(flashcore_\S+) (\S+)$", text, re.MULTILINE)}


def test_orders_and_live_stock_are_exported(client, make_sale, make_user, redis_db):
    sale = make_sale(total_stock=10, remaining=8)
    Order.objects.create(sale=sale, user=make_user(), status=Order.Status.CONFIRMED)
    Order.objects.create(sale=sale, user=make_user(), status=Order.Status.PENDING)
    services.start_sale(sale)
    redis_db.decr(f"sale:{sale.id}:stock")  # one reservation still on its way to PostgreSQL

    response = client.get("/metrics/")

    assert response.status_code == 200
    values = samples(response.content.decode())
    assert values['flashcore_orders{status="CONFIRMED"}'] == 1
    assert values['flashcore_orders{status="PENDING"}'] == 1
    assert values['flashcore_orders{status="FAILED"}'] == 0
    assert values["flashcore_orders_pending_over_1m"] == 0
    assert values[f'flashcore_sale_remaining{{sale_id="{sale.id}",source="postgres"}}'] == 8
    assert values[f'flashcore_sale_remaining{{sale_id="{sale.id}",source="redis"}}'] == 7


def test_sales_that_are_not_live_are_left_out(client, make_sale, redis_db):
    sale = make_sale(total_stock=10)  # never started: no Redis key
    values = samples(client.get("/metrics/").content.decode())
    assert not any(f'sale_id="{sale.id}"' in name for name in values)
