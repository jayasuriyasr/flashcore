import itertools
import os
from datetime import timedelta
from decimal import Decimal

# Tests use their own Redis database so they can flush it freely. The test DB's ids
# restart at 1, which would collide with real sale:{id} keys in db 0.
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://redis:6379/2")
os.environ.setdefault("LUA_DIR", "/lua")
# Deterministic, fast mock payments unless a test says otherwise.
os.environ["PAYMENT_FAILURE_RATE"] = "0"
os.environ["MOCK_PAYMENT_DELAY_MS"] = "10"

import pytest
from django.contrib.auth import get_user_model
from django.utils import timezone
from rest_framework.test import APIClient

from sales.models import FlashSale, Product

PASSWORD = "pass12345"
_seq = itertools.count(1)


@pytest.fixture
def make_user():
    def _make(username=None, password=PASSWORD):
        username = username or f"user{next(_seq)}"
        return get_user_model().objects.create_user(username=username, password=password)

    return _make


@pytest.fixture
def make_users():
    """Fast bulk users (no password hashing)."""

    def _make(n):
        User = get_user_model()
        prefix = f"bulk{next(_seq)}_"
        return User.objects.bulk_create([User(username=f"{prefix}{i}", password="!") for i in range(n)])

    return _make


@pytest.fixture
def other_db():
    """A second, independent PostgreSQL session on the test database (autocommit)."""
    import psycopg
    from django.db import connection

    s = connection.settings_dict
    conn = psycopg.connect(
        dbname=s["NAME"], user=s["USER"], password=s["PASSWORD"], host=s["HOST"], port=s["PORT"], autocommit=True
    )
    yield conn
    conn.close()


@pytest.fixture
def make_sale():
    def _make(total_stock=100, remaining=None, name="Phone"):
        product = Product.objects.create(name=name, price=Decimal("999.00"))
        now = timezone.now()
        return FlashSale.objects.create(
            product=product,
            starts_at=now,
            ends_at=now + timedelta(hours=1),
            total_stock=total_stock,
            remaining=remaining,
        )

    return _make


@pytest.fixture
def api():
    return APIClient()


@pytest.fixture(autouse=True)
def enqueued(monkeypatch):
    """Capture fulfilment hand-offs instead of sending them to the real Celery broker
    (the running worker would look for test-DB orders in the real DB)."""
    from sales import order_writer

    calls = []
    monkeypatch.setattr(order_writer, "enqueue_fulfilment", calls.append)
    return calls


@pytest.fixture
def redis_db():
    from sales import redis_client as rc

    r = rc.client()
    assert r.connection_pool.connection_kwargs.get("db", 0) != 0, "refusing to flush Redis db 0"
    r.flushdb()
    yield r
    r.flushdb()
