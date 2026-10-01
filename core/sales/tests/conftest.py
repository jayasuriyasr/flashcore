import itertools
import os
from datetime import timedelta
from decimal import Decimal

# Tests use their own Redis database so they can flush it freely. The test DB's ids
# restart at 1, which would collide with real sale:{id} keys in db 0.
os.environ["REDIS_URL"] = os.environ.get("TEST_REDIS_URL", "redis://redis:6379/2")
os.environ.setdefault("LUA_DIR", "/lua")

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


@pytest.fixture
def redis_db():
    from sales import redis_client as rc

    r = rc.client()
    assert r.connection_pool.connection_kwargs.get("db", 0) != 0, "refusing to flush Redis db 0"
    r.flushdb()
    yield r
    r.flushdb()
