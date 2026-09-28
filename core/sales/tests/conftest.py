import itertools
from datetime import timedelta
from decimal import Decimal

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
