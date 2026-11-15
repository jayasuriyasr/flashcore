"""The baseline must be as correct as the real system, or comparing their speed means nothing.

Runs inside the baseline container against itself:
  docker compose -f docker-compose.yml -f docker-compose.baseline.yml exec baseline pytest
Creates its own product, sale and users in the shared database and removes them afterwards.
"""
import asyncio
import os
import time
import uuid

import httpx
import jwt
import psycopg
import pytest

from app.main import DSN

BASE = "http://localhost:8002"
KEY = os.environ["JWT_SIGNING_KEY"]


def token(user_id: int) -> str:
    return jwt.encode({"user_id": user_id, "token_type": "access", "exp": int(time.time()) + 600}, KEY, "HS256")


@pytest.fixture
def sale():
    """A 50-unit sale and 200 users; yields (sale_id, user_ids)."""
    tag = uuid.uuid4().hex[:8]
    with psycopg.connect(DSN, autocommit=True) as conn:
        product_id = conn.execute(
            "INSERT INTO products (name, price) VALUES (%s, 1) RETURNING id", (f"baseline-test-{tag}",)
        ).fetchone()[0]
        sale_id = conn.execute(
            "INSERT INTO flash_sales (product_id, starts_at, ends_at, total_stock, remaining) "
            "VALUES (%s, now(), now() + interval '1 hour', 50, 50) RETURNING id", (product_id,)
        ).fetchone()[0]
        user_ids = [row[0] for row in conn.execute(
            "INSERT INTO auth_user (password, is_superuser, username, first_name, last_name, email, is_staff, "
            "is_active, date_joined) SELECT '!', false, %s || n, '', '', '', false, true, now() "
            "FROM generate_series(1, 200) n RETURNING id", (f"baseline-test-{tag}-",)
        ).fetchall()]
    yield sale_id, user_ids
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DELETE FROM orders WHERE sale_id = %s", (sale_id,))
        conn.execute("DELETE FROM flash_sales WHERE id = %s", (sale_id,))
        conn.execute("DELETE FROM products WHERE id = %s", (product_id,))
        conn.execute("DELETE FROM auth_user WHERE id = ANY(%s)", (user_ids,))


async def buy_all(sale_id, user_ids):
    async with httpx.AsyncClient(base_url=BASE, timeout=30) as client:
        return await asyncio.gather(*(
            client.post(f"/api/sales/{sale_id}/buy", headers={"Authorization": f"Bearer {token(u)}"})
            for u in user_ids
        ))


def test_200_concurrent_buyers_get_exactly_50_units(sale):
    sale_id, user_ids = sale
    responses = asyncio.run(buy_all(sale_id, user_ids))

    codes = [r.status_code for r in responses]
    assert codes.count(202) == 50
    assert codes.count(409) == 150
    assert all(r.json()["error"] == "sold_out" for r in responses if r.status_code == 409)
    with psycopg.connect(DSN) as conn:
        remaining = conn.execute("SELECT remaining FROM flash_sales WHERE id = %s", (sale_id,)).fetchone()[0]
        orders = conn.execute("SELECT count(*) FROM orders WHERE sale_id = %s", (sale_id,)).fetchone()[0]
    assert (remaining, orders) == (0, 50)


def test_one_unit_per_user_and_token_checks(sale):
    sale_id, user_ids = sale
    with httpx.Client(base_url=BASE, timeout=10) as client:
        first = client.post(f"/api/sales/{sale_id}/buy", headers={"Authorization": f"Bearer {token(user_ids[0])}"})
        again = client.post(f"/api/sales/{sale_id}/buy", headers={"Authorization": f"Bearer {token(user_ids[0])}"})
        no_token = client.post(f"/api/sales/{sale_id}/buy")
    assert first.status_code == 202
    assert (again.status_code, again.json()["error"]) == (409, "already_purchased")
    assert no_token.status_code == 401
    with psycopg.connect(DSN) as conn:
        remaining = conn.execute("SELECT remaining FROM flash_sales WHERE id = %s", (sale_id,)).fetchone()[0]
    assert remaining == 49  # the refused second buy changed nothing
