"""Gateway tests hit the RUNNING gateway over HTTP, with real Redis and Kafka.

Inside the gateway container the defaults just work. From the host, set:
  GATEWAY_URL=http://localhost:8001  TEST_REDIS_URL=redis://localhost:6379/0  TEST_KAFKA_BOOTSTRAP=localhost:9094
"""
import os
import random
import time
from pathlib import Path

import httpx
import jwt
import pytest
import redis

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8001")
WS_URL = GATEWAY_URL.replace("http", "ws", 1)
REDIS_URL = os.environ.get("TEST_REDIS_URL", os.environ.get("REDIS_URL", "redis://redis:6379/0"))
KAFKA_BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
JWT_KEY = os.environ.get("JWT_SIGNING_KEY", "dev-only-change-me-0123456789abcdef")
LUA_DIR = Path(__file__).resolve().parent.parent / "app" / "lua"


def make_token(user_id, *, expires_in=3600, key=JWT_KEY, token_type="access"):
    now = int(time.time())
    payload = {"user_id": user_id, "token_type": token_type, "iat": now, "exp": now + expires_in}
    return jwt.encode(payload, key, algorithm="HS256")


def auth(user_id, **kwargs):
    return {"Authorization": f"Bearer {make_token(user_id, **kwargs)}"}


def new_ids(n, start=None):
    """Random block of ids so parallel/previous runs never collide."""
    start = start or random.randint(10_000_000, 900_000_000)
    return list(range(start, start + n))


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * p / 100))]


@pytest.fixture
def r():
    client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def make_sale(r):
    """Create a sale in Redis only (the gateway never reads PostgreSQL)."""
    created = []

    def _make(stock):
        sale_id = random.randint(900_000_000, 2_000_000_000)
        r.set(f"sale:{sale_id}:stock", stock)
        r.delete(f"sale:{sale_id}:buyers")
        created.append(sale_id)
        return sale_id

    yield _make
    for sale_id in created:
        r.delete(f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers")


@pytest.fixture
async def client():
    async with httpx.AsyncClient(
        base_url=GATEWAY_URL, timeout=30, limits=httpx.Limits(max_connections=200)
    ) as c:
        yield c
