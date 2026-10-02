"""Stops the redis container. Run from the HOST: scripts/run_chaos_tests.ps1

Found by the first chaos run: during a Redis restart the gateway crashed requests (500),
nginx then marked both replicas down and answered 502 for 10 s. Now Redis errors become a
clean 503 try_again and nginx keeps routing.
"""
import subprocess
import time

import httpx
import pytest
import redis

from .conftest import GATEWAY_URL, REDIS_URL, auth, new_ids

pytestmark = pytest.mark.chaos


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def wait_for_redis(timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if redis.Redis.from_url(REDIS_URL).ping():
                return
        except redis.RedisError:
            pass
        time.sleep(1)
    raise AssertionError("redis did not come back")


def test_redis_down_gives_clean_503_and_recovers():
    client = httpx.Client(base_url=GATEWAY_URL, timeout=30)
    user = new_ids(1)[0]

    compose("stop", "redis")
    try:
        codes = [client.post("/api/sales/123/buy", headers=auth(user)).status_code for _ in range(30)]
        assert set(codes) == {503}, codes  # never 500, never 502 "no live upstreams"
        assert client.post("/api/sales/123/buy", headers=auth(user)).json() == {"error": "try_again"}
    finally:
        compose("start", "redis")
        wait_for_redis()

    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    sale_id = 1_500_000_000 + new_ids(1)[0] % 1_000_000
    r.set(f"sale:{sale_id}:stock", 5)
    try:
        deadline = time.monotonic() + 60
        status = None
        while time.monotonic() < deadline:
            status = client.post(f"/api/sales/{sale_id}/buy", headers=auth(user)).status_code
            if status == 202:
                break
            time.sleep(1)
        assert status == 202  # the gateway reconnected on its own
    finally:
        r.delete(f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers")
