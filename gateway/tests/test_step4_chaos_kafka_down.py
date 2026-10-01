"""Stops the kafka container. Run from the HOST: scripts/run_chaos_tests.ps1"""
import subprocess
import time

import httpx
import pytest

from .conftest import GATEWAY_URL, auth, new_ids

pytestmark = pytest.mark.chaos


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def test_kafka_down_returns_503_and_gives_the_unit_back(r, make_sale):
    sale_id = make_sale(10)
    user = new_ids(1)[0]
    client = httpx.Client(base_url=GATEWAY_URL, timeout=30)

    compose("stop", "kafka")
    try:
        start = time.monotonic()
        resp = client.post(f"/api/sales/{sale_id}/buy", headers=auth(user))
        elapsed = time.monotonic() - start

        assert resp.status_code == 503
        assert resp.json() == {"error": "try_again"}
        assert elapsed < 3.5, f"took {elapsed:.1f}s"
        assert int(r.get(f"sale:{sale_id}:stock")) == 10       # unit given back
        assert not r.sismember(f"sale:{sale_id}:buyers", user)  # user may retry
    finally:
        compose("start", "kafka")

    # The gateway reconnects on its own — no restart.
    deadline = time.monotonic() + 90
    status = None
    while time.monotonic() < deadline:
        status = client.post(f"/api/sales/{sale_id}/buy", headers=auth(user)).status_code
        if status == 202:
            break
        time.sleep(2)
    assert status == 202
    assert int(r.get(f"sale:{sale_id}:stock")) == 9
