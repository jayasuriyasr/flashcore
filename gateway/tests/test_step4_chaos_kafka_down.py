"""Stops the kafka container. Run from the HOST: scripts/run_chaos_tests.ps1

A buy waits 2 s for Kafka's ack (PRODUCE_TIMEOUT_S) and then answers 503, but the Kafka client
keeps trying to deliver for 30 s (KAFKA_MESSAGE_TIMEOUT_S). The reservation is kept until the real
outcome is known, so an event that is written late never belongs to a unit that was given back.
"""
import subprocess
import time

import httpx
import pytest

from .conftest import GATEWAY_URL, auth, new_ids

pytestmark = pytest.mark.chaos

MESSAGE_TIMEOUT_S = 30


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def buy(client, sale_id, user):
    return client.post(f"/api/sales/{sale_id}/buy", headers=auth(user))


def buy_until_owned(client, r, sale_id, user, timeout_s=180):
    """Retry like a real client until this user holds a unit for good. The gateway reconnects to
    Kafka on its own (no restart), but an attempt made while it is still reconnecting is answered 503
    and keeps its reservation, so the next attempt gets 409 already_purchased: then wait for that
    attempt's outcome (delivered late: the unit is theirs; given up: released, try again)."""
    buyers = f"sale:{sale_id}:buyers"
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = buy(client, sale_id, user).status_code
        if status == 202:
            return True
        if status == 409:
            settled = time.monotonic() + MESSAGE_TIMEOUT_S + 5
            while time.monotonic() < settled and r.sismember(buyers, user):
                time.sleep(1)
            if r.sismember(buyers, user):
                return True
            continue
        time.sleep(2)
    return False


def test_kafka_down_longer_than_the_delivery_window_gives_the_unit_back(r, make_sale):
    sale_id = make_sale(10)
    user = new_ids(1)[0]
    client = httpx.Client(base_url=GATEWAY_URL, timeout=30)
    stock, buyers = f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers"

    compose("stop", "kafka")
    try:
        start = time.monotonic()
        resp = buy(client, sale_id, user)
        elapsed = time.monotonic() - start

        assert resp.status_code == 503
        assert resp.json() == {"error": "try_again"}
        assert elapsed < 3.5, f"took {elapsed:.1f}s"
        # Not released yet: the event may still be written, so the unit can't be sold to someone else.
        assert int(r.get(stock)) == 9
        assert r.sismember(buyers, user)
        retry = buy(client, sale_id, user)
        assert (retry.status_code, retry.json()) == (409, {"error": "already_purchased"})

        # After the delivery window the Kafka client gives up for good: only now is the unit returned.
        deadline = start + MESSAGE_TIMEOUT_S + 15
        while time.monotonic() < deadline and int(r.get(stock)) != 10:
            time.sleep(1)
        assert int(r.get(stock)) == 10
        assert not r.sismember(buyers, user)  # the user may retry
    finally:
        compose("start", "kafka")

    assert buy_until_owned(client, r, sale_id, user)
    assert int(r.get(stock)) == 9


def test_kafka_back_within_the_delivery_window_keeps_the_reservation(r, make_sale):
    sale_id = make_sale(10)
    user, other = new_ids(2)
    client = httpx.Client(base_url=GATEWAY_URL, timeout=30)
    stock, buyers = f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers"

    compose("stop", "kafka")
    try:
        start = time.monotonic()
        assert buy(client, sale_id, user).status_code == 503
    finally:
        compose("start", "kafka")

    # Watch the reservation until well past the delivery window.
    held_throughout = True
    while time.monotonic() < start + MESSAGE_TIMEOUT_S + 10:
        held_throughout = held_throughout and bool(r.sismember(buyers, user))
        time.sleep(0.5)

    if held_throughout:
        # Kafka came back in time: the queued event was delivered late and the order proceeds.
        # The user who was told "try again" already has their unit.
        retry = buy(client, sale_id, user)
        assert (retry.status_code, retry.json()) == (409, {"error": "already_purchased"})
    else:
        # Slow machine: Kafka needed longer than the window, so the client gave up and the unit was
        # returned (the first test's path). The retry is then an ordinary buy.
        assert buy_until_owned(client, r, sale_id, user)
    # Either way exactly one unit is taken for this user, and the sale goes on for others.
    assert int(r.get(stock)) == 9
    assert r.sismember(buyers, user)
    assert buy_until_owned(client, r, sale_id, other)
    assert int(r.get(stock)) == 8
