"""Prometheus metrics of the gateway (app/metrics.py), read from the same replica the tests buy from."""
import re

from .conftest import auth, new_ids


def sample(text: str, name: str) -> float:
    match = re.search(rf"^{re.escape(name)} (\S+)$", text, re.MULTILINE)
    return float(match.group(1)) if match else 0.0


async def test_buys_are_counted_by_outcome_and_timed(client, make_sale):
    sale_id = make_sale(1)
    first, second = new_ids(2)
    before = (await client.get("/metrics")).text

    assert (await client.post(f"/api/sales/{sale_id}/buy", headers=auth(first))).status_code == 202
    assert (await client.post(f"/api/sales/{sale_id}/buy", headers=auth(second))).status_code == 409
    assert (await client.post(f"/api/sales/{sale_id}/buy")).status_code == 401

    after = (await client.get("/metrics")).text
    # /metrics merges every worker process of this replica, whichever one served each request.
    for outcome in ("reserved", "sold_out", "unauthorized"):
        name = f'flashcore_buy_requests_total{{outcome="{outcome}"}}'
        assert sample(after, name) - sample(before, name) == 1, outcome
    # The two authenticated buys were timed; only the reserved one waited for a Kafka ack.
    assert sample(after, "flashcore_buy_duration_seconds_count") - sample(before, "flashcore_buy_duration_seconds_count") == 2
    kafka = 'flashcore_buy_stage_seconds_count{stage="kafka"}'
    assert sample(after, kafka) - sample(before, kafka) == 1
