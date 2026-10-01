import asyncio
import time
from collections import defaultdict

from .conftest import auth, new_ids, percentile
from .kafka_helpers import TopicTail


async def buy(client, sale_id, user_id):
    return await client.post(f"/api/sales/{sale_id}/buy", headers=auth(user_id))


async def test_health_reports_kafka_up(client):
    assert (await client.get("/health")).json()["kafka"] == "up"


async def test_300_buys_produce_300_events_partitioned_by_sale(client, make_sale):
    sales = [make_sale(1000) for _ in range(3)]
    users = new_ids(300)

    async with TopicTail() as tail:
        responses = await asyncio.gather(
            *(buy(client, sales[i % 3], u) for i, u in enumerate(users))
        )
        assert [resp.status_code for resp in responses].count(202) == 300
        returned_ids = [resp.json()["order_id"] for resp in responses]

        found = await tail.collect(lambda e, m: e.get("sale_id") in sales, expected=300)

    assert len(found) == 300

    # Every event has the agreed fields and the key is the sale id.
    for partition, event, key in found:
        assert set(event) == {"order_id", "sale_id", "user_id", "ts"}
        assert key == str(event["sale_id"])

    # All events of one sale are in one partition (ordering per sale).
    partitions_by_sale = defaultdict(set)
    for partition, event, _ in found:
        partitions_by_sale[event["sale_id"]].add(partition)
    assert all(len(p) == 1 for p in partitions_by_sale.values())

    # Every 202 order_id appears exactly once.
    produced_ids = [event["order_id"] for _, event, _ in found]
    assert sorted(produced_ids) == sorted(returned_ids)
    assert len(set(produced_ids)) == 300


async def test_rejected_buys_produce_nothing(client, make_sale):
    sale_id = make_sale(0)
    async with TopicTail() as tail:
        resp = await buy(client, sale_id, new_ids(1)[0])
        assert resp.status_code == 409
        found = await tail.collect(lambda e, m: e.get("sale_id") == sale_id, expected=1, timeout_s=3)
    assert found == []


async def test_latency_2000_sequential_buys_with_kafka(client, make_sale):
    sale_id = make_sale(1_000_000)
    timings = []
    for user in new_ids(2000):
        start = time.perf_counter()
        resp = await buy(client, sale_id, user)
        timings.append((time.perf_counter() - start) * 1000)
        assert resp.status_code == 202
    print(f"\nbuy latency incl. Kafka ack (2000 sequential): "
          f"p50={percentile(timings, 50):.2f} ms  p99={percentile(timings, 99):.2f} ms")
