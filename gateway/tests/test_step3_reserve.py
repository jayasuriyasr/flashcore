import asyncio
import time

from .conftest import LUA_DIR, auth, new_ids, percentile


async def buy(client, sale_id, user_id, **token_kwargs):
    return await client.post(f"/api/sales/{sale_id}/buy", headers=auth(user_id, **token_kwargs))


async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["redis"] == "up"


async def test_oversell_1000_buyers_100_units(client, r, make_sale):
    sale_id = make_sale(100)
    users = new_ids(1000)
    in_flight = asyncio.Semaphore(100)  # 100 concurrent requests is plenty to race; more just queues in httpx

    async def limited_buy(u):
        async with in_flight:
            return await buy(client, sale_id, u)

    responses = await asyncio.gather(*(limited_buy(u) for u in users))
    codes = [resp.status_code for resp in responses]

    assert codes.count(202) == 100
    assert codes.count(409) == 900
    assert all(resp.json()["error"] == "sold_out" for resp in responses if resp.status_code == 409)
    assert int(r.get(f"sale:{sale_id}:stock")) == 0  # never negative
    assert r.scard(f"sale:{sale_id}:buyers") == 100
    winners = {u for u, resp in zip(users, responses) if resp.status_code == 202}
    assert {int(m) for m in r.smembers(f"sale:{sale_id}:buyers")} == winners


async def test_202_body_has_order_id(client, make_sale):
    sale_id = make_sale(5)
    resp = await buy(client, sale_id, new_ids(1)[0])
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "RESERVED"
    assert len(body["order_id"]) == 36


async def test_same_user_20_parallel_buys_only_one_wins(client, r, make_sale):
    sale_id = make_sale(50)
    user = new_ids(1)[0]

    responses = await asyncio.gather(*(buy(client, sale_id, user) for _ in range(20)))
    codes = [resp.status_code for resp in responses]

    assert codes.count(202) == 1
    assert codes.count(409) == 19
    assert all(resp.json()["error"] == "already_purchased" for resp in responses if resp.status_code == 409)
    assert int(r.get(f"sale:{sale_id}:stock")) == 49


async def test_missing_token_is_401(client, make_sale):
    resp = await client.post(f"/api/sales/{make_sale(5)}/buy")
    assert resp.status_code == 401
    assert resp.json() == {"error": "unauthorized"}


async def test_expired_token_is_401(client, make_sale):
    resp = await buy(client, make_sale(5), 1, expires_in=-60)
    assert resp.status_code == 401


async def test_wrong_signature_is_401(client, make_sale):
    resp = await buy(client, make_sale(5), 1, key="not-the-real-key-0123456789abcdef")
    assert resp.status_code == 401


async def test_refresh_token_cannot_buy(client, make_sale):
    resp = await buy(client, make_sale(5), 1, token_type="refresh")
    assert resp.status_code == 401


async def test_sale_not_started_is_sold_out(client):
    resp = await buy(client, 1_999_999_999, new_ids(1)[0])  # no stock key in Redis
    assert resp.status_code == 409
    assert resp.json()["error"] == "sold_out"


async def test_stock_endpoint(client, make_sale):
    sale_id = make_sale(42)
    resp = await client.get(f"/api/sales/{sale_id}/stock")
    assert resp.json() == {"sale_id": sale_id, "remaining": 42}


def test_release_lua_gives_unit_back_once(r, make_sale):
    sale_id = make_sale(10)
    keys = [f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers"]
    reserve = r.register_script((LUA_DIR / "reserve.lua").read_text())
    release = r.register_script((LUA_DIR / "release.lua").read_text())

    assert reserve(keys=keys, args=[7]) == 1
    assert int(r.get(keys[0])) == 9

    assert release(keys=keys, args=[7]) == 1
    assert int(r.get(keys[0])) == 10
    assert not r.sismember(keys[1], 7)

    assert release(keys=keys, args=[7]) == 0  # second release is a no-op
    assert int(r.get(keys[0])) == 10
    assert reserve(keys=keys, args=[7]) == 1  # the user can buy again


async def test_latency_2000_sequential_buys(client, make_sale):
    sale_id = make_sale(1_000_000)
    timings = []
    for user in new_ids(2000):
        start = time.perf_counter()
        resp = await buy(client, sale_id, user)
        timings.append((time.perf_counter() - start) * 1000)
        assert resp.status_code == 202
    print(f"\nbuy latency over 2000 sequential requests: "
          f"p50={percentile(timings, 50):.2f} ms  p99={percentile(timings, 99):.2f} ms")
