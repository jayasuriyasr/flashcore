import asyncio
import json
import time

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from .conftest import WS_URL, auth, make_token, new_ids


async def connect(user_id=None, sale_id=None, token=None):
    token = token if token is not None else make_token(user_id)
    url = f"{WS_URL}/ws?token={token}" + (f"&sale_id={sale_id}" if sale_id is not None else "")
    return await websockets.connect(url, open_timeout=10)


async def recv_json(ws, timeout=5.0):
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def drain(ws, seconds):
    """All messages received within `seconds`."""
    messages = []
    deadline = time.monotonic() + seconds
    while (left := deadline - time.monotonic()) > 0:
        try:
            messages.append(json.loads(await asyncio.wait_for(ws.recv(), left)))
        except asyncio.TimeoutError:
            break
    return messages


@pytest.mark.parametrize("token", ["", "garbage", make_token(1, expires_in=-60)])
async def test_bad_token_is_closed_with_4401(token):
    ws = await connect(token=token)
    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(ws.recv(), 5)
    assert ws.close_code == 4401


async def test_current_stock_is_sent_on_connect(make_sale):
    sale_id = make_sale(42)
    ws = await connect(new_ids(1)[0], sale_id)
    try:
        assert await recv_json(ws) == {"type": "stock", "sale_id": sale_id, "remaining": 42}
    finally:
        await ws.close()


async def test_order_updates_reach_only_their_user(r):
    alice, bob = new_ids(2)
    ws_alice, ws_bob = await connect(alice), await connect(bob)
    try:
        await asyncio.sleep(0.5)  # let both register
        update = {"type": "order", "order_id": "o-1", "sale_id": 1, "status": "CONFIRMED"}
        r.publish(f"user:{alice}", json.dumps(update))  # what Celery does after fulfilment

        assert await recv_json(ws_alice, timeout=10) == update
        assert await drain(ws_bob, 2) == []
    finally:
        await ws_alice.close()
        await ws_bob.close()


async def test_stock_updates_are_throttled(client, r, make_sale):
    sale_id = make_sale(100_000)
    sockets = [await connect(u, sale_id) for u in new_ids(3)]
    try:
        for ws in sockets:
            await recv_json(ws)  # initial value

        async def buy(u):
            return await client.post(f"/api/sales/{sale_id}/buy", headers=auth(u))

        started = time.monotonic()
        await asyncio.gather(*(buy(u) for u in new_ids(500)))
        received = await asyncio.gather(*(drain(ws, 3) for ws in sockets))
        window = time.monotonic() - started

        final = int(r.get(f"sale:{sale_id}:stock"))
        for messages in received:
            stock_msgs = [m for m in messages if m["type"] == "stock"]
            assert stock_msgs, "expected at least one stock update"
            assert len(stock_msgs) <= 2 * window + 1, f"{len(stock_msgs)} messages in {window:.1f}s"
            assert stock_msgs[-1]["remaining"] == final
    finally:
        for ws in sockets:
            await ws.close()


async def test_200_clients_connect_and_clean_up(client, make_sale):
    sale_id = make_sale(10)
    sockets = await asyncio.gather(*(connect(u, sale_id) for u in new_ids(200)))
    for ws in sockets:
        await recv_json(ws)
    assert (await client.get("/health")).status_code == 200

    await asyncio.gather(*(ws.close() for ws in sockets))

    # /health reports the count of whichever uvicorn worker answers; sample several times.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        counts = [(await client.get("/health")).json()["ws_connections"] for _ in range(10)]
        if all(c == 0 for c in counts):
            break
        await asyncio.sleep(0.5)
    assert all(c == 0 for c in counts), counts


async def test_one_dead_client_does_not_break_broadcasts(client, r, make_sale):
    sale_id = make_sale(1000)
    healthy = [await connect(u, sale_id) for u in new_ids(2)]
    doomed = await connect(new_ids(1)[0], sale_id)
    try:
        for ws in healthy + [doomed]:
            await recv_json(ws)
        doomed.transport.abort()  # vanish without a close frame

        r.set(f"sale:{sale_id}:stock", 777)  # changes are broadcast on the next tick
        for ws in healthy:
            messages = await drain(ws, 3)
            assert {"type": "stock", "sale_id": sale_id, "remaining": 777} in messages
    finally:
        for ws in healthy:
            await ws.close()
