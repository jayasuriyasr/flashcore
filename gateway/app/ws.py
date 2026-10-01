"""WebSocket fan-out for one gateway process.

- ONE Redis pub/sub listener per process (psubscribe user:*) forwards each order update
  to that user's sockets only. Every process receives every message and keeps the ones
  for sockets it holds, so it works with any number of replicas.
- ONE stock ticker per process reads the Redis counters every STOCK_TICK_S and
  broadcasts only when a value changed: 5,000 buys/s become at most 2 messages/s.
"""
import asyncio
import logging
from collections import defaultdict

import orjson
from fastapi import WebSocket

from .redis_ops import RedisOps, stock_key

log = logging.getLogger("gateway.ws")

SEND_TIMEOUT_S = 2.0


def stock_message(sale_id: int, remaining: int) -> str:
    return orjson.dumps({"type": "stock", "sale_id": sale_id, "remaining": remaining}).decode()


class ConnectionManager:
    def __init__(self, redis: RedisOps, tick_s: float):
        self.redis = redis
        self.tick_s = tick_s
        self.meta: dict[WebSocket, tuple[int, int | None]] = {}
        self.by_user: dict[int, set[WebSocket]] = defaultdict(set)
        self.by_sale: dict[int, set[WebSocket]] = defaultdict(set)
        self.last_stock: dict[int, int] = {}
        self._tasks: list[asyncio.Task] = []

    @property
    def count(self) -> int:
        return len(self.meta)

    def add(self, ws: WebSocket, user_id: int, sale_id: int | None) -> None:
        self.meta[ws] = (user_id, sale_id)
        self.by_user[user_id].add(ws)
        if sale_id is not None:
            self.by_sale[sale_id].add(ws)

    def discard(self, ws: WebSocket) -> None:
        info = self.meta.pop(ws, None)
        if info is None:
            return
        user_id, sale_id = info
        sockets = self.by_user.get(user_id)
        if sockets is not None:
            sockets.discard(ws)
            if not sockets:
                del self.by_user[user_id]
        if sale_id is not None:
            sockets = self.by_sale.get(sale_id)
            if sockets is not None:
                sockets.discard(ws)
                if not sockets:
                    del self.by_sale[sale_id]
                    self.last_stock.pop(sale_id, None)

    async def _send(self, ws: WebSocket, text: str) -> None:
        try:
            await asyncio.wait_for(ws.send_text(text), timeout=SEND_TIMEOUT_S)
        except Exception:
            # Dead or stuck client: drop it so it can't slow down or break the next broadcast.
            self.discard(ws)

    async def send_to_user(self, user_id: int, text: str) -> None:
        sockets = list(self.by_user.get(user_id, ()))
        if sockets:
            await asyncio.gather(*(self._send(ws, text) for ws in sockets))

    async def broadcast_sale(self, sale_id: int, text: str) -> None:
        sockets = list(self.by_sale.get(sale_id, ()))
        if sockets:
            await asyncio.gather(*(self._send(ws, text) for ws in sockets))

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._pubsub_loop()), asyncio.create_task(self._stock_loop())]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _pubsub_loop(self) -> None:
        while True:
            pubsub = self.redis.client.pubsub()
            try:
                await pubsub.psubscribe("user:*")
                async for message in pubsub.listen():
                    if message["type"] != "pmessage":
                        continue
                    try:
                        user_id = int(message["channel"].split(":", 1)[1])
                    except ValueError:
                        continue
                    await self.send_to_user(user_id, message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("pub/sub listener error (%s); reconnecting", exc)
                await asyncio.sleep(1)
            finally:
                await pubsub.aclose()

    async def _stock_loop(self) -> None:
        while True:
            await asyncio.sleep(self.tick_s)
            sale_ids = list(self.by_sale)
            if not sale_ids:
                continue
            try:
                values = await self.redis.client.mget([stock_key(s) for s in sale_ids])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("stock tick failed (%s)", exc)
                continue
            for sale_id, value in zip(sale_ids, values):
                remaining = int(value) if value is not None else 0
                if self.last_stock.get(sale_id) != remaining:
                    self.last_stock[sale_id] = remaining
                    await self.broadcast_sale(sale_id, stock_message(sale_id, remaining))
