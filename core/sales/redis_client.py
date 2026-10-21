"""Redis access for the Django side (admin, order writer, Celery).

The Lua scripts are the same files the gateway uses (gateway/app/lua), mounted
read-only at LUA_DIR by docker-compose, so both sides always run identical logic.
"""
import functools
import json
import os
from pathlib import Path

import redis

LUA_DIR = Path(os.environ.get("LUA_DIR", "/lua"))


def stock_key(sale_id: int) -> str:
    return f"sale:{sale_id}:stock"


def buyers_key(sale_id: int) -> str:
    return f"sale:{sale_id}:buyers"


@functools.cache
def client() -> redis.Redis:
    return redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)


@functools.cache
def _script(name: str):
    return client().register_script((LUA_DIR / f"{name}.lua").read_text())


def get_stock(sale_id: int) -> int | None:
    """None when the sale is not live in Redis."""
    value = client().get(stock_key(sale_id))
    return int(value) if value is not None else None


def release(sale_id: int, user_id: int) -> int:
    """Undo one reservation (INCR stock + SREM buyer). Safe to call twice."""
    return int(_script("release")(keys=[stock_key(sale_id), buyers_key(sale_id)], args=[user_id]))


def lower_stock(sale_id: int, target: int) -> int:
    """Lower the Redis counter to target if it is higher; returns units removed."""
    return int(_script("lower_stock")(keys=[stock_key(sale_id)], args=[target]))


def start_sale(sale_id: int, remaining: int, live_buyer_ids: list[int]) -> bool:
    """Load stock and rebuild the buyers set atomically (start_sale.lua).

    A live sale is never reset (that would oversell): returns False and changes nothing.
    """
    keys = [stock_key(sale_id), buyers_key(sale_id)]
    return int(_script("start_sale")(keys=keys, args=[remaining, *live_buyer_ids])) == 1


def end_sale(sale_id: int) -> bool:
    """Missing stock key = not live: the gateway answers sold_out from now on."""
    return bool(client().delete(stock_key(sale_id)))


def publish_order(user_id: int, order_id, sale_id: int, status: str) -> None:
    message = {"type": "order", "order_id": str(order_id), "sale_id": sale_id, "status": status}
    client().publish(f"user:{user_id}", json.dumps(message))
