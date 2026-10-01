from pathlib import Path

import redis.asyncio as redis

LUA_DIR = Path(__file__).parent / "lua"


def stock_key(sale_id: int) -> str:
    return f"sale:{sale_id}:stock"


def buyers_key(sale_id: int) -> str:
    return f"sale:{sale_id}:buyers"


class RedisOps:
    """One shared connection pool per process, Lua scripts loaded once (EVALSHA)."""

    def __init__(self, url: str, max_connections: int = 200):
        # Blocking pool: under a burst, requests wait briefly for a connection
        # instead of failing with "Too many connections".
        self.pool = redis.BlockingConnectionPool.from_url(
            url, max_connections=max_connections, timeout=5, decode_responses=True
        )
        self.client = redis.Redis(connection_pool=self.pool)
        self._reserve = self.client.register_script((LUA_DIR / "reserve.lua").read_text())
        self._release = self.client.register_script((LUA_DIR / "release.lua").read_text())

    async def reserve(self, sale_id: int, user_id: int) -> int:
        """1 reserved, 0 sold out / not started, -1 already bought."""
        return int(await self._reserve(keys=[stock_key(sale_id), buyers_key(sale_id)], args=[user_id]))

    async def release(self, sale_id: int, user_id: int) -> int:
        return int(await self._release(keys=[stock_key(sale_id), buyers_key(sale_id)], args=[user_id]))

    async def stock(self, sale_id: int) -> int:
        value = await self.client.get(stock_key(sale_id))
        return int(value) if value is not None else 0

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except redis.RedisError:
            return False

    async def close(self) -> None:
        await self.client.aclose()
        await self.pool.aclose()
