"""A small Redis lock: SET key token NX PX ttl, released with compare-and-delete.

The random token matters: if our lock expired and someone else took it, a plain DEL
would delete *their* lock. The Lua script only deletes the key if it still holds our token.
"""
import secrets
import time

from . import redis_client as rc

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


class LockNotAcquired(Exception):
    pass


class RedisLock:
    def __init__(self, key: str, ttl_ms: int = 10_000, wait_s: float = 0.0, client=None):
        self.key = key
        self.ttl_ms = ttl_ms
        self.wait_s = wait_s
        self.token = secrets.token_hex(16)
        self.client = client or rc.client()
        self._release = self.client.register_script(_RELEASE_LUA)

    def acquire(self) -> bool:
        deadline = time.monotonic() + self.wait_s
        while True:
            if self.client.set(self.key, self.token, nx=True, px=self.ttl_ms):
                return True
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.02)

    def release(self) -> bool:
        return self._release(keys=[self.key], args=[self.token]) == 1

    def __enter__(self):
        if not self.acquire():
            raise LockNotAcquired(self.key)
        return self

    def __exit__(self, *exc_info):
        self.release()
