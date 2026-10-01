import threading
import time

import pytest

from sales.locks import LockNotAcquired, RedisLock


def test_second_acquire_fails_while_held(redis_db):
    a = RedisLock("lock:test", ttl_ms=5000)
    b = RedisLock("lock:test", ttl_ms=5000)
    assert a.acquire()
    assert not b.acquire()
    assert a.release()
    assert b.acquire()
    b.release()


def test_expired_lock_is_taken_over_and_old_holder_cannot_delete_it(redis_db):
    a = RedisLock("lock:test", ttl_ms=200)
    assert a.acquire()
    time.sleep(0.35)  # a's lock expires (e.g. a long GC pause)

    b = RedisLock("lock:test", ttl_ms=5000)
    assert b.acquire()
    assert a.release() is False  # compare-and-delete: the key holds b's token now
    assert redis_db.get("lock:test") == b.token

    assert b.release() is True
    assert redis_db.get("lock:test") is None


def test_waiting_acquire_gets_the_lock_once_released(redis_db):
    holder = RedisLock("lock:test", ttl_ms=5000)
    assert holder.acquire()
    threading.Timer(0.2, holder.release).start()

    waiter = RedisLock("lock:test", ttl_ms=5000, wait_s=2.0)
    started = time.monotonic()
    assert waiter.acquire()
    assert 0.1 < time.monotonic() - started < 2.0
    waiter.release()


def test_context_manager_raises_when_busy(redis_db):
    with RedisLock("lock:test"):
        with pytest.raises(LockNotAcquired):
            with RedisLock("lock:test"):
                pass
    assert redis_db.get("lock:test") is None  # released on exit


def test_lock_has_a_ttl(redis_db):
    with RedisLock("lock:test", ttl_ms=10_000):
        assert 0 < redis_db.pttl("lock:test") <= 10_000
