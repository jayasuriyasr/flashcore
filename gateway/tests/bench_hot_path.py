"""CPU cost of each piece of the buy hot path, measured inside the gateway container.

    docker compose exec gateway python -m tests.bench_hot_path

Uses time.process_time() (CPU, not wall clock), so network waits don't count.
"""
import asyncio
import random
import time
import uuid

from app import config
from app.auth import decode_user_id
from app.kafka import OrderEventProducer
from app.redis_ops import RedisOps

from .conftest import make_token

N = 5000


def report(label, cpu_s, n=N):
    print(f"{label:<38} {cpu_s / n * 1e6:8.1f} us CPU per call")


async def main():
    tokens = [make_token(random.randint(1, 10**9)) for _ in range(N)]
    t = time.process_time()
    for tok in tokens:
        decode_user_id(tok)
    report("JWT verify (PyJWT, HS256)", time.process_time() - t)

    redis = RedisOps(config.REDIS_URL)
    sale_id = random.randint(10**9, 2 * 10**9)
    await redis.client.set(f"sale:{sale_id}:stock", 10**7)
    users = random.sample(range(10**9), N)
    t = time.process_time()
    await asyncio.gather(*(redis.reserve(sale_id, u) for u in users))
    report("Redis reserve.lua (redis-py asyncio)", time.process_time() - t)

    kafka = OrderEventProducer(config.KAFKA_BOOTSTRAP, "order-events", 10)
    kafka.start_in_background()
    while not kafka.connected:
        await asyncio.sleep(0.1)
    events = [{"order_id": str(uuid.uuid4()), "sale_id": sale_id, "user_id": u, "ts": 0} for u in users]
    t = time.process_time()
    await asyncio.gather(*(kafka.send(sale_id, e) for e in events))
    # process_time covers this thread and librdkafka's own threads in the same process.
    report("Kafka produce + ack (confluent-kafka)", time.process_time() - t)

    await kafka.stop()
    await redis.client.delete(f"sale:{sale_id}:stock", f"sale:{sale_id}:buyers")
    await redis.close()


if __name__ == "__main__":
    asyncio.run(main())
