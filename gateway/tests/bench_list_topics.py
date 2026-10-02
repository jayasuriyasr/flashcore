"""How long does a metadata request (list_topics) block the calling thread?

    docker compose exec gateway python -m tests.bench_list_topics
"""
import time

from confluent_kafka import Producer

from app import config

p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
samples = []
for _ in range(40):
    t = time.perf_counter()
    p.list_topics(config.KAFKA_ORDER_TOPIC, timeout=2)
    samples.append((time.perf_counter() - t) * 1000)
    time.sleep(0.05)
samples.sort()
print(f"list_topics: min {samples[0]:.1f} ms  median {samples[len(samples) // 2]:.1f} ms  max {samples[-1]:.1f} ms")
