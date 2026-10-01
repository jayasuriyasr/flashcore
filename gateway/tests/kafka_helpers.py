import asyncio
import json
import time

from aiokafka import AIOKafkaConsumer, TopicPartition

from .conftest import KAFKA_BOOTSTRAP

TOPIC = "order-events"


class TopicTail:
    """Reads only the messages produced after it was opened (no consumer group, no commits)."""

    def __init__(self, topic=TOPIC):
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP, group_id=None, enable_auto_commit=False
        )

    async def __aenter__(self):
        await self.consumer.start()
        await self.consumer.topics()  # forces a metadata fetch
        partitions = self.consumer.partitions_for_topic(self.topic)
        assert partitions, f"topic {self.topic} not found"
        tps = [TopicPartition(self.topic, p) for p in sorted(partitions)]
        self.consumer.assign(tps)
        for tp, offset in (await self.consumer.end_offsets(tps)).items():
            self.consumer.seek(tp, offset)
        return self

    async def __aexit__(self, *exc):
        await self.consumer.stop()

    async def collect(self, predicate, expected, timeout_s=30.0):
        """Return [(partition, event)] for messages matching predicate, until `expected` are seen."""
        found = []
        deadline = time.monotonic() + timeout_s
        while len(found) < expected and time.monotonic() < deadline:
            batches = await self.consumer.getmany(timeout_ms=500)
            for tp, messages in batches.items():
                for m in messages:
                    event = json.loads(m.value)
                    if predicate(event, m):
                        found.append((tp.partition, event, m.key.decode() if m.key else None))
        await asyncio.sleep(1)  # catch any unexpected extra copies
        for tp, messages in (await self.consumer.getmany(timeout_ms=500)).items():
            for m in messages:
                event = json.loads(m.value)
                if predicate(event, m):
                    found.append((tp.partition, event, m.key.decode() if m.key else None))
        return found
