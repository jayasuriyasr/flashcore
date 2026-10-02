import asyncio
import json
import time

from aiokafka import AIOKafkaConsumer

from .conftest import KAFKA_BOOTSTRAP

TOPIC = "order-events"


class TopicTail:
    """Reads only the messages produced after it was opened (no consumer group, no commits)."""

    def __init__(self, topic=TOPIC):
        self.topic = topic
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=None,               # no group: the consumer takes every partition itself
            enable_auto_commit=False,
            auto_offset_reset="latest",  # start at the current end of each partition
        )

    async def __aenter__(self):
        await self.consumer.start()
        deadline = time.monotonic() + 20
        while not self.consumer.assignment():
            assert time.monotonic() < deadline, f"no partitions assigned for {self.topic}"
            await asyncio.sleep(0.1)
        # Resolve the "latest" position now, so events produced after this point are never skipped.
        for tp in self.consumer.assignment():
            await self.consumer.position(tp)
        return self

    async def __aexit__(self, *exc):
        await self.consumer.stop()

    def _matching(self, batches, predicate):
        found = []
        for tp, messages in batches.items():
            for m in messages:
                event = json.loads(m.value)
                if predicate(event, m):
                    found.append((tp.partition, event, m.key.decode() if m.key else None))
        return found

    async def collect(self, predicate, expected, timeout_s=30.0):
        """Return [(partition, event, key)] for matching messages, until `expected` are seen."""
        found = []
        deadline = time.monotonic() + timeout_s
        while len(found) < expected and time.monotonic() < deadline:
            found += self._matching(await self.consumer.getmany(timeout_ms=500), predicate)
        await asyncio.sleep(1)  # catch any unexpected extra copies
        found += self._matching(await self.consumer.getmany(timeout_ms=500), predicate)
        return found
