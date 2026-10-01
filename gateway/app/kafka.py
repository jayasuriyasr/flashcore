import asyncio
import logging

import orjson
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

log = logging.getLogger("gateway.kafka")


class KafkaUnavailable(Exception):
    pass


class OrderEventProducer:
    """One idempotent producer per process.

    The gateway must keep starting even when Kafka is down, so the producer connects
    in a background task and keeps retrying. Until it is connected, sends fail fast
    with KafkaUnavailable (the buy endpoint then releases the reservation → 503).
    """

    def __init__(self, bootstrap: str, topic: str, timeout_s: float):
        self.bootstrap = bootstrap
        self.topic = topic
        self.timeout_s = timeout_s
        self.producer: AIOKafkaProducer | None = None
        self._starter: asyncio.Task | None = None

    @property
    def connected(self) -> bool:
        return self.producer is not None

    def start_in_background(self) -> None:
        self._starter = asyncio.create_task(self._connect_forever())

    async def _connect_forever(self) -> None:
        delay = 1.0
        while True:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap,
                acks="all",                # leader + in-sync replicas must have it
                enable_idempotence=True,   # retries never write the same event twice
                linger_ms=5,               # tiny wait so concurrent buys share one batch
                request_timeout_ms=int(self.timeout_s * 1000),
                key_serializer=str.encode,
                value_serializer=orjson.dumps,
            )
            try:
                await producer.start()
            except asyncio.CancelledError:  # gateway shutting down mid-connect
                await producer.stop()
                raise
            except Exception as exc:  # broker not reachable yet
                log.warning("kafka not reachable (%s); retrying in %.0fs", exc, delay)
                await producer.stop()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
                continue
            self.producer = producer
            log.info("kafka producer connected to %s", self.bootstrap)
            return

    async def send(self, sale_id: int, event: dict) -> None:
        """Wait for the broker ack. Raises KafkaUnavailable, KafkaError or TimeoutError."""
        if self.producer is None:
            raise KafkaUnavailable("producer not connected")
        # key = sale_id → every event of one sale lands in the same partition, in order.
        await asyncio.wait_for(
            self.producer.send_and_wait(self.topic, key=str(sale_id), value=event),
            timeout=self.timeout_s,
        )

    async def stop(self) -> None:
        if self._starter and not self._starter.done():
            self._starter.cancel()
        if self.producer is not None:
            await self.producer.stop()


SEND_ERRORS = (KafkaUnavailable, KafkaError, TimeoutError)
