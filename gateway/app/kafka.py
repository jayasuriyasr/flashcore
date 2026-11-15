"""Order-event producer for the gateway, on librdkafka (confluent-kafka).

Measured with tests/bench_hot_path.py: aiokafka (pure Python) cost ~1 ms of CPU per
message, half of the whole request. librdkafka does batching, retries and the network
in C; Python only pays for a small delivery callback.
"""
import asyncio
import logging
import threading

import orjson
from confluent_kafka import KafkaException, Producer

log = logging.getLogger("gateway.kafka")


class KafkaUnavailable(Exception):
    """Definitely not written: the message never left this process."""


class AckPending(Exception):
    """No verdict from Kafka within the request's wait. librdkafka keeps trying for up to
    message.timeout.ms; `future` resolves (ack) or fails (gave up) when the real outcome is known."""

    def __init__(self, future: asyncio.Future):
        super().__init__("no broker acknowledgement yet")
        self.future = future


def _resolve(future: asyncio.Future, err) -> None:
    if future.done():  # the request already timed out and moved on
        return
    if err is None:
        future.set_result(None)
    else:
        future.set_exception(KafkaException(err))


class OrderEventProducer:
    """One idempotent producer per process.

    produce() only queues the message in librdkafka; a background thread calls poll(),
    which runs the delivery callbacks, and each callback resolves the waiting request's
    asyncio future. Kafka being down never blocks startup.

    Two different timeouts, on purpose:
    - timeout_s: how long a buy request waits for the ack before answering 503.
    - message_timeout_s: how long librdkafka keeps trying to deliver. Much longer, so that its
      final report is the real outcome. When both were 2 s, messages already sent to a broker that
      stalled (a replica had just died) were reported as failed and then written anyway.
    """

    def __init__(self, bootstrap: str, topic: str, timeout_s: float, message_timeout_s: float = 30.0):
        self.topic = topic
        self.timeout_s = timeout_s
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",                    # leader + in-sync replicas must have it
            "enable.idempotence": True,       # retries never write the same event twice
            # Wait up to 15 ms so concurrent buys share one produce request. At 5 ms each of the
            # 6 producer processes sent ~1-2 messages per request and the broker burned ~2 CPU
            # cores on request overhead alone.
            "linger.ms": 15,
            "compression.type": "lz4",
            "message.timeout.ms": int(message_timeout_s * 1000),  # then report the message as failed
        })
        self.connected = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start_in_background(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._thread = threading.Thread(target=self._poll_forever, name="kafka-poll", daemon=True)
        self._thread.start()
        threading.Thread(target=self._check_forever, name="kafka-health", daemon=True).start()

    def _poll_forever(self) -> None:
        # Only poll here: anything slow in this thread delays every in-flight buy's ack.
        while not self._stop.is_set():
            self.producer.poll(0.05)  # serves delivery callbacks

    def _check_forever(self) -> None:
        # Liveness for /health, in its own thread so a slow metadata request never blocks acks.
        while not self._stop.is_set():
            try:
                self.producer.list_topics(self.topic, timeout=2)
                if not self.connected:
                    log.info("kafka producer connected")
                self.connected = True
            except KafkaException as exc:
                if self.connected:
                    log.warning("kafka unreachable (%s)", exc)
                self.connected = False
            self._stop.wait(5)

    async def send(self, sale_id: int, event: dict) -> None:
        """Wait for the broker ack.

        Raises KafkaUnavailable / KafkaException when the event was not written, and AckPending
        when there is no verdict yet (the caller must not treat that as a failure).
        """
        if self._loop is None:
            raise KafkaUnavailable("producer not started")
        future = self._loop.create_future()
        loop = self._loop

        def on_delivery(err, msg):  # runs in the poll thread
            loop.call_soon_threadsafe(_resolve, future, err)

        try:
            # key = sale_id → every event of one sale lands in the same partition, in order.
            self.producer.produce(self.topic, key=str(sale_id), value=orjson.dumps(event), on_delivery=on_delivery)
        except BufferError:
            raise KafkaUnavailable("local producer queue is full") from None
        except KafkaException as exc:
            raise KafkaUnavailable(str(exc)) from None
        try:
            # shield: on timeout the future stays alive for the delivery report that is still coming.
            await asyncio.wait_for(asyncio.shield(future), timeout=self.timeout_s)
        except TimeoutError:
            raise AckPending(future) from None

    async def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            await asyncio.to_thread(self._thread.join, 2)
        await asyncio.to_thread(self.producer.flush, 2)


SEND_ERRORS = (KafkaUnavailable, KafkaException)  # the event was not written
