import logging
import signal
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import close_old_connections

from sales import order_writer

log = logging.getLogger("sales.consume_orders")


class Command(BaseCommand):
    help = "Consume order events from Kafka and write them to PostgreSQL (the order writer)."

    def add_arguments(self, parser):
        parser.add_argument("--topic", default=settings.KAFKA_ORDER_TOPIC)
        parser.add_argument("--group-id", default="order-writer")
        parser.add_argument("--batch-size", type=int, default=500)
        parser.add_argument("--max-messages", type=int, default=0, help="exit after this many messages (tests)")
        parser.add_argument("--idle-exit-seconds", type=float, default=0, help="exit after this long without messages (tests)")

    def handle(self, *args, **opts):
        self.stopping = False
        consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": opts["group_id"],
            "enable.auto.commit": False,      # we commit only after PostgreSQL has committed
            "auto.offset.reset": "earliest",
        })
        consumer.subscribe([opts["topic"]])
        previous = {s: signal.signal(s, self._request_stop) for s in (signal.SIGTERM, signal.SIGINT)}
        log.info("order writer consuming %s as group %s", opts["topic"], opts["group_id"])

        seen = 0
        last_message_at = time.monotonic()
        try:
            while not self.stopping:
                messages = consumer.consume(num_messages=opts["batch_size"], timeout=0.1)
                if messages:
                    last_message_at = time.monotonic()
                    self._process(messages)
                    self._commit(consumer)  # at-least-once: a crash before this re-delivers the batch
                    seen += len(messages)
                if opts["max_messages"] and seen >= opts["max_messages"]:
                    break
                if opts["idle_exit_seconds"] and time.monotonic() - last_message_at > opts["idle_exit_seconds"]:
                    break
        finally:
            consumer.close()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
        log.info("order writer stopped after %d messages", seen)
        self.stdout.write(f"stopped after {seen} messages")

    def _request_stop(self, signum, frame):
        # Finish the batch in hand, commit, then exit cleanly.
        log.info("signal %s received, stopping after the current batch", signum)
        self.stopping = True

    def _process(self, messages):
        close_old_connections()  # long-running process: drop dead/expired DB connections
        events = []
        for message in messages:
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", message.error())
                continue
            try:
                events.append(order_writer.parse(message.value()))
            except order_writer.BadEvent as exc:
                log.error("skipping malformed message %s[%s]@%s: %s",
                          message.topic(), message.partition(), message.offset(), exc)
        if events:
            # If this raises (e.g. DB down) the process exits without committing offsets;
            # docker restarts it and the batch is consumed again.
            new_ids = order_writer.persist(events)
            order_writer.on_orders_committed(new_ids)

    @staticmethod
    def _commit(consumer):
        try:
            consumer.commit(asynchronous=False)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError._NO_OFFSET:
                raise
