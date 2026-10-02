import json
import uuid
from io import StringIO

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from django.conf import settings
from django.core.management import call_command


def _admin():
    return AdminClient({"bootstrap.servers": settings.KAFKA_BOOTSTRAP})


def create_topic(partitions=3) -> str:
    """A throwaway topic per test, so tests never read each other's (or real) events."""
    name = f"test-orders-{uuid.uuid4().hex[:10]}"
    admin = _admin()  # keep a reference: if the client is garbage-collected the futures fail with _DESTROY
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    futures[name].result(timeout=20)
    return name


def delete_topic(name: str) -> None:
    admin = _admin()
    admin.delete_topics([name])[name].result(timeout=20)


def order_event(sale_id, user_id, order_id=None) -> dict:
    return {"order_id": order_id or str(uuid.uuid4()), "sale_id": sale_id, "user_id": user_id, "ts": 0}


def produce(topic: str, messages) -> None:
    producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP, "enable.idempotence": True})
    for m in messages:
        if isinstance(m, dict):
            producer.produce(topic, key=str(m["sale_id"]).encode(), value=json.dumps(m).encode())
        else:
            producer.produce(topic, value=m)
    assert producer.flush(20) == 0, "messages not delivered"


def run_writer(topic: str, max_messages: int, group_id: str | None = None, idle_exit_seconds: float = 20) -> str:
    out = StringIO()
    call_command(
        "consume_orders",
        topic=topic,
        group_id=group_id or f"test-{uuid.uuid4().hex[:8]}",
        max_messages=max_messages,
        idle_exit_seconds=idle_exit_seconds,
        stdout=out,
    )
    return out.getvalue()
