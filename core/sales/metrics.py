"""Business metrics for Prometheus, computed from PostgreSQL, Redis and Kafka at scrape time.

Served by Django at /metrics/ (nginx doesn't route it: internal only). Each scrape runs a few cheap
queries; a failing source (e.g. Kafka down) just leaves its metric out instead of failing the scrape.
"""
import logging

from confluent_kafka import Consumer, KafkaException, TopicPartition
from django.conf import settings
from django.db import connection
from django.utils import timezone
from prometheus_client.core import GaugeMetricFamily

from . import redis_client as rc
from .models import FlashSale

log = logging.getLogger("sales.metrics")

_consumer = None


def _lag_consumer() -> Consumer:
    # Reads the order writer's committed offsets; it never subscribes, so it doesn't join the group.
    global _consumer
    if _consumer is None:
        _consumer = Consumer({
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP,
            "group.id": "order-writer",
            "enable.auto.commit": False,
        })
    return _consumer


def order_writer_lag() -> int:
    """Events in Kafka that the order writer hasn't committed to PostgreSQL yet."""
    consumer, topic = _lag_consumer(), settings.KAFKA_ORDER_TOPIC
    partitions = consumer.list_topics(topic, timeout=2).topics[topic].partitions
    committed = consumer.committed([TopicPartition(topic, p) for p in partitions], timeout=2)
    lag = 0
    for tp in committed:
        low, high = consumer.get_watermark_offsets(tp, timeout=2)
        lag += high - (tp.offset if tp.offset >= 0 else low)
    return lag


class SalesCollector:
    def describe(self):
        # Without this, registering the collector would run collect() (DB queries) at import time.
        return []

    def collect(self):
        with connection.cursor() as cur:
            cur.execute("SELECT status, count(*) FROM orders GROUP BY status")
            by_status = dict(cur.fetchall())
            cur.execute("SELECT count(*) FROM orders WHERE status = 'PENDING' AND created_at < now() - interval '1 minute'")
            stuck = cur.fetchone()[0]

        orders = GaugeMetricFamily("flashcore_orders", "Orders by status", labels=["status"])
        for status in ("PENDING", "CONFIRMED", "FAILED"):
            orders.add_metric([status], by_status.get(status, 0))
        yield orders
        yield GaugeMetricFamily("flashcore_orders_pending_over_1m",
                                "PENDING orders older than a minute (fulfilment is behind or stuck)", value=stuck)

        # Live sales: PostgreSQL remaining vs the Redis counter (Redis may only ever be lower).
        remaining = GaugeMetricFamily("flashcore_sale_remaining", "Units left per live sale", labels=["sale_id", "source"])
        sales = list(FlashSale.objects.filter(ends_at__gt=timezone.now()).values_list("id", "remaining")[:50])
        try:
            redis_values = rc.client().mget([rc.stock_key(sale_id) for sale_id, _ in sales]) if sales else []
        except Exception as exc:  # noqa: BLE001 - Redis down must not fail the scrape
            log.warning("metrics: redis unavailable (%s)", exc)
            redis_values = [None] * len(sales)
        for (sale_id, db_remaining), redis_value in zip(sales, redis_values, strict=True):
            if redis_value is None:
                continue  # not live in Redis
            remaining.add_metric([str(sale_id), "postgres"], db_remaining)
            remaining.add_metric([str(sale_id), "redis"], int(redis_value))
        yield remaining

        try:
            lag = order_writer_lag()
        except (KafkaException, KeyError) as exc:
            log.warning("metrics: kafka lag unavailable (%s)", exc)
        else:
            yield GaugeMetricFamily("flashcore_order_writer_lag", "Order events not yet written to PostgreSQL",
                                    value=lag)
