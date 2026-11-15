"""Prometheus metrics for the gateway, served at /metrics (nginx doesn't route it: internal only).

uvicorn runs several worker processes per container, so prometheus_client's multiprocess mode is
used: each process writes its samples to files in PROMETHEUS_MULTIPROC_DIR (set in the Dockerfile,
emptied on start) and /metrics merges them. Without that variable (e.g. a local run) the normal
single-process registry is used.
"""
import os

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client import multiprocess

# Buys are expected in tens of ms; the upper buckets catch Kafka-ack stalls and timeouts.
BUCKETS = (0.005, 0.01, 0.02, 0.03, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

BUYS = Counter("flashcore_buy_requests", "Buy requests by outcome", ["outcome"])
BUY_SECONDS = Histogram("flashcore_buy_duration_seconds", "Time to answer a buy (inside the gateway)", buckets=BUCKETS)
STAGE_SECONDS = Histogram("flashcore_buy_stage_seconds", "Time per buy stage: redis reserve, kafka ack",
                          ["stage"], buckets=BUCKETS)
WS_CONNECTIONS = Gauge("flashcore_ws_connections", "Open WebSocket connections", multiprocess_mode="livesum")


def render() -> tuple[bytes, str]:
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:
        registry = REGISTRY
    return generate_latest(registry), CONTENT_TYPE_LATEST
