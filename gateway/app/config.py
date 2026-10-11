import os

REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", "200"))

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_ORDER_TOPIC = os.environ.get("KAFKA_ORDER_TOPIC", "order-events")
PRODUCE_TIMEOUT_S = float(os.environ.get("PRODUCE_TIMEOUT_S", "2"))

JWT_SIGNING_KEY = os.environ.get("JWT_SIGNING_KEY", "dev-only-change-me-0123456789abcdef")

STOCK_TICK_S = float(os.environ.get("STOCK_TICK_S", "0.5"))

SLOW_BUY_S = float(os.environ.get("SLOW_BUY_MS", "100")) / 1000  # log buys slower than this

# Origins allowed to call the buy endpoint from a browser, e.g. the React app on Vercel.
# Empty = same origin only (local Docker, everything behind nginx).
CORS_ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("CORS_ALLOWED_ORIGINS", "").split(",") if o.strip()]
