import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from . import config
from .auth import current_user
from .errors import ApiError
from .kafka import SEND_ERRORS, OrderEventProducer
from .redis_ops import RedisOps

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = RedisOps(config.REDIS_URL, config.REDIS_MAX_CONNECTIONS)
    app.state.kafka = OrderEventProducer(config.KAFKA_BOOTSTRAP, config.KAFKA_ORDER_TOPIC, config.PRODUCE_TIMEOUT_S)
    app.state.kafka.start_in_background()
    yield
    await app.state.kafka.stop()
    await app.state.redis.close()


app = FastAPI(title="FlashCore gateway", lifespan=lifespan)


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse({"error": exc.code}, status_code=exc.status)


@app.post("/api/sales/{sale_id}/buy", status_code=202)
async def buy(sale_id: int, request: Request, user_id: int = Depends(current_user)):
    redis: RedisOps = request.app.state.redis
    kafka: OrderEventProducer = request.app.state.kafka

    # 1. Atomic stock gate in Redis.
    result = await redis.reserve(sale_id, user_id)
    if result == 0:
        raise ApiError(409, "sold_out")
    if result == -1:
        raise ApiError(409, "already_purchased")

    # 2. Durably record the purchase in Kafka before saying yes.
    order_id = str(uuid.uuid4())
    event = {"order_id": order_id, "sale_id": sale_id, "user_id": user_id, "ts": time.time()}
    try:
        await kafka.send(sale_id, event)
    except SEND_ERRORS as exc:
        # Compensate: give the unit back so it can be sold again, and let the user retry.
        await redis.release(sale_id, user_id)
        log.warning(
            "produce failed (%s); reservation released",
            type(exc).__name__,
            extra={"order_id": order_id, "sale_id": sale_id, "user_id": user_id},
        )
        raise ApiError(503, "try_again") from None

    return {"order_id": order_id, "status": "RESERVED"}


@app.get("/api/sales/{sale_id}/stock")
async def stock(sale_id: int, request: Request):
    return {"sale_id": sale_id, "remaining": await request.app.state.redis.stock(sale_id)}


@app.get("/health")
async def health(request: Request):
    redis_up = await request.app.state.redis.ping()
    kafka_up = request.app.state.kafka.connected
    return {
        "status": "ok" if redis_up and kafka_up else "degraded",
        "redis": "up" if redis_up else "down",
        "kafka": "up" if kafka_up else "down",
    }
