import logging
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from . import config
from .auth import InvalidToken, current_user, decode_user_id
from .errors import ApiError
from .kafka import SEND_ERRORS, OrderEventProducer
from .logging_setup import configure_logging
from .redis_ops import RedisOps
from .ws import ConnectionManager, stock_message

configure_logging()
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = RedisOps(config.REDIS_URL, config.REDIS_MAX_CONNECTIONS)
    app.state.kafka = OrderEventProducer(config.KAFKA_BOOTSTRAP, config.KAFKA_ORDER_TOPIC, config.PRODUCE_TIMEOUT_S)
    app.state.kafka.start_in_background()
    app.state.ws = ConnectionManager(app.state.redis, config.STOCK_TICK_S)
    app.state.ws.start()
    yield
    await app.state.ws.stop()
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


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket, token: str = "", sale_id: int | None = None):
    try:
        user_id = decode_user_id(token)
    except InvalidToken:
        # Accept first so the client receives our close code instead of a bare HTTP 403.
        await ws.accept()
        await ws.close(code=4401)
        return

    manager: ConnectionManager = ws.app.state.ws
    await ws.accept()
    manager.add(ws, user_id, sale_id)
    try:
        if sale_id is not None:
            remaining = await ws.app.state.redis.stock(sale_id)
            manager.last_stock.setdefault(sale_id, remaining)
            await ws.send_text(stock_message(sale_id, remaining))
        # We never expect messages from the client; receiving just tells us when it leaves.
        while True:
            message = await ws.receive()
            if message["type"] == "websocket.disconnect":
                break
    except WebSocketDisconnect:
        pass
    finally:
        manager.discard(ws)


@app.get("/health")
async def health(request: Request):
    redis_up = await request.app.state.redis.ping()
    kafka_up = request.app.state.kafka.connected
    return {
        "status": "ok" if redis_up and kafka_up else "degraded",
        "redis": "up" if redis_up else "down",
        "kafka": "up" if kafka_up else "down",
        "ws_connections": request.app.state.ws.count,  # this process only
    }
