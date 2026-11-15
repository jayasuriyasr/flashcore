import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from functools import partial

from fastapi import Depends, FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel
from redis.exceptions import RedisError

from . import config
from .auth import InvalidToken, current_user, decode_user_id
from .errors import ApiError
from .kafka import SEND_ERRORS, AckPending, OrderEventProducer
from .logging_setup import configure_logging
from .metrics import BUY_SECONDS, BUYS, LATE_KAFKA, STAGE_SECONDS, render
from .redis_ops import RedisOps
from .ws import ConnectionManager, stock_message

configure_logging()
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = RedisOps(config.REDIS_URL, config.REDIS_MAX_CONNECTIONS)
    app.state.kafka = OrderEventProducer(
        config.KAFKA_BOOTSTRAP, config.KAFKA_ORDER_TOPIC, config.PRODUCE_TIMEOUT_S, config.KAFKA_MESSAGE_TIMEOUT_S
    )
    app.state.kafka.start_in_background()
    app.state.ws = ConnectionManager(app.state.redis, config.STOCK_TICK_S)
    app.state.ws.start()
    yield
    await app.state.ws.stop()
    await app.state.kafka.stop()
    await app.state.redis.close()


app = FastAPI(title="FlashCore gateway", lifespan=lifespan)

if config.CORS_ALLOWED_ORIGINS:
    # Frontend on another origin (Vercel). WebSockets are not subject to CORS; /ws checks the token.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=config.CORS_ALLOWED_ORIGINS,
        allow_methods=["GET", "POST"],
        allow_headers=["Authorization", "Content-Type"],
        max_age=3600,  # one preflight per buy URL per hour
    )


def _is_buy(request: Request) -> bool:
    return request.url.path.endswith("/buy")


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    if _is_buy(request):
        BUYS.labels(exc.code).inc()  # sold_out, already_purchased, unauthorized, try_again
    return JSONResponse({"error": exc.code}, status_code=exc.status)


@app.exception_handler(RedisError)
async def redis_error_handler(request: Request, exc: RedisError):
    # Redis restarting/unreachable: nothing was reserved, the client can simply retry.
    # A clean 503 (instead of a crash) also keeps nginx from marking this replica dead.
    log.warning("redis unavailable (%s)", type(exc).__name__)
    if _is_buy(request):
        BUYS.labels("redis_unavailable").inc()
    return JSONResponse({"error": "try_again"}, status_code=503)


class Reserved(BaseModel):
    order_id: str
    status: str = "RESERVED"


@app.post("/api/sales/{sale_id}/buy", status_code=202)
async def buy(sale_id: int, request: Request, user_id: int = Depends(current_user)) -> Reserved:
    # The return type lets FastAPI serialize straight to JSON bytes via Pydantic (fast path).
    started = time.perf_counter()
    try:
        return await reserve_and_record(sale_id, user_id, request.app.state.redis, request.app.state.kafka, started)
    finally:
        BUY_SECONDS.observe(time.perf_counter() - started)  # every outcome, 202s and 409s/503s alike


async def reserve_and_record(sale_id: int, user_id: int, redis: RedisOps, kafka: OrderEventProducer,
                             started: float) -> Reserved:
    # 1. Atomic stock gate in Redis.
    result = await redis.reserve(sale_id, user_id)
    reserved_at = time.perf_counter()
    STAGE_SECONDS.labels("redis").observe(reserved_at - started)
    if result == 0:
        raise ApiError(409, "sold_out")
    if result == -1:
        raise ApiError(409, "already_purchased")

    # 2. Durably record the purchase in Kafka before saying yes.
    order_id = str(uuid.uuid4())
    event = {"order_id": order_id, "sale_id": sale_id, "user_id": user_id, "ts": time.time()}
    context = {"order_id": order_id, "sale_id": sale_id, "user_id": user_id}
    try:
        await kafka.send(sale_id, event)
    except AckPending as pending:
        # No verdict yet, and the event may still be written. Keep the reservation until Kafka's
        # client reports the real outcome: releasing now and having the event land later created
        # orders for units that were already back on sale (found by killing a broker under load).
        # Until then a retry by this user gets 409 already_purchased.
        pending.future.add_done_callback(partial(_settle_late, redis, context))
        log.warning("no kafka ack within %.0f s; reservation kept until the outcome is known",
                    config.PRODUCE_TIMEOUT_S, extra=context)
        raise ApiError(503, "try_again") from None
    except SEND_ERRORS as exc:
        # Not written: give the unit back so it can be sold again, and let the user retry.
        await _release(redis, context)
        log.warning("produce failed (%s); reservation released", type(exc).__name__, extra=context)
        raise ApiError(503, "try_again") from None

    done = time.perf_counter()
    STAGE_SECONDS.labels("kafka").observe(done - reserved_at)
    BUYS.labels("reserved").inc()
    if done - started > config.SLOW_BUY_S:
        # Slow-request log: tells whether a slow buy waited on Redis or on the Kafka ack.
        log.warning(
            "slow buy: %.0f ms (redis %.0f ms, kafka ack %.0f ms)",
            (done - started) * 1000, (reserved_at - started) * 1000, (done - reserved_at) * 1000,
            extra={"order_id": order_id, "sale_id": sale_id},
        )
    return Reserved(order_id=order_id)


async def _release(redis: RedisOps, context: dict) -> None:
    try:
        await redis.release(context["sale_id"], context["user_id"])
    except RedisError:
        # Redis is down too: the unit stays held (Redis below DB = safe undersell);
        # End + Start the sale resyncs it.
        log.error("could not release reservation", extra=context)


_late_releases: set[asyncio.Task] = set()  # keeps the tasks referenced until they finish


def _settle_late(redis: RedisOps, context: dict, future: asyncio.Future) -> None:
    """Kafka's verdict for a buy that was already answered 503 (runs in the event loop)."""
    if future.cancelled():
        return
    if future.exception() is None:
        LATE_KAFKA.labels("delivered").inc()
        log.warning("kafka ack arrived after the 503; the order proceeds", extra=context)
        return
    LATE_KAFKA.labels("released").inc()
    log.warning("kafka gave up on the event (%s); reservation released", future.exception(), extra=context)
    task = asyncio.get_running_loop().create_task(_release(redis, context))
    _late_releases.add(task)
    task.add_done_callback(_late_releases.discard)


@app.get("/metrics", include_in_schema=False)
async def metrics():
    body, content_type = render()
    return Response(body, media_type=content_type)


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
            try:
                remaining = await ws.app.state.redis.stock(sale_id)
                manager.last_stock.setdefault(sale_id, remaining)
                await ws.send_text(stock_message(sale_id, remaining))
            except RedisError:
                pass  # the stock ticker sends it once Redis is back
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
