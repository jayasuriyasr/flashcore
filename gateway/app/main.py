import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from . import config
from .auth import current_user
from .errors import ApiError
from .redis_ops import RedisOps

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = RedisOps(config.REDIS_URL, config.REDIS_MAX_CONNECTIONS)
    yield
    await app.state.redis.close()


app = FastAPI(title="FlashCore gateway", lifespan=lifespan)


@app.exception_handler(ApiError)
async def api_error_handler(request: Request, exc: ApiError):
    return JSONResponse({"error": exc.code}, status_code=exc.status)


@app.post("/api/sales/{sale_id}/buy", status_code=202)
async def buy(sale_id: int, request: Request, user_id: int = Depends(current_user)):
    redis: RedisOps = request.app.state.redis

    result = await redis.reserve(sale_id, user_id)
    if result == 0:
        raise ApiError(409, "sold_out")
    if result == -1:
        raise ApiError(409, "already_purchased")

    order_id = str(uuid.uuid4())
    # TODO(step 4): produce the order event to Kafka; on failure release the reservation and return 503.
    return {"order_id": order_id, "status": "RESERVED"}


@app.get("/api/sales/{sale_id}/stock")
async def stock(sale_id: int, request: Request):
    return {"sale_id": sale_id, "remaining": await request.app.state.redis.stock(sale_id)}


@app.get("/health")
async def health(request: Request):
    redis_up = await request.app.state.redis.ping()
    return {"status": "ok" if redis_up else "degraded", "redis": "up" if redis_up else "down"}
