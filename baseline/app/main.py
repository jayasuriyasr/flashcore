"""Baseline for comparison: the "just use PostgreSQL" flash sale (docs/SCALING.md).

The obvious design, built as well as it can be: one transaction per buy, no Redis, Kafka or Celery.

    INSERT INTO orders ... ON CONFLICT DO NOTHING        -- one live order per user (partial unique index)
    UPDATE flash_sales SET remaining = remaining - 1
     WHERE id = :sale AND remaining > 0                  -- never below zero

It is correct: a second buyer's UPDATE waits for the first one's row lock and then re-checks
`remaining > 0` on the committed row (Read Committed), so it can't oversell. But every buyer of a
sale updates the SAME row, so buys queue on that one lock, each holding it until its commit is
flushed to disk. The INSERT goes first so the hot row is locked for as short a time as possible.

Same JWT rules and response shape as the real gateway, so the k6 script runs unchanged against it.
"""
import os
import uuid
from contextlib import asynccontextmanager

import jwt
from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

DSN = (
    f"host={os.environ.get('POSTGRES_HOST', 'postgres')} port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
)
# "off" = don't wait for the WAL flush at commit: a crash can lose the last ~0.5 s of *acknowledged*
# buys. Only for the comparison in docs/SCALING.md; the default keeps PostgreSQL fully durable.
if os.environ.get("BASELINE_SYNCHRONOUS_COMMIT", "on") == "off":
    DSN += " options='-c synchronous_commit=off'"
JWT_SIGNING_KEY = os.environ["JWT_SIGNING_KEY"]
POOL_SIZE = int(os.environ.get("BASELINE_POOL_SIZE", "20"))  # per process; 6 processes stay under max_connections


class Refused(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = AsyncConnectionPool(DSN, min_size=POOL_SIZE, max_size=POOL_SIZE, open=False)
    await app.state.pool.open(wait=True)
    yield
    await app.state.pool.close()


app = FastAPI(title="FlashCore baseline (PostgreSQL only)", lifespan=lifespan)


@app.exception_handler(Refused)
async def refused(request: Request, exc: Refused):
    return JSONResponse({"error": exc.code}, status_code=exc.status)


def user_from(authorization: str | None) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise Refused(401, "unauthorized")
    try:
        payload = jwt.decode(authorization[7:], JWT_SIGNING_KEY, algorithms=["HS256"], options={"require": ["exp"]})
        if payload.get("token_type", "access") != "access":
            raise Refused(401, "unauthorized")
        return int(payload["user_id"])
    except (jwt.PyJWTError, KeyError, ValueError, TypeError):
        raise Refused(401, "unauthorized") from None


@app.post("/api/sales/{sale_id}/buy", status_code=202)
async def buy(sale_id: int, request: Request, authorization: str | None = Header(default=None)):
    user_id = user_from(authorization)
    order_id = uuid.uuid4()
    async with request.app.state.pool.connection() as conn:
        async with conn.transaction():  # an exception inside rolls the whole buy back
            cur = await conn.execute(
                "INSERT INTO orders (id, sale_id, user_id, status) VALUES (%s, %s, %s, 'PENDING') "
                "ON CONFLICT DO NOTHING RETURNING id",
                (order_id, sale_id, user_id),
            )
            if await cur.fetchone() is None:
                raise Refused(409, "already_purchased")
            cur = await conn.execute(
                "UPDATE flash_sales SET remaining = remaining - 1 WHERE id = %s AND remaining > 0", (sale_id,)
            )
            if cur.rowcount == 0:
                raise Refused(409, "sold_out")
    return {"order_id": str(order_id), "status": "RESERVED"}


@app.get("/health")
async def health(request: Request):
    async with request.app.state.pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"status": "ok"}
