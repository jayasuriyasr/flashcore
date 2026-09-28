# FlashCore — Architecture Spec

This file is the single source of truth. Every implementation step must follow it.
If a step needs to change a decision here, update this file in the same step.

## 1. Idea in one line

**Redis decides, Kafka remembers, Postgres records, Celery finishes, WebSocket tells.**

- **FastAPI gateway** owns the hot path only: `POST /buy` and `WS /ws`. It never touches PostgreSQL.
- **Django** owns everything else: schema + migrations, admin, auth (JWT), read APIs, the Kafka order writer, Celery tasks.

## 2. Components

| Service (compose name) | Tech | Job |
|---|---|---|
| `nginx` | Nginx | Load balancer, serves React build, routes `/api/sales/{id}/buy` + `/ws` to gateway, rest to Django |
| `gateway` | FastAPI + uvicorn | JWT check → Redis Lua reserve → Kafka produce → 202. WebSocket gateway. |
| `web` | Django + gunicorn | Admin, JWT login, read-only sales/orders API, "Start sale" admin action |
| `order-writer` | Django management command `consume_orders` | Kafka → PostgreSQL batch writes in REPEATABLE READ txn |
| `celery-worker` | Celery | `fulfil_order` (mock payment), stock release on failure, publishes updates |
| `celery-beat` | Celery beat | `reconcile` every 60 s |
| `postgres` | PostgreSQL 16 | Source of truth |
| `redis` | Redis 7 (AOF on) | Stock gate (Lua), locks, pub/sub, Celery broker (db 1) |
| `kafka` | apache/kafka 3.8 (KRaft, single broker) | `order-events` topic |
| `kafka-init` | one-shot | Creates topic `order-events` (3 partitions, RF 1) |

## 3. Tech versions / libraries

- Python 3.12 — Django 5, djangorestframework, djangorestframework-simplejwt, psycopg 3, confluent-kafka, celery 5, redis-py
- Gateway — fastapi, uvicorn[standard] (uvloop), redis.asyncio, aiokafka, PyJWT, orjson
- Tests — pytest, pytest-django, pytest-asyncio, httpx, websockets
- Frontend — React 18 + Vite
- Load test — k6 (run via `grafana/k6` Docker image on the compose network)
- Dev machine: Windows + Docker Desktop. All commands run via `docker compose`; tests run inside containers.

## 4. Environment variables (`.env`, with `.env.example` committed)

```
POSTGRES_DB=flashcore  POSTGRES_USER=flashcore  POSTGRES_PASSWORD=flashcore  POSTGRES_HOST=postgres
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
KAFKA_BOOTSTRAP=kafka:9092
JWT_SIGNING_KEY=dev-only-change-me        # shared by Django (sign) and gateway (verify), HS256
PAYMENT_FAILURE_RATE=0.05                 # mock payment; tests set 0 or 1
MOCK_PAYMENT_DELAY_MS=100                 # mock payment latency; load tests use 5
```

Host ports (dev): nginx 80, web 8000, gateway 8001 (removed once gateway is scaled behind nginx), postgres 5432, redis 6379, kafka 9094 (EXTERNAL listener).

## 5. Data model (Django app `sales`)

```
Product    (id, name, price)
FlashSale  (id, product FK, starts_at, ends_at, total_stock, remaining)
           CHECK (remaining >= 0)
Order      (id UUID PK — gateway always sends its own, sale FK, user FK, status, created_at, updated_at)
           status ∈ PENDING | CONFIRMED | FAILED
           UNIQUE (sale, user) WHERE status <> 'FAILED'   -- one live unit per user per sale;
                                                          -- partial, so a user whose payment FAILED can buy again
```

**Invariant (checked by `manage.py check_invariants`):**
`remaining + count(orders where status != FAILED) == total_stock` and `remaining >= 0`, for every sale.

## 6. Redis keys

| Key | Type | Meaning |
|---|---|---|
| `sale:{id}:stock` | string int | Units still reservable. Missing key = sale not started → treated as sold out |
| `sale:{id}:buyers` | set | user_ids who reserved |
| `lock:order:{id}` | string (token) | Per-order lock, `SET NX PX 10000`, released by compare-and-delete Lua |
| `lock:reconcile:{sale_id}` | string (token) | Stops two reconcile runs overlapping |
| channel `user:{id}` | pub/sub | Order status updates for one user |

Lua scripts live in `gateway/app/lua/` (`reserve.lua`, `release.lua`); Django/Celery load the same files.

`reserve.lua` → returns `1` reserved, `0` sold out / not started, `-1` already bought.
`release.lua` → `INCR stock` + `SREM buyers` (undo of a reservation).

## 7. Buy flow

**Sync (inside the request, target p99 < 50 ms):**
1. React → `POST /api/sales/{id}/buy` with `Authorization: Bearer <JWT>`.
2. Gateway verifies JWT locally (HS256, claim `user_id`) — no DB.
3. Gateway runs `reserve.lua`. `0` → 409 `sold_out`, `-1` → 409 `already_purchased`.
4. `1` → create `order_id` (uuid4), produce to Kafka (`acks=all`, idempotent producer, key = `sale_id`, 2 s timeout).
   Produce fails → run `release.lua`, return 503.
5. Return `202 {"order_id": ..., "status": "RESERVED"}`.

**Async:**
6. `order-writer` consumes batches (≤ 500 msgs / 100 ms), groups by sale, and per sale runs one REPEATABLE READ txn:
   ```sql
   BEGIN ISOLATION LEVEL REPEATABLE READ;
   INSERT INTO orders ... ON CONFLICT DO NOTHING RETURNING id;   -- n = new rows
   UPDATE flash_sales SET remaining = remaining - n WHERE id = :sale AND remaining >= n;
   COMMIT;
   ```
   - SQLSTATE 40001 → retry the txn (max 5, small backoff).
   - UPDATE hits 0 rows (Redis over-admitted) → roll back, redo one-by-one; orders that don't fit are inserted as FAILED
     (no stock change) and the user is notified via `PUBLISH user:{id}`. Do **not** run `release.lua` here — that unit never existed.
   - Kafka offsets committed **only after** the DB commit (at-least-once + idempotent insert = effectively once).
   - After commit → `fulfil_order.delay(order_id)` for each new PENDING order.
7. `fulfil_order` (Celery, `acks_late=True`): take `lock:order:{id}` → if status != PENDING return → mock payment →
   RR txn sets CONFIRMED, or FAILED + `remaining += 1` → after commit, on FAILED run `release.lua` → `PUBLISH user:{id}`.
8. Gateway WebSocket forwards `user:{id}` messages to that user's sockets. Separately it reads `sale:{id}:stock` every 500 ms and broadcasts only if the value changed.

## 8. Message formats

Kafka `order-events` value (JSON):
```json
{"order_id": "uuid", "sale_id": 1, "user_id": 42, "ts": 1759132800.123}
```

WebSocket `GET /ws?token=<JWT>&sale_id=<id>` → server messages:
```json
{"type": "stock", "sale_id": 1, "remaining": 42}
{"type": "order", "order_id": "uuid", "sale_id": 1, "status": "CONFIRMED"}
```
Bad/missing token → close with code 4401.

## 9. HTTP endpoints

| Method + path | Service | Notes |
|---|---|---|
| `POST /api/auth/token/` | web | simplejwt, returns access + refresh |
| `GET /api/sales/` , `GET /api/sales/{id}/` | web | read-only |
| `GET /api/orders/mine/` | web | JWT required |
| `POST /api/sales/{id}/buy` | gateway | 202 / 409 / 401 / 503 |
| `GET /api/sales/{id}/stock` | gateway | reads Redis |
| `GET /ws` | gateway | WebSocket |
| `/admin/` | web | Django admin, "Start sale" / "End sale" actions |

## 10. Consistency layers

1. **Redis Lua** — atomic check-and-decrement; no lock on the hot path (a per-product lock would serialise every buyer).
2. **Redis per-order lock** — Celery is at-least-once; lock + status check prevents double payment / double stock release.
3. **PostgreSQL REPEATABLE READ + constraints** — snapshot reads (no phantoms in the invariant check), 40001 retry on concurrent updates, conditional UPDATE, `CHECK (remaining >= 0)`, `UNIQUE (sale, user)`.

## 11. Reconcile (Celery beat, every 60 s, per active sale, under `lock:reconcile:{id}`)

- In one REPEATABLE READ txn read `remaining` and `count(non-FAILED orders)`; log an error if the invariant fails.
- If Redis stock > DB remaining (unsafe direction) → atomically lower Redis to DB remaining (Lua: only lowers).
  If Redis < DB remaining (safe direction) → log only; fixed by admin "Resync stock" when the sale is paused.
- Re-enqueue `fulfil_order` for orders PENDING longer than 2 minutes.

## 12. Repo layout

```
flash/
├─ docker-compose.yml   .env.example   README.md
├─ docs/ARCHITECTURE.md   docs/PLAN.md
├─ nginx/nginx.conf
├─ frontend/                 React + Vite
├─ gateway/                  FastAPI (never imports Django)
│  ├─ app/main.py  app/auth.py  app/redis_ops.py  app/kafka.py  app/ws.py
│  ├─ app/lua/reserve.lua  app/lua/release.lua
│  └─ tests/
├─ core/                     Django project
│  ├─ core/settings.py  core/celery.py
│  └─ sales/ models.py admin.py api.py tasks.py locks.py
│           management/commands/consume_orders.py
│           management/commands/check_invariants.py
│           management/commands/seed_load_users.py
│           tests/
├─ scripts/                  smoke / chaos helpers
└─ loadtest/buy.js           k6
```

## 13. Rules for every step

- Keep it simple. No new services or libraries beyond this file without updating it.
- Gateway never talks to PostgreSQL. All schema changes go through Django migrations.
- Every step ends with tests that run via `docker compose` and pass, then a git commit.
