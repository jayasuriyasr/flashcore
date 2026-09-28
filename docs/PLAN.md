# FlashCore — 10-Step Implementation Plan

Every step has a **Build prompt** and a **Test prompt**. Paste them into Claude Code (opened in `D:\flash`) one at a time.
All prompts rely on `docs/ARCHITECTURE.md`, so a fresh session per step is fine.

## How to use

1. Paste the **Build prompt**. Read what it wrote — you will have to explain this code in the interview.
2. Paste the **Test prompt**. Don't move on until everything is green and committed.
3. Never skip ahead. Each step's tests become regression tests for the next ones.

| # | Step | Resume claim it backs |
|---|---|---|
| 1 | Infra skeleton (Postgres, Redis, Kafka) | Docker |
| 2 | Django core: models, admin, JWT, read APIs | Django, PostgreSQL |
| 3 | FastAPI gateway + Redis Lua reservation | FastAPI, Redis, no overselling |
| 4 | Kafka producer + compensation | Kafka |
| 5 | Order writer: Kafka → Postgres (Repeatable Read) | Repeatable Read, phantom reads |
| 6 | Celery fulfilment + per-order Redis lock | Celery, distributed locks |
| 7 | WebSocket gateway | Real-time stock and order updates |
| 8 | React frontend + Nginx | React, full demo |
| 9 | Reconciliation + chaos testing | Consistency under failure |
| 10 | k6 load test, tuning, README | 5,000+ RPS, p99 < 50 ms |

## Testing rules (every Test prompt refers to these)

1. Run tests inside Docker (`docker compose exec <service> pytest -v` or `docker compose run --rm ...`) and show me the real output. Never claim a pass you didn't see.
2. If a test fails, fix the code, not the test — unless the test itself is wrong, and then explain why.
3. Re-run all tests from earlier steps too. Nothing that passed before may break.
4. When everything passes, `git commit -m "step N: <summary>"`.
5. Finish with: what this step does in 5 bullets, and 3 interview questions about it with short answers.

---

## Step 1 — Infrastructure skeleton

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 1 of docs/PLAN.md only.

I'm on Windows with Docker Desktop. Set up the project skeleton:
- git init, a .gitignore for Python/Node/.env, and .env.example + .env with the variables from section 4.
- docker-compose.yml with postgres:16, redis:7 (appendonly yes, appendfsync everysec), and apache/kafka:3.8 in KRaft mode (single broker; INTERNAL listener kafka:9092 for containers, EXTERNAL listener localhost:9094 for my machine).
- A one-shot kafka-init service that creates topic order-events with 3 partitions. It must be safe to run twice.
- Healthchecks: postgres (pg_isready), redis (redis-cli ping), kafka (topics list). Named volumes for postgres and redis data.
- Empty folders from section 12 with .gitkeep files, and a README.md with "how to start" commands.
Don't create the Django or FastAPI apps yet. At the end, tell me exactly which commands to run.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Verify Step 1:
1. docker compose up -d, then wait until postgres, redis and kafka are healthy (show docker compose ps).
2. Postgres: SELECT version(); via docker compose exec.
3. Redis: PING returns PONG and CONFIG GET appendonly returns yes.
4. Kafka: describe topic order-events — exactly 3 partitions. Produce one test message and consume it back with the console tools.
5. Run kafka-init a second time — it must succeed without error.
6. Create a test table in Postgres and a test key in Redis, run docker compose down then up -d, and show both still exist (volumes work).
Save these checks as scripts/check_infra.ps1 so I can re-run them later.
```

**Done when:** all three services are healthy, the topic has 3 partitions, and `scripts/check_infra.ps1` passes.

---

## Step 2 — Django core: models, admin, JWT, read APIs

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 2 of docs/PLAN.md only.

Create the Django project in core/ (project "core", app "sales") with its own Dockerfile and a "web" service in docker-compose (gunicorn on port 8000, depends on healthy postgres, runs migrate on start).
- Models exactly as section 5: Product, FlashSale, Order. Order has a UUID pk (default uuid4, but the gateway always sends its own).
- Real DB constraints in Meta.constraints: CHECK (remaining >= 0), and a PARTIAL unique constraint on (sale, user) where status != 'FAILED'. Indexes on Order(sale, status) and Order(status, created_at).
- Django admin for all three models.
- DRF + simplejwt: POST /api/auth/token/. SIMPLE_JWT uses JWT_SIGNING_KEY, HS256 and the "user_id" claim.
- Read-only APIs: GET /api/sales/, GET /api/sales/{id}/, GET /api/orders/mine/ (JWT required).
- psycopg 3, all settings from environment variables, pytest + pytest-django in a requirements-dev.txt.
- manage.py check_invariants: for every sale, check remaining >= 0 and remaining + count(non-FAILED orders) == total_stock. Read both inside ONE REPEATABLE READ transaction, print a table, and exit with code 1 on any violation.
Don't add Redis, Kafka or Celery code yet.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write pytest tests in core/sales/tests/ for Step 2 and run them with docker compose exec web pytest -v:
1. Migrations apply on an empty DB, and makemigrations --check reports nothing missing.
2. Updating a FlashSale to remaining = -1 raises IntegrityError (the DB CHECK, not Python validation).
3. Two PENDING orders for the same (sale, user) raise IntegrityError, but a FAILED order plus a new PENDING order for the same (sale, user) is allowed.
4. POST /api/auth/token/ with correct credentials returns an access token; decoding it with PyJWT + JWT_SIGNING_KEY (HS256) gives the right user_id. Wrong password → 401.
5. /api/orders/mine/ without a token → 401; with a token it returns only that user's orders.
6. check_invariants exits 0 on consistent data and 1 when the data is broken (e.g. remaining set too high).
Then guide me through a manual check: createsuperuser, open http://localhost:8000/admin, create a product and a sale.
```

**Done when:** all tests pass and you can create a sale in the admin.

---

## Step 3 — FastAPI gateway + Redis Lua reservation

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 3 of docs/PLAN.md only.

1. Create the FastAPI gateway in gateway/ with its own Dockerfile and a "gateway" service (uvicorn with uvloop, host port 8001). It must NOT import Django or connect to Postgres.
2. app/auth.py: verify the Bearer JWT with PyJWT (HS256, JWT_SIGNING_KEY, check exp) and return user_id. Invalid → 401.
3. app/lua/reserve.lua and release.lua exactly as section 6. Load them once at startup with redis.asyncio register_script, using one shared connection pool.
4. POST /api/sales/{sale_id}/buy: run reserve.lua →
   409 {"error":"sold_out"} / 409 {"error":"already_purchased"} / 202 {"order_id": <uuid4>, "status":"RESERVED"}.
   Leave a clear TODO where the Kafka produce goes (Step 4).
5. GET /api/sales/{sale_id}/stock returns the Redis counter (0 if missing). GET /health returns ok.
6. In Django, add admin actions "Start sale" (SET sale:{id}:stock <remaining> NX, delete sale:{id}:buyers) and "End sale" (DEL sale:{id}:stock). Django will need release.lua later, so make the Lua files available to the web container too — pick the simplest way and explain it.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write pytest-asyncio tests in gateway/tests/ that hit the RUNNING gateway container over HTTP with httpx.AsyncClient and real Redis. Create JWTs in the tests with PyJWT and the shared key. Run with docker compose exec gateway pytest -v.
1. Oversell test: stock = 100, fire 1,000 concurrent buys from 1,000 different users with asyncio.gather → exactly 100 × 202 and 900 × 409 sold_out. Redis stock = 0 (never negative) and the buyers set has exactly 100 members.
2. The same user sending 20 parallel buys → exactly one 202.
3. Missing, expired or wrongly-signed token → 401. Sale not started (no stock key) → 409 sold_out.
4. release.lua increments stock by 1 and removes the user from the buyers set.
5. Latency: 2,000 sequential buys against a large stock — print p50/p99 (print only, no assert yet).
Also test the Django "Start sale" action: it sets the key, and running it twice does not reset stock mid-sale (NX).
```

**Done when:** the 1,000-vs-100 test gives exactly 100 successes every run. Run it 5 times.

---

## Step 4 — Kafka producer + compensation

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 4 of docs/PLAN.md only.

In the gateway add app/kafka.py with ONE aiokafka producer created in the FastAPI lifespan: acks="all", enable_idempotence=True, linger_ms=5, orjson serializer, key = str(sale_id).
Replace the Step 3 TODO in the buy endpoint: after reserve.lua returns 1, produce the event from section 8 and await the ack with a 2-second timeout (asyncio.wait_for).
If the produce fails or times out → run release.lua for that user and return 503 {"error":"try_again"}. Log it with the order_id.
The gateway must still start when Kafka is temporarily down: retry the producer start in the background, and have /health report kafka: down until it connects.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Add tests to gateway/tests/ and run them:
1. 300 buys across 3 different sales → exactly 300 messages on order-events (read with a fresh consumer group from the beginning). Every message has the right JSON fields, and all messages for one sale_id are in the same partition.
2. Every order_id returned in a 202 appears exactly once on the topic.
3. Failure test: docker compose stop kafka, then buy → 503 within about 3 seconds, Redis stock unchanged, and the user NOT in the buyers set (so they can retry). docker compose start kafka → buys return 202 again without restarting the gateway.
4. Print p50/p99 of 2,000 buys again and compare with Step 3.
Test 3 stops a container, so put it in its own file marked @pytest.mark.chaos and give me the command to run it separately.
```

**Done when:** no reservation is lost when Kafka is down (stock goes back), and the gateway recovers by itself.

---

## Step 5 — Order writer: Kafka → PostgreSQL (Repeatable Read)

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 5 of docs/PLAN.md only.

Create the Django management command consume_orders using confluent-kafka, and a compose service "order-writer":
- group.id = order-writer, enable.auto.commit = false, auto.offset.reset = earliest. Poll in batches (up to 500 messages or 100 ms).
- Group each batch by sale_id. For each sale run ONE REPEATABLE READ transaction exactly as section 7 step 6 (raw SQL is fine): INSERT ... ON CONFLICT DO NOTHING RETURNING id, then the conditional UPDATE using the number of rows actually inserted.
- Retry the whole transaction on SQLSTATE 40001 (serialization failure): max 5 attempts, small backoff. Put this in a reusable helper sales/db.py run_repeatable_read(fn) — Celery will use it too.
- If the UPDATE hits 0 rows: roll back and process that sale's events one by one. Orders that don't fit are inserted as FAILED with no stock change, and a FAILED message is published to user:{id}. Do NOT run release.lua here — explain why in a comment.
- Commit Kafka offsets ONLY after every DB transaction in the batch has committed.
- After commit, call on_orders_committed(order_ids). For now it only logs; Step 6 wires Celery.
- Handle SIGTERM: finish the current batch, commit, exit cleanly.
- Add --max-messages and --idle-exit-seconds flags so tests can run it for a bounded time.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write pytest-django tests (transactional DB) in core/sales/tests/ that produce events straight to Kafka and then run consume_orders with the bounded-run flags:
1. 500 events for one sale (stock 1,000) → 500 PENDING orders, remaining = 500, check_invariants passes.
2. Idempotency: produce the same 50 events twice → still 50 orders, and remaining dropped by exactly 50.
3. Over-admission: stock 10, 15 events from 15 users → exactly 10 PENDING + 5 FAILED, remaining = 0, never negative.
4. Retry: unit-test run_repeatable_read with a function that raises a serialization failure twice and then succeeds — it must succeed on attempt 3. Then an integration test with two real DB connections updating the same flash_sales row concurrently: show that one gets 40001, succeeds on retry, and the final remaining is correct.
5. Phantom-read demo: show that under READ COMMITTED the two invariant SELECTs can disagree when another connection commits between them, and under REPEATABLE READ they cannot. Explain what a phantom read is in a comment.
6. Crash safety (manual — give me the commands): run a buy load, docker compose kill order-writer halfway, start it again, wait, run check_invariants → no duplicates and the invariant holds.
```

**Done when:** replaying Kafka messages never creates duplicates, and stock in the DB never goes negative.

---

## Step 6 — Celery fulfilment + per-order Redis lock

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 6 of docs/PLAN.md only.

1. core/celery.py with broker CELERY_BROKER_URL (Redis db 1), task_acks_late=True, worker_prefetch_multiplier=1. Add compose services celery-worker and celery-beat (beat has no schedules yet).
2. sales/locks.py: a small Redis lock. Acquire with SET key <random token> NX PX ttl; release with a Lua compare-and-delete (only delete if the token matches). Make it a context manager. No external lock library.
3. sales/tasks.py fulfil_order(order_id):
   take lock:order:{id} (if busy, retry the task in 1 s) → reload the order; if status != PENDING, return (idempotent) → mock_payment() (sleep MOCK_PAYMENT_DELAY_MS, fail with probability PAYMENT_FAILURE_RATE) → run_repeatable_read: set CONFIRMED, or FAILED + remaining = remaining + 1 → AFTER the commit, if FAILED run release.lua → PUBLISH user:{user_id} with the order message from section 8.
4. Wire on_orders_committed in the order writer to call fulfil_order.delay for each new PENDING order.
5. Count payment attempts per order in Redis (INCR payments:{order_id}, 1-hour TTL) so tests can prove nobody was charged twice.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write tests in core/sales/tests/ and run them:
1. Lock unit tests: a second acquire fails while the lock is held; after the TTL expires another holder gets it; the first holder's release does NOT delete the second holder's lock (compare-and-delete works).
2. Double execution: run fulfil_order for the same order from 5 threads at once (real Redis, real DB) → payments:{order_id} == 1 and the order is CONFIRMED.
3. Failure path with PAYMENT_FAILURE_RATE=1: order FAILED, DB remaining +1, Redis stock +1, user removed from buyers — and that user can buy again through the gateway and gets a new PENDING order.
4. Pub/sub: subscribe to user:{id} in the test, fulfil one order, and receive exactly one message with the right status.
5. End to end through the running stack: 200 buys via the gateway → within 15 s every order is CONFIRMED or FAILED (none PENDING), and check_invariants passes.
6. Worker crash (manual — give me the commands): docker compose kill celery-worker during a run, start it again → no payments:* counter above 1.
```

**Done when:** no order is ever charged twice, even when a worker crashes.

---

## Step 7 — WebSocket gateway

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 7 of docs/PLAN.md only.

In the gateway add app/ws.py with GET /ws?token=<JWT>&sale_id=<id>:
- Verify the token BEFORE accepting. Bad or missing token → close with code 4401.
- A ConnectionManager that tracks sockets by user_id and by sale_id in each gateway process.
- ONE Redis pub/sub listener per process (not per socket) that psubscribes to user:* and forwards each message only to that user's sockets.
- ONE stock ticker per process: every 500 ms, for each sale with connected sockets, GET sale:{id}:stock and broadcast {"type":"stock",...} only if the value changed since the last tick. Send the current value immediately when a socket connects.
- Clean up on disconnect, and ignore errors from dead sockets so one bad client can't break a broadcast.
- Start and stop both background tasks in the FastAPI lifespan. Expose the current connection count in /health.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write async tests in gateway/tests/ using the websockets library against the running gateway:
1. No token or a bad token → connection closed with 4401.
2. On connect you immediately get a stock message with the current Redis value.
3. Users A and B connected; A buys → A receives {"type":"order", status CONFIRMED or FAILED} within 10 s; B receives no order message.
4. Throttling: 3 clients connected, 500 buys in 2 seconds → each client gets at most 2 stock messages per second (+1 slack), and the last stock message equals Redis.
5. 200 clients connect, then all disconnect → /health still answers and the connection count goes back to 0.
6. Kill one client abruptly (no close frame) during broadcasts → the other clients keep receiving.
```

**Done when:** users see only their own order updates, and 500 buys don't flood clients with messages.

---

## Step 8 — React frontend + Nginx

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 8 of docs/PLAN.md only.

1. frontend/ with React + Vite. Keep it small, no UI library:
   - Login page → POST /api/auth/token/; keep the access token in memory + sessionStorage.
   - Sales list → GET /api/sales/.
   - Sale page: countdown to starts_at, live stock from the WebSocket, a Buy button (disabled when sold out, while a request is in flight, and after a successful buy), and an order status badge RESERVED → CONFIRMED / FAILED driven by WebSocket messages. On page reload, restore the status from /api/orders/mine/.
   - Reconnect the WebSocket with backoff if it drops.
2. nginx/nginx.conf + an nginx service with a multi-stage build (node build → nginx static files):
   - location ~ ^/api/sales/\d+/(buy|stock)$ and /ws → upstream gateway (WebSocket upgrade headers, upstream keepalive).
   - /api/, /admin/, /static/ → web.  / → React build with SPA fallback.
3. Scale the gateway to 2 replicas behind nginx (remove its host port). Serve Django admin static files with collectstatic + whitenoise.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md.
1. npm run build succeeds with no errors or warnings that matter.
2. Write scripts/smoke.py (httpx + websockets) that goes ONLY through http://localhost (nginx): log in, open the WebSocket, buy, and wait for CONFIRMED/FAILED. With the gateway scaled to 2 replicas, run it 20 times — all 20 must receive their order message (this proves pub/sub works across replicas).
3. Prove the routing: /api/sales/ is served by Django and /api/sales/1/buy by the gateway (show headers or logs).
4. /admin/ loads with its CSS through nginx.
5. Give me a manual checklist and walk me through it: two browser windows with two users, stock drops live in both, sold out disables the button, refreshing keeps the order status, and docker compose stop on one gateway replica makes the WebSocket reconnect.
```

**Done when:** you can demo a live flash sale in the browser. This is your interview demo.

---

## Step 9 — Reconciliation + chaos testing

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 9 of docs/PLAN.md only.

1. Celery beat task reconcile (every 60 s) exactly as section 11, each sale under lock:reconcile:{sale_id}:
   - one REPEATABLE READ read of remaining + count of non-FAILED orders → log ERROR if the invariant fails;
   - Redis stock > DB remaining → lower Redis with a Lua script that can only lower, never raise. Redis < DB → log WARNING only;
   - re-enqueue fulfil_order for orders PENDING for more than 2 minutes.
2. Django admin action "Resync stock" (only for a paused or ended sale): set Redis stock = DB remaining.
3. Docker hardening: restart: unless-stopped on app services, depends_on with condition: service_healthy, healthchecks for web and gateway.
4. Structured JSON logs including order_id in gateway, order-writer and celery.
5. scripts/load.py: async buy load at a fixed rate (e.g. 300 req/s for 90 s) with unique users. It prints how many 202/409/503 responses it got.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md. Write unit tests for reconcile, then run a chaos test and show the results.
Unit tests:
1. Redis stock set higher than DB remaining → reconcile lowers it. Redis lower than DB → unchanged and a WARNING logged.
2. An order PENDING for 3 minutes (fake created_at) → reconcile re-enqueues it and it ends CONFIRMED/FAILED.
3. Two reconcile runs at the same time → only one does the work (lock).
Chaos test (stock 30,000; scripts/load.py at 300 req/s for 90 s). While it runs:
- at 20 s: docker compose restart order-writer
- at 40 s: docker compose kill celery-worker, start it again at 50 s
- at 60 s: docker compose restart redis
After the load, wait up to 3 minutes, then prove:
- check_invariants passes; no PENDING orders left; no duplicate orders; no payments:* counter above 1;
- orders in the DB == number of 202s the load script counted (print both);
- Redis stock <= DB remaining (safe direction) — print the difference.
Put the whole chaos run in scripts/chaos_test.ps1 so I can demo it.
```

**Done when:** the invariant holds after the chaos run. This is your strongest interview story.

---

## Step 10 — k6 load test, tuning, README

**Build prompt**
```text
Read docs/ARCHITECTURE.md. Implement Step 10 of docs/PLAN.md only.

1. manage.py seed_load_users --count 500000: bulk_create users in batches of 5,000 with an unusable password (no hashing, so it's fast), usernames load_1..N. Print the id range.
2. loadtest/buy.js (k6): sign HS256 JWTs inside k6 with k6/crypto and JWT_SIGNING_KEY (no huge token file). Every iteration uses a unique user id from the seeded range.
   - Scenario "throughput": constant-arrival-rate 5,000/s for 60 s against a sale with 1,000,000 stock, so every request is a real write. Set preAllocatedVUs/maxVUs high enough.
   - Scenario "hot_item": 5,000/s for 20 s against a sale with 1,000 stock, using a different user range.
   - Thresholds: http_req_duration{scenario:throughput} p(99)<50, http_req_failed<1%.
   - Run k6 with the grafana/k6 image on the compose network, hitting nginx.
3. Tuning — explain each change: uvicorn workers per gateway replica = CPU cores, replica count, orjson responses, access logs off, one Redis pool per process, aiokafka batching (linger_ms), nginx upstream keepalive + worker_connections, order-writer batch size, Celery --pool=threads --concurrency=32 with MOCK_PAYMENT_DELAY_MS=5 during load tests.
4. README.md: architecture diagram (Mermaid), how to run, how to run every test, and a Results table for the REAL measured numbers.
```

**Test prompt**
```text
Follow the Testing rules at the top of docs/PLAN.md.
1. Run the "throughput" scenario. Show the real k6 summary: achieved requests/s, dropped_iterations, p50/p95/p99 latency, error rate. Capture docker stats every 10 s while it runs.
2. After it ends, wait until the order-writer consumer lag is 0 (show it), then: orders in DB == number of 202s from k6, check_invariants passes, Redis stock == DB remaining. Also report how long Celery took to drain all fulfilments.
3. Run "hot_item": exactly 1,000 non-FAILED orders, zero oversell, Redis stock 0, DB remaining 0.
4. If we're below 5,000 RPS or above 50 ms p99, don't fake it. Find the bottleneck with docker stats (which container is at 100% CPU), change ONE thing at a time, re-measure, and show before/after numbers.
5. Fill the README Results table with the final real numbers, my machine specs (CPU, RAM, Docker Desktop resource limits) and today's date.
If my machine can't reach 5,000 RPS, tell me the highest number we reliably got and why, so I can explain it honestly in the interview.
```

**Done when:** the README has real, reproducible numbers and zero oversell under load.
