# Trade-offs, failure modes and scaling

Questions this design has to answer, with measured numbers where there are any.

## 1. Why not just PostgreSQL?

The obvious design is one transaction per buy:

```sql
INSERT INTO orders (...) ON CONFLICT DO NOTHING;                          -- one live order per user
UPDATE flash_sales SET remaining = remaining - 1 WHERE id = :sale AND remaining > 0;
COMMIT;
```

It is **correct**. A second buyer's `UPDATE` waits for the first one's row lock, then re-checks
`remaining > 0` against the committed row, so it can't oversell. It is built as a real service
([baseline/](../baseline/app/main.py)) with the same JWT checks, the same server settings (uvicorn,
6 processes) and the same nginx in front, and it has its own correctness test: 200 concurrent buyers,
50 units, exactly 50 sold.

Measured 2026-11-15 with the same k6 script, on the same laptop, in the same session
(`loadtest/run_k6.ps1 -Target baseline`). One sale with 1,000,000 units, every request a new buyer:

| Design | 1,000 buys/s offered for 30 s | Most it could do (5,000/s offered) |
|---|---|---|
| **FlashCore**, 1 Kafka broker | **1,000/s**, p50 14.6 ms, **p99 45.5 ms**, 0 dropped | **2,303/s** |
| **FlashCore**, 3 Kafka brokers (replication 3, `min.insync.replicas=2`) | **999/s**, p50 18.6 ms, p99 94.7 ms, 0 dropped | **1,987/s** |
| PostgreSQL only, durable commits (default) | 377/s, p50 3.5 s, p99 22.7 s, 52% of requests never started | 363/s |
| PostgreSQL only, `synchronous_commit=off` | 800/s, p50 1.4 s, p99 6.3 s, 13% never started | 856/s |

Every run of every design kept the guarantees: no oversell, no duplicate order per user, all
accepted orders stored.

**Is the comparison fair on durability?** Partly, and the table shows both sides. PostgreSQL flushes
every commit to disk. A single Kafka broker with `acks=all` does not flush per message: Kafka's
durability comes from replication. So the like-for-like pairs are:

- *survives a machine crash:* PostgreSQL durable (377/s) vs FlashCore on 3 brokers (1,987/s): **5×**;
- *can lose the last moments on a crash:* PostgreSQL `synchronous_commit=off` (856/s) vs FlashCore on
  1 broker (2,303/s): **2.7×**.

The laptop also runs k6 and every other service, so the absolute numbers are low; the ratio is the point.

**Why the difference:** every buyer of a sale updates the *same row*. PostgreSQL serialises them on
that row's lock, and each buyer holds the lock from its `UPDATE` until its commit returns (including
the disk flush, unless that is turned off). Throughput for one sale is therefore capped at about
1 / (lock hold time), however many cores or connections you add: 377/s is one buyer every 2.7 ms.
Buyers arriving faster than that queue up, which is where the multi-second latencies come from. FlashCore takes that row out of the hot path in three steps:

1. **Redis** decides who gets a unit with one atomic in-memory operation (`reserve.lua`, microseconds).
2. **Kafka** makes the "yes" durable with a sequential, batched append (`linger.ms=15`, `acks=all`).
3. The **order writer** updates the hot row once per *batch* of up to 500 orders, not once per buyer.

**When the baseline is the right answer:** for sales up to a few hundred buys per second, or when
operating three more systems isn't worth it. It has fewer moving parts, no Redis/DB reconciliation,
and the answer is final when the request returns. Choosing it for a small shop would be good
engineering, not a shortcut.

**If I had to cut FlashCore in half:**
- **Kafka goes first.** Replace it with a Redis Stream, since Redis is already there. The price is
  durability: AOF `everysec` can lose about 1 s of accepted buys on a Redis crash, while Kafka with
  `acks=all` and 3 replicas loses none (the broker-kill test in section 2b).
- **Celery goes second.** The order writer could charge payments itself. The price is that slow
  payment calls would then block order writes.

## 2. Failure modes

| What fails | What buyers see | Data | Recovery | Proven by |
|---|---|---|---|---|
| Kafka (single broker) | 503 `try_again` after 2 s. The unit stays reserved while the Kafka client keeps trying (30 s), then goes back on sale; if Kafka returns sooner, the order goes through | nothing lost, and no order for a unit that was given back | automatic when Kafka returns | `test_step4_chaos_kafka_down` (both paths) |
| **One of 3 Kafka brokers** (HA mode) | a slower "yes" for a few seconds while leaders move (measured: 99.76% of buys accepted, p99 7.6 s during the kills) | **no acknowledged order lost** (`acks=all`, `min.insync.replicas=2`) | broker rejoins and catches up | `chaos_test.ps1 -Scenario kafka-ha` |
| Redis | 503 `try_again`, never an oversell | AOF `everysec`: up to about 1 s of reservations; reconcile lowers Redis if it ends up above the DB | automatic restart | `test_step9_chaos_redis_down`, chaos run |
| PostgreSQL | **buys keep working**: the gateway never touches PostgreSQL (logins and the read APIs fail until it is back) | events wait in Kafka; the order writer exits without committing offsets and retries | written when PostgreSQL returns | `chaos_test.ps1 -Scenario postgres-down` |
| Order writer | buys keep working; orders appear later | Kafka keeps the events (offsets committed only after the DB commit) | restart policy | chaos run (restart mid-load) |
| Celery worker killed mid-payment | order stays "confirming" a little longer | task redelivered (`acks_late`); payment idempotency key = order id, so no double charge | another worker, or reconcile after 2 min | chaos run (`kill -9`) |
| A gateway replica | nginx retries the other one | none (stateless) | restart policy | nginx `max_fails` |
| Host reboot | site down while it boots | everything on volumes | every service `restart: unless-stopped` | crash test of each data store |
| The whole host / region | site down | volumes on that host | not covered: section 4 | – |

### 2b. Measured (2026-11-15, 300 buys/s for 90 s = 27,000 buyers, failures injected mid-run)

| Failure injected | Accepted (202) | 503 | Orders stored | Lost / duplicated / double-charged |
|---|---|---|---|---|
| PostgreSQL stopped for 30 s | 27,000 | 0 | 27,000 | 0 / 0 / 0 |
| Order writer restarted, Celery `kill -9`, Redis restarted | 26,014 | 902 ¹ | 26,014 | 0 / 0 / 0 |
| 2 of 3 Kafka brokers `kill -9`'d, one after the other (HA mode) | 26,934 | 66 | 27,000 | 0 / 0 / 0 |

¹ All during the seconds Redis was reloading its data; nothing was reserved for those requests.

In the Kafka row, the 66 buyers who got a 503 kept their reservation and their orders were written
once the cluster had re-elected leaders (27,000 stored = 26,934 + 66); a retry tells them
"already purchased".

**What the first broker-kill run found.** Before the fix above, the same test gave 6,252 × 503 and
**4,608 orders for buyers who had been told "try again"**. When a replica dies, `acks=all` writes
wait until the cluster drops it from the in-sync set (9 s by default). The gateway gave up after 2 s
and returned the unit to stock, but those events were already at the broker and were committed a few
seconds later. The database guards kept stock correct (conditional `UPDATE`, unique index, reconcile),
but an order appeared for someone who was told it failed, for a unit already back on sale. Three changes:

1. The request's wait (2 s) and the Kafka client's delivery window (`message.timeout.ms`, now 30 s)
   are separate. A 503 on timeout **keeps the reservation** until the client reports the real
   outcome: late ack → the order proceeds; gave up → the unit is released then.
2. In HA mode the gateway waits up to 8 s, so a failover is a slow "yes" instead of a "try again".
3. In HA mode a dead broker is detected in 4 s instead of 9 s.

What remains: a message still in flight when the 30 s window ends can be reported as failed and yet
be written. It needs a stall longer than 30 s followed by a recovery; the database guards cover it.
The complete fix is an idempotency key supplied by the client, so a retry can be matched to the first
attempt.

## 3. Scaling path: what breaks first, and the next step

1. **This laptop's CPU.** Measured: the gateways, k6, nginx and the order writer share 6 cores and
   saturate at about 2,300 buys/s. Next step: separate hosts, and load from another machine.
2. **Gateway.** Stateless, so scale it horizontally behind any load balancer. Each buy costs one
   Redis round trip and one Kafka produce.
3. **Redis, one key per sale.** A single Redis core does on the order of 100k simple Lua calls per
   second, so one sale is bounded by one shard.
   - *Many concurrent sales:* Redis Cluster, sharded by `sale_id`.
   - *One enormous sale:* split its stock into N buckets (`sale:{id}:stock:{k}`) and pick the
     bucket by user hash. That gives N times the throughput, at the cost of a few units undersold
     at the very end until the buckets are rebalanced.
4. **Kafka.** Partitions are the unit of parallelism, and the key is `sale_id`, so one sale uses one
   partition and one order-writer thread. For a mega-sale, key by `order_id` instead. Correctness
   doesn't depend on ordering, because the DB constraints and the conditional `UPDATE` are
   order-independent.
5. **PostgreSQL.** Writes are already batched, so the hot row is touched once per batch. Next:
   partition `orders` by sale, add read replicas for the read APIs, and use managed HA (RDS Multi-AZ
   or Patroni).
6. **Celery.** Add workers. The real limit becomes the payment provider's rate limit, so queue and
   retry against it.
7. **WebSockets.** Every gateway receives every `user:*` message and keeps its own. At large scale,
   shard users across channels, or move pushes to a dedicated service.

## 4. Multi-region

Stock is one global number, and a strongly consistent decrement needs a single writer. Two options:

- **Home region per sale.** Every buy for a sale goes to the region that owns it. This is simple and
  exact, but far-away users pay the round trip.
- **Stock quotas per region.** For example, 60% sold from India and 40% from the US, each region
  selling its quota locally with the design above and no cross-region coordination. One region can
  sell out while another still has stock, so leftovers are rebalanced periodically.

Order ids are UUIDs and payment idempotency keys are order ids, so orders and payments merge across
regions without collisions.

## 5. Deliberately simplified

- **Payments:** a mock provider with Stripe's idempotency-key semantics.
- **Single PostgreSQL and single Redis:** production would use managed HA, or Redis
  Sentinel/Cluster. Kafka HA is implemented (`docker-compose.ha.yml`); the default stays
  single-broker to fit on a laptop.
- **JWT:** HS256 with a shared secret. In production, RS256 with a JWKS endpoint, so the gateway
  holds only the public key.
- **One host:** the 2 gateway replicas are separate processes on one machine, not separate machines.
