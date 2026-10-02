"""Open-loop buy load: RATE requests/s for DURATION seconds, every request from a different user.

Requests are sent on a fixed schedule whether or not earlier ones have finished (like real
traffic), and JWTs are signed locally for user ids first_user_id .. first_user_id + N - 1
(create them first with `manage.py seed_load_users`).

    python scripts/load.py --sale-id 7 --rate 300 --duration 90 --first-user-id 1234 --out result.json
"""
import argparse
import asyncio
import json
import os
import time
from collections import Counter
from pathlib import Path

import aiohttp  # ~30x faster than httpx's async client on Windows hosts
import jwt


def read_key() -> str:
    if os.environ.get("JWT_SIGNING_KEY"):
        return os.environ["JWT_SIGNING_KEY"]
    env = Path(__file__).resolve().parent.parent / ".env"
    for line in env.read_text().splitlines():
        if line.startswith("JWT_SIGNING_KEY="):
            return line.split("=", 1)[1].strip()
    raise SystemExit("JWT_SIGNING_KEY not found")


def token_for(user_id: int, key: str) -> str:
    now = int(time.time())
    return jwt.encode({"user_id": user_id, "token_type": "access", "iat": now, "exp": now + 3600}, key, algorithm="HS256")


async def run(args) -> dict:
    key = args.key or read_key()
    total = int(args.rate * args.duration)
    statuses: Counter = Counter()
    latencies: list[float] = []
    connector = aiohttp.TCPConnector(limit=args.concurrency)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(base_url=args.base, connector=connector, timeout=timeout) as session:
        async def one(i: int):
            headers = {"Authorization": f"Bearer {token_for(args.first_user_id + i, key)}"}
            started = time.perf_counter()
            try:
                async with session.post(f"/api/sales/{args.sale_id}/buy", headers=headers) as resp:
                    await resp.read()
                    statuses[str(resp.status)] += 1
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                statuses[f"error:{type(exc).__name__}"] += 1
            latencies.append((time.perf_counter() - started) * 1000)

        tasks = []
        sent = 0
        next_report = args.rate * 10
        start = time.monotonic()
        while sent < total:
            # Launch every request that is due by now, then nap. Sleeping before each request
            # would cap the rate at ~60/s on Windows, where timers tick every ~15 ms.
            due = min(total, int((time.monotonic() - start) * args.rate) + 1)
            while sent < due:
                tasks.append(asyncio.create_task(one(sent)))
                sent += 1
            if sent >= next_report:
                print(f"  {sent / args.rate:.0f}s: sent {sent}, so far {dict(statuses)}", flush=True)
                next_report += args.rate * 10
            await asyncio.sleep(0.005)
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start

    latencies.sort()
    pct = lambda p: round(latencies[min(len(latencies) - 1, int(len(latencies) * p / 100))], 2) if latencies else None
    errors = sum(n for code, n in statuses.items() if code == "503" or code.startswith("error"))
    return {
        "sale_id": args.sale_id,
        "requested": total,
        "elapsed_s": round(elapsed, 1),
        "achieved_rps": round(total / elapsed, 1),
        "status_counts": dict(statuses),
        "accepted": statuses.get("202", 0),
        "errors": errors,
        "latency_ms": {"p50": pct(50), "p95": pct(95), "p99": pct(99)},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost")
    parser.add_argument("--sale-id", type=int, required=True)
    parser.add_argument("--rate", type=int, default=300)
    parser.add_argument("--duration", type=float, default=90)
    parser.add_argument("--first-user-id", type=int, required=True)
    parser.add_argument("--concurrency", type=int, default=1000)
    parser.add_argument("--key", default=None)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    summary = asyncio.run(run(args))
    print(json.dumps(summary, indent=2))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(summary, indent=2))
