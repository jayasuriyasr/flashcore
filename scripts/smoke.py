"""Full user journey THROUGH NGINX: routing checks, then per user log in → open the
WebSocket → buy → wait for the CONFIRMED/FAILED push.

With the gateway scaled to 2 replicas the buy and the WebSocket often land on different
replicas, so every user getting their update proves the Redis pub/sub fan-out works.

    powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1 -SaleId 1 -Users 20
"""
import argparse
import asyncio
import json
import sys
import time

import httpx
import websockets


def check_routing(base: str, sale_id: int) -> list[str]:
    problems = []
    with httpx.Client(base_url=base, timeout=15) as c:
        r = c.get("/api/sales/")
        if r.status_code != 200 or r.headers.get("x-upstream") != "web":
            problems.append(f"/api/sales/ -> {r.status_code}, upstream={r.headers.get('x-upstream')} (want 200 web)")
        r = c.post(f"/api/sales/{sale_id}/buy")
        if r.status_code != 401 or r.headers.get("x-upstream") != "gateway":
            problems.append(f"/api/sales/{sale_id}/buy -> {r.status_code}, upstream={r.headers.get('x-upstream')} (want 401 gateway)")
        r = c.get("/admin/login/")
        if r.status_code != 200:
            problems.append(f"/admin/login/ -> {r.status_code}")
        r = c.get("/static/admin/css/base.css")
        if r.status_code != 200 or "css" not in r.headers.get("content-type", ""):
            problems.append(f"/static/admin/css/base.css -> {r.status_code} {r.headers.get('content-type')}")
        r = c.get("/")
        if r.status_code != 200 or "<div id=\"root\">" not in r.text:
            problems.append(f"/ -> {r.status_code} (React index.html not served)")
    return problems


async def journey(base: str, sale_id: int, username: str, password: str, timeout: float) -> str:
    ws_base = base.replace("http", "ws", 1)
    async with httpx.AsyncClient(base_url=base, timeout=15) as c:
        r = await c.post("/api/auth/token/", json={"username": username, "password": password})
        if r.status_code != 200:
            return f"login -> {r.status_code}"
        token = r.json()["access"]
        async with websockets.connect(f"{ws_base}/ws?token={token}&sale_id={sale_id}", open_timeout=10) as ws:
            first = json.loads(await asyncio.wait_for(ws.recv(), 10))
            if first.get("type") != "stock":
                return f"first message was {first}"
            r = await c.post(f"/api/sales/{sale_id}/buy", headers={"Authorization": f"Bearer {token}"})
            if r.status_code != 202:
                return f"buy -> {r.status_code} {r.text}"
            order_id = r.json()["order_id"]
            deadline = time.monotonic() + timeout
            try:
                while (left := deadline - time.monotonic()) > 0:
                    msg = json.loads(await asyncio.wait_for(ws.recv(), left))
                    if msg.get("type") == "order" and msg.get("order_id") == order_id:
                        return msg["status"]
            except asyncio.TimeoutError:
                pass
            return "timeout (no order update)"


async def main_async(args) -> int:
    problems = check_routing(args.base, args.sale_id)
    for p in problems:
        print(f"[FAIL] routing: {p}")
    if not problems:
        print("[PASS] routing: /api/sales/ -> web, /buy -> gateway, admin + static + React served")

    users = [f"{args.user_prefix}{i}" for i in range(1, args.users + 1)]
    results = await asyncio.gather(*(journey(args.base, args.sale_id, u, args.password, args.timeout) for u in users))
    ok = 0
    for user, result in zip(users, results):
        good = result in ("CONFIRMED", "FAILED")
        ok += good
        print(f"[{'PASS' if good else 'FAIL'}] {user}: {result}")
    print(f"\n{ok}/{len(users)} users received their order update over the WebSocket")
    return 0 if ok == len(users) and not problems else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://localhost")
    parser.add_argument("--sale-id", type=int, required=True)
    parser.add_argument("--users", type=int, default=20)
    parser.add_argument("--user-prefix", default="demo")
    parser.add_argument("--password", default="demo12345")
    parser.add_argument("--timeout", type=float, default=30)
    sys.exit(asyncio.run(main_async(parser.parse_args())))
