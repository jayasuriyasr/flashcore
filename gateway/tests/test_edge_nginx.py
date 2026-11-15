"""nginx in front of everything: per-IP login rate limiting (nginx/nginx.conf, zone "login").

Runs in the gateway container and reaches nginx over the compose network, like any client would.
"""
import asyncio

import httpx

NGINX = "http://nginx"


async def burst_of_logins(n: int) -> list[int]:
    async with httpx.AsyncClient(base_url=NGINX, timeout=60) as client:
        responses = await asyncio.gather(*(
            client.post("/api/auth/token/", json={"username": "nobody", "password": "wrong-password"})
            for _ in range(n)
        ))
    return [r.status_code for r in responses]


def test_password_guessing_is_rate_limited():
    # 5/s with a burst of 30: 60 attempts at once -> about 30 reach Django (401), the rest get 429
    # immediately, without costing a password hash each.
    codes = asyncio.run(burst_of_logins(60))
    assert set(codes) <= {401, 429}
    assert 25 <= codes.count(401) <= 35
    assert codes.count(429) >= 25
