"""CORS for the React app on Vercel (docs/DEPLOY.md).

The middleware is added at import time from CORS_ALLOWED_ORIGINS, so the app is imported in a fresh
interpreter with the variable set. Preflights are answered by the middleware, so no Redis/Kafka
(no lifespan) is needed.
"""
import os
import subprocess
import sys
import textwrap
from pathlib import Path

FRONTEND = "https://flashcore.vercel.app"

SCRIPT = textwrap.dedent(
    """
    import asyncio, httpx
    from app.main import app

    async def preflight(client, origin):
        res = await client.options("/api/sales/1/buy", headers={
            "Origin": origin,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "authorization,content-type",
        })
        return f"RESULT {res.status_code} {res.headers.get('access-control-allow-origin')}"

    async def main():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as client:
            print(await preflight(client, "%s"))
            print(await preflight(client, "https://evil.example"))

    asyncio.run(main())
    """
    % FRONTEND
)


def run(cors):
    env = {**os.environ, "CORS_ALLOWED_ORIGINS": cors}
    out = subprocess.run(
        [sys.executable, "-c", SCRIPT], env=env, cwd=Path(__file__).parents[1],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    # stdout also carries the app's JSON log lines
    return [line.removeprefix("RESULT ") for line in out.stdout.splitlines() if line.startswith("RESULT ")]


def test_buy_preflight_allowed_only_for_the_frontend_origin():
    assert run(FRONTEND) == [f"200 {FRONTEND}", "400 None"]


def test_no_cors_when_not_configured():
    # Local Docker: same origin, no middleware, so a preflight is just an unknown OPTIONS route.
    allowed, other = run("")
    assert allowed == other == "405 None"
