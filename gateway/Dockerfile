FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dev dependencies are included on purpose: tests run inside this container.
COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

COPY . .

EXPOSE 8001
# One uvicorn process per CPU you give it; each process has its own Redis pool,
# Kafka producer, WebSocket manager and stock ticker.
# --timeout-keep-alive must be LONGER than nginx's upstream keepalive_timeout (60 s): with
# uvicorn's default 5 s, nginx sometimes reused a pooled connection just as uvicorn closed it
# ("connection reset by peer"), and a burst of those made nginx mark the replica down.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8001 --workers ${GATEWAY_WORKERS:-2} --loop uvloop --http httptools --no-access-log --timeout-keep-alive 75"]
