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
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8001 --workers ${GATEWAY_WORKERS:-2} --loop uvloop --http httptools --no-access-log"]
