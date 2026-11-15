FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dev dependencies are included on purpose: its test runs inside this container.
# constraints.txt pins every package (direct and transitive) to the tested versions: without it a
# rebuild picked up Celery 5.7, which needs redis-py 6, and the worker crash-looped.
COPY requirements.txt requirements-dev.txt constraints.txt ./
RUN pip install -r requirements-dev.txt -c constraints.txt

COPY . .

EXPOSE 8002
# Same server settings as the real gateway, so the comparison is about the design, not the server.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port 8002 --workers ${BASELINE_WORKERS:-6} --loop uvloop --http httptools --no-access-log --timeout-keep-alive 75"]
