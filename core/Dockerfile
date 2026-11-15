FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dev dependencies are included on purpose: tests run inside this container.
# constraints.txt pins every package (direct and transitive) to the tested versions: without it a
# rebuild picked up Celery 5.7, which needs redis-py 6, and the worker crash-looped.
COPY requirements.txt requirements-dev.txt constraints.txt ./
RUN pip install -r requirements-dev.txt -c constraints.txt

COPY . .

EXPOSE 8000
CMD ["gunicorn", "core.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3"]
