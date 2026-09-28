# FlashCore — Distributed Flash Sale Engine

Django · FastAPI · React · PostgreSQL · Redis · Kafka · Celery · WebSockets · Docker

- Design: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- Build plan: [docs/PLAN.md](docs/PLAN.md)

## Requirements

- Docker Desktop (Windows / macOS) or Docker Engine + Compose v2
- PowerShell (for the helper scripts in `scripts/`)

## Start

```powershell
Copy-Item .env.example .env        # first time only
docker compose up -d               # starts everything; kafka-init creates the topic and exits
docker compose ps                  # postgres, redis, kafka should be "healthy"
```

## Check the infrastructure

```powershell
powershell -ExecutionPolicy Bypass -File scripts/check_infra.ps1                     # quick checks
powershell -ExecutionPolicy Bypass -File scripts/check_infra.ps1 -CheckPersistence   # also restarts the stack to prove volumes persist
```

## Stop

```powershell
docker compose down        # keeps data volumes
docker compose down -v     # also deletes all data
```

## Ports (dev)

| Service | Host port |
|---|---|
| PostgreSQL | 5432 |
| Redis | 6379 |
| Kafka (external listener) | 9094 |
