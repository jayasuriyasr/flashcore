#!/usr/bin/env bash
# The automated test suite, exactly as CI runs it (.github/workflows/ci.yml).
# Works on Linux, macOS and Git Bash on Windows.
#
#   bash scripts/ci.sh             # build, start, migrations check, Django + gateway tests, smoke
#   CHAOS=1 bash scripts/ci.sh     # also the chaos tests that stop Kafka / restart Redis (needs Python 3.12)
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n== %s\n' "$*"; }

[ -f .env ] || cp .env.example .env

step "Build and start the stack"
docker compose up -d --build
status=""
for _ in $(seq 1 120); do
  # nginx only turns healthy after Django and both gateway replicas are healthy.
  status=$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q nginx)" 2>/dev/null || true)
  [ "$status" = healthy ] && break
  sleep 2
done
if [ "$status" != healthy ]; then
  docker compose ps -a
  docker compose logs --tail 80
  exit 1
fi

step "Migrations match the models"
docker compose exec -T web python manage.py makemigrations --check --dry-run

step "Django tests (models, constraints, order writer, REPEATABLE READ, Celery, locks, reconcile)"
docker compose exec -T web pytest -q

step "Gateway tests (atomic reserve, 1000-vs-100 oversell, Kafka events, WebSocket, CORS)"
docker compose exec -T gateway pytest -q

step "End to end: 200 buys -> stored, paid, invariant holds"
docker compose exec -T web python manage.py smoke_test --buys 200

if [ "${CHAOS:-0}" = 1 ]; then
  step "Chaos tests: Kafka stopped, Redis restarted (run from the host)"
  python="${PYTHON:-python3}"
  "$python" -m venv .venv-ci
  venv_python=.venv-ci/bin/python
  [ -x "$venv_python" ] || venv_python=.venv-ci/Scripts/python.exe   # Git Bash on Windows
  "$venv_python" -m pip install -q -r scripts/requirements.txt -c scripts/constraints.txt
  export GATEWAY_URL=http://localhost TEST_REDIS_URL=redis://localhost:6379/0 TEST_KAFKA_BOOTSTRAP=localhost:9094
  JWT_SIGNING_KEY=$(sed -n 's/^JWT_SIGNING_KEY=//p' .env)
  export JWT_SIGNING_KEY
  (cd gateway && "../$venv_python" -m pytest -v -m chaos tests)
fi

step "All checks passed"
