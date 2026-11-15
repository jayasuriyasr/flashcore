# Common tasks for Linux, macOS, WSL and Git Bash (Windows PowerShell commands: see README).
.PHONY: up down test smoke ci ci-chaos demo logs baseline observability

up:            ## build and start the stack
	docker compose up -d --build
down:          ## stop it (data kept; `docker compose down -v` wipes it)
	docker compose down
test:          ## Django + gateway test suites (stack must be running)
	docker compose exec -T web pytest -q
	docker compose exec -T gateway pytest -q
smoke:         ## 200 buys end to end, then the invariant check
	docker compose exec -T web python manage.py smoke_test --buys 200
ci:            ## what CI runs: build, start, migrations check, tests, smoke
	bash scripts/ci.sh
ci-chaos:      ## the same plus the chaos tests that stop Kafka and restart Redis
	CHAOS=1 bash scripts/ci.sh
demo:          ## a live sale and users demo1..demo20 (password demo12345)
	docker compose exec -T web python manage.py create_demo --stock 100 --users 20
logs:          ## follow the hot path logs
	docker compose logs -f gateway order-writer celery-worker
baseline:      ## start the PostgreSQL-only comparison service behind nginx at /baseline/ (docs/SCALING.md)
	docker compose -f docker-compose.yml -f docker-compose.baseline.yml up -d --build
observability: ## Prometheus http://localhost:9090 and Grafana http://localhost:3000
	docker compose --profile observability up -d
