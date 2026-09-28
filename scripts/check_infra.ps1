# Verifies the Step 1 infrastructure: Postgres, Redis, Kafka + the order-events topic.
#   powershell -ExecutionPolicy Bypass -File scripts/check_infra.ps1 [-CheckPersistence]
param([switch]$CheckPersistence)

$ErrorActionPreference = 'Continue'
Set-Location (Split-Path $PSScriptRoot -Parent)
$script:failures = 0

function Pass([string]$m) { Write-Host "[PASS] $m" -ForegroundColor Green }
function Fail([string]$m) { Write-Host "[FAIL] $m" -ForegroundColor Red; $script:failures++ }

function Wait-Healthy([string[]]$services, [int]$timeoutSec = 180) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $notReady = @()
        foreach ($s in $services) {
            $id = docker compose ps -q $s
            if (-not $id) { $notReady += $s; continue }
            $h = docker inspect -f '{{.State.Health.Status}}' $id
            if ($h -ne 'healthy') { $notReady += $s }
        }
        if ($notReady.Count -eq 0) { return $true }
        Start-Sleep -Seconds 3
    }
    Write-Host "Not healthy in time: $($notReady -join ', ')"
    return $false
}

$infra = @('postgres', 'redis', 'kafka')
$kt = '/opt/kafka/bin'

Write-Host "== Starting infrastructure ==" -ForegroundColor Cyan
docker compose up -d @infra | Out-Null
if (Wait-Healthy $infra) { Pass "postgres, redis, kafka are healthy" } else { Fail "services did not become healthy" }
docker compose ps

Write-Host "`n== Topic creation (kafka-init) ==" -ForegroundColor Cyan
docker compose run --rm kafka-init | Out-Null
if ($LASTEXITCODE -eq 0) { Pass "kafka-init run #1 succeeded" } else { Fail "kafka-init run #1 failed" }
docker compose run --rm kafka-init | Out-Null
if ($LASTEXITCODE -eq 0) { Pass "kafka-init run #2 succeeded (idempotent)" } else { Fail "kafka-init run #2 failed" }

Write-Host "`n== Postgres ==" -ForegroundColor Cyan
$ver = docker compose exec -T postgres psql -U flashcore -d flashcore -tAc "SELECT version();"
if ($ver -match '^PostgreSQL 16') { Pass "Postgres: $($ver.Substring(0, 30))..." } else { Fail "Postgres version query returned: $ver" }

Write-Host "`n== Redis ==" -ForegroundColor Cyan
$pong = docker compose exec -T redis redis-cli ping
if ($pong -eq 'PONG') { Pass "Redis PING -> PONG" } else { Fail "Redis PING returned: $pong" }
$aof = docker compose exec -T redis redis-cli config get appendonly
if ($aof[1] -eq 'yes') { Pass "Redis appendonly = yes" } else { Fail "Redis appendonly = $($aof -join ' ')" }

Write-Host "`n== Kafka ==" -ForegroundColor Cyan
$desc = docker compose exec -T kafka "$kt/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic order-events
$desc | ForEach-Object { Write-Host "  $_" }
$parts = @($desc | Where-Object { $_ -match 'Partition:\s*\d' }).Count
if ($parts -eq 3) { Pass "order-events has 3 partitions" } else { Fail "order-events has $parts partitions (expected 3)" }

# Round-trip on a throwaway topic so order-events never gets junk messages.
$msg = "infra-check-$([guid]::NewGuid().ToString('N').Substring(0, 8))"
docker compose exec -T kafka "$kt/kafka-topics.sh" --bootstrap-server localhost:9092 --create --if-not-exists --topic infra-check --partitions 1 --replication-factor 1 | Out-Null
$msg | docker compose exec -T kafka "$kt/kafka-console-producer.sh" --bootstrap-server localhost:9092 --topic infra-check
$got = docker compose exec -T kafka "$kt/kafka-console-consumer.sh" --bootstrap-server localhost:9092 --topic infra-check --from-beginning --max-messages 1 --timeout-ms 20000 2>$null
docker compose exec -T kafka "$kt/kafka-topics.sh" --bootstrap-server localhost:9092 --delete --topic infra-check | Out-Null
if ($got -contains $msg) { Pass "Kafka produce/consume round-trip ($msg)" } else { Fail "Kafka round-trip: sent '$msg', got '$got'" }

$tcp = Test-NetConnection -ComputerName localhost -Port 9094 -WarningAction SilentlyContinue
if ($tcp.TcpTestSucceeded) { Pass "Kafka EXTERNAL listener reachable on localhost:9094" } else { Fail "localhost:9094 not reachable" }

if ($CheckPersistence) {
    Write-Host "`n== Volume persistence (down -> up) ==" -ForegroundColor Cyan
    docker compose exec -T postgres psql -U flashcore -d flashcore -qc "DROP TABLE IF EXISTS infra_check; CREATE TABLE infra_check (v int); INSERT INTO infra_check VALUES (42);" | Out-Null
    docker compose exec -T redis redis-cli set infra:check 42 | Out-Null
    docker compose exec -T redis redis-cli save | Out-Null   # force a snapshot on top of the AOF
    docker compose down | Out-Null
    docker compose up -d @infra | Out-Null
    if (-not (Wait-Healthy $infra)) { Fail "services did not come back healthy" }
    $pgv = docker compose exec -T postgres psql -U flashcore -d flashcore -tAc "SELECT v FROM infra_check;"
    $rv = docker compose exec -T redis redis-cli get infra:check
    if ("$pgv".Trim() -eq '42') { Pass "Postgres data survived down/up" } else { Fail "Postgres data lost (got '$pgv')" }
    if ("$rv".Trim() -eq '42') { Pass "Redis data survived down/up" } else { Fail "Redis data lost (got '$rv')" }
    docker compose exec -T postgres psql -U flashcore -d flashcore -qc "DROP TABLE infra_check;" | Out-Null
    docker compose exec -T redis redis-cli del infra:check | Out-Null
    $still = @(docker compose exec -T kafka "$kt/kafka-topics.sh" --bootstrap-server localhost:9092 --list) -contains 'order-events'
    if ($still) { Pass "Kafka topic order-events survived down/up" } else { Fail "Kafka topic lost after down/up" }
}

Write-Host ""
if ($script:failures -eq 0) { Write-Host "ALL CHECKS PASSED" -ForegroundColor Green; exit 0 }
Write-Host "$($script:failures) CHECK(S) FAILED" -ForegroundColor Red; exit 1
