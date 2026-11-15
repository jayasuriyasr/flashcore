# Chaos test: steady buy load while services are restarted/killed, then prove nothing was lost,
# duplicated, oversold or double-charged.
#   powershell -ExecutionPolicy Bypass -File scripts/chaos_test.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/chaos_test.ps1 -Scenario kafka-ha
#     kills Kafka brokers instead; needs the replicated cluster:
#     docker compose -f docker-compose.yml -f docker-compose.ha.yml up -d
#   powershell -ExecutionPolicy Bypass -File scripts/chaos_test.ps1 -Scenario postgres-down
#     stops PostgreSQL for 30 s mid-load: buys must keep being accepted (the gateway never
#     touches PostgreSQL) and every order must be written once it is back.
# Needs the full stack running (docker compose up -d) and Python 3.12 on PATH.
param(
    [ValidateSet("services", "kafka-ha", "postgres-down")][string]$Scenario = "services",
    [int]$Rate = 300,
    [int]$Duration = 90,
    [int]$Stock = 30000
)
$ErrorActionPreference = 'Continue'   # native tools write warnings to stderr; we check exit codes
Set-Location (Split-Path $PSScriptRoot -Parent)
$root = (Get-Location).Path
# The broker services only exist with the HA file.
$dc = if ($Scenario -eq "kafka-ha") { @("-f", "docker-compose.yml", "-f", "docker-compose.ha.yml") } else { @() }

$venv = ".venv-tests"
if (-not (Test-Path "$venv\Scripts\python.exe")) { python -m venv $venv }
& "$venv\Scripts\python.exe" -m pip install -q -r scripts/requirements.txt -c scripts/constraints.txt
$py = (Resolve-Path "$venv\Scripts\python.exe").Path

function Manage([string[]]$cmdArgs) {
    $out = docker compose exec -T web python manage.py @cmdArgs 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "manage.py $($cmdArgs -join ' ') failed:`n$out" }
    return $out
}

Write-Host "== Seeding load users" -ForegroundColor Cyan
$count = $Rate * $Duration
# One prefix per block size: a block is created in one go, so its ids are contiguous.
$seed = Manage @('seed_load_users', '--count', [string]$count, '--prefix', "u${count}_")
$firstId = [regex]::Match($seed, 'first_id=(\d+)').Groups[1].Value
Write-Host "   first user id: $firstId"

Write-Host "== Creating a live sale with $Stock units" -ForegroundColor Cyan
$demo = Manage @('create_demo', '--name', 'Chaos test', '--stock', [string]$Stock, '--users', '0')
$saleId = [regex]::Match($demo, 'Sale #(\d+)').Groups[1].Value
Write-Host "   sale id: $saleId"

New-Item -ItemType Directory -Force loadtest\results | Out-Null
$out = "loadtest\results\chaos-sale-$saleId.json"

Write-Host "== Load: $Rate req/s for $Duration s (unique users) while breaking things" -ForegroundColor Cyan
$job = Start-Job -ScriptBlock {
    param($root, $py, $sale, $rate, $dur, $first, $out)
    Set-Location $root
    & $py scripts/load.py --base http://localhost --sale-id $sale --rate $rate --duration $dur --first-user-id $first --out $out 2>&1
} -ArgumentList $root, $py, $saleId, $Rate, $Duration, $firstId, $out

$t0 = Get-Date
function At([int]$second, [string]$what, [scriptblock]$action) {
    $wait = $second - ((Get-Date) - $t0).TotalSeconds
    if ($wait -gt 0) { Start-Sleep -Seconds $wait }
    Write-Host ("   [{0,3}s] {1}" -f $second, $what) -ForegroundColor Yellow
    & $action 2>&1 | Out-Null
}
if ($Scenario -eq "kafka-ha") {
    # Each kill takes away the leader of about a third of the partitions mid-burst.
    At 20 "kill -9 kafka-3 (1 of 3 brokers)"           { docker compose @dc kill kafka-3 }
    At 40 "start kafka-3"                              { docker compose @dc start kafka-3 }
    At 55 "kill -9 kafka (broker 1, the first bootstrap address)" { docker compose @dc kill kafka }
    At 75 "start kafka"                                { docker compose @dc start kafka }
} elseif ($Scenario -eq "postgres-down") {
    At 20 "stop postgres"   { docker compose stop postgres }
    At 50 "start postgres"  { docker compose start postgres }
} else {
    At 20 "restart order-writer"   { docker compose restart order-writer }
    At 40 "kill -9 celery-worker"  { docker compose kill celery-worker }
    At 50 "start celery-worker"    { docker compose start celery-worker }
    At 60 "restart redis"          { docker compose restart redis }
}

Receive-Job $job -Wait -AutoRemoveJob | ForEach-Object { Write-Host "   $_" }
$summary = Get-Content $out -Raw | ConvertFrom-Json
Write-Host "   202 accepted: $($summary.accepted), 503/errors: $($summary.errors)"

Write-Host "== Verifying (waits up to 10 minutes for Celery, retries and reconcile)" -ForegroundColor Cyan
docker compose exec -T web python manage.py verify_sale --sale-id $saleId --accepted $summary.accepted --errors $summary.errors --wait 600
exit $LASTEXITCODE
