# Load test with k6 (in Docker, on the compose network, through nginx), then verify the sale.
#   powershell -ExecutionPolicy Bypass -File loadtest/run_k6.ps1 -Scenario throughput
#   powershell -ExecutionPolicy Bypass -File loadtest/run_k6.ps1 -Scenario hot_item
#   ... -Target baseline   the same test against the PostgreSQL-only baseline (docs/SCALING.md);
#                          start it first: docker compose -f docker-compose.yml -f docker-compose.baseline.yml up -d --build
# For a fair throughput run start the stack with the load-test settings first (see README).
param(
    [ValidateSet("throughput", "hot_item")][string]$Scenario = "throughput",
    [ValidateSet("system", "baseline")][string]$Target = "system",
    [int]$Rate = 5000,
    [int]$Seconds = 0,
    # On a single machine Celery's fulfilment competes with the write path for the same CPUs.
    # -PauseCelery stops the worker during the burst; it drains the backlog afterwards.
    [switch]$PauseCelery
)
$ErrorActionPreference = 'Continue'   # native tools write warnings to stderr; we check exit codes
Set-Location (Split-Path $PSScriptRoot -Parent)
$root = (Get-Location).Path

if ($Seconds -eq 0) { $Seconds = if ($Scenario -eq "hot_item") { 20 } else { 60 } }
$stock = if ($Scenario -eq "hot_item") { 1000 } else { 1000000 }
# One user per request, plus one second of margin: k6's constant-arrival-rate can start
# Rate*Seconds + 1 iterations, and the extra one used to hit an unseeded user id (its order
# was correctly dropped by the order writer, but the run reported 60,001 accepted vs 60,000 stored).
$users = $Rate * ($Seconds + 1)
$key = ((Get-Content .env | Where-Object { $_ -match '^JWT_SIGNING_KEY=' }) -split '=', 2)[1]

function Manage([string[]]$cmdArgs) {
    $out = docker compose exec -T web python manage.py @cmdArgs 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) { throw "manage.py $($cmdArgs -join ' ') failed:`n$out" }
    return $out
}

Write-Host "== Seeding $users load users (the first run takes a minute)" -ForegroundColor Cyan
# One prefix per block size: a block is created in one go, so its ids are contiguous
# (extending an older block would interleave with users created since).
$seed = Manage @('seed_load_users', '--count', [string]$users, '--prefix', "u${users}_")
$firstId = [regex]::Match($seed, 'first_id=(\d+)').Groups[1].Value

Write-Host "== Creating a live sale with $stock units" -ForegroundColor Cyan
# The baseline reads stock from PostgreSQL only, so its sale is not made live in Redis.
$live = if ($Target -eq "baseline") { @('--starts-in', '1') } else { @() }
$demo = Manage (@('create_demo', '--name', "k6 $Scenario $Target", '--stock', [string]$stock, '--users', '0') + $live)
$saleId = [regex]::Match($demo, 'Sale #(\d+)').Groups[1].Value
Write-Host "   sale $saleId, users from id $firstId"

New-Item -ItemType Directory -Force loadtest\results | Out-Null
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$summaryName = "$Scenario-$Target-$stamp.json"
$statsFile = Join-Path $root "loadtest\results\$Scenario-$Target-$stamp-docker-stats.csv"
$baseUrl = if ($Target -eq "baseline") { "http://nginx/baseline" } else { "http://nginx" }

# CPU/memory per container every 10 s: shows which service is the bottleneck.
$statsJob = Start-Job -ScriptBlock {
    param($file, $seconds)
    "time,container,cpu,memory" | Out-File $file -Encoding utf8
    $end = (Get-Date).AddSeconds($seconds + 15)
    while ((Get-Date) -lt $end) {
        $t = Get-Date -Format "HH:mm:ss"
        docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | ForEach-Object { "$t,$_" } | Out-File $file -Append -Encoding utf8
        Start-Sleep -Seconds 10
    }
} -ArgumentList $statsFile, $Seconds

if ($PauseCelery) {
    Write-Host "== Pausing celery-worker for the burst (fulfilment drains afterwards)" -ForegroundColor Cyan
    docker compose stop celery-worker 2>&1 | Out-Null
}
Write-Host "== k6: $Scenario at $Rate req/s for $Seconds s against the $Target" -ForegroundColor Cyan
docker run --rm --network flashcore_default -v "${root}\loadtest:/loadtest" `
    -e SCENARIO=$Scenario -e SALE_ID=$saleId -e FIRST_USER_ID=$firstId -e JWT_SIGNING_KEY=$key `
    -e RATE=$Rate -e DURATION="${Seconds}s" -e BASE_URL=$baseUrl `
    grafana/k6:2.3.0 run --summary-export=/loadtest/results/$summaryName /loadtest/buy.js
$k6Exit = $LASTEXITCODE
if ($PauseCelery) {
    docker compose start celery-worker 2>&1 | Out-Null
}
Wait-Job $statsJob -Timeout 30 | Out-Null
Remove-Job $statsJob -Force

$summary = Get-Content "loadtest\results\$summaryName" -Raw | ConvertFrom-Json
$m = $summary.metrics
$accepted = [int]($m.reserved.count)
$busy = [int]($m.try_again.count)
$failedRate = [double]($m.http_req_failed.value)
Write-Host ""
Write-Host ("   achieved: {0:N0} req/s | p50 {1:N1} ms | p95 {2:N1} ms | p99 {3:N1} ms | failed {4:P2} | dropped {5}" -f `
    $m.http_reqs.rate, $m.http_req_duration.med, $m.http_req_duration.'p(95)', $m.http_req_duration.'p(99)', `
    $failedRate, [int]($m.dropped_iterations.count)) -ForegroundColor Green
Write-Host "   202 reserved: $accepted | 409 sold out: $([int]($m.sold_out.count)) | 503: $busy"
Write-Host "   summary: loadtest/results/$summaryName   docker stats: $statsFile"

Write-Host "== Verifying sale $saleId (waits for the order writer and Celery to drain)" -ForegroundColor Cyan
if ($Target -eq "baseline") {
    $expect = if ($Scenario -eq "hot_item") { @('--expect-sold', '1000') } else { @() }
    docker compose exec -T web python manage.py verify_sale --sale-id $saleId --accepted $accepted --baseline @expect
} elseif ($Scenario -eq "hot_item") {
    docker compose exec -T web python manage.py verify_sale --sale-id $saleId --accepted $accepted --sold-out --wait 300
} else {
    docker compose exec -T web python manage.py verify_sale --sale-id $saleId --accepted $accepted --errors $busy --wait 1200
}
$verifyExit = $LASTEXITCODE

if ($k6Exit -ne 0) { Write-Host "k6 thresholds failed (exit $k6Exit)" -ForegroundColor Red }
if ($k6Exit -ne 0 -or $verifyExit -ne 0) { exit 1 }
exit 0
