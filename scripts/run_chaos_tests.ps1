# Runs the tests marked "chaos" (they stop/start containers, so they run on the HOST).
#   powershell -ExecutionPolicy Bypass -File scripts/run_chaos_tests.ps1
# Needs: the stack running (docker compose up -d) and Python 3.12 on PATH.
$ErrorActionPreference = 'Continue'   # native tools write warnings to stderr; we check exit codes
Set-Location (Split-Path $PSScriptRoot -Parent)

$venv = ".venv-tests"
if (-not (Test-Path "$venv\Scripts\python.exe")) { python -m venv $venv }
& "$venv\Scripts\python.exe" -m pip install -q -r scripts/requirements.txt

# Host-side addresses of the services published by docker-compose (the gateway is behind nginx).
$env:GATEWAY_URL = "http://localhost"
$env:TEST_REDIS_URL = "redis://localhost:6379/0"
$env:TEST_KAFKA_BOOTSTRAP = "localhost:9094"
$env:JWT_SIGNING_KEY = ((Get-Content .env | Where-Object { $_ -match '^JWT_SIGNING_KEY=' }) -split '=', 2)[1]

Push-Location gateway
try {
    & "..\$venv\Scripts\python.exe" -m pytest -v -m chaos tests @args
    $code = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $code
