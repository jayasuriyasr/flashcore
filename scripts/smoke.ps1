# End-to-end smoke test through nginx (http://localhost).
#   docker compose exec web python manage.py create_demo --stock 100 --users 20   # prints the sale id
#   powershell -ExecutionPolicy Bypass -File scripts/smoke.ps1 -SaleId <id> -Users 20
param(
    [Parameter(Mandatory = $true)][int]$SaleId,
    [int]$Users = 20,
    [string]$Base = "http://localhost"
)
$ErrorActionPreference = 'Continue'   # native tools write warnings to stderr; we check exit codes
Set-Location (Split-Path $PSScriptRoot -Parent)

$venv = ".venv-tests"
if (-not (Test-Path "$venv\Scripts\python.exe")) { python -m venv $venv }
& "$venv\Scripts\python.exe" -m pip install -q -r scripts/requirements.txt -c scripts/constraints.txt
& "$venv\Scripts\python.exe" scripts/smoke.py --base $Base --sale-id $SaleId --users $Users
exit $LASTEXITCODE
