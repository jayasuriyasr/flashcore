<#
Free fallback: serve the backend from THIS laptop through a Cloudflare quick tunnel (HTTPS, no
account, no open ports) and point the Vercel frontend at it. Works only while the laptop and
Docker Desktop are on. See docs/DEPLOY.md.

  powershell -ExecutionPolicy Bypass -File deploy/tunnel.ps1 -Cors https://flashcore.vercel.app
  powershell -ExecutionPolicy Bypass -File deploy/tunnel.ps1 -Stop      # back to local development

Runs the stack with production settings (DEBUG off, no published ports, code baked into images).
The tunnel URL changes every time the tunnel starts, so this redeploys the frontend each time
(skip with -SkipFrontend).
#>
param(
    [string]$Cors = "",          # Vercel origin(s); remembered in .env
    [switch]$SkipFrontend,
    [switch]$Stop
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)
$prod = @("-f", "docker-compose.yml", "-f", "docker-compose.prod.yml", "--profile", "tunnel")

# Windows PowerShell turns redirected native stderr into a terminating error under "Stop",
# so commands whose stderr is redirected run with "Continue" and are checked by exit code.
function Invoke-Quiet([scriptblock]$command) {
    $ErrorActionPreference = "Continue"
    & $command 2>&1 | Out-String
}

Invoke-Quiet { docker info } | Out-Null
if ($LASTEXITCODE) { throw "Docker is not running: start Docker Desktop first" }

if ($Stop) {
    docker compose @prod rm -sf cloudflared
    docker compose up -d --build    # development settings again (bind mounts, ports, DEBUG)
    return
}

# .env: replace the dev-only secrets before anything is public (the JWT key signs logins).
function Set-EnvLine([string]$name, [string]$value) {
    $lines = @(Get-Content .env | Where-Object { $_ -notmatch "^$name=" }) + "$name=$value"
    [IO.File]::WriteAllText((Resolve-Path .env), ($lines -join "`n") + "`n")   # LF, no BOM
}
function New-Secret {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    -join ($bytes | ForEach-Object { $_.ToString("x2") })
}
$envText = Get-Content .env -Raw
foreach ($name in "JWT_SIGNING_KEY", "DJANGO_SECRET_KEY") {
    if ($envText -match "(?m)^$name=(dev-|$)") { Set-EnvLine $name (New-Secret); Write-Host "New random $name in .env" }
}
Set-EnvLine "CSRF_TRUSTED_ORIGINS" "https://*.trycloudflare.com"
if ($Cors) { Set-EnvLine "CORS_ALLOWED_ORIGINS" $Cors }

docker compose @prod up -d --build --remove-orphans
if ($LASTEXITCODE) { throw "docker compose up failed" }

Write-Host "Waiting for the tunnel URL ..."
$url = $null
foreach ($i in 1..60) {
    $logs = Invoke-Quiet { docker compose @prod logs cloudflared }
    $found = [regex]::Matches($logs, 'https://[a-z0-9-]+\.trycloudflare\.com')
    if ($found.Count) { $url = $found[$found.Count - 1].Value; break }
    Start-Sleep -Seconds 2
}
if (-not $url) { throw "no tunnel URL in 'docker compose logs cloudflared'" }
Write-Host "Backend: $url" -ForegroundColor Green

if (-not $SkipFrontend) {
    # README quick start creates demo1..demo20 locally
    & (Join-Path $PSScriptRoot "deploy_frontend.ps1") -ApiUrl $url -DemoLogin "demo1 to demo20, password demo12345"
}
if (-not ((Get-Content .env) -match '^CORS_ALLOWED_ORIGINS=https')) {
    Write-Warning "CORS_ALLOWED_ORIGINS is not set: run again with -Cors https://<your-project>.vercel.app"
}
