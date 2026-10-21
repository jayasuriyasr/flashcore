<#
Deploys (or updates) the backend on a Linux VM over SSH: uploads the last commit, then runs
deploy/server_setup.sh there (Docker, firewall, .env with random secrets, build, start, demo data).
See docs/DEPLOY.md.

  powershell -ExecutionPolicy Bypass -File deploy/deploy_vm.ps1 -Server ubuntu@1.2.3.4
  powershell -ExecutionPolicy Bypass -File deploy/deploy_vm.ps1 -Server ubuntu@1.2.3.4 -Cors https://flashcore.vercel.app

-Site defaults to <ip-with-dashes>.sslip.io, a free hostname that resolves to the VM's IP, so
Caddy can get a Let's Encrypt certificate without buying a domain.
Ships `git archive HEAD`: commit first, uncommitted changes are not deployed.
#>
param(
    [Parameter(Mandatory)][string]$Server,   # user@host
    [string]$Site = "",                       # public hostname; default <ip>.sslip.io
    [string]$Cors = "",                       # frontend origin(s), comma-separated
    [string]$Key = ""                         # SSH private key; default: ssh's own (~/.ssh/id_ed25519)
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)

$hostPart = $Server.Split("@")[-1]
if (-not $Site) {
    if ($hostPart -notmatch '^\d{1,3}(\.\d{1,3}){3}$') { throw "-Server is not an IPv4 address: pass -Site <hostname>" }
    $Site = ($hostPart -replace '\.', '-') + ".sslip.io"
}
$sshOpts = @("-o", "StrictHostKeyChecking=accept-new", "-o", "ServerAliveInterval=30")
if ($Key) { $sshOpts += @("-i", $Key) }

if (git status --porcelain) { Write-Warning "Uncommitted changes are NOT deployed (git archive HEAD)." }
New-Item -ItemType Directory -Force deploy/.out | Out-Null
git archive --format=tar.gz -o deploy/.out/flashcore.tar.gz HEAD
if ($LASTEXITCODE) { throw "git archive failed" }

Write-Host "Uploading $(git rev-parse --short HEAD) to $Server ..."
scp @sshOpts deploy/.out/flashcore.tar.gz "${Server}:/tmp/flashcore.tar.gz"
if ($LASTEXITCODE) { throw "scp failed" }

Write-Host "Setting up https://$Site (the first run installs Docker and builds images: 10-20 min) ..."
ssh @sshOpts $Server "mkdir -p ~/flashcore && tar -xzf /tmp/flashcore.tar.gz -C ~/flashcore && bash ~/flashcore/deploy/server_setup.sh '$Site' '$Cors'"
if ($LASTEXITCODE) { throw "server setup failed (exit $LASTEXITCODE)" }

Write-Host "Checking https://$Site/api/sales/ from here ..."
$ok = $false
foreach ($i in 1..12) {   # Caddy needs a few seconds for the first certificate
    # -s without -S: no stderr. Windows PowerShell turns redirected native stderr into a
    # terminating error under ErrorActionPreference=Stop, which would end this retry loop.
    curl.exe -fs --max-time 10 "https://$Site/api/sales/" -o NUL
    if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    Start-Sleep -Seconds 5
}
if ($ok) { Write-Host "Backend is live: https://$Site" -ForegroundColor Green }
else { Write-Warning "https://$Site is not reachable yet. Are ports 80 and 443 open in the cloud firewall (Oracle: VCN security list)?" }
