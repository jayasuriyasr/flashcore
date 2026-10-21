<#
Deploys the React app to Vercel (free Hobby plan), built against the given backend URL.
See docs/DEPLOY.md.

  npx vercel login                                    # once, yourself (opens the browser)
  powershell -ExecutionPolicy Bypass -File deploy/deploy_frontend.ps1 -ApiUrl https://1-2-3-4.sslip.io

The first run creates/links the Vercel project (frontend/.vercel, git-ignored).
VITE_API_URL is baked in at build time, so a new backend URL needs a new deploy (this script).
#>
param(
    [Parameter(Mandatory)][string]$ApiUrl,   # backend origin, https://...
    [string]$Project = "flashcore",
    # Shown on the login page (visitors can't sign up); "" hides it. Matches server_setup.sh's create_demo.
    [string]$DemoLogin = "demo1 to demo50, password demo12345"
)
$ErrorActionPreference = "Stop"
if ($ApiUrl -notmatch '^https://') { throw "-ApiUrl must start with https:// (a page on Vercel cannot call http:// or ws://)" }
Set-Location (Join-Path (Split-Path $PSScriptRoot) "frontend")

if (-not (Test-Path .vercel/project.json)) {
    npx --yes vercel@latest link --yes --project $Project
    if ($LASTEXITCODE) { throw "vercel link failed: run 'npx vercel login' first" }
}
npx --yes vercel@latest deploy --prod --yes `
    --build-env "VITE_API_URL=$($ApiUrl.TrimEnd('/'))" --build-env "VITE_DEMO_LOGIN=$DemoLogin"
if ($LASTEXITCODE) { throw "vercel deploy failed" }
