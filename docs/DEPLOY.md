# Deploying FlashCore for free

```
Browser ──► Vercel: React app (free Hobby plan)
   │
   └── HTTPS + WSS ──► Oracle Cloud VM (Always Free): Caddy (HTTPS) → nginx → gateway ×2 / Django
                                                      → Redis, Kafka, PostgreSQL, order writer, Celery
```

**Why not everything on Vercel:** Vercel runs static sites and short serverless functions. FlashCore
needs processes that run all the time: a Kafka broker and consumer, Celery workers, PostgreSQL,
Redis, and long-lived WebSocket connections. So Vercel hosts the React app and a VM runs the same
Docker Compose stack used for testing, with production overrides
([docker-compose.prod.yml](../docker-compose.prod.yml)).

| Piece | Service | Cost |
|---|---|---|
| React app | Vercel Hobby | free |
| Backend VM | Oracle Cloud Always Free (Ampere A1, ARM) | free |
| Hostname | `<ip-with-dashes>.sslip.io` (resolves to the VM's IP, no signup) | free |
| HTTPS certificate | Let's Encrypt via Caddy, renewed automatically | free |

## Option A: Oracle Cloud VM (always online)

### 1. Create the VM (you, in the browser, about 15 minutes)

1. Sign up at **cloud.oracle.com** → *Start for free*. Oracle asks for a card to verify identity;
   Always Free resources are never charged. Choose the home region carefully (it can't be changed),
   e.g. *India South (Hyderabad)* or *India West (Mumbai)*.
2. **Compute → Instances → Create instance**
   - **Image:** Canonical Ubuntu 24.04.
   - **Shape:** Ampere → `VM.Standard.A1.Flex`, **2 OCPUs, 12 GB** (Always Free allows up to
     4 OCPUs / 24 GB in total). If it says *out of capacity*, try another availability domain or
     try again later.
   - **Networking:** create a new virtual cloud network with a public subnet and
     *Assign a public IPv4 address*.
   - **Add SSH keys → Paste public keys:** paste the output of
     `Get-Content $HOME\.ssh\id_ed25519.pub`.
3. **Open ports 80 and 443:** on the instance page, click the subnet → *Security Lists* →
   the default list → **Add Ingress Rules**: source CIDR `0.0.0.0/0`, IP protocol TCP,
   destination port range `80,443`.
4. Copy the instance's **public IP address**.

Why 2 OCPUs / 12 GB and not the full 24 GB: Oracle may reclaim Always Free instances that stay
idle for 7 days, meaning CPU, network **and** memory all under 20%. The stack uses about 3 GB,
which is above 20% of 12 GB but below 20% of 24 GB.

### 2. Deploy the backend (one command)

```powershell
powershell -ExecutionPolicy Bypass -File deploy/deploy_vm.ps1 -Server ubuntu@<public-ip>
```

[deploy_vm.ps1](../deploy/deploy_vm.ps1) uploads the last commit and runs
[server_setup.sh](../deploy/server_setup.sh) on the VM. That script:

- opens 80/443 in Oracle's iptables rules;
- installs Docker;
- writes `.env` with **fresh random secrets** (Postgres password, JWT key, Django key);
- builds and starts all services;
- waits for health checks;
- creates the demo sale and users `demo1`…`demo50` (password `demo12345`).

The first run takes 10–20 minutes. Result: **https://\<ip-with-dashes\>.sslip.io**. Check it with
`https://<ip-with-dashes>.sslip.io/api/sales/`.

### 3. Deploy the frontend to Vercel

```powershell
npx vercel login                                  # once, you: opens the browser
powershell -ExecutionPolicy Bypass -File deploy/deploy_frontend.ps1 -ApiUrl https://<ip-with-dashes>.sslip.io
```

This prints the production URL, e.g. `https://flashcore.vercel.app` (Vercel adds a suffix if the
name is taken).

### 4. Allow the frontend's origin (CORS)

```powershell
powershell -ExecutionPolicy Bypass -File deploy/deploy_vm.ps1 -Server ubuntu@<public-ip> -Cors https://flashcore.vercel.app
```

Use the exact Vercel URL, with no trailing slash. Open it and log in as `demo1` / `demo12345`.
The login page shows the demo accounts (visitors can't sign up); `deploy_frontend.ps1 -DemoLogin ""` hides it.

**Before you share the link** (e.g. before an interview), give every demo account a fresh sale to buy from,
since each account can buy once per sale:

```powershell
ssh ubuntu@<public-ip> "cd flashcore && sudo docker compose exec -T web python manage.py create_demo --stock 100 --users 50 --minutes 43200"
```

### 5. Admin account (you)

```powershell
ssh ubuntu@<public-ip>
cd flashcore && sudo docker compose exec web python manage.py createsuperuser
```

Admin: `https://<ip-with-dashes>.sslip.io/admin/` (start/end sales, live Redis stock).

### Updating

Commit, then rerun the backend and/or frontend script:

- `deploy_vm.ps1` keeps `.env`, data and certificates.
- `deploy_frontend.ps1` only rebuilds the React app.

On the VM, `.env` holds `COMPOSE_FILE` and `COMPOSE_PROFILES`, so plain `sudo docker compose ps`,
`logs` and `exec` use the production files.

## Option B: your laptop + Cloudflare Tunnel (no signup, only while the laptop is on)

```powershell
powershell -ExecutionPolicy Bypass -File deploy/tunnel.ps1 -Cors https://flashcore.vercel.app
```

[tunnel.ps1](../deploy/tunnel.ps1) does the following:

1. Replaces the dev-only secrets in `.env`.
2. Runs the stack with production settings plus a Cloudflare quick tunnel. You get
   `https://<random>.trycloudflare.com`, with no account and no open ports.
3. Redeploys the Vercel frontend against that URL.

The tunnel URL changes every time the tunnel starts, after a reboot for example, so run the script
again each time. `tunnel.ps1 -Stop` goes back to local development.

## What production changes compared with local

| | Local (`docker compose up`) | Deployed (`+ docker-compose.prod.yml`) |
|---|---|---|
| Django | `DEBUG=1`, gunicorn `--reload`, code bind-mounted | `DEBUG=0`, code baked into the images |
| Exposed ports | Postgres 5432, Redis 6379, Kafka 9094, Django 8000, nginx 80 | only Caddy 80/443 (VM) or none (tunnel) |
| HTTPS | none | Caddy + Let's Encrypt, or Cloudflare |
| Frontend | served by nginx, same origin | Vercel, other origin: CORS allow-list on Django and the gateway |
| Secrets | dev defaults from `.env.example` | random, generated on the server, `.env` is `chmod 600` and never committed |
| Kafka heap | 1 GB | 512 MB |

## Troubleshooting

| Symptom | Check |
|---|---|
| `https://…sslip.io` not reachable | Ports 80/443 in the Oracle security list; `sudo docker compose logs caddy` |
| Certificate errors / Let's Encrypt rate limit in the Caddy log | Use your own hostname instead. A free **DuckDNS** name (duckdns.org, sign in, point it at the VM's IP) works: `deploy_vm.ps1 -Server ubuntu@<ip> -Site <name>.duckdns.org`, then redeploy the frontend with the new URL |
| Browser console: *blocked by CORS policy* | `-Cors` must be exactly the Vercel URL in the address bar |
| Login works, live stock doesn't update | WebSocket: `sudo docker compose logs gateway`; the page must be HTTPS and the API URL `https://` |
| A service keeps restarting | `sudo docker compose ps`, `sudo docker compose logs --tail 100 <service>` |
