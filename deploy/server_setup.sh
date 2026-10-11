#!/usr/bin/env bash
# Runs ON THE SERVER (Ubuntu 22.04/24.04, x86 or ARM). deploy/deploy_vm.ps1 uploads the code and
# calls this; it is safe to run again (updates keep .env, data and certificates).
#
#   bash deploy/server_setup.sh <site-address> [cors-origins]
#   e.g. bash deploy/server_setup.sh 1-2-3-4.sslip.io https://flashcore.vercel.app
set -euo pipefail
cd "$(dirname "$0")/.."

SITE="${1:?usage: server_setup.sh <site-address> [cors-origins]}"
CORS="${2:-}"

# 1. Firewall. Oracle Cloud's Ubuntu images REJECT everything except SSH in iptables (and the
#    VCN security list must allow 80/443 as well, see docs/DEPLOY.md). Done before Docker is
#    installed so its rules never end up in the saved rule file.
RULES=/etc/iptables/rules.v4
if [ -f "$RULES" ] && grep -q -- "-A INPUT -j REJECT" "$RULES"; then
  for port in 443 80; do
    if ! grep -q -- "--dport $port -j ACCEPT" "$RULES"; then
      sudo sed -i "/-A INPUT -j REJECT/i -A INPUT -p tcp -m state --state NEW -m tcp --dport $port -j ACCEPT" "$RULES"
    fi
    sudo iptables -C INPUT -p tcp --dport "$port" -j ACCEPT 2>/dev/null \
      || sudo iptables -I INPUT 1 -p tcp --dport "$port" -j ACCEPT
  done
fi

# 2. Swap on small machines: building the images needs memory.
if [ -z "$(swapon --show)" ] && [ "$(awk '/MemTotal/ {print $2}' /proc/meminfo)" -lt 6000000 ]; then
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

# 3. Docker Engine + compose plugin (Docker's official install script).
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"   # takes effect at the next login; this script uses sudo
fi

# 4. .env: fresh random secrets on the first run, never overwritten afterwards.
set_env() {  # set_env KEY VALUE: replace the line or append it
  if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi
}
if [ ! -f .env ]; then
  cp .env.example .env
  set_env POSTGRES_PASSWORD "$(openssl rand -hex 24)"
  set_env JWT_SIGNING_KEY "$(openssl rand -hex 32)"
  set_env DJANGO_SECRET_KEY "$(openssl rand -hex 32)"
  set_env DJANGO_DEBUG 0
  # Plain `docker compose ...` on this server = production files + Caddy.
  set_env COMPOSE_FILE docker-compose.yml:docker-compose.prod.yml
  set_env COMPOSE_PROFILES vm
  # Sized for a small VM and demo traffic (raise for load tests).
  set_env GATEWAY_REPLICAS 2
  set_env GATEWAY_WORKERS 1
  set_env CELERY_CONCURRENCY 4
fi
set_env SITE_ADDRESS "$SITE"
set_env DJANGO_ALLOWED_HOSTS "$SITE,localhost,127.0.0.1,web"
set_env CSRF_TRUSTED_ORIGINS "https://$SITE"
if [ -n "$CORS" ]; then set_env CORS_ALLOWED_ORIGINS "$CORS"; fi
chmod 600 .env

# 5. Build and start (or update) everything.
sudo docker compose up -d --build --remove-orphans

# 6. nginx turns healthy only after Django and the gateways are healthy.
status=""
for _ in $(seq 1 60); do
  status=$(sudo docker inspect -f '{{.State.Health.Status}}' "$(sudo docker compose ps -q nginx)" 2>/dev/null || true)
  [ "$status" = healthy ] && break
  sleep 5
done
if [ "$status" != healthy ]; then
  sudo docker compose ps
  echo "nginx did not become healthy; see: sudo docker compose logs --tail 50" >&2
  exit 1
fi

# 7. Demo sale + users, once.
if [ ! -f .demo-seeded ]; then
  sudo docker compose exec -T web python manage.py create_demo --stock 100 --users 50 --minutes 43200
  touch .demo-seeded
fi

sudo docker compose ps --format 'table {{.Service}}\t{{.Status}}'
echo
echo "Backend: https://$SITE  (API check: https://$SITE/api/sales/ , admin: https://$SITE/admin/)"
