#!/usr/bin/env bash
set -euo pipefail

# Enables provider-usage (viewing activity) parsing from Nginx Proxy Manager logs.
#
# What it does:
# - Detects NPM host bind mount for /data
# - Adds a read-only volume mount to dispatcharr-maid-web: /app/npm_logs
# - Writes usage.* settings into config.yaml (via the running Maid container)
# - Rebuilds/restarts dispatcharr-maid-web
# - Prints a quick API sample

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found" >&2
  exit 1
fi

if ! docker ps >/dev/null 2>&1; then
  echo "ERROR: docker is not accessible (are you root / in docker group?)" >&2
  exit 1
fi

NPM_CONTAINER="${NPM_CONTAINER:-nginx-proxy-manager_app_1}"
MAID_SERVICE="${MAID_SERVICE:-dispatcharr-maid-web}"

if ! docker inspect "$NPM_CONTAINER" >/dev/null 2>&1; then
  echo "ERROR: NPM container '$NPM_CONTAINER' not found. Set NPM_CONTAINER=... and retry." >&2
  exit 1
fi

NPM_DATA_HOST="$(docker inspect "$NPM_CONTAINER" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}')"
if [ -z "${NPM_DATA_HOST}" ]; then
  echo "ERROR: Could not detect NPM /data mount source." >&2
  exit 1
fi

NPM_LOGS_HOST="${NPM_DATA_HOST%/}/logs"
if [ ! -d "$NPM_LOGS_HOST" ]; then
  echo "ERROR: Expected NPM logs dir not found on host: $NPM_LOGS_HOST" >&2
  exit 1
fi

COMPOSE_FILE="docker-compose.yml"
if [ ! -f "$COMPOSE_FILE" ]; then
  echo "ERROR: $COMPOSE_FILE not found in $ROOT_DIR" >&2
  exit 1
fi

echo "Detected NPM logs on host: $NPM_LOGS_HOST"

# Patch docker-compose.yml (idempotent)
if ! grep -q "/app/npm_logs:ro" "$COMPOSE_FILE"; then
  # Insert after the .env mount if present, otherwise after the 'volumes:' key.
  if grep -q "\./\.env:/app/\.env" "$COMPOSE_FILE"; then
    sed -i "/\.\/\.env:\/app\/\.env/a\\      - ${NPM_LOGS_HOST}:/app/npm_logs:ro" "$COMPOSE_FILE"
  else
    sed -i "/^[[:space:]]*volumes:[[:space:]]*$/a\\      - ${NPM_LOGS_HOST}:/app/npm_logs:ro" "$COMPOSE_FILE"
  fi
  echo "Patched $COMPOSE_FILE to mount NPM logs."
else
  echo "$COMPOSE_FILE already mounts /app/npm_logs:ro"
fi

# Restart/rebuild Maid first so we can use its Python/YAML libs to edit config.yaml reliably.
docker compose up -d --build "$MAID_SERVICE"

# Update config.yaml using python inside the Maid container (edits the bind-mounted file).
docker exec "$MAID_SERVICE" python3 - <<'PY'
import yaml
from pathlib import Path

p = Path("/app/config.yaml")
cfg = yaml.safe_load(p.read_text()) if p.exists() else {}
cfg = cfg or {}
usage = cfg.get("usage") or {}
usage.update({
    "access_log_dir": "/app/npm_logs",
    "access_log_glob": "proxy-host-*_access.log*",
    "lookback_days": 7,
})
cfg["usage"] = usage
p.write_text(yaml.safe_dump(cfg, sort_keys=False))
print("Updated /app/config.yaml usage.* settings")
PY

docker compose up -d --build "$MAID_SERVICE"

echo "Verifying /app/npm_logs exists in $MAID_SERVICE..."
docker exec "$MAID_SERVICE" sh -lc 'ls -lah /app/npm_logs | head'

echo "Sample API response:"
curl -s "http://127.0.0.1:5000/api/usage/providers?days=7&proxy_host=1" | python3 -m json.tool | head -n 120

