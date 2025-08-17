#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/apps/ctv-bookedbiz-db"
PY="/opt/apps/ctv-bookedbiz-db/.venv/bin/python"
LOCK_FILE="/tmp/ctv-db-sync.lock"

# DO NOT source /etc/ctv-db-sync.env here (systemd loads it)
PUB_ENV="$APP_DIR/.env.public"
if [[ -r "$PUB_ENV" ]]; then set -a; . "$PUB_ENV"; set +a; fi

cd "$APP_DIR"

# Prevent overlap
exec 9>"$LOCK_FILE"
if ! flock -n 9; then logger -t ctv-db-sync "Another sync is running; exiting."; exit 0; fi

if "$PY" cli_db_sync.py upload; then
  logger -t ctv-db-sync "Upload OK"
else
  rc=$?
  msg="Dropbox DB upload FAILED (exit $rc) on $(hostname) at $(date '+%F %T')"
  logger -t ctv-db-sync "$msg"
  if [[ -n "${NTFY_TOPIC:-}" ]]; then
    curl -fsS -H "Title: ctv-db-sync FAIL" -H "Priority: 5" -d "$msg" "https://ntfy.sh/$NTFY_TOPIC" || true
  fi
  if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
    payload=$(printf '{"text":"%s"}' "$msg")
    curl -fsS -X POST -H 'Content-type: application/json' --data "$payload" "$SLACK_WEBHOOK_URL" || true
  fi
  exit "$rc"
fi
