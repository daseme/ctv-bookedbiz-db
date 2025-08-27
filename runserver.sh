#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/apps/ctv-bookedbiz-db"
PORT="${PORT:-8000}"

cd "$APP_DIR"
. "$APP_DIR/.venv/bin/activate"
export PYTHONPATH="$APP_DIR"

# Let systemd handle restarts; no manual lsof/kill here.
exec uvicorn src.web.asgi:app --host 0.0.0.0 --port "$PORT"