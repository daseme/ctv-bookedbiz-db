#!/usr/bin/env bash
set -Eeuo pipefail

echo "🚂 Railway Startup Script Starting..."

APP_DIR="/app"
DB_DIR="$APP_DIR/data/database"
PROC_DIR="$APP_DIR/data/processed"
DB_PATH="$DB_DIR/production.db"

mkdir -p "$DB_DIR" "$PROC_DIR"
echo "📁 Ensured data directories"

# Always run restore: it compares Dropbox content_hash vs local and only replaces if different
echo "🔄 Checking Dropbox vs local database (hash compare)…"
export PYTHONPATH="$APP_DIR"
python "$APP_DIR/railway_db_sync.py" download || true

echo "🚀 Starting application: $*"
exec "$@"
