#!/bin/bash

set -e

# === CONFIGURATION ===
DROPBOX_DB_PATH="/mnt/c/Users/Kurt/Crossings TV Dropbox/kurt olmstead/Financial/Sales/WeeklyReports/ctv-bookedbiz-db/data/database/production.db"
WSL_DB_PATH="$HOME/ctv-data/staging/production.db"

WEEKLY_FILE="$1"
if [ -z "$WEEKLY_FILE" ]; then
    echo "Usage: $0 path/to/weekly-YYYYMM.xlsx"
    exit 1
fi

echo "🚚 Staging DB from Dropbox..."
mkdir -p "$(dirname "$WSL_DB_PATH")"
cp "$DROPBOX_DB_PATH" "$WSL_DB_PATH"

echo "🚀 Running Weekly Import..."
if uv run python src/cli/weekly_update.py "$WEEKLY_FILE" --db-path "$WSL_DB_PATH"; then
    echo "✅ Import successful. Syncing DB back to Dropbox..."
    cp "$WSL_DB_PATH" "$DROPBOX_DB_PATH"
    echo "📦 Done: Dropbox updated."
else
    echo "❌ Import failed. Dropbox was NOT modified."
    exit 1
fi
