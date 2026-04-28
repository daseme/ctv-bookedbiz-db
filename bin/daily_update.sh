#!/bin/bash
set -euo pipefail

LOG_DIR="/var/log/ctv-daily-update"
LOG_FILE="$LOG_DIR/update.log"
LOCK_FILE="/tmp/ctv-daily-update.lock"
DATED_FILE="/app/data/raw/daily/Commercial Log $(date +%y%m%d).xlsx"
NTFY_TOPIC="${NTFY_TOPIC:-ctv-import-2a11ef7e7a84}"

mkdir -p "$LOG_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

exec 9>"$LOCK_FILE"
flock -n 9 || { log "ERROR: Already running"; exit 1; }

log "INFO: Daily update starting — $DATED_FILE"

if docker compose -f /opt/spotops/docker-compose.yml exec -T spotops \
    uv run python cli/daily_update.py "$DATED_FILE" \
    --auto-setup --unattended --log-file "$LOG_FILE" \
    --verbose >> "$LOG_FILE" 2>&1; then
    log "INFO: Daily update completed successfully"
    curl -fsS -H "Title: CTV Daily Update" -H "Tags: white_check_mark" \
        -d "Daily update completed successfully" \
        "https://ntfy.sh/$NTFY_TOPIC" >/dev/null 2>&1 || true
else
    log "ERROR: Daily update failed"
    curl -fsS -H "Title: CTV Daily Update" -H "Priority: 5" -H "Tags: rotating_light" \
        -d "Daily update FAILED — check logs on spotops-bee" \
        "https://ntfy.sh/$NTFY_TOPIC" >/dev/null 2>&1 || true
    exit 1
fi
