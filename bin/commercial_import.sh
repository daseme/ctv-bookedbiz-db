#!/bin/bash
set -euo pipefail

LOG_DIR="/var/log/ctv-commercial-import"
LOG_FILE="$LOG_DIR/import.log"
LOCK_FILE="/tmp/commercial-import.lock"
K_DRIVE_SOURCE="/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx"
DEST_DIR="/srv/spotops/data/raw/daily"
DATED_FILE="$DEST_DIR/Commercial Log $(date +%y%m%d).xlsx"

mkdir -p "$LOG_DIR" "$DEST_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"; }

# Lock
exec 9>"$LOCK_FILE"
flock -n 9 || { log "ERROR: Already running"; exit 1; }

log "INFO: Commercial import starting"

# Ensure K-drive is mounted
if ! mountpoint -q /mnt/k-drive; then
    log "INFO: Mounting K-drive"
    sudo mount /mnt/k-drive
fi

if [ ! -f "$K_DRIVE_SOURCE" ]; then
    log "ERROR: Source file not found: $K_DRIVE_SOURCE"
    exit 1
fi

cp "$K_DRIVE_SOURCE" "$DATED_FILE"
log "INFO: Copied to $DATED_FILE"

# Notify
if [ -n "${NTFY_TOPIC:-}" ]; then
    curl -fsS -H "Title: CTV Commercial Import" -d "Import copied successfully" \
        "https://ntfy.sh/$NTFY_TOPIC" >/dev/null 2>&1 || true
fi

log "INFO: Commercial import complete"
