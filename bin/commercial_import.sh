#!/usr/bin/env bash
# Commercial Log Daily Import Wrapper
# Handles K drive mounting and runs the import script

set -euo pipefail

# Configuration
SCRIPT_DIR="/opt/apps/ctv-bookedbiz-db"
MOUNT_POINT="/mnt/k-drive"
NETWORK_SHARE="//100.102.206.113/K Drive"
LOG_FILE="/var/log/ctv-commercial-import/import.log"

# Credentials (will be loaded from environment file)
CIFS_USERNAME="${CIFS_USERNAME:-usrjp}"
CIFS_PASSWORD="${CIFS_PASSWORD:-}"
CIFS_DOMAIN="${CIFS_DOMAIN:-CTVETERE}"

# Notification settings (optional)
NTFY_TOPIC="${NTFY_TOPIC:-}"
SLACK_WEBHOOK_URL="${SLACK_WEBHOOK_URL:-}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

notify_failure() {
    local message="$1"
    log "FAILURE: $message"
    
    # Send notifications if configured
    if [[ -n "$NTFY_TOPIC" ]]; then
        curl -s -d "$message" "https://ntfy.sh/$NTFY_TOPIC" || true
    fi
    
    if [[ -n "$SLACK_WEBHOOK_URL" ]]; then
        curl -s -X POST -H 'Content-type: application/json' \
             --data "{\"text\":\"🚨 Commercial Log Import Failed: $message\"}" \
             "$SLACK_WEBHOOK_URL" || true
    fi
}

check_network() {
    log "Testing network connectivity to K drive server..."
    if ! ping -c 2 -W 5 100.102.206.113 >/dev/null 2>&1; then
        notify_failure "Cannot reach K drive server (100.102.206.113)"
        return 1
    fi
    log "Network connectivity OK"
    return 0
}

is_mounted() {
    mount | grep -q "$MOUNT_POINT"
}

mount_k_drive() {
    log "Mounting K drive..."
    
    # Ensure mount point exists
    sudo mkdir -p "$MOUNT_POINT"
    
    # Check if already mounted
    if is_mounted; then
        log "K drive already mounted, testing access..."
        if [[ -d "$MOUNT_POINT/Traffic" ]]; then
            log "K drive mount verified"
            return 0
        else
            log "K drive mounted but not accessible, remounting..."
            sudo umount "$MOUNT_POINT" 2>/dev/null || true
        fi
    fi
    
    # Mount the drive
    if sudo mount -t cifs "$NETWORK_SHARE" "$MOUNT_POINT" \
        -o "username=$CIFS_USERNAME,password=$CIFS_PASSWORD,domain=$CIFS_DOMAIN,vers=2.0,sec=ntlmv2,uid=1000,gid=1000,file_mode=0644,dir_mode=0755"; then
        log "K drive mounted successfully"
        
        # Verify mount worked
        if [[ -d "$MOUNT_POINT/Traffic" ]]; then
            log "K drive mount verified - Traffic folder accessible"
            return 0
        else
            notify_failure "K drive mounted but Traffic folder not found"
            return 1
        fi
    else
        notify_failure "Failed to mount K drive"
        return 1
    fi
}

run_import() {
    log "Starting commercial log import..."
    
    cd "$SCRIPT_DIR"
    
    # Activate virtual environment and run import
    if source .venv/bin/activate && uv run python ./src/importers/commercial_log_importer.py; then
        log "Commercial log import completed successfully"
        
        # Show what was created
        local latest_file=$(ls -t data/raw/daily/Commercial*.xlsx 2>/dev/null | head -1)
        if [[ -n "$latest_file" ]]; then
            local file_size=$(stat -c%s "$latest_file" 2>/dev/null || echo "unknown")
            log "Created: $latest_file ($file_size bytes)"
        fi
        
        return 0
    else
        notify_failure "Commercial log import script failed"
        return 1
    fi
}

cleanup() {
    # Note: We leave K drive mounted for potential other uses
    # If you want to unmount after each run, uncomment:
    # sudo umount "$MOUNT_POINT" 2>/dev/null || true
    log "Import process completed"
}

main() {
    log "=== Commercial Log Daily Import Started ==="
    
    # Create log directory
    sudo mkdir -p "$(dirname "$LOG_FILE")"
    sudo chown daseme:ctvapps "$(dirname "$LOG_FILE")" 2>/dev/null || true
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Check network first
    if ! check_network; then
        exit 1
    fi
    
    # Mount K drive
    if ! mount_k_drive; then
        exit 1
    fi
    
    # Run the import
    if ! run_import; then
        exit 1
    fi
    
    log "=== Commercial Log Daily Import Completed Successfully ==="
}

# Flock prevents overlapping runs
exec 200>/tmp/commercial-log-import.lock
if ! flock -n 200; then
    echo "Another instance is already running" >&2
    exit 1
fi

main "$@"