#!/bin/bash
# Pi2 Daily Database Download Script
# Location: /opt/apps/ctv-bookedbiz-db/bin/daily-download.sh
# Runs after spotops uploads to ensure pi2 has latest database

set -e

PROJECT_DIR="/opt/apps/ctv-bookedbiz-db"
VENV_PATH="$PROJECT_DIR/.venv"
LOG_FILE="/var/log/ctv-pi2-download/download.log"

# Ensure log directory exists
sudo mkdir -p "$(dirname "$LOG_FILE")"
sudo chown daseme:ctvapps "$(dirname "$LOG_FILE")"

# Function to log with timestamp
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

log "🔄 Starting daily database download on Pi2"

# Change to project directory
cd "$PROJECT_DIR"

# Get latest code
log "📥 Updating code from GitHub..."
if git pull origin main; then
    log "✓ Code updated successfully"
else
    log "❌ Failed to update code from GitHub"
    exit 1
fi

# Activate virtual environment and download database
log "💾 Downloading latest database from Dropbox..."
source "$VENV_PATH/bin/activate"

if python cli_db_sync.py download; then
    # Check database size
    if [[ -f "$PROJECT_DIR/data/database/production.db" ]]; then
        DB_SIZE=$(stat -f%z "$PROJECT_DIR/data/database/production.db" 2>/dev/null || stat -c%s "$PROJECT_DIR/data/database/production.db" 2>/dev/null)
        DB_SIZE_MB=$((DB_SIZE / 1024 / 1024))
        log "✓ Database downloaded successfully (${DB_SIZE_MB}MB)"
        
        if [[ $DB_SIZE_MB -lt 100 ]]; then
            log "⚠️  WARNING: Database seems unexpectedly small (${DB_SIZE_MB}MB)"
        fi
    else
        log "❌ Database file not found after download"
        exit 1
    fi
else
    log "❌ Failed to download database"
    exit 1
fi

# Test database integrity (optional quick check)
if command -v sqlite3 >/dev/null; then
    if sqlite3 "$PROJECT_DIR/data/database/production.db" "SELECT COUNT(*) FROM sqlite_master;" >/dev/null 2>&1; then
        log "✓ Database integrity check passed"
    else
        log "⚠️  Database integrity check failed"
    fi
fi

log "✅ Daily download completed successfully"
log "   Pi2 is ready for emergency failover"
