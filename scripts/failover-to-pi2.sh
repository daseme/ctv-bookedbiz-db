#!/bin/bash
# Pi2 Failover Script - Take over from spotops when it goes down
# Location: /opt/apps/ctv-bookedbiz-db/scripts/failover-to-pi2.sh

set -e  # Exit on any error

PROJECT_DIR="/opt/apps/ctv-bookedbiz-db"
VENV_PATH="$PROJECT_DIR/.venv"
DB_PATH="$PROJECT_DIR/data/database/production.db"

echo "🚨 INITIATING FAILOVER TO PI2 (Control Station Alpha)"
echo "======================================================"

# Change to project directory
cd "$PROJECT_DIR"

echo "📥 Step 1: Getting latest code from GitHub..."
git pull origin main
echo "✓ Code updated"

echo "📋 Step 2: Checking database readiness..."

# Check if database exists and when it was last updated
if [[ -f "$DB_PATH" ]]; then
    DB_MODIFIED=$(date -r "$DB_PATH" '+%Y-%m-%d %H:%M:%S')
    echo "✓ Database ready (last updated: $DB_MODIFIED)"
    
    # Check if database is older than 36 hours (missed daily sync)
    DB_AGE_HOURS=$(( ( $(date +%s) - $(date -r "$DB_PATH" +%s) ) / 3600 ))
    if [[ $DB_AGE_HOURS -gt 36 ]]; then
        echo "⚠️  WARNING: Database is ${DB_AGE_HOURS} hours old"
        echo "   Daily sync may have failed. Download fresh copy? (y/N)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            source "$VENV_PATH/bin/activate"
            python cli_db_sync.py download
            echo "✓ Fresh database downloaded"
        fi
    fi
else
    echo "❌ No local database found! Downloading from Dropbox..."
    source "$VENV_PATH/bin/activate"
    python cli_db_sync.py download
    echo "✓ Database downloaded"
fi

# Check database exists and has reasonable size
if [[ -f "$DB_PATH" ]]; then
    DB_SIZE=$(stat -f%z "$DB_PATH" 2>/dev/null || stat -c%s "$DB_PATH" 2>/dev/null)
    DB_SIZE_MB=$((DB_SIZE / 1024 / 1024))
    echo "✓ Database size: ${DB_SIZE_MB}MB"
    
    if [[ $DB_SIZE_MB -lt 100 ]]; then
        echo "⚠️  WARNING: Database seems small (${DB_SIZE_MB}MB). Continue? (y/N)"
        read -r response
        if [[ ! "$response" =~ ^[Yy]$ ]]; then
            echo "❌ Failover aborted"
            exit 1
        fi
    fi
else
    echo "❌ ERROR: Database file not found after download"
    exit 1
fi

echo "🚀 Step 3: Starting Flask service..."
sudo systemctl start flaskapp
echo "✓ Flask service started"

echo "⏱️  Step 4: Checking service health..."
sleep 5  # Give service time to start

# Health check
if sudo systemctl is-active --quiet flaskapp; then
    echo "✓ Flask service is running"
else
    echo "❌ ERROR: Flask service failed to start"
    sudo systemctl status flaskapp
    exit 1
fi

# Test HTTP endpoint
echo "🔍 Step 5: Testing HTTP endpoint..."
if curl -sf http://localhost:8000/api/system-stats >/dev/null; then
    echo "✓ HTTP endpoint responding"
else
    echo "❌ ERROR: HTTP endpoint not responding"
    echo "Check logs: sudo journalctl -u flaskapp -n 50"
    exit 1
fi

echo ""
echo "🎯 FAILOVER COMPLETE!"
echo "===================="
echo "• Pi2 Flask service: http://100.96.96.109:8000"
echo "• Service status:    sudo systemctl status flaskapp"
echo "• View logs:         sudo journalctl -u flaskapp -f"
echo "• Stop service:      sudo systemctl stop flaskapp"
echo ""
echo "📊 Control Station Alpha Dashboard: http://100.96.96.109:5001"
echo "🔧 Kuma Monitoring:               http://100.96.96.109:3001"
echo ""
echo "To failback to spotops, run: ./scripts/failback-to-spotops.sh"
