#!/bin/bash
# Pi2 Failback Script - Return control to pi-ctv when it comes back online
# Location: /opt/apps/ctv-bookedbiz-db/scripts/failback-to-pi-ctv.sh

set -e  # Exit on any error

PROJECT_DIR="/opt/apps/ctv-bookedbiz-db"

echo "🔄 INITIATING FAILBACK TO PI-CTV"
echo "=================================="

echo "🔍 Step 1: Testing pi-ctv availability..."
if curl -sf http://100.81.73.46:8000/api/system-stats >/dev/null; then
    echo "✓ Pi-ctv is responding and healthy"
else
    echo "❌ ERROR: Pi-ctv is not responding"
    echo "Cannot failback - pi-ctv must be healthy first"
    exit 1
fi

echo "💾 Step 2: Database changes during failover period..."
echo "⚠️  Pi2 was active - any database changes will be lost during failback"
echo ""
echo "🔍 Upload database changes from failover period? (y/N)"
read -r upload_response
if [[ "$upload_response" =~ ^[Yy]$ ]]; then
    echo ""
    echo "⚠️  ⚠️  DOUBLE CONFIRMATION REQUIRED ⚠️  ⚠️"
    echo "This will upload pi2's database as a BACKUP copy to avoid conflicts"  
    echo "Are you absolutely sure? This cannot be undone! (yes/NO)"
    read -r confirm_response
    if [[ "$confirm_response" == "yes" ]]; then
        cd "$PROJECT_DIR"
        source .venv/bin/activate
        
        # Upload as backup with timestamp to avoid conflicts
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        echo "📤 Uploading as backup: failover_backup_${TIMESTAMP}.db"
        python cli_db_sync.py backup "failover_backup_${TIMESTAMP}.db"
        echo "✓ Failover database changes backed up (not primary database)"
        echo "✓ Manual merge required if you want to restore this data"
    else
        echo "✓ Database upload cancelled - failover data will be lost"
    fi
else
    echo "✓ Skipping database upload - failover data will be lost"
fi
echo ""

echo "🛑 Step 3: Stopping Flask service on pi2..."
sudo systemctl stop flaskapp
echo "✓ Flask service stopped on pi2"

echo "⏱️  Step 4: Final health check of pi-ctv..."
sleep 3
if curl -sf http://100.81.73.46:8000/api/system-stats >/dev/null; then
    echo "✓ Pi-ctv confirmed healthy and serving traffic"
else
    echo "⚠️  WARNING: Pi-ctv may not be fully healthy"
    echo "Check pi-ctv status before considering failback complete"
fi

echo ""
echo "✅ FAILBACK COMPLETE!"
echo "===================="
echo "• Pi-ctv Flask service:  http://100.81.73.46:8000"  
echo "• Pi2 Flask service:     STOPPED"
echo "• Pi2 remains ready for future failover"
echo ""
echo "📊 Control Station Alpha Dashboard: http://100.96.96.109:5001"
echo "🔧 Kuma Monitoring:               http://100.96.96.109:3001"
echo ""
echo "If pi-ctv fails again, run: ./scripts/failover-to-pi2.sh"
