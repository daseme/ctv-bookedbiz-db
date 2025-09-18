# Commercial Log Import and Daily Update System

## Overview
Automated daily pipeline for multi-sheet Commercial Log data processing, consisting of two coordinated systems: (1) Commercial Log import from K: drive network share processing both **Commercials** and **Worldlink Lines** sheets to local storage, and (2) Daily update processing that imports the combined data into the CTV BookedBiz database with automatic market setup, language assignment, and source tracking.

## System Components

### Commercial Log Import (Stage 1)
- **Source**: `/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx`
- **Processing**: Multi-sheet processing - Commercials + Worldlink Lines sheets
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial Log YYMMDD.xlsx`
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/src/importers/commercial_log_importer.py`
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/commercial_import.sh`
- **Environment File**: `/etc/ctv-commercial-import.env`

### Daily Update Processing (Stage 2)
- **Source**: Multi-sheet commercial log files from Stage 1
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/database/production.db`
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/cli/daily_update.py`
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/daily_update.sh`
- **Environment File**: `/etc/ctv-daily-update.env`
- **Log Directory**: `/var/log/ctv-daily-update/`

### Storage Management
- **Rotation Script**: `/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh`
- **Archive Directory**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`

### systemd Services

#### Commercial Log Import Services
- **Import Service**: `ctv-commercial-import.service`
- **Import Timer**: `ctv-commercial-import.timer` (daily at 1:00 AM)
- **Rotation Service**: `ctv-commercial-rotation.service`
- **Rotation Timer**: `ctv-commercial-rotation.timer` (weekly on Sundays at 2:30 AM)

#### Daily Update Processing Services
- **Update Service**: `ctv-daily-update.service`
- **Update Timer**: `ctv-daily-update.timer` (daily at 1:30 AM)

## Data Processing

### Sheet Sources
- **Commercials Sheet**: Primary commercial log data (~10,197 records typical)
- **Worldlink Lines Sheet**: Additional WorldLink data (~42 records typical)
- **Source Tracking**: Each record tagged with `filename:sheet_name` format

### File Retention
- **Recent Files**: Keep 7 days of individual Excel files
- **Monthly Archives**: Compress older files into monthly ZIP archives
- **Archive Retention**: Keep 12 months of archived data
- **Automatic Cleanup**: Files older than retention periods automatically deleted

### Storage Locations
- **Active Files**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/`
- **Archives**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`
- **Logs**: `/var/log/ctv-commercial-import/` and `/var/log/ctv-daily-update/`

## Processing Pipeline

### Stage 1: Commercial Log Import (1:00 AM)
1. Timer triggers daily at 1:00 AM (+5 min random delay)
2. Test connectivity to K: drive server (100.102.206.113)
3. Mount CIFS share with stored credentials
4. Read both "Commercials" and "Worldlink Lines" sheets
5. Combine sheets with `sheet_source` tracking column
6. Create single combined file in Pi project structure
7. Log operations with sheet-specific statistics

### Stage 2: Daily Update Processing (1:30 AM)
1. Timer triggers daily at 1:30 AM (+10 min random delay)
2. Read combined commercial log file with sheet source awareness
3. Automatically detect and create new markets
4. Process all sheets while maintaining source tracking
5. Apply business rules for language assignment
6. Commit changes with source tracking in database
7. Generate processing statistics and logs

## Critical Environment Configuration

**IMPORTANT**: The daily update service must use local files created by the commercial import service, NOT direct K drive access.

### Correct Configuration (/etc/ctv-daily-update.env):
```bash
# Configuration for automated daily update
# CRITICAL: Do NOT set DAILY_UPDATE_DATA_FILE - let script use local dated files
# DAILY_UPDATE_DATA_FILE="/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx"  # ❌ WRONG
DAILY_UPDATE_EXTRA_ARGS="--verbose"
# NTFY_TOPIC="ctv-daily-updates"
# SLACK_WEBHOOK_URL=""
```

### Data Flow - CORRECTED
1. **Stage 1 (01:00 AM)**: K drive multi-sheet source → Local combined storage 
   - Creates: `Commercial Log YYMMDD.xlsx` (28+ months historical data)
2. **Stage 2 (01:30 AM)**: Local combined storage → Database with source tracking
   - Processes: Full historical dataset including all Worldlink data
3. **Database Backup (02:05 AM)**: Database → Dropbox backup

**CRITICAL**: Stage 2 must use local files from Stage 1, NOT direct K drive access

## Daily Operations

### System Status
```bash
# View all CTV automation timers
systemctl list-timers | grep ctv

# Check recent activity
sudo systemctl status ctv-commercial-import.service
sudo systemctl status ctv-daily-update.service

# View recent logs
tail -20 /var/log/ctv-commercial-import/import.log
tail -20 /var/log/ctv-daily-update/update.log
```

### Commercial Log Import Operations
```bash
# View import timer status
systemctl list-timers | grep commercial

# Check recent import service activity
sudo systemctl status ctv-commercial-import.service

# Verify multi-sheet processing
uv run python -c "
import pandas as pd
file_path = 'data/raw/daily/Commercial Log $(date +%y%m%d).xlsx'
df = pd.read_excel(file_path, sheet_name='Commercials')
if 'sheet_source' in df.columns:
    print('Multi-sheet breakdown:', dict(df['sheet_source'].value_counts()))
else:
    print('Single-sheet file')
"

# Check storage status
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

### Daily Update Processing Operations
```bash
# View update timer status
systemctl list-timers | grep ctv-daily-update

# Check recent update service activity
sudo systemctl status ctv-daily-update.service

# Check database source tracking
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('''
    SELECT source_file, COUNT(*) as spots 
    FROM spots 
    WHERE source_file LIKE '%Commercial Log%'
    GROUP BY source_file 
    ORDER BY COUNT(*) DESC
    LIMIT 10
''')
print('Recent source file breakdown:')
for source_file, count in cursor.fetchall():
    print(f'  {source_file}: {count:,} spots')
conn.close()
"

# Follow update logs in real-time
tail -f /var/log/ctv-daily-update/update.log
```

### Manual Operations
```bash
# Run commercial log import immediately
sudo systemctl start ctv-commercial-import.service

# Run daily update processing immediately
sudo systemctl start ctv-daily-update.service

# Test daily update with preview
uv run python cli/daily_update.py data/raw/daily/Commercial\ Log\ $(date +%y%m%d).xlsx --auto-setup --dry-run --verbose

# Run actual daily update
uv run python cli/daily_update.py data/raw/daily/Commercial\ Log\ $(date +%y%m%d).xlsx --auto-setup --verbose
```

## Database Queries

```sql
-- Query all WorldLink data
SELECT * FROM spots WHERE source_file LIKE '%:Worldlink Lines';

-- Revenue comparison by source
SELECT 
    CASE 
        WHEN source_file LIKE '%:Worldlink Lines' THEN 'WorldLink' 
        WHEN source_file LIKE '%:Commercials' THEN 'Commercial'
        ELSE 'Other' 
    END as data_source,
    COUNT(*) as spots,
    SUM(station_net) as total_revenue
FROM spots 
WHERE source_file LIKE '%Commercial Log%'
GROUP BY 1;

-- Recent WorldLink activity
SELECT broadcast_month, COUNT(*), SUM(station_net) 
FROM spots 
WHERE source_file LIKE '%:Worldlink Lines'
GROUP BY broadcast_month 
ORDER BY broadcast_month DESC;

-- Daily multi-sheet breakdown
SELECT 
    DATE(load_date) as import_date,
    CASE 
        WHEN source_file LIKE '%:Worldlink Lines' THEN 'WorldLink' 
        WHEN source_file LIKE '%:Commercials' THEN 'Commercial'
        ELSE 'Other' 
    END as source,
    COUNT(*) as spots
FROM spots 
WHERE load_date >= date('now', '-7 days')
GROUP BY 1, 2
ORDER BY import_date DESC, spots DESC;
```

## Troubleshooting

### Missing Revenue/Incomplete Data Processing
**Symptoms**:
- Automated runs process fewer months than manual commands
- Missing Worldlink revenue data
- Logs show "Commercial Log.xlsx" instead of "Commercial Log YYMMDD.xlsx"

**Root Cause**: Environment misconfiguration causing K drive bypass

**Solution**:
```bash
# 1. Check environment configuration
sudo cat /etc/ctv-daily-update.env

# 2. If DAILY_UPDATE_DATA_FILE is set, comment it out:
sudo nano /etc/ctv-daily-update.env
# Change: DAILY_UPDATE_DATA_FILE="/mnt/k-drive/..."
# To:     # DAILY_UPDATE_DATA_FILE="/mnt/k-drive/..."

# 3. Reload systemd configuration
sudo systemctl daemon-reload

# 4. Test the fix
sudo -u daseme /opt/apps/ctv-bookedbiz-db/bin/daily_update.sh
```

### Commercial Log Import Issues
```bash
# Check if K drive is mounted
mount | grep k-drive

# Test network connectivity
ping -c 2 100.102.206.113

# View detailed import service logs
sudo journalctl -u ctv-commercial-import.service -n 20

# Check if both sheets exist in source file
uv run python -c "
from openpyxl import load_workbook
wb = load_workbook('/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx', read_only=True)
print('Available sheets:', wb.sheetnames)
print('Commercials found:', 'Commercials' in wb.sheetnames)
print('Worldlink Lines found:', 'Worldlink Lines' in wb.sheetnames)
wb.close()
"

# Check disk space usage
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/daily/
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/archive/
```

### Daily Update Processing Issues
```bash
# View detailed update service logs
sudo journalctl -u ctv-daily-update.service -n 20

# Check database accessibility
ls -la /opt/apps/ctv-bookedbiz-db/data/database/production.db

# Test update script manually
cd /opt/apps/ctv-bookedbiz-db
sudo -u daseme bin/daily_update.sh

# Verify multi-sheet data in database
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('''
    SELECT 
        CASE WHEN source_file LIKE '%:Worldlink Lines' THEN 'WorldLink' ELSE 'Commercial' END as source,
        COUNT(*) 
    FROM spots 
    WHERE import_batch_id = (SELECT batch_id FROM import_batches ORDER BY import_date DESC LIMIT 1)
    GROUP BY 1
''')
print('Latest batch breakdown:', dict(cursor.fetchall()))
conn.close()
"
```

## Verification Commands

### Verify Correct File Processing
```bash
# Check what file the automated system processes
grep "File:" /var/log/ctv-daily-update/update.log | tail -5

# Should show: "Commercial Log YYMMDD.xlsx" (with date)
# NOT: "Commercial Log.xlsx" (without date)
```

### Verify Worldlink Data Processing
```bash
# Check if Worldlink data is being imported
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('SELECT COUNT(*) FROM spots WHERE source_file LIKE \"%Worldlink%\" AND DATE(load_date) = DATE(\"now\")')
worldlink_count = cursor.fetchone()[0]
print(f'Worldlink records today: {worldlink_count}')
conn.close()
"

# Should show: 40-50+ records (not 0)
```

### Verify Full Dataset Processing  
```bash
# Compare record counts between manual and automated
echo "Manual command record count:"
uv run python cli/daily_update.py "data/raw/daily/Commercial Log $(date +%y%m%d).xlsx" --auto-setup --force --dry-run | grep "spots across"

echo "Automated service record count:" 
grep "spots across" /var/log/ctv-daily-update/update.log | tail -1

# Should match (both showing 28+ months, 260k+ spots)
```

## System Monitoring

### Quick Health Check
```bash
# Check if both systems are scheduled and operational
systemctl list-timers | grep ctv

# Check recent success/failure status
grep "SUCCESS\|ERROR" /var/log/ctv-commercial-import/import.log | tail -5
grep "SUCCESS\|ERROR" /var/log/ctv-daily-update/wrapper.log | tail -5

# Verify correct data source processing
grep -E "File:|Processing.*file" /var/log/ctv-daily-update/update.log | tail -10

# Check for data source configuration issues
if grep -q "Commercial Log.xlsx" /var/log/ctv-daily-update/update.log; then
    echo "⚠️  WARNING: System is bypassing local files and using K drive directly"
    echo "   This causes incomplete data processing. Check /etc/ctv-daily-update.env"
else
    echo "✅ System correctly using local dated files"
fi
```

### Performance Monitoring
```bash
# View processing times and record counts
grep -E "Duration:|Sheet breakdown:" /var/log/ctv-daily-update/update.log | tail -15

# Check for recurring errors
grep -E "ERROR|WorldLink" /var/log/ctv-daily-update/update.log | tail -10
grep -E "ERROR|WorldLink" /var/log/ctv-commercial-import/import.log | tail -10

# Monitor storage usage trends
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/daily/ /opt/apps/ctv-bookedbiz-db/data/raw/archive/

# Check database source tracking health
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('''
    SELECT 
        COUNT(*) as total_spots,
        COUNT(CASE WHEN source_file LIKE '%:Commercials' THEN 1 END) as commercial_spots,
        COUNT(CASE WHEN source_file LIKE '%:Worldlink Lines' THEN 1 END) as worldlink_spots,
        COUNT(CASE WHEN source_file LIKE '%Commercial Log%' AND source_file NOT LIKE '%:%' THEN 1 END) as old_format
    FROM spots 
    WHERE load_date >= date('now', '-7 days')
''')
result = cursor.fetchone()
print(f'Last 7 days: Total={result[0]:,}, Commercial={result[1]:,}, WorldLink={result[2]:,}, Old Format={result[3]:,}')
conn.close()
"
```

## Processing Results

### Commercial Log Import (Stage 1)
- **Commercials Sheet**: ~10,197 entries
- **Worldlink Lines Sheet**: ~42 entries  
- **Total Combined**: ~10,239 entries
- **File Size**: ~1.4MB Excel file
- **Processing Time**: ~20-25 seconds

### Daily Update Processing (Stage 2)
- **Records Imported**: ~10,239 database records
- **Sheet Breakdown**: Commercials: 10,197, WorldLink: 42
- **Processing Time**: ~30-35 seconds
- **New Markets**: 0-3 markets automatically created as needed
- **Language Assignment**: ~10,200+ spots categorized
- **Trade Records**: ~44 records filtered per business rules

## Integration with Workflow

### Complete Daily Schedule
- **1:00 AM**: Multi-sheet commercial log import from network share
- **1:30 AM**: Database processing with multi-sheet awareness, automatic market setup, and language assignment
- **2:05 AM**: Database backup to Dropbox (existing system)
- **2:30 AM (Sundays)**: File archival and cleanup

### Storage Efficiency
- **Active Files**: 7 × 1.4MB = ~10MB (combined multi-sheet logs)
- **Monthly Archives**: 12 × ~42MB = ~504MB (compressed multi-sheet data)
- **Log Files**: ~120MB annually (with rotation)
- **Total Storage**: ~634MB (with automatic cleanup)

## Security and Credentials

### Commercial Log Import
- Network share credentials stored in `/etc/ctv-commercial-import.env` (root:ctvapps 644)
- Service runs as `daseme` user with `ctvapps` group permissions
- K drive mount persists between runs for efficiency

### Daily Update Processing
- Configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 644)
- Service runs as `daseme` user with `ctvapps` group permissions
- Database access through existing connection patterns
- Unattended operation with error logging

### Environment File Security
- Daily update configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 644)
- **CRITICAL**: `DAILY_UPDATE_DATA_FILE` variable should be commented out to use local files
- Incorrect configuration causes system to bypass local processing and use K drive directly

## Monitoring and Alerting

### Built-in Monitoring
- **Structured Logging**: All operations logged with sheet-specific progress
- **Error Detection**: Proper exit codes and error messages for automated monitoring
- **Health Checks**: Prerequisites validated before each run
- **Performance Tracking**: Processing times and record counts logged

### Optional Notification Systems
Configure in respective environment files:
- **ntfy.sh**: Set `NTFY_TOPIC` in environment files for mobile push notifications
- **Slack**: Set `SLACK_WEBHOOK_URL` in environment files for team notifications

Both systems alert on import or processing failures for proactive monitoring.