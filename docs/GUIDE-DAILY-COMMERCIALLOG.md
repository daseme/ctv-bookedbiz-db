# Commercial Log Import and Daily Update System

## Overview
Automated daily pipeline for multi-sheet Commercial Log data processing, consisting of two coordinated systems: (1) Commercial Log import from K: drive network share processing both **Commercials** and **Worldlink Lines** sheets to local storage, and (2) Daily update processing that imports the combined data into the CTV BookedBiz database with automatic market setup, language assignment, and source tracking.

## System Components

### Commercial Log Import (Stage 1)
- **Source**: `/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx` ⚠️ Note: "Media library" (lowercase 'l')
- **Network Share**: `//100.102.206.113/K Drive` ⚠️ Note: "K Drive" (with space)
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
- **Rotation Script**: `/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh` ⚠️ Known Issue: Fails with filenames containing spaces
- **Archive Directory**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`

### Network Access and Mounting
- **K Drive Credentials**: `/etc/cifs-credentials` (secured with 600 permissions)
- **Automatic Mounting**: Configured via `/etc/fstab` with systemd automount
- **Dependencies**: Requires Tailscale connectivity to reach 100.102.206.113

### systemd Services

#### Commercial Log Import Services
- **Import Service**: `ctv-commercial-import.service`
- **Import Timer**: `ctv-commercial-import.timer` (daily at 1:00 AM)
- **Rotation Service**: `ctv-commercial-rotation.service` ⚠️ Currently failing due to space handling
- **Rotation Timer**: `ctv-commercial-rotation.timer` (weekly on Sundays at 2:30 AM)

#### Daily Update Processing Services
- **Update Service**: `ctv-daily-update.service`
- **Update Timer**: `ctv-daily-update.timer` (daily at 1:30 AM)

## Business Logic

### Import Mode: WEEKLY_UPDATE (Daily Operations)

The daily update uses `WEEKLY_UPDATE` mode which implements the following business rules:

#### Month Classification
1. **Closed Months**: Months that have been officially closed (finalized for reporting). These are **never modified** by daily updates.
2. **Open Months**: Months that are still active and can receive updates.

#### Data Replacement Rules

For each **open month** found in the Excel file:
1. Delete all existing records for that month
2. Import new records from Excel for that month

**Critical Safeguard - Month Preservation:**

If an open month exists in the database but has **NO data in the incoming Excel file**, the system will **preserve the existing data** rather than deleting it. This protects against data loss when:
- The daily commercial log ages out previous month data
- A month hasn't been closed yet but is no longer in the daily feed
- There's a gap in the source data

Example scenario:
```
Database has: Nov-24 (closed), Dec-24 (open, 8,500 records), Jan-25 (open, 2,100 records)
Excel contains: Jan-25 (9,200 records) - December aged out of daily log

Result:
- Nov-24: SKIPPED (closed month, protected)
- Dec-24: PRESERVED (open but no Excel data, existing 8,500 records kept)
- Jan-25: REPLACED (deleted 2,100, imported 9,200)
```

#### Processing Flow

```
1. Analyze Excel file
   ├── Extract all broadcast months present
   ├── Count records per month
   └── Identify sheet sources (Commercials, Worldlink Lines)

2. Classify months
   ├── Query closed_months table
   ├── Separate into closed vs open months
   └── Log status of each

3. Preservation check (WEEKLY_UPDATE only)
   ├── For each open month with 0 records in Excel:
   │   ├── Check if database has existing records
   │   ├── If yes: ADD to preservation list
   │   └── Log preserved month with record count and revenue
   └── Remove preserved months from processing list

4. Execute import (for non-preserved open months only)
   ├── Delete existing records for target months
   ├── Import new records from Excel
   ├── Validate customer alignment
   └── Auto-correct any mismatches

5. Post-import validation
   ├── Verify customer_id alignment with normalization system
   ├── Log any corrections made
   └── Fail if alignment cannot be achieved
```

### Import Mode: HISTORICAL

Used for bulk historical imports. Key differences from WEEKLY_UPDATE:
- Processes ALL months in Excel (ignores closed status)
- Closes all imported months after successful import
- Requires `--closed-by` parameter to track who closed the months

### Import Mode: MANUAL

Used for targeted manual corrections:
- Fails if Excel contains any closed months
- No automatic month preservation (assumes intentional)
- Requires explicit handling of closed month conflicts

## Data Processing

### Sheet Sources
- **Commercials Sheet**: Primary commercial log data (~10,197 records typical)
- **Worldlink Lines Sheet**: Additional WorldLink data (~42 records typical)
- **Source Tracking**: Each record tagged with `filename:sheet_name` format

### Customer Normalization

The import system integrates with the customer normalization system:
1. **Pre-import**: Bill codes are added to `raw_customer_inputs` table
2. **During import**: Customer IDs resolved via `v_customer_normalization_audit` view
3. **Post-import validation**: Verifies all spots align with normalization system
4. **Auto-correction**: Fixes any customer_id mismatches automatically

### File Retention
- **Recent Files**: Keep 7 days of individual Excel files
- **Monthly Archives**: Compress older files into monthly ZIP archives ⚠️ Currently failing
- **Archive Retention**: Keep 12 months of archived data
- **Automatic Cleanup**: Files older than retention periods automatically deleted

### Storage Locations
- **Active Files**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/`
- **Archives**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`
- **Logs**: `/var/log/ctv-commercial-import/` and `/var/log/ctv-daily-update/`

## Processing Pipeline

### Stage 1: Commercial Log Import (1:00 AM)
1. Timer triggers daily at 1:00 AM (+5 min random delay)
2. Test connectivity to K: drive server (100.102.206.113) via Tailscale
3. Mount CIFS share with stored credentials (if not already mounted via automount)
4. Read both "Commercials" and "Worldlink Lines" sheets from `/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx`
5. Combine sheets with `sheet_source` tracking column
6. Create single combined file in Pi project structure
7. Log operations with sheet-specific statistics

### Stage 2: Daily Update Processing (1:30 AM)
1. Timer triggers daily at 1:30 AM (+10 min random delay)
2. Read combined commercial log file with sheet source awareness
3. **Analyze months in Excel and compare against database**
4. **Preserve open months that have no Excel data** (safeguard)
5. Automatically detect and create new markets
6. Process non-preserved open months (delete + reimport)
7. Apply business rules for language assignment
8. Validate and correct customer alignment
9. Commit changes with source tracking in database
10. Generate processing statistics and logs

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
   - **Preserves**: Open months with no Excel data
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

### Network and Mount Status
```bash
# Check Tailscale connectivity
sudo tailscale status

# Verify K drive mount status  
mount | grep k-drive

# Test K drive access
ls -la "/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx"

# Manual mount if needed
sudo mount /mnt/k-drive
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

# Check storage status (rotation currently failing)
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

# Test daily update with preview (shows preservation logic)
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

-- Check preserved months (open months with data but not in recent imports)
SELECT 
    broadcast_month,
    COUNT(*) as spots,
    SUM(gross_rate) as gross_revenue,
    MAX(load_date) as last_updated
FROM spots
WHERE broadcast_month NOT IN (
    SELECT DISTINCT broadcast_month 
    FROM spots 
    WHERE DATE(load_date) = DATE('now')
)
AND broadcast_month NOT IN (
    SELECT broadcast_month FROM closed_months
)
GROUP BY broadcast_month
ORDER BY broadcast_month DESC;
```

## Troubleshooting

### Post-Reboot System Recovery
After a power outage or system reboot, follow these steps to verify system integrity:

#### 1. Verify Tailscale Connectivity
```bash
# Check Tailscale status
sudo tailscale status

# Look for 100.102.206.113 etere-datamover in the list
# Should show as 'active' or '-' (not 'offline')
```

#### 2. Verify K Drive Access
```bash
# Check if automount is active
mount | grep k-drive
# Should show: systemd-1 on /mnt/k-drive type autofs

# Test access (triggers actual mount)
ls -la "/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx"

# Manual mount if needed
sudo mount /mnt/k-drive
```

#### 3. Verify Services and Timers
```bash
# Check that timers are active and scheduled
systemctl list-timers | grep ctv

# If timers are inactive, restart them
sudo systemctl enable --now ctv-commercial-import.timer
sudo systemctl enable --now ctv-daily-update.timer
```

#### 4. Test Import Process
```bash
# Test commercial import
sudo systemctl start ctv-commercial-import.service

# Check logs
tail -10 /var/log/ctv-commercial-import/import.log

# Should show successful import with file creation
```

### Month Preservation Issues

**Symptom**: Open month data unexpectedly deleted

**Check preservation logic**:
```bash
# Run dry-run to see preservation decisions
uv run python cli/daily_update.py data/raw/daily/Commercial\ Log\ $(date +%y%m%d).xlsx --auto-setup --dry-run --verbose

# Look for output like:
# "⚠️ PRESERVATION: Protecting 1 open month(s) with no Excel data:"
# "   Dec-24: 8,500 existing records, $234,567.00 revenue - PRESERVED"
```

**If preservation didn't trigger when expected**:
1. Verify the month is NOT in `closed_months` table
2. Verify the Excel file truly has 0 records for that month
3. Check logs for any errors in `_get_months_with_data_in_excel`

### Path and Case Sensitivity Issues
**Symptoms**:
- "Source file not found" errors in logs
- Import service failing with exit code 1

**Common Causes**:
1. **Incorrect share name**: Use "K Drive" (with space), not "Traffic"
2. **Case sensitivity**: Directory is "Media library" (lowercase 'l'), not "Media Library"
3. **Network connectivity**: Ensure Tailscale is running and etere-datamover is accessible

**Solution Steps**:
```bash
# 1. Verify network share name
smbclient -L 100.102.206.113 -U usrjp%Cro88ings -W CTVETERE
# Should show "K Drive" in the list

# 2. Check directory case sensitivity
ls -la /mnt/k-drive/Traffic/
# Should show "Media library" directory

# 3. Fix import script if needed
grep -n "Media Library" /opt/apps/ctv-bookedbiz-db/src/importers/commercial_log_importer.py
# Change any "Media Library" to "Media library"
```

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

### Network Mount Issues
**Symptoms**:
- K drive not mounted after reboot
- "Connection timed out" or "Operation now in progress" errors

**Solutions**:
```bash
# Check if Tailscale is connected
sudo tailscale status | grep etere-datamover

# Test network connectivity
ping -c 2 100.102.206.113

# Check fstab configuration
tail -3 /etc/fstab
# Should have automount options: noauto,x-systemd.automount,x-systemd.device-timeout=30

# Manual mount test
sudo mount /mnt/k-drive

# Check for boot-time mount errors
sudo journalctl -b | grep -i "k-drive\|cifs\|mount"
```

### File Rotation Issues ⚠️ KNOWN ISSUE
**Symptoms**:
- ctv-commercial-rotation.service fails with exit code 12
- Archive directory remains empty despite log showing "Archiving X files"

**Root Cause**: 
The rotation script improperly handles filenames with spaces. Filenames like "Commercial Log 250915.xlsx" are split into separate array elements: "Commercial", "Log", "250915.xlsx", causing zip command to fail.

**Current Status**: Issue identified but not yet fixed

**Workaround**:
```bash
# Manual cleanup of old files (if disk space becomes an issue)
find /opt/apps/ctv-bookedbiz-db/data/raw/daily/ -name "Commercial Log *.xlsx" -mtime +7 -exec ls -la {} \;

# Manual archive creation (if needed)
cd /opt/apps/ctv-bookedbiz-db/data/raw/daily/
zip "../archive/manual-archive-$(date +%Y%m).zip" Commercial\ Log\ *.xlsx
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
wb = load_workbook('/mnt/k-drive/Traffic/Media library/Commercial Log.xlsx', read_only=True)
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

### Verify Month Preservation Logic
```bash
# Check preservation decisions in recent logs
grep -E "PRESERVATION|PRESERVED|NO DATA in Excel" /var/log/ctv-daily-update/update.log | tail -20

# Query for potentially preserved months
uv run python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')

# Get open months not updated today
cursor = conn.execute('''
    SELECT 
        s.broadcast_month,
        COUNT(*) as spots,
        ROUND(SUM(s.gross_rate), 2) as revenue,
        MAX(DATE(s.load_date)) as last_updated
    FROM spots s
    LEFT JOIN closed_months cm ON s.broadcast_month = cm.broadcast_month
    WHERE cm.broadcast_month IS NULL
    GROUP BY s.broadcast_month
    ORDER BY s.broadcast_month DESC
''')

print('Open months in database:')
for row in cursor.fetchall():
    status = '(updated today)' if row[3] == str(__import__('datetime').date.today()) else '(preserved)'
    print(f'  {row[0]}: {row[1]:,} spots, \${row[2]:,.2f} {status}')
conn.close()
"
```

### Verify Network Dependencies
```bash
# Check automount trigger
ls -la /mnt/k-drive/ >/dev/null 2>&1 && echo "Automount triggered successfully" || echo "Automount failed"

# Verify Tailscale connectivity
tailscale ping 100.102.206.113

# Test CIFS connectivity
smbclient -L 100.102.206.113 -U usrjp%Cro88ings -W CTVETERE >/dev/null 2>&1 && echo "CIFS server accessible" || echo "CIFS server unreachable"
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

# Check for preservation activity
grep -E "PRESERVATION|PRESERVED" /var/log/ctv-daily-update/update.log | tail -5

# Check for data source configuration issues
if grep -q "Commercial Log.xlsx" /var/log/ctv-daily-update/update.log; then
    echo "⚠️  WARNING: System is bypassing local files and using K drive directly"
    echo "   This causes incomplete data processing. Check /etc/ctv-daily-update.env"
else
    echo "✅ System correctly using local dated files"
fi

# Verify network dependencies
echo "=== Network Status ==="
sudo tailscale status | grep etere-datamover
mount | grep k-drive
ping -c 1 100.102.206.113 >/dev/null 2>&1 && echo "✅ K drive server reachable" || echo "❌ K drive server unreachable"
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
- **Records Imported**: ~10,239 database records (varies by month content)
- **Sheet Breakdown**: Commercials: 10,197, WorldLink: 42
- **Processing Time**: ~30-35 seconds
- **New Markets**: 0-3 markets automatically created as needed
- **Language Assignment**: ~10,200+ spots categorized
- **Trade Records**: ~44 records filtered per business rules
- **Month Preservation**: Open months without Excel data protected

## Integration with Workflow

### Complete Daily Schedule
- **1:00 AM**: Multi-sheet commercial log import from network share
- **1:30 AM**: Database processing with multi-sheet awareness, automatic market setup, month preservation, and language assignment
- **2:05 AM**: Database backup to Dropbox (existing system)
- **2:30 AM (Sundays)**: File archival and cleanup ⚠️ Currently failing

### Storage Efficiency
- **Active Files**: 7 × 1.4MB = ~10MB (combined multi-sheet logs)
- **Monthly Archives**: 12 × ~42MB = ~504MB (compressed multi-sheet data) ⚠️ Archives not created due to rotation failure
- **Log Files**: ~120MB annually (with rotation)
- **Total Storage**: ~634MB (with automatic cleanup when rotation works)

## Security and Credentials

### Commercial Log Import
- Network share credentials stored in `/etc/cifs-credentials` (root:root 600)
- Service runs as `daseme` user with `ctvapps` group permissions
- K drive mount persists between runs via systemd automount

### Daily Update Processing
- Configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 644)
- Service runs as `daseme` user with `ctvapps` group permissions
- Database access through existing connection patterns
- Unattended operation with error logging

### Environment File Security
- Daily update configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 644)
- **CRITICAL**: `DAILY_UPDATE_DATA_FILE` variable should be commented out to use local files
- Incorrect configuration causes system to bypass local processing and use K drive directly

### Network Security
- Access to K drive via Tailscale VPN (100.102.206.113)
- CIFS credentials secured with restricted permissions
- Automatic mounting configured for resilience across reboots

## Monitoring and Alerting

### Built-in Monitoring
- **Structured Logging**: All operations logged with sheet-specific progress
- **Error Detection**: Proper exit codes and error messages for automated monitoring
- **Health Checks**: Prerequisites validated before each run
- **Performance Tracking**: Processing times and record counts logged
- **Network Monitoring**: Automatic mount triggers and connectivity validation
- **Preservation Logging**: Clear indication when months are preserved

### Optional Notification Systems
Configure in respective environment files:
- **ntfy.sh**: Set `NTFY_TOPIC` in environment files for mobile push notifications
- **Slack**: Set `SLACK_WEBHOOK_URL` in environment files for team notifications

Both systems alert on import or processing failures for proactive monitoring.

## Known Issues and Limitations

### File Rotation Script (ctv-commercial-rotation.service)
- **Status**: Failing with exit code 12
- **Cause**: Improper handling of filenames with spaces in bash arrays
- **Impact**: Old files accumulate in daily directory, no monthly archives created
- **Workaround**: Manual cleanup if disk space becomes limited
- **Resolution**: Requires script modification to properly quote filenames with spaces

### Path Case Sensitivity
- **Issue**: Directory names are case-sensitive on the network share
- **Requirement**: Must use exact case "Media library" (lowercase 'l')
- **Fixed**: Import script updated to use correct case

### Network Timing Dependencies
- **Issue**: K drive mount requires Tailscale to be fully connected
- **Solution**: Implemented systemd automount with timeout handling
- **Status**: Resolved with fstab automount configuration

### Environment Configuration Sensitivity
- **Issue**: Environment variables can cause service to bypass local files
- **Critical**: DAILY_UPDATE_DATA_FILE must remain commented out
- **Impact**: Incorrect configuration causes incomplete data processing
- **Monitoring**: Log analysis can detect incorrect file usage patterns