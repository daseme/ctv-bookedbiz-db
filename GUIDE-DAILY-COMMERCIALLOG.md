# Enhanced Commercial Log Import and Daily Update System

## Overview
Automated daily pipeline for **multi-sheet** Commercial Log data processing on Raspberry Pi, consisting of two coordinated systems: (1) Enhanced Commercial Log import from K: drive network share processing both **Commercials** and **Worldlink Lines** sheets to local storage, and (2) Enhanced Daily update processing that imports the combined multi-sheet data into the CTV BookedBiz application database with automatic market setup, language assignment, and **comprehensive source tracking**.

## System Components

### Enhanced Commercial Log Import (Stage 1)
- **Source**: `/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx` (K: drive network share)
- **Processing**: **Multi-sheet processing** - Commercials + Worldlink Lines sheets
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial Log YYMMDD.xlsx` (combined data)
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/src/importers/commercial_log_importer.py` (enhanced with multi-sheet support)
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/commercial_import.sh` (enhanced with multi-sheet logging)
- **Environment File**: `/etc/ctv-commercial-import.env` (contains CIFS credentials)
- **Rotation Script**: `/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh`
- **Archive Directory**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`

### Enhanced Daily Update Processing (Stage 2)
- **Source**: Multi-sheet commercial log files from Stage 1 with sheet source tracking
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/database/production.db` with enhanced source_file tracking
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/cli/daily_update.py` (enhanced with multi-sheet awareness and unattended mode)
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/daily_update.sh` (enhanced with multi-sheet progress reporting)
- **Environment File**: `/etc/ctv-daily-update.env` (processing configuration)
- **Log Directory**: `/var/log/ctv-daily-update/`

### systemd Services

#### Enhanced Commercial Log Import Services
- **Import Service**: `ctv-commercial-import.service` (enhanced one-shot multi-sheet import process)
- **Import Timer**: `ctv-commercial-import.timer` (scheduled daily at 1:00 AM)
- **Rotation Service**: `ctv-commercial-rotation.service` (file archival and cleanup)
- **Rotation Timer**: `ctv-commercial-rotation.timer` (scheduled weekly on Sundays at 2:30 AM)

#### Enhanced Daily Update Processing Services
- **Update Service**: `ctv-daily-update.service` (enhanced one-shot database processing with multi-sheet support)
- **Update Timer**: `ctv-daily-update.timer` (scheduled daily at 1:30 AM)

## Enhanced Multi-Sheet Processing

### Sheet Sources Processed
- **Commercials Sheet**: Primary commercial log data (~10,197 records typical)
- **Worldlink Lines Sheet**: Additional WorldLink data (~42 records typical)
- **Combined Processing**: All data processed through identical business logic
- **Source Tracking**: Each record tagged with `filename:sheet_name` format for audit trail

### Enhanced Source Tracking
- **Database Field**: `source_file` contains format like `"Commercial Log 250904.xlsx:Commercials"`
- **Query Capability**: Easy filtering by data source for reporting and analysis
- **Audit Trail**: Complete lineage tracking from Excel sheet to database record
- **Reporting**: Sheet-specific breakdowns available in all logs and monitoring

## Storage Management

### File Retention Strategy
- **Recent Files**: Keep 7 days of individual Excel files (now containing combined multi-sheet data)
- **Monthly Archives**: Compress older files into monthly ZIP archives (`commercial-logs-YYYY-MM.zip`)
- **Archive Retention**: Keep 12 months of archived data
- **Automatic Cleanup**: Files and archives older than retention periods are automatically deleted

### Storage Locations
- **Active Files**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/` (last 7 days, combined multi-sheet data)
- **Archives**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/` (monthly ZIP files)
- **Rotation Logs**: `/var/log/ctv-commercial-import/rotation.log`
- **Update Logs**: `/var/log/ctv-daily-update/update.log` and `wrapper.log` (enhanced with sheet breakdowns)

### Automated Schedule
- **Daily 1:00 AM**: Import and combine commercial log multi-sheet file from K: drive
- **Daily 1:30 AM**: Process combined multi-sheet data into database (automatic market setup, language assignment with source tracking)
- **Daily 2:05 AM**: Database backup to Dropbox (existing system)
- **Sunday 2:30 AM**: Archive files older than 7 days, cleanup old archives

## Enhanced Processing Pipeline

### Stage 1: Enhanced Commercial Log Import (1:00 AM)
1. **Timer Activation**: systemd timer triggers daily at 1:00 AM (+5 min random delay)
2. **Network Check**: Tests connectivity to K: drive server (100.102.206.113)
3. **Mount K Drive**: Automatically mounts CIFS share with stored credentials
4. **Multi-Sheet Processing**: Python script reads both "Commercials" and "Worldlink Lines" sheets
5. **Data Combination**: Combines sheets with `sheet_source` tracking column
6. **Local Storage**: Creates single combined file with multi-sheet data in Pi project structure
7. **Enhanced Logging**: All operations logged with sheet-specific breakdowns and statistics

### Stage 2: Enhanced Daily Update Processing (1:30 AM)
1. **Timer Activation**: systemd timer triggers daily at 1:30 AM (+10 min random delay)
2. **Multi-Sheet Detection**: Reads combined commercial log file with sheet source awareness
3. **Market Setup**: Automatically detects and creates new markets from all sheet sources
4. **Enhanced Data Import**: Processes all sheets while maintaining source tracking in database
5. **Language Assignment**: Applies business rules to both Commercials and WorldLink data identically
6. **Database Update**: Commits all changes with enhanced `source_file` tracking
7. **Comprehensive Logging**: Sheet-specific breakdowns and processing statistics

## Daily Operations

### Check Enhanced System Status
```bash
# View all CTV automation timers
systemctl list-timers | grep ctv

# Check recent multi-sheet activity
sudo systemctl status ctv-commercial-import.service
sudo systemctl status ctv-daily-update.service

# View recent logs with multi-sheet breakdowns
tail -20 /var/log/ctv-commercial-import/import.log
tail -20 /var/log/ctv-daily-update/update.log
```

### Enhanced Commercial Log Import Operations
```bash
# View import timer status and next run time
systemctl list-timers | grep commercial

# Check recent multi-sheet import service activity
sudo systemctl status ctv-commercial-import.service

# View recent import logs (now shows sheet breakdowns)
tail -20 /var/log/ctv-commercial-import/import.log

# Verify multi-sheet processing in latest file
uv run python -c "
import pandas as pd
file_path = 'data/raw/daily/Commercial Log $(date +%y%m%d).xlsx'
df = pd.read_excel(file_path, sheet_name='Commercials')
if 'sheet_source' in df.columns:
    print('Multi-sheet breakdown:', dict(df['sheet_source'].value_counts()))
else:
    print('Single-sheet file')
"

# Check storage status and file counts
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

### Enhanced Daily Update Processing Operations
```bash
# View update timer status and next run time
systemctl list-timers | grep ctv-daily-update

# Check recent update service activity with multi-sheet awareness
sudo systemctl status ctv-daily-update.service

# View recent update logs (now shows sheet source breakdowns)
tail -20 /var/log/ctv-daily-update/update.log
tail -10 /var/log/ctv-daily-update/wrapper.log

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

### Enhanced Manual Operations
```bash
# Run enhanced multi-sheet commercial log import immediately
sudo systemctl start ctv-commercial-import.service

# Run enhanced daily update processing immediately
sudo systemctl start ctv-daily-update.service

# Test multi-sheet daily update with preview
uv run python cli/daily_update.py data/raw/daily/Commercial\ Log\ $(date +%y%m%d).xlsx --auto-setup --dry-run --verbose

# Run actual multi-sheet daily update
uv run python cli/daily_update.py data/raw/daily/Commercial\ Log\ $(date +%y%m%d).xlsx --auto-setup --verbose

# Check current and archived files
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial*.xlsx
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/archive/commercial-logs-*.zip

# View storage usage summary
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

### Enhanced Database Queries

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

### Troubleshooting

#### Enhanced Commercial Log Import Issues
```bash
# Check if K drive is mounted
mount | grep k-drive

# Test network connectivity
ping -c 2 100.102.206.113

# View detailed multi-sheet import service logs
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

#### Enhanced Daily Update Processing Issues
```bash
# View detailed update service logs with multi-sheet info
sudo journalctl -u ctv-daily-update.service -n 20

# Check database accessibility
ls -la /opt/apps/ctv-bookedbiz-db/data/database/production.db

# Test enhanced update script manually
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

# Check Python environment
/opt/apps/ctv-bookedbiz-db/.venv/bin/python --version
```

#### Check Enhanced System Integration
```bash
# Check all CTV timers
systemctl list-timers | grep ctv

# View environment configurations
sudo cat /etc/ctv-commercial-import.env
sudo cat /etc/ctv-daily-update.env

# Manually test rotation script
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status

# Test complete multi-sheet pipeline
sudo systemctl start ctv-commercial-import.service && sleep 30 && sudo systemctl start ctv-daily-update.service
```

## Enhanced File Processing Results

### Commercial Log Import (Stage 1) - Multi-Sheet
**Typical Output**:
- **Commercials Sheet**: ~10,197 entries
- **Worldlink Lines Sheet**: ~42 entries  
- **Total Combined**: ~10,239 entries
- **File Size**: ~1.4MB Excel file (combined data)
- **Processing Time**: ~20-25 seconds
- **Filename Format**: `Commercial Log 250904.xlsx` (contains both sheets)

### Daily Update Processing (Stage 2) - Enhanced
**Typical Output**:
- **Records Imported**: ~10,239 database records (multi-sheet)
- **Sheet Breakdown**: Commercials: 10,197, WorldLink: 42
- **Processing Time**: ~30-35 seconds (enhanced processing)
- **New Markets**: 0-3 markets automatically created as needed
- **Language Assignment**: ~10,200+ spots categorized and processed
- **Source Tracking**: All records tagged with `filename:sheet_name` format
- **Trade Records**: ~44 records filtered per business rules (expected)

## Integration with Existing Workflow

This enhanced dual-system pipeline integrates seamlessly with the broader Pi-based workflow while adding comprehensive multi-sheet processing:

### Enhanced Data Flow
1. **K: Drive Multi-Sheet Source** → **Pi Local Combined Storage** (Enhanced Commercial Log Import)
2. **Pi Combined Storage** → **Database with Source Tracking** (Enhanced Daily Update)
3. **Enhanced Database** → **Dropbox Backup** (Existing System)

### Complete Daily Schedule
- **1:00 AM**: Multi-sheet commercial log import from network share (Commercials + WorldLink)
- **1:30 AM**: Enhanced database processing with multi-sheet awareness, automatic market setup, and language assignment
- **2:05 AM**: Database backup to Dropbox (existing system)
- **2:30 AM (Sundays)**: File archival and cleanup

### Enhanced Storage Management
- **Data Collection**: Multi-sheet commercial log files stored locally in `data/raw/daily/`
- **Processing**: Recent files (7 days) processed automatically with full source tracking
- **Archival**: Automatic archival and cleanup maintains optimal disk usage
- **Backup**: Database changes backed up to Dropbox nightly
- **Historical Access**: Archived files available in monthly ZIP archives for up to 12 months
- **Monitoring**: Comprehensive logs with sheet-specific breakdowns integrated with existing Pi logging infrastructure

## Enhanced Storage Efficiency

**Before Multi-Sheet Enhancement**: 
- Single-sheet processing missed WorldLink data
- Manual intervention required for complete data capture

**After Multi-Sheet Enhancement**:
- **Active Files**: 7 × 1.4MB = ~10MB (combined multi-sheet logs)
- **Monthly Archives**: 12 × ~42MB = ~504MB (compressed multi-sheet data)
- **Log Files**: ~120MB annually (with enhanced logging and rotation)
- **Total Storage**: ~634MB (with automatic cleanup)
- **Processing**: Zero manual intervention, complete data capture
- **Reliability**: Automated error handling with multi-sheet awareness and recovery
- **Data Coverage**: 100% of available data sources processed automatically

## Security and Credentials

### Enhanced Commercial Log Import
- Network share credentials stored in `/etc/ctv-commercial-import.env` (root:ctvapps 644)
- Service runs as `daseme` user with `ctvapps` group permissions
- K drive mount persists between runs for efficiency
- Multi-sheet processing with concurrent run prevention via flock mechanism
- Enhanced error handling for sheet-specific issues

### Enhanced Daily Update Processing
- Configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 644)
- Service runs as `daseme` user with `ctvapps` group permissions
- Database access through existing connection patterns with enhanced source tracking
- Unattended operation with comprehensive multi-sheet error logging
- Concurrent run prevention via flock mechanism

## Enhanced Monitoring and Alerting

### Built-in Enhanced Monitoring
- **Multi-Sheet Structured Logging**: All operations logged with sheet-specific breakdowns and progress milestones
- **Enhanced Error Detection**: Proper exit codes and sheet-specific error messages for automated monitoring
- **Health Checks**: Prerequisites validated before each run, including sheet availability
- **Performance Tracking**: Processing times and record counts logged per sheet source
- **Source Tracking Verification**: Database source_file format validation in logs

### Optional Notification Systems
Configure in respective environment files:
- **ntfy.sh**: Set `NTFY_TOPIC` in environment files for mobile push notifications
- **Slack**: Set `SLACK_WEBHOOK_URL` in environment files for team notifications

Both systems will alert on import or processing failures with multi-sheet specific error details for proactive monitoring.

## Enhanced System Status Commands

### Quick Multi-Sheet Health Check
```bash
# Check if both systems are scheduled and operational
systemctl list-timers | grep ctv

# Check recent success/failure status with multi-sheet info
grep "Multi-sheet\|SUCCESS\|ERROR" /var/log/ctv-commercial-import/import.log | tail -5
grep "Multi-sheet\|SUCCESS\|ERROR" /var/log/ctv-daily-update/wrapper.log | tail -5

# Check today's multi-sheet activity
grep "$(date '+%Y-%m-%d')" /var/log/ctv-daily-update/update.log | grep -i "sheet\|worldlink\|commercials"

# Verify latest multi-sheet processing
uv run python -c "
import pandas as pd
import glob
files = glob.glob('data/raw/daily/Commercial Log *.xlsx')
if files:
    latest = max(files)
    df = pd.read_excel(latest, sheet_name='Commercials')
    if 'sheet_source' in df.columns:
        print(f'Latest file: {latest}')
        print('Sheet breakdown:', dict(df['sheet_source'].value_counts()))
    else:
        print('No sheet source tracking found')
else:
    print('No commercial log files found')
"
```

### Enhanced Performance Monitoring
```bash
# View processing times and record counts with sheet breakdowns
grep -E "Duration:|Sheet breakdown:|Multi-sheet" /var/log/ctv-daily-update/update.log | tail -15

# Check for any recurring multi-sheet errors
grep -E "ERROR|WorldLink|sheet.*error" /var/log/ctv-daily-update/update.log | tail -10
grep -E "ERROR|WorldLink|sheet.*error" /var/log/ctv-commercial-import/import.log | tail -10

# Monitor enhanced storage usage trends
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

This comprehensive enhanced automation system ensures reliable, hands-free daily processing of **multi-sheet** commercial log data with full source tracking, complete visibility into operations, errors, and performance metrics across both Commercials and WorldLink data sources.