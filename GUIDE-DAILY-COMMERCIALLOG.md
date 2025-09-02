# Commercial Log Import and Daily Update System

## Overview
Automated daily pipeline for Commercial Log data processing on Raspberry Pi, consisting of two coordinated systems: (1) Commercial Log import from K: drive network share to local storage, and (2) Daily update processing that imports the data into the CTV BookedBiz application database with automatic market setup and language assignment.

## System Components

### Commercial Log Import (Stage 1)
- **Source**: `/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx` (K: drive network share)
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial Log YYMMDD.xlsx`
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/src/importers/commercial_log_importer.py`
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/commercial_import.sh`
- **Environment File**: `/etc/ctv-commercial-import.env` (contains CIFS credentials)
- **Rotation Script**: `/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh`
- **Archive Directory**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`

### Daily Update Processing (Stage 2)
- **Source**: Local commercial log files from Stage 1
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/database/production.db`
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/cli/daily_update.py` (enhanced with unattended mode)
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/daily_update.sh`
- **Environment File**: `/etc/ctv-daily-update.env` (processing configuration)
- **Log Directory**: `/var/log/ctv-daily-update/`

### systemd Services

#### Commercial Log Import Services
- **Import Service**: `ctv-commercial-import.service` (one-shot import process)
- **Import Timer**: `ctv-commercial-import.timer` (scheduled daily at 1:00 AM)
- **Rotation Service**: `ctv-commercial-rotation.service` (file archival and cleanup)
- **Rotation Timer**: `ctv-commercial-rotation.timer` (scheduled weekly on Sundays at 2:30 AM)

#### Daily Update Processing Services
- **Update Service**: `ctv-daily-update.service` (one-shot database processing)
- **Update Timer**: `ctv-daily-update.timer` (scheduled daily at 1:30 AM)

## Storage Management

### File Retention Strategy
- **Recent Files**: Keep 7 days of individual Excel files for immediate access
- **Monthly Archives**: Compress older files into monthly ZIP archives (`commercial-logs-YYYY-MM.zip`)
- **Archive Retention**: Keep 12 months of archived data
- **Automatic Cleanup**: Files and archives older than retention periods are automatically deleted

### Storage Locations
- **Active Files**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/` (last 7 days)
- **Archives**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/` (monthly ZIP files)
- **Rotation Logs**: `/var/log/ctv-commercial-import/rotation.log`
- **Update Logs**: `/var/log/ctv-daily-update/update.log` and `wrapper.log`

### Automated Schedule
- **Daily 1:00 AM**: Import new commercial log file from K: drive
- **Daily 1:30 AM**: Process commercial log data into database (automatic market setup, language assignment)
- **Daily 2:05 AM**: Database backup to Dropbox (existing system)
- **Sunday 2:30 AM**: Archive files older than 7 days, cleanup old archives

## Processing Pipeline

### Stage 1: Commercial Log Import (1:00 AM)
1. **Timer Activation**: systemd timer triggers daily at 1:00 AM (+5 min random delay)
2. **Network Check**: Tests connectivity to K: drive server (100.102.206.113)
3. **Mount K Drive**: Automatically mounts CIFS share with stored credentials
4. **File Copy**: Python script copies "Commercials" sheet from Excel file
5. **Local Storage**: Creates dated file in Pi project structure
6. **Logging**: All operations logged with timestamps and status

### Stage 2: Daily Update Processing (1:30 AM)
1. **Timer Activation**: systemd timer triggers daily at 1:30 AM (+10 min random delay)
2. **File Detection**: Reads latest commercial log file from local storage
3. **Market Setup**: Automatically detects and creates new markets if needed
4. **Data Import**: Replaces open month data while protecting closed historical months
5. **Language Assignment**: Applies business rules and categorizes spots automatically
6. **Database Update**: Commits all changes to production database
7. **Comprehensive Logging**: All operations logged for monitoring and debugging

## Daily Operations

### Check Overall System Status
```bash
# View all CTV automation timers
systemctl list-timers | grep ctv

# Check recent activity across both systems
sudo systemctl status ctv-commercial-import.service
sudo systemctl status ctv-daily-update.service

# View recent logs from both systems
tail -20 /var/log/ctv-commercial-import/import.log
tail -20 /var/log/ctv-daily-update/update.log
```

### Commercial Log Import Operations
```bash
# View import timer status and next run time
systemctl list-timers | grep commercial

# Check recent import service activity
sudo systemctl status ctv-commercial-import.service

# View recent import logs
tail -20 /var/log/ctv-commercial-import/import.log

# Check storage status and file counts
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

### Daily Update Processing Operations
```bash
# View update timer status and next run time
systemctl list-timers | grep ctv-daily-update

# Check recent update service activity
sudo systemctl status ctv-daily-update.service

# View recent update logs
tail -20 /var/log/ctv-daily-update/update.log
tail -10 /var/log/ctv-daily-update/wrapper.log

# Follow update logs in real-time
tail -f /var/log/ctv-daily-update/update.log
```

### Manual Operations
```bash
# Run commercial log import immediately
sudo systemctl start ctv-commercial-import.service

# Run daily update processing immediately
sudo systemctl start ctv-daily-update.service

# Run file rotation immediately
sudo systemctl start ctv-commercial-rotation.service

# Check current and archived files
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial*.xlsx
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/archive/commercial-logs-*.zip

# View storage usage summary
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

### Troubleshooting

#### Commercial Log Import Issues
```bash
# Check if K drive is mounted
mount | grep k-drive

# Test network connectivity
ping -c 2 100.102.206.113

# View detailed import service logs
sudo journalctl -u ctv-commercial-import.service -n 20

# Check disk space usage
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/daily/
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/archive/
```

#### Daily Update Processing Issues
```bash
# View detailed update service logs
sudo journalctl -u ctv-daily-update.service -n 20

# Check database accessibility
ls -la /opt/apps/ctv-bookedbiz-db/data/database/production.db

# Test update script manually
cd /opt/apps/ctv-bookedbiz-db
sudo -u daseme bin/daily_update.sh

# Check Python environment
/opt/apps/ctv-bookedbiz-db/.venv/bin/python --version
```

#### Check System Integration
```bash
# Check all CTV timers
systemctl list-timers | grep ctv

# View environment configurations
sudo cat /etc/ctv-commercial-import.env
sudo cat /etc/ctv-daily-update.env

# Manually test rotation script
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status
```

## File Processing Results

### Commercial Log Import (Stage 1)
**Typical Output**:
- **Rows Processed**: ~15,500 commercial log entries
- **File Size**: ~2MB Excel file
- **Processing Time**: ~15 seconds
- **Filename Format**: `Commercial Log 250828.xlsx` (YYMMDD format)

### Daily Update Processing (Stage 2)
**Typical Output**:
- **Records Imported**: ~15,576 database records
- **Processing Time**: ~25-30 seconds
- **New Markets**: 0-3 markets automatically created as needed
- **Language Assignment**: ~15,000+ spots categorized and processed
- **Trade Records**: ~44 records filtered per business rules (expected)

## Integration with Existing Workflow

This dual-system pipeline integrates seamlessly with the broader Pi-based workflow:

### Data Flow
1. **K: Drive** → **Pi Local Storage** (Commercial Log Import)
2. **Pi Local Storage** → **Database Processing** (Daily Update)
3. **Database** → **Dropbox Backup** (Existing System)

### Complete Daily Schedule
- **1:00 AM**: Commercial log import from network share
- **1:30 AM**: Database processing with automatic market setup and language assignment
- **2:05 AM**: Database backup to Dropbox
- **2:30 AM (Sundays)**: File archival and cleanup

### Storage Management
- **Data Collection**: Commercial log files stored locally in `data/raw/daily/`
- **Processing**: Recent files (7 days) processed automatically into database
- **Archival**: Automatic archival and cleanup maintains optimal disk usage
- **Backup**: Database changes backed up to Dropbox nightly
- **Historical Access**: Archived files available in monthly ZIP archives for up to 12 months
- **Monitoring**: Comprehensive logs integrated with existing Pi logging infrastructure

## Storage Efficiency

**Before Automation**: 
- Manual processing required daily intervention
- Risk of missed updates and data inconsistency

**After Full Automation**:
- **Active Files**: 7 × 2MB = ~14MB (commercial logs)
- **Monthly Archives**: 12 × ~60MB = ~720MB (compressed)
- **Log Files**: ~100MB annually (with rotation)
- **Total Storage**: ~834MB (with automatic cleanup)
- **Processing**: Zero manual intervention required
- **Reliability**: Automated error handling and recovery

## Security and Credentials

### Commercial Log Import
- Network share credentials stored in `/etc/ctv-commercial-import.env` (root:ctvapps 640)
- Service runs as `daseme` user with `ctvapps` group permissions
- K drive mount persists between runs for efficiency
- Concurrent run prevention via flock mechanism

### Daily Update Processing
- Configuration stored in `/etc/ctv-daily-update.env` (root:ctvapps 640)
- Service runs as `daseme` user with `ctvapps` group permissions
- Database access through existing connection patterns
- Unattended operation with comprehensive error logging
- Concurrent run prevention via flock mechanism

## Monitoring and Alerting

### Built-in Monitoring
- **Structured Logging**: All operations logged with timestamps and progress milestones
- **Error Detection**: Proper exit codes and error messages for automated monitoring
- **Health Checks**: Prerequisites validated before each run
- **Performance Tracking**: Processing times and record counts logged

### Optional Notification Systems
Configure in respective environment files:
- **ntfy.sh**: Set `NTFY_TOPIC` in environment files for mobile push notifications
- **Slack**: Set `SLACK_WEBHOOK_URL` in environment files for team notifications

Both systems will alert on import or processing failures for proactive monitoring.

## System Status Commands

### Quick Health Check
```bash
# Check if both systems are scheduled and operational
systemctl list-timers | grep ctv

# Check recent success/failure status
grep "SUCCESS\|ERROR" /var/log/ctv-commercial-import/import.log | tail -3
grep "SUCCESS\|ERROR" /var/log/ctv-daily-update/wrapper.log | tail -3

# Check today's activity
grep "$(date '+%Y-%m-%d')" /var/log/ctv-daily-update/update.log
```

### Performance Monitoring
```bash
# View processing times and record counts
grep "Duration:" /var/log/ctv-daily-update/update.log | tail -10

# Check for any recurring errors
grep "ERROR" /var/log/ctv-daily-update/update.log | tail -10
grep "ERROR" /var/log/ctv-commercial-import/import.log | tail -10

# Monitor storage usage trends
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/daily/ /opt/apps/ctv-bookedbiz-db/data/raw/archive/
```

This comprehensive automation system ensures reliable, hands-free daily processing of commercial log data with full visibility into operations, errors, and performance metrics.