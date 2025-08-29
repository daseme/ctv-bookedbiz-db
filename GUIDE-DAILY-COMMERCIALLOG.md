# Commercial Log Import System

## Overview
Automated daily import of Commercial Log data from the K: drive network share to local Raspberry Pi storage for processing by the CTV BookedBiz application.

## System Components

### Files and Locations
- **Source**: `/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx` (K: drive network share)
- **Destination**: `/opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial Log YYMMDD.xlsx`
- **Python Script**: `/opt/apps/ctv-bookedbiz-db/src/importers/commercial_log_importer.py`
- **Wrapper Script**: `/opt/apps/ctv-bookedbiz-db/bin/commercial_import.sh`
- **Environment File**: `/etc/ctv-commercial-import.env` (contains CIFS credentials)
- **Rotation Script**: `/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh`
- **Archive Directory**: `/opt/apps/ctv-bookedbiz-db/data/raw/archive/`

### systemd Services
- **Import Service**: `ctv-commercial-import.service` (one-shot import process)
- **Import Timer**: `ctv-commercial-import.timer` (scheduled daily at 3:30 AM to account for the working day in Malaysia)
- **Rotation Service**: `ctv-commercial-rotation.service` (file archival and cleanup)
- **Rotation Timer**: `ctv-commercial-rotation.timer` (scheduled weekly on Sundays at 2:30 AM)

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

### Automated Schedule
- **Daily 1:00 AM**: Import new commercial log file
- **Sunday 2:30 AM**: Archive files older than 7 days, cleanup old archives

1. **Timer Activation**: systemd timer triggers daily at 1:00 AM (+5 min random delay)
2. **Network Check**: Tests connectivity to K: drive server (100.102.206.113)
3. **Mount K Drive**: Automatically mounts CIFS share with stored credentials
4. **File Copy**: Python script copies "Commercials" sheet from Excel file
5. **Local Storage**: Creates dated file in Pi project structure
6. **Logging**: All operations logged with timestamps and status

## Daily Operations

### Check Status
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

### Manual Operations
```bash
# Run import immediately
sudo systemctl start ctv-commercial-import.service

# Run file rotation immediately
sudo systemctl start ctv-commercial-rotation.service

# Check current and archived files
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial*.xlsx
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/archive/commercial-logs-*.zip

# View storage usage summary
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status

# Follow import logs in real-time
tail -f /var/log/ctv-commercial-import/import.log

# View rotation logs
tail -20 /var/log/ctv-commercial-import/rotation.log
```

### Troubleshooting
```bash
# Check if K drive is mounted
mount | grep k-drive

# Test network connectivity
ping -c 2 100.102.206.113

# View detailed import service logs
sudo journalctl -u ctv-commercial-import.service -n 20

# View detailed rotation service logs
sudo journalctl -u ctv-commercial-rotation.service -n 20

# Check all commercial log timers
systemctl list-timers | grep commercial

# Manually test rotation script
/opt/apps/ctv-bookedbiz-db/bin/rotate_commercial_logs.sh status

# Check disk space usage
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/daily/
du -sh /opt/apps/ctv-bookedbiz-db/data/raw/archive/
```

## File Processing Results

**Typical Output**:
- **Rows Processed**: ~15,500 commercial log entries
- **File Size**: ~2MB Excel file
- **Processing Time**: ~15 seconds
- **Filename Format**: `Commercial Log 250828.xlsx` (YYMMDD format)

## Integration with Existing Workflow

This system fits into the broader Pi-based workflow:
- **Data Collection**: Commercial log files stored locally in `data/raw/daily/`
- **Storage Management**: Automatic archival and cleanup maintains optimal disk usage
- **Database Sync**: Existing nightly backup to Dropbox continues at 2:05 AM
- **Processing**: Recent files (7 days) available for immediate import by other BookedBiz services
- **Historical Access**: Archived files available in monthly ZIP archives for up to 12 months
- **Monitoring**: Logs integrated with existing Pi logging infrastructure

## Storage Efficiency

**Before Rotation System**: 
- 365 files/year × 2MB = ~730MB annually
- Linear growth with no cleanup

**After Rotation System**:
- Active files: 7 × 2MB = ~14MB
- Monthly archives: 12 × ~60MB = ~720MB (compressed)
- Total storage: ~734MB (with automatic cleanup)
- Stable storage footprint with historical data preserved

## Credentials and Security

- Network share credentials stored in `/etc/ctv-commercial-import.env` (root-only access)
- Service runs as `daseme` user with `ctvapps` group permissions
- K drive mount persists between runs for efficiency
- Concurrent run prevention via flock mechanism

## Future Enhancements

Optional notification systems can be configured:
- **ntfy.sh**: Set `NTFY_TOPIC` in environment file
- **Slack**: Set `SLACK_WEBHOOK_URL` in environment file

Both will alert on import failures for proactive monitoring.