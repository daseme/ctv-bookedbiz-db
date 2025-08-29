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
- **Log File**: `/var/log/ctv-commercial-import/import.log`

### systemd Services
- **Service**: `ctv-commercial-import.service` (one-shot import process)
- **Timer**: `ctv-commercial-import.timer` (scheduled daily at 1:00 AM)

## How It Works

1. **Timer Activation**: systemd timer triggers daily at 1:00 AM (+5 min random delay)
2. **Network Check**: Tests connectivity to K: drive server (100.102.206.113)
3. **Mount K Drive**: Automatically mounts CIFS share with stored credentials
4. **File Copy**: Python script copies "Commercials" sheet from Excel file
5. **Local Storage**: Creates dated file in Pi project structure
6. **Logging**: All operations logged with timestamps and status

## Daily Operations

### Check Status
```bash
# View timer status and next run time
systemctl list-timers | grep commercial

# Check recent service activity
sudo systemctl status ctv-commercial-import.service

# View recent logs
tail -20 /var/log/ctv-commercial-import/import.log
```

### Manual Operations
```bash
# Run import immediately
sudo systemctl start ctv-commercial-import.service

# Check what files have been created
ls -la /opt/apps/ctv-bookedbiz-db/data/raw/daily/Commercial*.xlsx

# Follow logs in real-time
tail -f /var/log/ctv-commercial-import/import.log
```

### Troubleshooting
```bash
# Check if K drive is mounted
mount | grep k-drive

# Test network connectivity
ping -c 2 100.102.206.113

# View detailed service logs
sudo journalctl -u ctv-commercial-import.service -n 20
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
- **Database Sync**: Existing nightly backup to Dropbox continues at 2:05 AM
- **Processing**: Files available for immediate import by other BookedBiz services
- **Monitoring**: Logs integrated with existing Pi logging infrastructure

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