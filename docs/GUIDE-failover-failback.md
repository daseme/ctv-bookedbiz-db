# Backup and Failover Guide

Production database: `/var/lib/ctv-bookedbiz-db/production.db` (~1.4 GB SQLite, owned by `ctvbooked`)

---

## Backup Stack

Three independent layers, each covering different failure modes:

| Layer | Tool | Target | RPO | Runs as |
|-------|------|--------|-----|---------|
| Continuous WAL replication | Litestream 0.5.9 | Backblaze B2 (`ctv-bookedbiz-wal`) | ~1 second | `litestream.service` (root) |
| Nightly full-DB snapshot | Dropbox upload | Dropbox `/database.db` + `/backups/` | 24 hours | `ctv-db-sync.service` (daseme) |
| Cold standby mirror | Pi2 download | Pi2 local DB | 24 hours | `ctv-pi2-download.service` (Pi2) |

**Recovery priority**: Litestream first (freshest), Dropbox second (full snapshots), Pi2 third (cold standby).

---

## Layer 1: Litestream (Continuous Replication)

Litestream continuously replicates SQLite WAL frames to Backblaze B2 with a 1-second sync interval. This is the primary disaster recovery mechanism.

### Configuration

- **Service**: `litestream.service` (enabled, runs as root)
- **Config**: `/etc/litestream.yml`
- **Env**: `/etc/ctv-litestream.env` (B2 credentials)
- **Source DB**: `/var/lib/ctv-bookedbiz-db/production.db`
- **Destination**: `s3://ctv-bookedbiz-wal/production` via `s3.us-west-004.backblazeb2.com`
- **Log**: `/var/log/litestream/replicate.log`

### Daily operations

```bash
# Check replication status
systemctl status litestream

# View recent replication activity
tail -20 /var/log/litestream/replicate.log

# Verify WAL frames are flowing (look for compaction messages)
grep "compaction complete" /var/log/litestream/replicate.log | tail -5
```

### Recovery from Litestream

Restore the database to a local path from B2:

```bash
# Install litestream if not present
# (already installed at /usr/bin/litestream)

# Restore to a temp path first
sudo litestream restore -config /etc/litestream.yml \
  -o /tmp/restored-production.db \
  /var/lib/ctv-bookedbiz-db/production.db

# Verify integrity
sqlite3 /tmp/restored-production.db "PRAGMA integrity_check;"

# Stop the app, replace the DB, restart
sudo systemctl stop ctv-bookedbiz-db.service
sudo systemctl stop litestream.service
sudo cp /tmp/restored-production.db /var/lib/ctv-bookedbiz-db/production.db
sudo chown ctvbooked:ctvbooked /var/lib/ctv-bookedbiz-db/production.db
sudo systemctl start litestream.service
sudo systemctl start ctv-bookedbiz-db.service
```

### Troubleshooting

- **"initialized db" but no compaction**: Check B2 credentials in `/etc/ctv-litestream.env`
- **Service keeps restarting**: Check `journalctl -u litestream -n 50` for auth errors
- **Stale replication**: Verify the production DB is receiving writes (check file mtime)

---

## Layer 2: Dropbox Nightly Backup

A nightly systemd timer uploads the full production database to Dropbox and creates timestamped backup copies. Keeps the last 7 backups and runs an integrity check after each upload.

### Configuration

- **Timer**: `ctv-db-sync.timer` — daily at 02:05 with up to 5 min jitter
- **Service**: `ctv-db-sync.service` (oneshot, runs as `daseme:ctvapps`)
- **Script**: `/opt/apps/ctv-bookedbiz-db/bin/db_sync.sh`
- **Env**: `/etc/ctv-db-sync.env` (`DATABASE_PATH`, Dropbox OAuth tokens)
- **Log**: `/var/log/ctv-db-sync/sync.log`
- **Dropbox paths**: `/database.db` (latest) + `/backups/database_backup_YYYYMMDD_HHMMSS.db`
- **Retention**: 7 most recent backups; older ones pruned automatically

### Sandboxing

The service runs with `ProtectSystem=strict`, which blocks access to most of the filesystem. To read the production DB at `/var/lib/`, the service unit needs:

```ini
ReadOnlyPaths=/var/lib/ctv-bookedbiz-db
```

Without this line, the service cannot see the production database and uploads will fail silently (the backup script reports "Local database not found").

### Daily operations

```bash
# Check timer status and next trigger
systemctl list-timers | grep ctv-db-sync

# View recent backup results
tail -30 /var/log/ctv-db-sync/sync.log

# Manual backup run
sudo systemctl start ctv-db-sync.service
journalctl -u ctv-db-sync.service -n 20 --no-pager

# Verify backup integrity line in logs
grep "Integrity" /var/log/ctv-db-sync/sync.log | tail -3
```

### Recovery from Dropbox

```bash
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Download latest database from Dropbox
python cli_db_sync.py download

# Or list available backups
python cli_db_sync.py info

# Test Dropbox connection
python cli_db_sync.py test
```

---

## Layer 3: Pi2 Cold Standby

Pi2 downloads the latest database from Dropbox daily at 02:30 (25 minutes after the Spotops upload). It can serve as a warm standby with ~30-second failover activation.

### Network

| Host | Tailscale IP | Role |
|------|-------------|------|
| Spotops (primary) | 100.99.11.55 | Production app on port 8000 |
| Pi2 (standby) | 100.96.96.109 | Mirror, failover on port 8000 |

### Pi2 services (on Pi2 only)

- **Timer**: `ctv-pi2-download.timer` — daily at 02:30
- **Service**: `ctv-pi2-download.service`
- **Log**: `/var/log/ctv-pi2-download/download.log`
- **Flask**: `flaskapp.service` (disabled by default, started on failover)

### Failover (Spotops down)

```bash
ssh daseme@100.96.96.109
cd /opt/apps/ctv-bookedbiz-db
./scripts/failover-to-pi2.sh
```

The script pulls the latest code, validates the local database, starts Flask, and runs a health check. Total time ~30 seconds.

**Post-failover URL**: `http://100.96.96.109:8000`

### Failback (Spotops restored)

```bash
./scripts/failback-to-spotops.sh
```

Prompts for optional data backup of any changes made during failover, then stops Flask on Pi2.

---

## Operational Services

### Insertion Order Scanner

Scans `/mnt/k-drive/Insertion Orders` hourly for pending insertion order folders and spreadsheets. Writes a JSON summary that the web app reads to display pending orders on AE dashboards.

- **Timer**: `ctv-io-scanner.timer` — hourly at :10 past the hour, 2 min jitter
- **Service**: `ctv-io-scanner.service` (oneshot, runs as `daseme:ctvapps`)
- **Script**: `/opt/apps/ctv-bookedbiz-db/scripts/scan_insertion_orders.py`
- **Output**: `/opt/apps/ctv-bookedbiz-db/data/pending_orders.json` (mode 0640, group `apps-deploy`)
- **Venv**: `/opt/apps/ctv-bookedbiz-db/.venv` (requires `openpyxl`)
- **K drive mount**: `/mnt/k-drive/Insertion Orders`

```bash
# Check timer status and next run
systemctl list-timers | grep ctv-io-scanner

# View last scan result
journalctl -u ctv-io-scanner.service -n 10 --no-pager

# Manual scan
sudo systemctl start ctv-io-scanner.service

# View current pending orders
cat /opt/apps/ctv-bookedbiz-db/data/pending_orders.json | python3 -m json.tool
```

**Permission note**: The script writes JSON via `tempfile.mkstemp` and explicitly sets mode `0640` so the web app (running as `ctvbooked` in group `apps-deploy`) can read it. If the file reverts to `0600`, the dashboard silently shows "No pending orders."

---

## Monitoring

```bash
# Full backup health check (run from Spotops)
systemctl status litestream                          # Continuous replication
systemctl list-timers | grep ctv-db-sync             # Nightly Dropbox backup
tail -5 /var/log/litestream/replicate.log            # Recent WAL activity
tail -5 /var/log/ctv-db-sync/sync.log                # Last Dropbox result

# Spotops app health
curl -sf http://100.99.11.55:8000/api/system-stats

# Pi2 readiness (from Pi2)
systemctl list-timers | grep ctv-pi2-download
ls -lh /opt/apps/ctv-bookedbiz-db/data/database/production.db
```

### Alert channels

- ntfy.sh push notifications (on Dropbox backup failure)
- Slack webhook (on Dropbox backup failure)
- Kuma monitoring dashboard on Pi2 port 3001

---

## Recovery Time/Point Objectives

| Scenario | RTO | RPO | Recovery source |
|----------|-----|-----|-----------------|
| DB corruption (Spotops intact) | 5 min | ~1 sec | Litestream restore from B2 |
| Spotops hardware failure | 30 sec | 24 hours | Pi2 failover (Dropbox copy) |
| Spotops hardware failure (with Litestream restore) | 15 min | ~1 sec | Litestream restore to Pi2 |
| Both Pis destroyed | 30 min | ~1 sec | Litestream restore to new host |
| All cloud storage lost | N/A | 24 hours | Pi2 local copy |

---

## Important File Locations

### Spotops

```
# Production database
/var/lib/ctv-bookedbiz-db/production.db       (owned by ctvbooked)

# Litestream
/usr/bin/litestream
/etc/litestream.yml
/etc/ctv-litestream.env
/var/log/litestream/replicate.log

# Dropbox backup
/opt/apps/ctv-bookedbiz-db/bin/db_sync.sh
/etc/ctv-db-sync.env
/etc/systemd/system/ctv-db-sync.service
/etc/systemd/system/ctv-db-sync.timer
/var/log/ctv-db-sync/sync.log

# Insertion Order Scanner
/opt/apps/ctv-bookedbiz-db/scripts/scan_insertion_orders.py
/opt/apps/ctv-bookedbiz-db/data/pending_orders.json
/etc/systemd/system/ctv-io-scanner.service
/etc/systemd/system/ctv-io-scanner.timer

# Application
/opt/apps/ctv-bookedbiz-db/                   (app code)
/etc/ctv-bookedbiz-db/ctv-bookedbiz-db.env    (app env)
```

### Pi2

```
/opt/apps/ctv-bookedbiz-db/scripts/failover-to-pi2.sh
/opt/apps/ctv-bookedbiz-db/scripts/failback-to-spotops.sh
/opt/apps/ctv-bookedbiz-db/bin/daily-download.sh
/etc/systemd/system/ctv-pi2-download.service
/etc/systemd/system/ctv-pi2-download.timer
/var/log/ctv-pi2-download/download.log
```

---

## Incident Log

### Feb 11 – Mar 2, 2026: Dropbox backups failing for 20 days

**Cause**: The `ctv-db-sync.service` runs with `ProtectSystem=strict`, which blocks read access to `/var/lib/`. The service was configured with `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db` but couldn't actually read that path.

**Symptoms**: 21 consecutive log entries of `"Local database not found at /var/lib/ctv-bookedbiz-db/production.db"` with no alerts (the upload script exited before reaching the notification code path).

**Correct fix (applied Mar 2)**:
1. Set `DATABASE_PATH=/var/lib/ctv-bookedbiz-db/production.db` in `/etc/ctv-db-sync.env`
2. Add `ReadOnlyPaths=/var/lib/ctv-bookedbiz-db` to the service unit (lifts systemd sandbox)
3. `sudo chgrp ctvapps /var/lib/ctv-bookedbiz-db` — directory was `0750 ctvbooked:ctvbooked`, service runs as `daseme:ctvapps`
4. `sudo chgrp ctvapps /var/lib/ctv-bookedbiz-db/production.db` — file group also needed changing

All three layers were required: systemd sandbox access, directory group, and file group.

**Mitigating factor**: Litestream was running continuously throughout, so no data was at risk. The Dropbox/Pi2 backup layers were degraded but the primary replication layer was unaffected.

**Lessons**:
- Nightly backup failures should trigger alerts. The backup script's error path needs to reach the ntfy/Slack notification code even for "file not found" errors.
- If the production DB file is ever recreated (e.g., by Litestream restore or SQLite vacuum), the group will revert to `ctvbooked`. Verify group ownership after any DB restore operation.
