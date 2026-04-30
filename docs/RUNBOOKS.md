# Runbooks

**Audience:** Developers and ops-savvy operators with shell access to the SpotOps host
**Purpose:** Command-oriented procedures for the live operational surface — container lifecycle, daily import debugging, backup/restore, token rotation, recovery
**Last reviewed:** 2026-04-30

> **For non-technical operators:** Use [HUMAN_OPERATOR_GUIDE.md](HUMAN_OPERATOR_GUIDE.md) instead. This file assumes shell access and that you know what `systemctl` and `docker compose` mean.
>
> **For code changes:** See [DEV_WORKFLOW.md](DEV_WORKFLOW.md).
>
> **For system architecture:** See [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Quick contents

- [Health checks](#health-checks)
- [Container lifecycle](#container-lifecycle)
- [Logs reference](#logs-reference)
- [Daily import — manual run / debug](#daily-import--manual-run--debug)
- [Database operations](#database-operations)
- [Backup posture (4 layers)](#backup-posture)
- [Backup verification](#backup-verification)
- [Restore procedures](#restore-procedures)
- [Sheet-export endpoint operations](#sheet-export-endpoint-operations)
- [Customer normalization sync](#customer-normalization-raw-input-sync)
- [Failover / failback](#failover--failback)
- [Railway emergency activation](#railway-emergency-activation)
- [Decommissioned subsystems](#decommissioned-subsystems-do-not-revive)

---

## Health checks

### App is up and serving

```bash
curl -sf http://localhost:8000/health/ && echo OK
```

Expected: `OK` printed, exit code 0.

> **Note:** `/health` works; `/api/health` returns 401 (the auth allow-list misses it). Use `/health` for liveness probes.

### Container is running

```bash
docker compose -f /opt/spotops/docker-compose.yml ps
```

Expected:

```
NAME                IMAGE              STATUS              PORTS
spotops-spotops-1   spotops-spotops    Up X hours          127.0.0.1:8000->8000/tcp
```

### Timers are firing

```bash
systemctl list-timers --all | grep -E 'ctv-|litestream|restic'
```

Expected: 4 ctv-* timers + restic-backup.timer with recent `LAST` and future `NEXT` columns. `litestream.service` is not a timer; check separately:

```bash
systemctl status litestream.service --no-pager | head -10
```

### Database has fresh data

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT MAX(load_date) FROM spots;"
```

Expected: today's date (or yesterday during off-hours).

---

## Container lifecycle

All commands assume `cd /opt/spotops` first (or pass `-f /opt/spotops/docker-compose.yml`).

| Operation | Command |
|---|---|
| Status | `docker compose ps` |
| Live log tail | `docker compose logs -f spotops` |
| Last 200 log lines | `docker compose logs --tail=200 spotops` |
| Shell into container | `docker exec -it spotops-spotops-1 bash` |
| Stop (preserves container) | `docker compose stop spotops` |
| Start | `docker compose start spotops` |
| Recreate after env/compose change | `docker compose up -d spotops` |
| **Rebuild after code change** | `docker compose up -d --build spotops` |
| Bounce a healthy container | `docker compose restart spotops` |
| Tear down (preserves volumes) | `docker compose down` |
| Rebuild with no cache | `docker compose build --no-cache spotops && docker compose up -d spotops` |

**Critical footgun:** `restart` does **not** pick up Python source changes. After any `git pull` that touched code, use `up -d --build`.

---

## Logs reference

| What | Path | Source |
|---|---|---|
| App / container logs | `docker compose logs -f spotops` | uvicorn / Flask inside container |
| Daily update wrapper | `/var/log/ctv-daily-update/update.log` | `bin/daily_update.sh` |
| Commercial import wrapper | `/var/log/ctv-commercial-import/import.log` | `bin/commercial_import.sh` |
| Dropbox sync | `/var/log/ctv-db-sync/sync.log` | `bin/db_sync.sh` |
| Litestream replication | `journalctl -u litestream -n 100 --no-pager` | systemd journal |
| Restic backup | `/var/log/restic/backup.log` | `restic-backup.service` |
| systemd unit history | `journalctl -u <unit> -n 200 --no-pager` | per-unit |

---

## Daily import — manual run / debug

The daily import is two systemd timers running 4×/day:

| Timer | What it does |
|---|---|
| `ctv-commercial-import.timer` | `bin/commercial_import.sh`: copies K-drive `Commercial Log.xlsx` → `/srv/spotops/data/raw/daily/Commercial Log YYMMDD.xlsx` |
| `ctv-daily-update.timer` (≈30 min after) | `bin/daily_update.sh`: runs `docker compose exec spotops uv run python cli/daily_update.py "<DATED_FILE>" --auto-setup --unattended` |

### Verify the timers

```bash
systemctl list-timers --all | grep -E 'ctv-(commercial|daily)'
systemctl status ctv-commercial-import.timer ctv-daily-update.timer --no-pager
```

### Inspect a recent run

```bash
journalctl -u ctv-daily-update.service -n 200 --no-pager
tail -200 /var/log/ctv-daily-update/update.log
```

### Manual run — commercial import (host → /srv/spotops/data/raw/daily/)

```bash
sudo systemctl start ctv-commercial-import.service
journalctl -u ctv-commercial-import.service -n 50 --no-pager
```

Or run the script directly to step through:

```bash
sudo /opt/spotops/bin/commercial_import.sh
```

Common failure: K-drive not mounted. Recover with `sudo mount /mnt/k-drive` then re-run.

### Manual run — daily update (import into DB)

```bash
sudo systemctl start ctv-daily-update.service
journalctl -u ctv-daily-update.service -n 100 --no-pager
```

Or directly inside the container if you want to target a specific file:

```bash
DATED_FILE="/app/data/raw/daily/Commercial Log $(date +%y%m%d).xlsx"
docker compose -f /opt/spotops/docker-compose.yml exec -T spotops \
  uv run python cli/daily_update.py "$DATED_FILE" --auto-setup --unattended --verbose
```

> Path inside the container is `/app/data/raw/daily/...` because `/srv/spotops/data` is mounted to `/app/data` (per `docker-compose.yml`). The host-side path is `/srv/spotops/data/raw/daily/...`.

### ntfy notifications

The daily-update script posts to ntfy on success and failure:

- Success: `Title: CTV Daily Update`, `Tags: white_check_mark`
- Failure: `Title: CTV Daily Update`, `Priority: 5`, `Tags: rotating_light`, body includes the failing context

Topic is set in `/opt/spotops/.env` as `NTFY_TOPIC` (current value: `ctv-import-2a11ef7e7a84` — fall back to literal in `bin/daily_update.sh` if env unset).

---

## Database operations

The live DB is `/srv/spotops/db/production.db`. The host and container both see the same file via the volume mount.

### Read-only query

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT COUNT(*) FROM spots WHERE broadcast_month = 'Apr-26';"
```

### Take a hot snapshot (online backup, safe with concurrent writers)

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  ".backup '/srv/spotops/db/snap-$(date +%F_%H%M).sqlite3'"
```

This is what `bin/db_sync.sh` does internally before uploading to Dropbox. Don't `cp production.db` directly while the app is running — you'll get a torn copy.

### Integrity check

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "PRAGMA integrity_check;"
```

Expected: `ok` (single line).

### Schema inspection

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db ".schema spots"
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db ".indexes"
```

### Apply a migration

See [DEV_WORKFLOW.md → Schema changes](DEV_WORKFLOW.md#schema-changes).

### Sync from Dropbox (read what's on Dropbox right now)

```bash
cd /opt/spotops
./.venv/bin/python cli_db_sync.py info       # shows Dropbox-side metadata
./.venv/bin/python cli_db_sync.py test       # tests connectivity / auth
```

### Lock-investigation (when a write seems blocked)

The container's app process holds an open DB handle continuously; that's normal. Litestream also opens a read handle. To see who's holding what:

```bash
sudo lsof /srv/spotops/db/production.db
```

Expected: uvicorn process inside the container (visible via Docker namespace), `litestream`, possibly an in-flight import.

For destructive operations that need exclusive access (full file replace, vacuum, schema rewrite), stop the writers first:

```bash
sudo systemctl stop litestream.service
docker compose -f /opt/spotops/docker-compose.yml stop spotops
# ... do the destructive thing ...
docker compose -f /opt/spotops/docker-compose.yml start spotops
sudo systemctl start litestream.service
```

> The old "kill datasette/uvicorn before pipeline run" recipes from the pre-Docker era no longer apply — pipelines run *inside* the same container as the app, and SQLite WAL handles concurrent reader+writer cases. Don't go hunting for processes to kill on the host.

---

## Backup posture

Four independent backup layers, each protecting different things:

| Layer | What it covers | Frequency | Target | Service |
|---|---|---|---|---|
| **Litestream** | Live SQLite DB (continuous WAL) | every few seconds | Backblaze B2 | `litestream.service` |
| **Dropbox** | Full DB snapshot file | nightly 02:04 (+5min jitter) | Dropbox `/database.db` + `/backups/database_backup_YYYYMMDD_HHMMSS.db` (retain 7 most recent) | `ctv-db-sync.timer` |
| **Restic** | `/srv/spotops/{data,processed,uploads}` | nightly 02:30 (+10min jitter) | Backblaze B2 (separate repo from Litestream) | `restic-backup.timer` |
| **Pi2 mirror** | (intentionally not running today) | — | — | NOT installed; see [Decommissioned subsystems](#decommissioned-subsystems-do-not-revive) |

**Why two B2 layers?** Litestream covers the SQLite DB only (WAL replication, near-zero RPO). Restic covers everything *else* under `/srv/spotops` — raw imports, processed artifacts, future uploads — that Litestream wouldn't see. Dropbox is the human-friendly "I just want a file I can download" backup.

### Litestream → Backblaze (continuous DB)

| Item | Value |
|---|---|
| Service | `litestream.service` (enabled, runs as `root`) |
| Config | `/etc/litestream.yml` |
| Env file | `/etc/litestream.env` (B2 credentials) |
| Replicate command | `/usr/bin/litestream replicate -config /etc/litestream.yml` |
| Logs | `journalctl -u litestream -f` |

### Dropbox nightly (full DB)

| Item | Value |
|---|---|
| Timer | `ctv-db-sync.timer` (nightly ≈02:04 + 5 min jitter) |
| Service | `ctv-db-sync.service` (User=`daseme`) |
| Script | `/opt/spotops/bin/db_sync.sh` |
| Env | `/opt/spotops/.env` (loaded via `EnvironmentFile=`) |
| CLI | `cli_db_sync.py` (host venv `/opt/spotops/.venv/bin/python` — deliberately host-side so backup works when the container is unhealthy) |
| Logs | `/var/log/ctv-db-sync/sync.log` |
| Retention | 7 most recent timestamped snapshots in Dropbox `/backups/` |

The script takes a SQLite online-backup snapshot at `/srv/spotops/db/.snapshot.db`, uploads, and cleans up via EXIT trap (also removes any `-journal`, `-wal`, `-shm` siblings).

### Restic → Backblaze (everything else under /srv/spotops)

| Item | Value |
|---|---|
| Timer | `restic-backup.timer` (nightly 02:30 + 10 min jitter) |
| Service | `restic-backup.service` (User=`root`, `Type=oneshot`) |
| Env file | `/etc/restic.env` (repo URL + password) |
| Backed up | `/srv/spotops/data` `/srv/spotops/processed` `/srv/spotops/uploads` |
| Retention | `--keep-daily 7 --keep-weekly 4 --keep-monthly 12 --prune` (runs in `ExecStartPost`) |
| Logs | `/var/log/restic/backup.log` |

---

## Backup verification

### Litestream is replicating

```bash
journalctl -u litestream --since "1 hour ago" --no-pager | tail -20
```

Look for periodic `level=INFO ... database` lines. Errors show as `level=ERROR`. Sustained errors mean B2 credentials or network are wrong.

### Dropbox snapshot is current

```bash
cd /opt/spotops
./.venv/bin/python cli_db_sync.py info
```

Look at the timestamp of `/database.db`; should be within the last 26 hours. Also check `/var/log/ctv-db-sync/sync.log` for the most recent run:

```bash
tail -30 /var/log/ctv-db-sync/sync.log
```

### Restic snapshot is current

```bash
sudo systemd-run --pipe --unit=restic-list \
  --setenv=RESTIC_REPOSITORY="$(grep RESTIC_REPOSITORY /etc/restic.env | cut -d= -f2-)" \
  --setenv=RESTIC_PASSWORD_FILE="$(grep RESTIC_PASSWORD_FILE /etc/restic.env | cut -d= -f2-)" \
  /usr/bin/restic snapshots --last 5
```

Or (simpler, if you can read the env file):

```bash
sudo bash -c 'set -a; . /etc/restic.env; set +a; restic snapshots --last 5'
```

Expected: today's snapshot at the top.

```bash
tail -30 /var/log/restic/backup.log
```

### Backup health summary (one-shot)

```bash
echo "--- timers ---"
systemctl list-timers --all | grep -E 'ctv-db-sync|restic-backup'
echo "--- litestream ---"
systemctl is-active litestream.service
echo "--- dropbox last run ---"
tail -3 /var/log/ctv-db-sync/sync.log
echo "--- restic last run ---"
tail -3 /var/log/restic/backup.log
```

---

## Restore procedures

### Restore from a Dropbox snapshot

```bash
cd /opt/spotops
./.venv/bin/python cli_db_sync.py download   # pulls /database.db locally
# Inspect the file before deploying:
sqlite3 ./downloaded-production.db "PRAGMA integrity_check;"
sqlite3 ./downloaded-production.db "SELECT COUNT(*) FROM spots;"
# Stop writers, swap in:
sudo systemctl stop litestream.service
docker compose -f /opt/spotops/docker-compose.yml stop spotops
sudo cp ./downloaded-production.db /srv/spotops/db/production.db
sudo chown daseme:spotops-team /srv/spotops/db/production.db
docker compose -f /opt/spotops/docker-compose.yml start spotops
sudo systemctl start litestream.service
```

> **Footgun:** if the DB file is recreated rather than overwritten in place, group ownership can revert. Verify with `ls -l /srv/spotops/db/production.db` after the restore.

### Pull latest from Backblaze via Litestream (one-off, doesn't change config)

The image only restores from B2 once at container start (when `RESTORE_ON_START=true`). To force a fresh pull:

```bash
# Stop the app so the file isn't held open
docker compose -f /opt/spotops/docker-compose.yml stop spotops

# One-off restore using the same image, override entrypoint
sudo docker run --rm \
  --entrypoint litestream \
  --env-file /etc/litestream.env \
  -v /srv/spotops/db:/srv/spotops/db \
  spotops-spotops restore -if-replica-exists \
    -config /etc/litestream.yml \
    -o /srv/spotops/db/production.db \
    /srv/spotops/db/production.db

docker compose -f /opt/spotops/docker-compose.yml start spotops
```

> This **overwrites** the local `production.db` with data rebuilt from B2. Local writes that never reached B2 are lost.

### Restore a single file from restic

```bash
sudo bash -c 'set -a; . /etc/restic.env; set +a; \
  restic snapshots --last 5'
# Pick the snapshot ID, then:
sudo bash -c 'set -a; . /etc/restic.env; set +a; \
  restic restore <snapshot-id> --target /tmp/restic-restore --include /srv/spotops/data/raw/daily'
```

Then inspect under `/tmp/restic-restore/` and copy in what you need.

### Rollback after a bad import

```bash
# 1. Take a fresh hot snapshot of current (potentially-bad) state for forensics
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  ".backup '/srv/spotops/db/forensic-$(date +%F_%H%M).sqlite3'"

# 2. Restore from the most recent good Dropbox snapshot (see procedure above)

# 3. Verify before resuming traffic
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db "PRAGMA integrity_check;"
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db \
  "SELECT broadcast_month, COUNT(*) FROM spots GROUP BY broadcast_month ORDER BY broadcast_month DESC LIMIT 5;"
```

---

## Sheet-export endpoint operations

The `/api/revenue/sheet-export` and `/api/revenue/planning-export` endpoints share one auth secret: `SHEET_EXPORT_TOKEN` in `/opt/spotops/.env`. Without it set, both return `503`.

### Smoke test

```bash
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/sheet-export \
  | jq '.metadata'
```

Expected: `{"generated_at": "...", "hash_version": "v1", "row_count": N, ...}` with `N > 0` on a populated DB.

```bash
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/planning-export \
  | jq '.metadata, .rows[0]'
```

Expected: metadata with `schema_version="1.0"` and a row with all 10 fields.

### Rotate the shared token

1. Generate a new secret:
   ```bash
   openssl rand -hex 32
   ```
2. Update `/opt/spotops/.env` — change `SHEET_EXPORT_TOKEN=…`.
3. Recreate the container so the new env loads:
   ```bash
   docker compose -f /opt/spotops/docker-compose.yml up -d spotops
   ```
4. Update the workbook's `Config!ApiToken` named range.
5. Run the smoke test above to confirm.

### Failure-mode table

| Symptom | Likely cause | Fix |
|---|---|---|
| `503` | `SHEET_EXPORT_TOKEN` unset on server | Set in `/opt/spotops/.env`, recreate container |
| `401` | Wrong token in workbook | Update `Config!ApiToken` |
| Empty `rows` array | DB query returned nothing — usually a closed-month / data issue | Check `MAX(load_date)` in spots; investigate import |
| `hash_version` mismatch in workbook | Server bumped hash version | Update `Config!HashVersion`, invalidate `tblKnownRows` |
| `504` timeout | DB under heavy load | Retry; if persistent, check container logs |

See [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) for the full endpoint contract.

---

## Customer normalization (raw input sync)

After major imports, `raw_customer_inputs` can lag behind `spots.bill_code`. The Canon Tool's auto-suggestion features depend on this table being current.

### Sync raw inputs

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db <<'SQL'
INSERT OR IGNORE INTO raw_customer_inputs (raw_text)
SELECT DISTINCT bill_code
FROM spots
WHERE bill_code IS NOT NULL
  AND bill_code != ''
  AND bill_code NOT IN (SELECT raw_text FROM raw_customer_inputs);
SQL
```

Suggested cadence: weekly, or after any monthly close / large historical import.

### Verify

```bash
docker exec spotops-spotops-1 sqlite3 /srv/spotops/db/production.db <<'SQL'
SELECT
  (SELECT COUNT(DISTINCT bill_code) FROM spots WHERE bill_code IS NOT NULL AND bill_code != '') AS spots_distinct,
  (SELECT COUNT(*) FROM raw_customer_inputs) AS raw_inputs;
SQL
```

After sync, `raw_inputs` should be ≥ `spots_distinct`.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the canon system and view chain.

---

## Failover / failback

> **Status note (2026-04-30):** the `ctv-pi2-download.timer` that nightly-mirrored the DB to Pi2 is **no longer installed**. The host scripts at `/opt/spotops/scripts/failover-to-pi2.sh` and `/opt/spotops/scripts/failback-to-spotops.sh` still exist as historical artifacts. Before relying on them in an outage, **verify Pi2 is reachable and has a recent DB**. Don't assume the prior runbook is current.

### What still works (verified)

- Restoring the live DB from Backblaze via Litestream — see [Restore procedures](#restore-procedures)
- Restoring from a Dropbox snapshot — see [Restore procedures](#restore-procedures)
- Restoring `/srv/spotops/{data,processed,uploads}` from restic — see [Restore procedures](#restore-procedures)

### Pi2 cold-standby (treat as historical until reverified)

The previous pattern (full detail preserved in `docs/ARCHIVE/GUIDE-failover-failback.md`) used:

- Pi2 IP `100.96.96.109`, port 8000
- Nightly DB mirror via `ctv-pi2-download.timer` (NOT currently running)
- Manual cutover scripts at `/opt/spotops/scripts/failover-to-pi2.sh` and `failback-to-spotops.sh`

To make Pi2 cold-standby viable again, the nightly mirror needs to be reinstated or replaced (e.g., point Pi2 at the Litestream B2 replica).

### Escalation order during a real outage

1. **App is down but DB is fine** → check container, restart: `docker compose up -d spotops`
2. **DB is corrupt or missing** → restore from Dropbox (most recent good snapshot) or from B2 via Litestream (most recent WAL)
3. **Host is unreachable** → check Tailscale; if the host itself is dead, fall back to Pi2 (verify state first) or stand up the Railway emergency target
4. **Multi-day outage** → Railway with a Dropbox restore (full procedure in archived `GUIDE-Railway.md`)

---

## Railway emergency activation

Railway is a **last-resort** emergency target. The Railway service may or may not still be paid for / reachable — verify before relying.

### Activate (scale 0 → 1)

```bash
railway login
railway link
railway service scale --replicas 1
# wait ~2-3 minutes for deployment
railway domain   # show the public URL
railway logs
```

Or via the Railway Dashboard: find the service → Settings → Replicas → set to `1`.

### Deactivate (scale 1 → 0)

```bash
railway service scale --replicas 0
```

> Full setup details (Railway env vars, restore-from-Dropbox-on-boot script, security caveats) are preserved in `docs/ARCHIVE/GUIDE-Railway.md`. The doc dates from 2025-08-27 and uses the legacy service name `ctv-bookedbiz-db`; verify the project still exists and re-confirm secret values before relying on it in an emergency.

---

## Decommissioned subsystems (do not revive)

The following systemd units / unit templates exist on disk but are **not installed** as live services. Don't enable them without re-verifying their assumptions — they were retired as the Docker migration changed the surrounding world.

| Subsystem | Unit templates (in `/opt/spotops/scripts/`) | Why retired |
|---|---|---|
| Pi2 nightly download | (template only — gone from `systemctl`) | Pi2 mirror is no longer being pulled nightly. Before re-enabling, verify scripts/paths still match the current DB location |
| Insertion Order Scanner | `ctv-io-scanner.service` / `.timer` | Service not installed; `pending_orders.json` does not exist on disk. Dashboard "pending orders" widget appears to be defunct or fed elsewhere |
| DB validation timer | `ctv-db-validation.service` / `.timer` | Never installed; intent unclear |

If you find yourself wanting one of these, look at the unit file in `/opt/spotops/scripts/`, confirm every path it references still exists, and update before `systemctl enable`-ing.

---

## When something looks wrong (decision tree)

```
App not serving?
├── docker compose ps shows container missing → docker compose up -d spotops
├── container "Up" but /health/ fails → check logs (`docker compose logs --tail=200 spotops`)
└── container "Restarting" loop → likely env / DB issue; logs will say which

Imports stale?
├── ntfy shows recent failure alerts → read journalctl for ctv-daily-update.service
├── timers haven't fired (LAST is old) → systemctl status ctv-commercial-import.timer
└── timers fired but no DB change → check K-drive mount: mountpoint -q /mnt/k-drive

Dashboard slow?
├── one specific page → check the relevant route in src/web/routes/
├── all pages → check container CPU: docker stats spotops-spotops-1
└── DB queries hanging → check sqlite locks: sudo lsof /srv/spotops/db/production.db

Backup hasn't fired?
├── Dropbox: tail /var/log/ctv-db-sync/sync.log; check /etc/ctv-db-sync.env? (no — env is /opt/spotops/.env)
├── Restic: tail /var/log/restic/backup.log; verify /etc/restic.env is readable by root
└── Litestream: journalctl -u litestream --since "1 hour ago"
```

---

## Related docs

- [HUMAN_OPERATOR_GUIDE.md](HUMAN_OPERATOR_GUIDE.md) — operator-friendly version (no commands)
- [DEV_WORKFLOW.md](DEV_WORKFLOW.md) — code changes, schema migrations, branching
- [ARCHITECTURE.md](ARCHITECTURE.md) — system topology, DR posture, data model
- [API_AND_EXPORT_CONTRACTS.md](API_AND_EXPORT_CONTRACTS.md) — endpoint contracts
- `docs/ARCHIVE/GUIDE-Railway.md` — full Railway DR setup (historical, dated 2025-08-27)
- `docs/ARCHIVE/GUIDE-failover-failback.md` — pre-Docker failover details (Pi-era paths; useful general lessons preserved)
