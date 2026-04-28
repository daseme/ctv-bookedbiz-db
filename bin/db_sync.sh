#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/spotops"
# Host venv is intentional: this backup must work even when the container is
# unhealthy, so it deliberately doesn't go through `docker compose exec`.
# Keep `/opt/spotops/.venv` in sync with `cli_db_sync.py`'s deps (currently
# just `dropbox`) — don't "consolidate" it into the container.
PY="/opt/spotops/.venv/bin/python"
LOCK_FILE="/tmp/ctv-db-sync.lock"

# systemd loads .env via EnvironmentFile=; also source it so manual runs work.
ENV_FILE="$APP_DIR/.env"
if [[ -r "$ENV_FILE" ]]; then set -a; . "$ENV_FILE"; set +a; fi

cd "$APP_DIR"

# Prevent overlap
exec 9>"$LOCK_FILE"
if ! flock -n 9; then logger -t ctv-db-sync "Another sync is running; exiting."; exit 0; fi

# Snapshot the live DB to a stable file before uploading. SQLite's online
# backup API produces a transactionally consistent copy even with concurrent
# writers, unlike a plain `cp` of the .db file. Same volume as the live DB
# → fast, no cross-fs copy. Cleaned up on EXIT (incl. WAL/SHM/journal).
export SNAPSHOT="/srv/spotops/db/.snapshot.db"
trap 'rm -f "$SNAPSHOT" "$SNAPSHOT-journal" "$SNAPSHOT-wal" "$SNAPSHOT-shm"' EXIT
rm -f "$SNAPSHOT" "$SNAPSHOT-journal" "$SNAPSHOT-wal" "$SNAPSHOT-shm"
"$PY" - <<'PY'
import os, sqlite3
src = sqlite3.connect(os.environ["DATABASE_PATH"])
dst = sqlite3.connect(os.environ["SNAPSHOT"])
with dst:
    src.backup(dst)
src.close()
dst.close()
PY

if DATABASE_PATH="$SNAPSHOT" "$PY" cli_db_sync.py upload \
   && DATABASE_PATH="$SNAPSHOT" "$PY" cli_db_sync.py backup; then
  logger -t ctv-db-sync "Upload OK"
else
  rc=$?
  msg="Dropbox DB upload FAILED (exit $rc) on $(hostname) at $(date '+%F %T')"
  logger -t ctv-db-sync "$msg"
  if [[ -n "${NTFY_TOPIC:-}" ]]; then
    curl -fsS -H "Title: ctv-db-sync FAIL" -H "Priority: 5" -d "$msg" "https://ntfy.sh/$NTFY_TOPIC" || true
  fi
  if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
    payload=$(printf '{"text":"%s"}' "$msg")
    curl -fsS -X POST -H 'Content-type: application/json' --data "$payload" "$SLACK_WEBHOOK_URL" || true
  fi
  exit "$rc"
fi

# --- prune old Dropbox backups (keep last 7) ---
"$PY" - <<'PY'
import os, sys, dropbox
KEEP = 7
dbx = dropbox.Dropbox(
    oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    app_key=os.environ["DROPBOX_APP_KEY"],
    app_secret=os.environ["DROPBOX_APP_SECRET"],
)
resp = dbx.files_list_folder("/backups")
dbs = [e for e in resp.entries if getattr(e, "name", "").endswith(".db")]
dbs.sort(key=lambda e: e.name, reverse=True)  # timestamped names sort lexicographically
for e in dbs[KEEP:]:
    try:
        dbx.files_delete_v2(e.path_lower)
        print(f"Pruned: {e.name}")
    except Exception as ex:
        print(f"Prune error for {e.name}: {ex}", file=sys.stderr)
PY
# --- end prune ---

# --- integrity check: compare latest Dropbox backup vs the snapshot we
#     just uploaded. (Comparing against the live DB would race against any
#     writes that happened between snapshot and now.) ---
DATABASE_PATH="$SNAPSHOT" "$PY" - <<'PY'
import os, dropbox, hashlib, sys

def dropbox_hash(path):
    h_block = []
    with open(path, "rb") as f:
        while True:
            chunk = f.read(4*1024*1024)
            if not chunk: break
            h = hashlib.sha256(); h.update(chunk); h_block.append(h.digest())
    h = hashlib.sha256()
    for d in h_block: h.update(d)
    return h.hexdigest()

dbx = dropbox.Dropbox(
    oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
    app_key=os.environ["DROPBOX_APP_KEY"],
    app_secret=os.environ["DROPBOX_APP_SECRET"],
)
local = os.environ["DATABASE_PATH"]

# compute local hash
try:
    local_hash = dropbox_hash(local)
except Exception as e:
    print(f"Integrity check failed: cannot read local DB ({e})", file=sys.stderr)
    sys.exit(0)

# get most recent backup
resp = dbx.files_list_folder("/backups")
dbs = [e for e in resp.entries if getattr(e, "name", "").endswith(".db")]
if not dbs:
    print("Integrity check skipped: no backups found")
    sys.exit(0)

dbs.sort(key=lambda e: e.name, reverse=True)
latest = dbs[0]
remote = dbx.files_get_metadata(latest.path_lower)

# compare
if remote.content_hash == local_hash:
    print(f"✓ Integrity OK: latest backup {latest.name} matches uploaded snapshot")
else:
    print(f"✗ Integrity FAIL: {latest.name} differs from uploaded snapshot")
PY
# --- end integrity check ---
