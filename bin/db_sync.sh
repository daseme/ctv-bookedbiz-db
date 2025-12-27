#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="/opt/apps/ctv-bookedbiz-db"
PY="/opt/apps/ctv-bookedbiz-db/.venv/bin/python"
LOCK_FILE="/tmp/ctv-db-sync.lock"

# DO NOT source /etc/ctv-db-sync.env here (systemd loads it)
PUB_ENV="$APP_DIR/.env.public"
if [[ -r "$PUB_ENV" ]]; then set -a; . "$PUB_ENV"; set +a; fi

cd "$APP_DIR"

# Prevent overlap
exec 9>"$LOCK_FILE"
if ! flock -n 9; then logger -t ctv-db-sync "Another sync is running; exiting."; exit 0; fi

if "$PY" cli_db_sync.py upload && "$PY" cli_db_sync.py backup; then
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

# --- integrity check: compare latest backup vs local DB ---
"$PY" - <<'PY'
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
    print(f"✓ Integrity OK: latest backup {latest.name} matches local DB")
else:
    print(f"✗ Integrity FAIL: {latest.name} differs from local DB")
PY
# --- end integrity check ---
