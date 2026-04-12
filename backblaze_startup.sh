#!/usr/bin/env bash
set -euo pipefail

echo "==> Backblaze startup script"

APP_MODE="${APP_MODE:-replica_readonly}"
DB_PATH="${DB_PATH:-${DATABASE_PATH:-/srv/spotops/db/production.db}}"
DATA_PATH="${DATA_PATH:-/srv/spotops/processed}"
RESTORE_ON_START="${RESTORE_ON_START:-true}"
LITESTREAM_CONFIG="${LITESTREAM_CONFIG:-/etc/litestream.yml}"

mkdir -p "$(dirname "$DB_PATH")" "$DATA_PATH"
export DB_PATH
export DATABASE_PATH="$DB_PATH"

if [[ "$RESTORE_ON_START" == "true" ]]; then
  echo "==> Restoring DB from Backblaze via Litestream: $DB_PATH"
  if ! litestream restore -if-replica-exists -config "$LITESTREAM_CONFIG" -o "$DB_PATH" "$DB_PATH"; then
    if [[ "$APP_MODE" == "failover_primary" ]]; then
      echo "ERROR: restore failed in failover_primary mode"
      exit 1
    fi
    echo "WARN: restore failed in replica_readonly mode; continuing with existing DB"
  fi
fi

case "$APP_MODE" in
  replica_readonly)
    export READ_ONLY_MODE=true
    echo "==> Starting in replica_readonly mode"
    ;;
  failover_primary)
    export READ_ONLY_MODE=false
    echo "==> Starting in failover_primary mode"
    ;;
  *)
    echo "ERROR: APP_MODE must be replica_readonly or failover_primary (got: $APP_MODE)"
    exit 1
    ;;
esac

echo "==> DB_PATH=$DB_PATH"
exec "$@"

