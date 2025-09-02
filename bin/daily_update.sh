#!/bin/bash
set -euo pipefail

# Configuration
PROJECT_ROOT="/opt/apps/ctv-bookedbiz-db"
LOCK_FILE="/tmp/ctv-daily-update.lock"
LOG_DIR="/var/log/ctv-daily-update"
LOG_FILE="${LOG_DIR}/update.log"
PYTHON_VENV="${PROJECT_ROOT}/.venv/bin/python"
DAILY_UPDATE_SCRIPT="${PROJECT_ROOT}/cli/daily_update.py"

# Source environment variables if available
if [[ -f "/etc/ctv-daily-update.env" ]]; then
    set -a
    source /etc/ctv-daily-update.env
    set +a
fi

# Default data file path
DATA_FILE="${DAILY_UPDATE_DATA_FILE:-/mnt/k-drive/Traffic/Media Library/Commercial Log.xlsx}"

# Create log directory
mkdir -p "${LOG_DIR}"

# Function to send notifications
send_notification() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    
    echo "${timestamp} - ${level} - ${message}" >> "${LOG_FILE}"
    
    if [[ -n "${NTFY_TOPIC:-}" ]]; then
        curl -s -d "${level}: CTV Daily Update - ${message}" "ntfy.sh/${NTFY_TOPIC}" >/dev/null 2>&1 || true
    fi
    
    if [[ -n "${SLACK_WEBHOOK_URL:-}" ]]; then
        curl -s -X POST -H 'Content-type: application/json' \
            --data "{\"text\":\"${level}: CTV Daily Update - ${message}\"}" \
            "${SLACK_WEBHOOK_URL}" >/dev/null 2>&1 || true
    fi
}

# Function to check prerequisites
check_prerequisites() {
    if [[ ! -r "${DATA_FILE}" ]]; then
        send_notification "ERROR" "Data file not found or not readable: ${DATA_FILE}"
        return 1
    fi
    
    if [[ ! -x "${PYTHON_VENV}" ]]; then
        send_notification "ERROR" "Python virtual environment not found: ${PYTHON_VENV}"
        return 1
    fi
    
    if [[ ! -f "${DAILY_UPDATE_SCRIPT}" ]]; then
        send_notification "ERROR" "Daily update script not found: ${DAILY_UPDATE_SCRIPT}"
        return 1
    fi
    
    local db_path="${PROJECT_ROOT}/data/database/production.db"
    if [[ ! -f "${db_path}" ]]; then
        send_notification "ERROR" "Database not found: ${db_path}"
        return 1
    fi
    
    return 0
}

# Main execution with flock protection
main() {
    local start_time=$(date)
    local exit_code=0
    
    send_notification "INFO" "Daily update started"
    
    cd "${PROJECT_ROOT}"
    
    if ! check_prerequisites; then
        exit 1
    fi
    
    "${PYTHON_VENV}" "${DAILY_UPDATE_SCRIPT}" \
        "${DATA_FILE}" \
        --auto-setup \
        --unattended \
        --log-file "${LOG_FILE}" \
        ${DAILY_UPDATE_EXTRA_ARGS:-} \
        || exit_code=$?
    
    local end_time=$(date)
    if [[ ${exit_code} -eq 0 ]]; then
        send_notification "SUCCESS" "Daily update completed successfully (started: ${start_time}, ended: ${end_time})"
    else
        send_notification "ERROR" "Daily update failed with exit code ${exit_code} (started: ${start_time}, ended: ${end_time})"
    fi
    
    return ${exit_code}
}

# Use flock to prevent concurrent runs
(
    flock -n 200 || {
        echo "ERROR: Another daily update process is already running" >&2
        send_notification "ERROR" "Daily update skipped - another process already running"
        exit 1
    }
    
    main "$@"
    
) 200>"${LOCK_FILE}"
