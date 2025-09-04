#!/bin/bash
#
# Enhanced Commercial Log Import Wrapper Script
# Supports multi-sheet processing (Commercials + Worldlink Lines)
# Designed for unattended operation with comprehensive logging and error handling
#
# This script is called by systemd service: ctv-commercial-import.service
# Runs daily at 1:00 AM via ctv-commercial-import.timer
#
# Author: Enhanced for multi-sheet support
# Date: 2025-09-04
#

set -euo pipefail  # Exit on any error, undefined variable, or pipe failure

# ============================================================================
# Configuration
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="/var/log/ctv-commercial-import"
LOG_FILE="$LOG_DIR/import.log"
LOCK_FILE="/tmp/commercial-import.lock"

# Paths
VENV_PATH="$PROJECT_ROOT/.venv"
IMPORTER_SCRIPT="$PROJECT_ROOT/src/importers/commercial_log_importer.py"
K_DRIVE_MOUNT="/mnt/k-drive"
SOURCE_FILE="$K_DRIVE_MOUNT/Traffic/Media Library/Commercial Log.xlsx"

# Load environment variables (credentials, notifications)
if [ -f "/etc/ctv-commercial-import.env" ]; then
    source /etc/ctv-commercial-import.env
fi

# ============================================================================
# Logging Functions
# ============================================================================

log_info() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO - $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR - $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - WARNING - $1" | tee -a "$LOG_FILE"
}

# ============================================================================
# Notification Functions  
# ============================================================================

send_notification() {
    local message="$1"
    local level="${2:-info}"  # info, warning, error
    
    # ntfy.sh notification
    if [ -n "${NTFY_TOPIC:-}" ]; then
        curl -s -X POST "https://ntfy.sh/$NTFY_TOPIC" \
            -H "Title: CTV Commercial Import" \
            -H "Priority: ${level}" \
            -d "$message" || true
    fi
    
    # Slack notification  
    if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
        local emoji
        case "$level" in
            "error") emoji=":x:" ;;
            "warning") emoji=":warning:" ;;
            *) emoji=":white_check_mark:" ;;
        esac
        
        curl -s -X POST "$SLACK_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{\"text\":\"$emoji CTV Commercial Import: $message\"}" || true
    fi
}

# ============================================================================
# Infrastructure Functions
# ============================================================================

check_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local pid=$(cat "$LOCK_FILE" 2>/dev/null || echo "unknown")
        if kill -0 "$pid" 2>/dev/null; then
            log_error "Commercial import already running (PID: $pid)"
            exit 1
        else
            log_warning "Stale lock file found, removing..."
            rm -f "$LOCK_FILE"
        fi
    fi
    
    # Create lock file
    echo $$ > "$LOCK_FILE"
    
    # Ensure lock file is removed on exit
    trap 'rm -f "$LOCK_FILE"' EXIT
}

check_dependencies() {
    # Check if Python virtual environment exists
    if [ ! -d "$VENV_PATH" ]; then
        log_error "Python virtual environment not found: $VENV_PATH"
        exit 1
    fi
    
    # Check if Python importer script exists
    if [ ! -f "$IMPORTER_SCRIPT" ]; then
        log_error "Commercial log importer script not found: $IMPORTER_SCRIPT"
        exit 1
    fi
    
    # Check if K: drive is mounted
    if [ ! -d "$K_DRIVE_MOUNT" ]; then
        log_error "K: drive not mounted: $K_DRIVE_MOUNT"
        exit 1
    fi
    
    # Check if source file exists
    if [ ! -f "$SOURCE_FILE" ]; then
        log_error "Source file not found: $SOURCE_FILE"
        exit 1
    fi
}

check_network_connectivity() {
    # Test network connectivity to K: drive server
    local k_drive_ip="100.102.206.113"
    
    if ! ping -c 2 -W 5 "$k_drive_ip" >/dev/null 2>&1; then
        log_error "Cannot reach K: drive server ($k_drive_ip)"
        exit 1
    fi
    
    log_info "Network connectivity verified"
}

ensure_k_drive_mounted() {
    # Check if already mounted
    if mountpoint -q "$K_DRIVE_MOUNT" 2>/dev/null; then
        log_info "K: drive already mounted"
        return 0
    fi
    
    log_info "Mounting K: drive..."
    
    # Create mount point if it doesn't exist
    sudo mkdir -p "$K_DRIVE_MOUNT"
    
    # Mount using credentials from environment
    if [ -n "${CIFS_USERNAME:-}" ] && [ -n "${CIFS_PASSWORD:-}" ]; then
        sudo mount -t cifs "//100.102.206.113/K Drive" "$K_DRIVE_MOUNT" \
            -o username="$CIFS_USERNAME",password="$CIFS_PASSWORD",domain=CTVETERE,vers=2.0,sec=ntlmv2
        
        if mountpoint -q "$K_DRIVE_MOUNT"; then
            log_info "K: drive mounted successfully"
        else
            log_error "Failed to mount K: drive"
            exit 1
        fi
    else
        log_error "CIFS credentials not found in environment"
        exit 1
    fi
}

# ============================================================================
# Main Import Function
# ============================================================================

run_commercial_import() {
    local start_time=$(date +%s)
    
    log_info "=== Enhanced Commercial Log Import Started ==="
    log_info "Processing: Commercials + Worldlink Lines sheets"
    log_info "Source: $SOURCE_FILE"
    log_info "Destination: $PROJECT_ROOT/data/raw/daily/"
    
    # Change to project directory
    cd "$PROJECT_ROOT"
    
    # Activate Python virtual environment
    source "$VENV_PATH/bin/activate"
    
    # Run the enhanced commercial log importer
    log_info "Running enhanced multi-sheet importer..."
    
    if python "$IMPORTER_SCRIPT" 2>&1 | tee -a "$LOG_FILE"; then
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_info "Enhanced commercial log import completed successfully in ${duration}s"
        send_notification "Enhanced commercial import successful (Commercials + Worldlink Lines)" "info"
        return 0
    else
        local end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        log_error "Enhanced commercial log import failed after ${duration}s"
        send_notification "Enhanced commercial import failed - check logs" "error"
        return 1
    fi
}

# ============================================================================
# Storage Management
# ============================================================================

check_storage_space() {
    local destination_dir="$PROJECT_ROOT/data/raw/daily"
    local available_space_kb=$(df "$destination_dir" | awk 'NR==2 {print $4}')
    local available_space_mb=$((available_space_kb / 1024))
    
    # Require at least 50MB free space
    if [ "$available_space_mb" -lt 50 ]; then
        log_error "Insufficient storage space: ${available_space_mb}MB available (50MB required)"
        send_notification "Commercial import failed - insufficient storage" "error"
        exit 1
    fi
    
    log_info "Storage space check: ${available_space_mb}MB available"
}

rotate_old_files() {
    # This would be handled by the separate rotation service
    # Just log current file count for monitoring
    local daily_dir="$PROJECT_ROOT/data/raw/daily"
    local file_count=$(find "$daily_dir" -name "Commercial Log *.xlsx" -type f | wc -l)
    
    if [ "$file_count" -gt 10 ]; then
        log_warning "High number of daily files: $file_count (rotation service should clean these up)"
    fi
    
    log_info "Current daily files: $file_count"
}

# ============================================================================
# Health Checks
# ============================================================================

verify_imported_file() {
    local daily_dir="$PROJECT_ROOT/data/raw/daily"
    local today_pattern="Commercial Log $(date +%y%m%d).xlsx"
    local expected_file="$daily_dir/$today_pattern"
    
    if [ -f "$expected_file" ]; then
        local file_size=$(stat -f%z "$expected_file" 2>/dev/null || stat -c%s "$expected_file" 2>/dev/null || echo "0")
        local file_size_mb=$((file_size / 1024 / 1024))
        
        log_info "Import verification: $today_pattern created (${file_size_mb}MB)"
        
        # Basic size check - expect at least 1MB for realistic data
        if [ "$file_size" -lt 1048576 ]; then
            log_warning "Imported file seems unusually small: ${file_size_mb}MB"
        fi
        
        return 0
    else
        log_error "Expected import file not found: $expected_file"
        return 1
    fi
}

# ============================================================================
# Main Execution
# ============================================================================

main() {
    # Setup
    mkdir -p "$LOG_DIR"
    check_lock
    
    log_info "Enhanced Commercial Import starting..."
    log_info "Support: Commercials + Worldlink Lines sheets"
    
    # Pre-flight checks
    check_dependencies
    check_storage_space
    check_network_connectivity
    ensure_k_drive_mounted
    
    # Execute import
    if run_commercial_import; then
        # Post-import verification
        verify_imported_file
        rotate_old_files
        
        log_info "=== Enhanced Commercial Log Import Completed Successfully ==="
        exit 0
    else
        log_error "=== Enhanced Commercial Log Import Failed ==="
        exit 1
    fi
}

# Execute main function with all output logged
main "$@" 2>&1