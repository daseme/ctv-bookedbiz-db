#!/usr/bin/env bash
# Commercial Log File Rotation Script
# Keeps 7 days of individual files, archives older files by month

set -euo pipefail

# Configuration
DATA_DIR="/opt/apps/ctv-bookedbiz-db/data/raw/daily"
ARCHIVE_DIR="/opt/apps/ctv-bookedbiz-db/data/raw/archive"
LOG_FILE="/var/log/ctv-commercial-import/rotation.log"
KEEP_DAYS=7
KEEP_MONTHS=12

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

ensure_directories() {
    mkdir -p "$ARCHIVE_DIR"
    mkdir -p "$(dirname "$LOG_FILE")"
}

get_file_date() {
    local filename="$1"
    # Extract YYMMDD from "Commercial Log 250828.xlsx"
    if [[ "$filename" =~ Commercial\ Log\ ([0-9]{6})\.xlsx ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo ""
    fi
}

convert_to_full_date() {
    local short_date="$1"  # Format: YYMMDD
    local year="20${short_date:0:2}"
    local month="${short_date:2:2}"
    local day="${short_date:4:2}"
    echo "${year}-${month}-${day}"
}

get_files_to_archive() {
    local cutoff_date=$(date -d "$KEEP_DAYS days ago" '+%Y-%m-%d')
    
    find "$DATA_DIR" -name "Commercial Log *.xlsx" -type f | while read -r file; do
        local basename=$(basename "$file")
        local file_date_short=$(get_file_date "$basename")
        
        if [[ -n "$file_date_short" ]]; then
            local file_date=$(convert_to_full_date "$file_date_short")
            if [[ "$file_date" < "$cutoff_date" ]]; then
                echo "$file"
            fi
        fi
    done
}

archive_by_month() {
    local files_to_archive=$(get_files_to_archive)
    
    if [[ -z "$files_to_archive" ]]; then
        log "No files older than $KEEP_DAYS days found"
        return 0
    fi
    
    # Group files by month
    declare -A monthly_files
    
    while IFS= read -r file; do
        local basename=$(basename "$file")
        local file_date_short=$(get_file_date "$basename")
        
        if [[ -n "$file_date_short" ]]; then
            local year_month="20${file_date_short:0:2}-${file_date_short:2:2}"
            monthly_files["$year_month"]+="$file "
        fi
    done <<< "$files_to_archive"
    
    # Create archives for each month
    for month in "${!monthly_files[@]}"; do
        local archive_file="$ARCHIVE_DIR/commercial-logs-$month.zip"
        local temp_archive="$archive_file.tmp"
        local files_array=(${monthly_files[$month]})
        
        log "Archiving ${#files_array[@]} files for $month"
        
        # Create/update zip archive
        if [[ -f "$archive_file" ]]; then
            # Update existing archive
            cp "$archive_file" "$temp_archive"
            zip -j "$temp_archive" "${files_array[@]}" >/dev/null 2>&1
        else
            # Create new archive
            zip -j "$temp_archive" "${files_array[@]}" >/dev/null 2>&1
        fi
        
        # Verify archive was created successfully
        if zip -T "$temp_archive" >/dev/null 2>&1; then
            mv "$temp_archive" "$archive_file"
            
            # Remove original files
            local removed_count=0
            for file in "${files_array[@]}"; do
                if [[ -f "$file" ]]; then
                    rm "$file"
                    ((removed_count++))
                fi
            done
            
            local archive_size=$(stat -c%s "$archive_file" 2>/dev/null || echo "0")
            log "Created $archive_file ($(($archive_size / 1024))KB) - removed $removed_count files"
        else
            log "ERROR: Failed to create archive for $month"
            rm -f "$temp_archive"
        fi
    done
}

cleanup_old_archives() {
    local cutoff_date=$(date -d "$KEEP_MONTHS months ago" '+%Y-%m')
    
    find "$ARCHIVE_DIR" -name "commercial-logs-*.zip" -type f | while read -r archive; do
        local basename=$(basename "$archive")
        
        # Extract YYYY-MM from "commercial-logs-2024-08.zip"
        if [[ "$basename" =~ commercial-logs-([0-9]{4}-[0-9]{2})\.zip ]]; then
            local archive_month="${BASH_REMATCH[1]}"
            if [[ "$archive_month" < "$cutoff_date" ]]; then
                log "Removing old archive: $basename (older than $KEEP_MONTHS months)"
                rm "$archive"
            fi
        fi
    done
}

show_status() {
    log "=== Commercial Log Storage Status ==="
    
    # Current files
    local current_files=$(find "$DATA_DIR" -name "Commercial Log *.xlsx" -type f | wc -l)
    local current_size=$(find "$DATA_DIR" -name "Commercial Log *.xlsx" -type f -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum/1024/1024}')
    log "Current files: $current_files (${current_size:-0} MB)"
    
    # Archives
    local archive_count=$(find "$ARCHIVE_DIR" -name "commercial-logs-*.zip" -type f 2>/dev/null | wc -l)
    local archive_size=0
    if [[ "$archive_count" -gt 0 ]]; then
        archive_size=$(find "$ARCHIVE_DIR" -name "commercial-logs-*.zip" -type f -exec stat -c%s {} + 2>/dev/null | awk '{sum+=$1} END {print sum/1024/1024}')
    fi
    log "Archive files: $archive_count (${archive_size:-0} MB)"
    
    # Oldest and newest files
    local oldest=$(find "$DATA_DIR" -name "Commercial Log *.xlsx" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | head -1 | cut -d' ' -f2-)
    local newest=$(find "$DATA_DIR" -name "Commercial Log *.xlsx" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
    
    if [[ -n "$oldest" ]]; then
        log "Oldest current file: $(basename "$oldest")"
    fi
    if [[ -n "$newest" ]]; then
        log "Newest current file: $(basename "$newest")"
    fi
}

main() {
    log "=== Commercial Log Rotation Started ==="
    
    ensure_directories
    
    case "${1:-rotate}" in
        "status")
            show_status
            ;;
        "rotate")
            archive_by_month
            cleanup_old_archives
            show_status
            log "=== Commercial Log Rotation Completed ==="
            ;;
        *)
            echo "Usage: $0 [rotate|status]"
            echo "  rotate: Archive old files and cleanup (default)"
            echo "  status:  Show current storage status"
            exit 1
            ;;
    esac
}

main "$@"