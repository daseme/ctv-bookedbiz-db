# Monthly Data Closing Procedure

**When**: After month-end close is complete (timing varies due to manual components)
**Purpose**: Import updated annual cash revenue recap into database AND close the month

## Prerequisites

- [ ] Month-end close completed by accounting
- [ ] K drive accessible via network mount
- [ ] Database backup completed

## Step-by-Step Process

### 1. Mount K Drive (if not already mounted)
```bash
# Check if already mounted
mount | grep k-drive

# Manual mount if needed
sudo mount /mnt/k-drive
```

### 2. Retrieve Updated Annual Recap
```bash
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

# Copy current year's cash revenue recap - use current year
./scripts/update_yearly_recap.sh 2025 
```

### 3. Import Closed Data (IMPORTANT: No --skip-closed flag)
```bash
# Process the updated annual recap into database
# IMPORTANT: Do NOT use --skip-closed flag for monthly closing
# The absence of --skip-closed triggers HISTORICAL mode which closes imported months
uv run python cli/import_closed_data.py data/raw/2025.xlsx --year 2025 --closed-by "Jenna"

# Verify import completed successfully and months were closed
echo "Import completed. Check logs to confirm months were closed."
```

### 4. Verification
```bash
# Quick sanity check - verify new data loaded
python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('''
    SELECT broadcast_month, COUNT(*) as spots, ROUND(SUM(station_net), 2) as revenue
    FROM spots 
    WHERE load_date >= date('now', '-1 day')
    GROUP BY broadcast_month 
    ORDER BY broadcast_month DESC 
    LIMIT 5
''')
print('Recent imports:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]:,} spots, \${row[2]:,.2f}')
conn.close()
"

# Verify month closure status (should show green in dashboard)
python -c "
import sqlite3
conn = sqlite3.connect('data/database/production.db')
cursor = conn.execute('''
    SELECT broadcast_month, status, closed_at, closed_by
    FROM broadcast_month_closures 
    ORDER BY broadcast_month DESC 
    LIMIT 5
''')
print('\nMonth closure status:')
for row in cursor.fetchall():
    status = '🟢 CLOSED' if row[1] == 'CLOSED' else '🟡 OPEN'
    print(f'  {status} {row[0]}: {row[2] or \"Not closed\"} by {row[3] or \"N/A\"}')
conn.close()
"
```

### 5. Database Backup
```bash
# Create immediate backup after monthly update
python cli_db_sync.py backup
```

## Quick Script (Recommended)

A complete automation script is available at `scripts/update_yearly_recap.sh`:

```bash
# Run the automated monthly update
./scripts/update_yearly_recap.sh
```

This script handles all steps above including K drive verification, file copy, data import, and backup.

## Important Notes

### Import Modes Explained

**HISTORICAL mode** (default - no `--skip-closed` flag):
- Processes ALL months in the Excel file
- Replaces existing data completely
- **CLOSES all imported months permanently** ✅
- Use this for monthly closing procedures

**WEEKLY_UPDATE mode** (`--skip-closed` flag):
- Skips already-closed months
- Only processes open months
- **Does NOT close months** ❌
- Use this for mid-month updates

### Common Issues

**Problem**: Dashboard shows yellow (open) instead of green (closed) after import
**Cause**: Used `--skip-closed` flag which triggers WEEKLY_UPDATE mode
**Solution**: Re-run import WITHOUT the `--skip-closed` flag

```bash
# Correct command for monthly closing:
uv run python cli/import_closed_data.py data/raw/2025.xlsx --year 2025 --closed-by "Jenna"

# Incorrect (does not close months):
uv run python cli/import_closed_data.py data/raw/2025.xlsx --year 2025 --closed-by "Jenna" --skip-closed
```

## Additional Notes

- Run during business hours when manual verification is possible
- HISTORICAL mode replaces all existing data for imported months
- K drive path is case-sensitive: "Media library" (lowercase 'l')
- Backup is automatic but immediate post-import backup recommended
- Import script handles duplicate detection and updates existing records
- Month closure is permanent and cannot be undone without database restoration