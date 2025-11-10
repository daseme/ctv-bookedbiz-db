# Monthly Data Closing Procedure

**When**: After month-end close is complete (timing varies due to manual components)
**Purpose**: Import updated annual cash revenue recap into database

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

# Copy current year's cash revenue recap
YEAR=$(date +%Y)
SOURCE_PATH="/mnt/k-drive/Sales/Yearly Reports/${YEAR} Cash Revenue Recap.xlsx"
DEST_PATH="data/raw/${YEAR}.xlsx"

# Verify source file exists and copy
if [ -f "$SOURCE_PATH" ]; then
    cp "$SOURCE_PATH" "$DEST_PATH"
    echo "✅ Updated ${YEAR}.xlsx successfully"
    ls -la "$DEST_PATH"
else
    echo "❌ Source file not found: $SOURCE_PATH"
    exit 1
fi
```

### 3. Import Closed Data
```bash
# Process the updated annual recap into database
python cli/import_closed_data.py

# Verify import completed successfully
echo "Import completed. Check logs for any errors."
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

## Notes

- Run during business hours when manual verification is possible
- Process imports all data (not just new month) - existing records updated
- K drive path is case-sensitive: "Media library" (lowercase 'l')
- Backup is automatic but immediate post-import backup recommended
- Import script handles duplicate detection and updates existing records