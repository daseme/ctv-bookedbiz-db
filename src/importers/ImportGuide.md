# Sales Database Import Guide

Complete guide for all data import scenarios in the CTV Booked Business Database system.

## 🎯 Overview

This system provides bulletproof data import workflows with transaction management, automatic month protection, and memory-efficient processing for large Excel files.

## 🏗️ System Architecture

- **BaseService**: Transaction management preventing deadlocks
- **Month Closure Protection**: Closed months cannot be accidentally modified
- **Memory-Efficient Processing**: Handles 400K+ records without issues
- **Comprehensive Column Mapping**: Captures all 29 columns from any Excel format
- **Audit Trail**: Complete tracking of all import operations

---

## 📋 Import Scenarios

### 🏛️ **SCENARIO 1: Historical Annual Import**

**Use when:**
- Initial database setup with full year of data
- Complete refresh of historical data
- Importing a complete annual report

**Command:**
```bash
uv run python src/cli/bulk_import_historical.py data/raw/YYYY.xlsx --year YYYY --closed-by "Kurt" --db-path data/database/production.db
```

**What it does:**
- ✅ Imports entire year (all 12 months)
- ✅ Automatically closes ALL months (permanent protection)
- ✅ Marks everything as historical
- ✅ Handles 400K+ records efficiently
- ✅ Memory-optimized streaming import
- ✅ Complete audit trail

**Examples:**
```bash
# Import complete 2024 annual data
uv run python src/cli/bulk_import_historical.py data/raw/2024.xlsx --year 2024 --closed-by "Kurt" --db-path data/database/production.db

# Preview before importing (recommended)
uv run python src/cli/bulk_import_historical.py data/raw/2024.xlsx --year 2024 --closed-by "Kurt" --db-path data/database/production.db --dry-run

# Import complete 2025 annual data (when year ends)
uv run python src/cli/bulk_import_historical.py data/raw/2025.xlsx --year 2025 --closed-by "Kurt" --db-path data/database/production.db
```

**Expected Output:**
```
🎉 OPTIMIZED HISTORICAL IMPORT COMPLETED
📊 Results:
  Success: ✅
  Duration: 208.45 seconds
  Records imported: 403,203
  Months closed: ['Jan-24', 'Feb-24', ..., 'Dec-24']
✅ All months permanently protected
```

---

### 📅 **SCENARIO 2: New Closed Month Import** ⭐

**Use when:**
- Monthly process: adding newly closed month
- You have complete workbook but only want to import new month
- Avoid duplicating already-imported months

**Command:**
```bash
uv run python src/importers/smart_monthly_import.py data/raw/YYYY_complete.xlsx --year YYYY --closed-by "Kurt"
```

**What it does:**
- 🔍 Analyzes complete workbook intelligently
- 🔒 Automatically skips already-closed months
- ✅ Imports only new/open months
- ✅ Automatically closes newly imported months
- 🧹 Creates temp filtered file (auto-cleaned)
- ✅ Full 29-column data capture
- ⚡ Fast and efficient

**Examples:**
```bash
# Add May-25 from complete 2025 workbook (skips Jan-Apr which are already closed)
uv run python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt"

# Dry run to see what would be imported (recommended)
uv run python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt" --dry-run

# Import without auto-closing (for testing)
uv run python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt" --no-close
```

**Expected Output:**
```
📊 Found 5 months in workbook: ['Jan-25', 'Feb-25', 'Mar-25', 'Apr-25', 'May-25']
🔒 Already closed: 4 months
   📋 Jan-25 - SKIP (already closed)
   📋 Feb-25 - SKIP (already closed)  
   📋 Mar-25 - SKIP (already closed)
   📋 Apr-25 - SKIP (already closed)
📂 Open/new months: 1 months
   ✅ May-25 - IMPORT

🎯 Import Plan:
  • Skip 4 already-closed months
  • Import 1 open months: ['May-25']
  
✅ Smart import completed successfully!
💡 Only new/open months were imported - closed months were protected
```

---

### 🔄 **SCENARIO 3: Weekly Updates**

**Use when:**
- Weekly business updates
- Updating current/open months with latest bookings
- Data contains only open months (no closed months)

**Command:**
```bash
uv run python src/cli/weekly_update.py data/raw/weekly_update.xlsx
```

**What it does:**
- ✅ Updates only open months
- ⚠️ Blocks if file contains closed months (protection)
- ✅ Replaces existing open month data
- ❌ Does NOT close months
- ✅ Fast incremental updates

**Examples:**
```bash
# Update current open months with latest bookings
uv run python src/cli/weekly_update.py data/raw/june_2025_weekly.xlsx

# Preview what would be updated (recommended)
uv run python src/cli/weekly_update.py data/raw/june_2025_weekly.xlsx --dry-run

# Force update (skip confirmation)
uv run python src/cli/weekly_update.py data/raw/june_2025_weekly.xlsx --force
```

**Expected Output (Success):**
```
🎉 WEEKLY UPDATE COMPLETED
📊 Results:
  Success: ✅
  Months updated: 2
  Records imported: 15,432
  Records deleted: 14,203
  Net change: +1,229
✅ Weekly update completed successfully!
```

**Expected Output (Blocked):**
```
❌ Weekly update cannot proceed due to validation errors
💡 Common solutions:
  • Remove closed month data from Excel file
  • Use smart_monthly_import.py for closed months
```

---

### 🛠️ **SCENARIO 4: Manual Month Management**

**Close a Single Month:**
```bash
uv run python src/cli/close_month.py "Jun-25" --closed-by "Kurt" --notes "Month end closing"
```

**Check Month Status:**
```bash
uv run python src/cli/close_month.py --status "Jun-25"
```

**List All Closed Months:**
```bash
uv run python src/cli/close_month.py --list
```

**Get Month Statistics:**
```bash
uv run python src/cli/close_month.py --month-stats "May-25"
```

**Examples:**
```bash
# Close current month
uv run python src/cli/close_month.py "Jun-25" --closed-by "Kurt" --notes "End of month closing"

# Check if month is closed
uv run python src/cli/close_month.py --status "Jun-25"

# View all closed months
uv run python src/cli/close_month.py --list

# Get detailed statistics
uv run python src/cli/close_month.py --month-stats "May-25"
```

---

## 📊 Decision Flowchart

```
📁 What data do you have?
│
├── 📅 Complete year (annual report)
│   └── Use: bulk_import_historical.py
│
├── 📅 Complete workbook with new month
│   └── Use: smart_monthly_import.py ⭐ (RECOMMENDED)
│
├── 📅 Weekly update file (open months only)
│   └── Use: weekly_update.py
│
└── 🛠️ Need to manage individual months
    └── Use: close_month.py
```

## 🎯 Typical Monthly Workflow

```bash
# 1. New month becomes available in complete workbook
uv run python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt"

# 2. Throughout the month, update open months as needed
uv run python src/cli/weekly_update.py data/raw/weekly_bookings.xlsx

# 3. Manual month management if needed
uv run python src/cli/close_month.py --status "current_month"

# 4. When month ends, it gets included in next month's complete workbook
# (Repeat step 1 with updated workbook)
```

---

## ⚠️ Important Notes

### **Safety First**
- **Always use `--dry-run` first** on important imports
- **Historical imports are permanent** - they close months forever
- **Weekly updates are blocked** if file contains closed months (built-in protection)
- **Smart monthly import** automatically protects closed months

### **Data Protection**
- **Closed months cannot be modified** - system prevents accidental overwrites
- **Complete audit trail** - every import is tracked with batch IDs
- **Transaction safety** - no deadlocks or corruption
- **Memory efficient** - handles large files without crashing

### **Column Mapping**
- **Always captures 29 columns** from any Excel format
- **Handles 2024 and 2025 format differences** automatically
- **Maps alternative column names** (e.g., "Gross" vs "Unit rate Gross")
- **Ignores unmapped columns** safely

---

## 🏆 Your Perfect Setup

| Scenario | Tool | Use Case |
|----------|------|----------|
| **Annual Setup** | `bulk_import_historical.py` | Complete year import |
| **Monthly Process** | `smart_monthly_import.py` ⭐ | **Your go-to tool!** |
| **Weekly Updates** | `weekly_update.py` | Ongoing bookings |
| **Month Management** | `close_month.py` | Individual month control |

---

## 🚀 Quick Start Examples

### **Most Common: Add New Month**
```bash
# This is what you'll use most often
uv run python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt"
```

### **Weekly Business Updates**
```bash
# Update open months with latest bookings
uv run python src/cli/weekly_update.py data/raw/current_bookings.xlsx --dry-run
uv run python src/cli/weekly_update.py data/raw/current_bookings.xlsx
```

### **Check System Status**
```bash
# See what months are closed
uv run python src/cli/close_month.py --list

# Check specific month
uv run python src/cli/close_month.py --status "Jun-25"
```

---

## 🎉 Success Indicators

**Successful Import:**
- ✅ "Success: ✅" in results
- ✅ Records imported count matches expectations
- ✅ Months closed automatically (for historical/monthly imports)
- ✅ No error messages
- ✅ Duration reasonable (3-8 minutes for large files)

**System Health:**
- ✅ Transaction management working (no "database locked" errors)
- ✅ Memory usage stable (MB not GB)
- ✅ Closed months protected from modification
- ✅ Audit trail complete

---

## 🛠️ Troubleshooting

**Common Issues:**

1. **"Database locked" errors**
   - ✅ **SOLVED** - BaseService transaction management eliminates this

2. **"Month already closed" errors**
   - ✅ **SOLVED** - Smart monthly import automatically skips closed months

3. **Memory issues with large files**
   - ✅ **SOLVED** - Streaming import handles 400K+ records efficiently

4. **Missing columns**
   - ✅ **SOLVED** - Comprehensive column mapping captures all 29 columns

5. **Transaction deadlocks**
   - ✅ **SOLVED** - BaseService prevents nested transaction conflicts

---

## 📞 Quick Reference Commands

```bash
# Most used command (monthly process)
uv run python src/importers/smart_monthly_import.py data/raw/YYYY_complete.xlsx --year YYYY --closed-by "Kurt"

# Check what months are closed
uv run python src/cli/close_month.py --list

# Weekly updates
uv run python src/cli/weekly_update.py data/raw/weekly.xlsx --dry-run

# Close individual month
uv run python src/cli/close_month.py "Month-YY" --closed-by "Kurt"

# Annual/historical import
uv run python src/cli/bulk_import_historical.py data/raw/YYYY.xlsx --year YYYY --closed-by "Kurt"
```

---

*This system provides bulletproof, production-ready data import workflows with complete protection against data corruption, transaction deadlocks, and accidental overwrites. All import operations are logged and auditable.*

**🚀 You now have enterprise-grade data import capabilities!**