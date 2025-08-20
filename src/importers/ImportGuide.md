# Sales Database Import Guide

Complete guide for all data import scenarios in the CTV Booked Business Database system.

## Overview

This system provides bulletproof data import workflows with transaction management, automatic month protection, and memory-efficient processing for large Excel files.

## System Architecture

- **CLI Directory**: User-facing tools organized in `/cli/` for easy access
- **BaseService**: Transaction management preventing deadlocks
- **Month Closure Protection**: Closed months cannot be accidentally modified
- **Memory-Efficient Processing**: Handles 400K+ records without issues
- **Comprehensive Column Mapping**: Captures all 29 columns from any Excel format
- **Language Assignment Integration**: Automatic categorization and processing
- **Audit Trail**: Complete tracking of all import operations

---

## CLI Tools Overview

Your primary tools are now organized in the `cli/` directory:

| **Tool** | **Purpose** | **Usage** |
|----------|-------------|-----------|
| `cli/import_closed_data.py` | **Primary import tool** | Monthly/historical imports |
| `cli/assign_languages.py` | Language assignment processing | After imports |
| `src/importers/smart_monthly_import.py` | Smart filtering from complete workbooks | As needed |

---

## Import Scenarios

### **SCENARIO 1: Closed Month Import** ⭐ **PRIMARY WORKFLOW**

**Use when:**
- Monthly process: importing newly closed month
- Historical data import (annual or bulk)
- Any data requiring permanent month closure protection

**Command:**
```bash
python cli/import_closed_data.py data/raw/YYYY.xlsx --year YYYY --closed-by "Kurt"
```

**What it does:**
- Imports closed sales data with progress tracking
- Creates missing markets and schedule assignments as needed
- Processes language assignments using business rules
- Closes imported months permanently (read-only protection)
- Provides comprehensive audit trail

**Examples:**
```bash
# Monthly closed month import (typical workflow)
python cli/import_closed_data.py data/raw/May-2025.xlsx --year 2025 --closed-by "Kurt"

# Historical annual import (initial setup)
python cli/import_closed_data.py data/raw/2024-complete.xlsx --year 2024 --closed-by "Kurt" --auto-setup

# Preview before importing
python cli/import_closed_data.py data/raw/March-2025.xlsx --year 2025 --closed-by "Kurt" --dry-run
```

**Expected Output:**
```
Production Sales Data Import Tool
Excel file: data/raw/May-2025.xlsx
Expected year: 2025
Closed by: Kurt

PRODUCTION IMPORT COMPLETED
Overall Results:
  Success: ✓
  Duration: 45.23 seconds
  Records imported: 15,432
  Language assignments processed: 15,432
  Flagged for review: 23
```

---

### **SCENARIO 2: Smart Monthly Import from Complete Workbook**

**Use when:**
- You have complete workbook but only want to import new months
- Avoid duplicating already-imported months
- Need intelligent filtering of closed vs open months

**Command:**
```bash
python src/importers/smart_monthly_import.py data/raw/YYYY_complete.xlsx --year YYYY --closed-by "Kurt"
```

**What it does:**
- Analyzes complete workbook intelligently
- Automatically skips already-closed months
- Imports only new/open months
- Creates temp filtered file (auto-cleaned)
- Full 29-column data capture

**Examples:**
```bash
# Add May-25 from complete 2025 workbook (skips already-closed months)
python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt"

# Preview what would be imported
python src/importers/smart_monthly_import.py data/raw/2025_complete.xlsx --year 2025 --closed-by "Kurt" --dry-run
```

---

### **SCENARIO 3: Weekly Updates**

**Use when:**
- Weekly business updates
- Updating current/open months with latest bookings
- Data contains only open months (no closed months)

**Command:**
```bash
python src/cli/weekly_update.py data/raw/weekly_update.xlsx
```

**What it does:**
- Updates only open months
- Blocks if file contains closed months (protection)
- Replaces existing open month data
- Does NOT close months
- Fast incremental updates

**Examples:**
```bash
# Update current open months with latest bookings
python src/cli/weekly_update.py data/raw/june_2025_weekly.xlsx

# Preview what would be updated
python src/cli/weekly_update.py data/raw/june_2025_weekly.xlsx --dry-run
```

---

### **SCENARIO 4: Manual Month Management**

**Close a Single Month:**
```bash
python src/cli/close_month.py "Jun-25" --closed-by "Kurt" --notes "Month end closing"
```

**Check Month Status:**
```bash
python src/cli/close_month.py --status "Jun-25"
```

**List All Closed Months:**
```bash
python src/cli/close_month.py --list
```

---

## Language Assignment Integration

All closed month imports automatically process language assignments:

### **Automatic Processing:**
```bash
# Import automatically includes language processing
python cli/import_closed_data.py data/raw/May-2025.xlsx --year 2025 --closed-by "Kurt"

# Check for spots requiring manual review
python cli/assign_languages.py --review-required
```

### **Manual Language Processing:**
```bash
# If you need to run language assignment separately
python cli/assign_languages.py --categorize-all
python cli/assign_languages.py --process-all-categories
python cli/assign_languages.py --status
```

### **Language Assignment Categories:**
- **LANGUAGE_ASSIGNMENT_REQUIRED**: Algorithmic language detection needed
- **REVIEW_CATEGORY**: Manual business review required
- **DEFAULT_ENGLISH**: Should default to English based on business rules

---

## Decision Flowchart

```
What data do you have?
│
├── Closed month data (Excel file)
│   └── Use: cli/import_closed_data.py ⭐ PRIMARY TOOL
│
├── Complete workbook with new month
│   └── Use: src/importers/smart_monthly_import.py
│
├── Weekly update file (open months only)
│   └── Use: src/cli/weekly_update.py
│
└── Need to manage individual months
    └── Use: src/cli/close_month.py
```

## Typical Monthly Workflow

```bash
# 1. Import newly closed month (most common)
python cli/import_closed_data.py data/raw/May-2025.xlsx --year 2025 --closed-by "Kurt"

# 2. Check for spots requiring manual review
python cli/assign_languages.py --review-required

# 3. Throughout the month, update open months as needed
python src/cli/weekly_update.py data/raw/weekly_bookings.xlsx

# 4. Manual month management if needed
python src/cli/close_month.py --status "current_month"
```

---

## Important Notes

### **Safety First**
- Always use `--dry-run` first on important imports
- Closed month imports are permanent - they close months forever
- Weekly updates are blocked if file contains closed months (built-in protection)
- Smart monthly import automatically protects closed months

### **Data Protection**
- Closed months cannot be modified - system prevents accidental overwrites
- Complete audit trail - every import is tracked with batch IDs
- Transaction safety - no deadlocks or corruption
- Memory efficient - handles large files without crashing

### **Column Mapping**
- Always captures 29 columns from any Excel format
- Handles 2024 and 2025 format differences automatically
- Maps alternative column names (e.g., "Gross" vs "Unit rate Gross")
- Ignores unmapped columns safely

### **Language Assignment**
- All imports automatically process language assignments
- Business rules categorize spots for appropriate processing
- Manual review queue populated for complex cases
- Complete integration with import workflow

---

## Tool Summary

| Scenario | Tool | Use Case | Frequency |
|----------|------|----------|-----------|
| **Closed Month Import** | `cli/import_closed_data.py` ⭐ | **Primary tool for all closed data** | **Monthly/Historical** |
| **Smart Filtering** | `src/importers/smart_monthly_import.py` | Complete workbook filtering | As needed |
| **Weekly Updates** | `src/cli/weekly_update.py` | Open month updates | Weekly |
| **Month Management** | `src/cli/close_month.py` | Individual month control | As needed |
| **Language Assignment** | `cli/assign_languages.py` | Manual language processing | After imports |

---

## Quick Start Examples

### **Most Common: Import Closed Month**
```bash
# This is your primary workflow
python cli/import_closed_data.py data/raw/May-2025.xlsx --year 2025 --closed-by "Kurt"
```

### **Check Language Assignment Status**
```bash
# After any import, check for review items
python cli/assign_languages.py --review-required
python cli/assign_languages.py --status
```

### **Weekly Business Updates**
```bash
# Update open months with latest bookings
python src/cli/weekly_update.py data/raw/current_bookings.xlsx --dry-run
python src/cli/weekly_update.py data/raw/current_bookings.xlsx
```

### **Check System Status**
```bash
# See what months are closed
python src/cli/close_month.py --list

# Check specific month
python src/cli/close_month.py --status "Jun-25"
```

---

## Success Indicators

**Successful Import:**
- "Success: ✓" in results
- Records imported count matches expectations
- Months closed automatically (for closed month imports)
- Language assignments processed
- No error messages
- Duration reasonable (1-8 minutes depending on file size)

**System Health:**
- Transaction management working (no "database locked" errors)
- Memory usage stable (MB not GB)
- Closed months protected from modification
- Language assignments completed
- Audit trail complete

---

## Troubleshooting

**Common Issues:**

1. **"Database locked" errors**
   - SOLVED - BaseService transaction management eliminates this

2. **"Month already closed" errors**
   - SOLVED - Smart monthly import automatically skips closed months

3. **Language assignment errors**
   - Check: `uv run python cli/assign_languages.py --status`
   - Review: `uv run python cli/assign_languages.py --review-required`

---

## Quick Reference Commands

```bash
# Primary import tool (monthly/historical)
uv run python cli/import_closed_data.py data/raw/YYYY.xlsx --year YYYY --closed-by "Kurt"

# Language assignment management
uv run python cli/assign_languages.py --review-required
uv run python cli/assign_languages.py --status

# Check what months are closed
uv run python src/cli/close_month.py --list

# Weekly updates
uv run python src/cli/weekly_update.py data/raw/weekly.xlsx --dry-run

# Close individual month
uv run python src/cli/close_month.py "Month-YY" --closed-by "Kurt"
```

---



