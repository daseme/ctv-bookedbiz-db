# Diff-Based Commercial Log Import — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the delete-and-reinsert import with a contract-group fingerprint diff that only writes rows that actually changed, eliminating ~132K unnecessary WAL writes per day.

**Architecture:** Compare `(bill_code, contract, broadcast_month)` → `(sum_cents, row_count)` fingerprints between Excel and DB. Only delete+reinsert groups where the fingerprint differs. Falls back to full flush (with ntfy alert) if >80% of groups changed.

**Tech Stack:** Python 3, SQLite, openpyxl, pytest, existing BroadcastMonthImportService

**Spec:** `docs/superpowers/specs/2026-03-24-diff-based-import-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `src/services/import_diff.py` | Create | Fingerprint building, comparison, and diff application — all diff logic lives here |
| `src/services/broadcast_month_import_service.py` | Modify | Wire diff into `_execute_import_workflow`, add `import_strategy` param, extract row-processing helper |
| `src/models/import_workflow.py` | Modify | Add `import_strategy` field to `ImportContext` dataclass |
| `tests/services/test_import_diff.py` | Create | Unit tests for fingerprint logic |
| `tests/services/test_diff_integration.py` | Create | Integration test: full diff import against temp DB |

### Design Decisions (from spec review)

1. **Month parsing**: `import_diff.py` reuses `BroadcastMonthImportService._parse_month_value` via import — no duplicate parser. The `_parse_month` helper is just a lightweight wrapper that delegates to it.
2. **Sheet name tracking**: When building `all_rows` for fingerprinting, each row is tagged with its sheet name as an extra element appended to the tuple. `_apply_diff` extracts it before passing to `_process_single_row`.
3. **Fallback threshold**: Only counts groups that exist in both Excel and DB (not purely added/removed). If `total_comparable == 0` (e.g., entirely new month), no fallback fires — this is correct since adding new data is not anomalous.
4. **Column index caching**: `_process_single_row` receives `month_col_index` as a parameter rather than recomputing per call.

---

### Task 1: Fingerprint Building — Pure Functions

**Files:**
- Create: `src/services/import_diff.py`
- Create: `tests/services/test_import_diff.py`

- [ ] **Step 1: Write failing test for DB fingerprint building**

```python
# tests/services/test_import_diff.py
import pytest
import sqlite3
import tempfile
import os

SCHEMA = """
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    bill_code TEXT,
    contract TEXT,
    broadcast_month TEXT,
    spot_value DECIMAL(12,2),
    air_date DATE,
    import_batch_id TEXT
);
CREATE TABLE month_closures (
    broadcast_month TEXT PRIMARY KEY
);
"""

@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.close()
    yield path
    os.unlink(path)


@pytest.fixture()
def conn(db_path):
    c = sqlite3.connect(db_path)
    yield c
    c.close()


class TestBuildDbFingerprints:
    def test_groups_by_contract(self, conn):
        conn.executemany(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
            [
                ("Acme:Widget", "100", "Mar-26", 150.00),
                ("Acme:Widget", "100", "Mar-26", 250.00),
                ("Acme:Widget", "101", "Mar-26", 300.00),
            ],
        )
        from src.services.import_diff import build_db_fingerprints

        fps = build_db_fingerprints(["Mar-26"], conn)
        assert fps[("Acme:Widget", "100", "Mar-26")] == (40000, 2)  # $400.00 = 40000 cents
        assert fps[("Acme:Widget", "101", "Mar-26")] == (30000, 1)

    def test_coalesces_null_contract(self, conn):
        conn.execute(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
            ("Acme:Widget", None, "Mar-26", 100.00),
        )
        from src.services.import_diff import build_db_fingerprints

        fps = build_db_fingerprints(["Mar-26"], conn)
        assert ("Acme:Widget", "", "Mar-26") in fps

    def test_coalesces_null_spot_value(self, conn):
        conn.execute(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
            ("Acme:Widget", "100", "Mar-26", None),
        )
        from src.services.import_diff import build_db_fingerprints

        fps = build_db_fingerprints(["Mar-26"], conn)
        assert fps[("Acme:Widget", "100", "Mar-26")] == (0, 1)

    def test_empty_months_returns_empty(self, conn):
        from src.services.import_diff import build_db_fingerprints

        fps = build_db_fingerprints([], conn)
        assert fps == {}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestBuildDbFingerprints -v`
Expected: FAIL — `import_diff` module not found

- [ ] **Step 3: Implement build_db_fingerprints**

```python
# src/services/import_diff.py
"""
Contract-group fingerprint diff for commercial log imports.

Compares (bill_code, contract, broadcast_month) groups between Excel and DB.
Only groups with changed fingerprints (sum_cents, row_count) get deleted and reinserted.
"""

import logging
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Type aliases
GroupKey = Tuple[str, str, str]  # (bill_code, contract, broadcast_month)
Fingerprint = Tuple[int, int]   # (sum_cents, row_count)


def build_db_fingerprints(
    months: List[str], conn: sqlite3.Connection
) -> Dict[GroupKey, Fingerprint]:
    """Build fingerprints from existing DB spots for the given months.

    Returns dict mapping (bill_code, contract, broadcast_month) to (sum_cents, row_count).
    Uses integer cents to avoid floating-point comparison issues.
    """
    if not months:
        return {}

    placeholders = ",".join("?" * len(months))
    cursor = conn.execute(
        f"""
        SELECT bill_code,
               COALESCE(contract, ''),
               broadcast_month,
               CAST(ROUND(SUM(COALESCE(spot_value, 0)) * 100, 0) AS INTEGER),
               COUNT(*)
        FROM spots
        WHERE broadcast_month IN ({placeholders})
        GROUP BY bill_code, COALESCE(contract, ''), broadcast_month
        """,
        months,
    )
    return {
        (row[0], row[1], row[2]): (row[3], row[4])
        for row in cursor.fetchall()
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestBuildDbFingerprints -v`
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/services/import_diff.py tests/services/test_import_diff.py
git commit -m "feat: add build_db_fingerprints for contract-group diff"
```

---

### Task 2: Excel Fingerprint Building

**Files:**
- Modify: `src/services/import_diff.py`
- Modify: `tests/services/test_import_diff.py`

- [ ] **Step 1: Write failing test for Excel fingerprint building**

```python
# Add to tests/services/test_import_diff.py

class TestBuildExcelFingerprints:
    def test_groups_rows_and_computes_cents(self):
        from src.services.import_diff import build_excel_fingerprints

        # Simulate raw Excel rows — tuples matching EXCEL_COLUMN_POSITIONS
        # Index 0=bill_code, 17=spot_value, 18=broadcast_month, 27=contract
        rows = [
            self._make_row("Acme:Widget", "100", "2026-03-01", 150.00),
            self._make_row("Acme:Widget", "100", "2026-03-01", 250.00),
            self._make_row("Acme:Widget", "101", "2026-03-01", 300.00),
        ]

        fps, grouped, months = build_excel_fingerprints(rows)
        assert fps[("Acme:Widget", "100", "Mar-26")] == (40000, 2)
        assert fps[("Acme:Widget", "101", "Mar-26")] == (30000, 1)
        assert len(grouped[("Acme:Widget", "100", "Mar-26")]) == 2
        assert "Mar-26" in months

    def test_null_spot_value_treated_as_zero(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [self._make_row("Acme:Widget", "100", "2026-03-01", None)]
        fps, grouped, months = build_excel_fingerprints(rows)
        assert fps[("Acme:Widget", "100", "Mar-26")] == (0, 1)

    def test_null_contract_coalesced(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [self._make_row("Acme:Widget", None, "2026-03-01", 100.00)]
        fps, grouped, months = build_excel_fingerprints(rows)
        assert ("Acme:Widget", "", "Mar-26") in fps

    def test_skips_rows_without_broadcast_month(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [self._make_row("Acme:Widget", "100", None, 100.00)]
        fps, grouped, months = build_excel_fingerprints(rows)
        assert len(fps) == 0

    @staticmethod
    def _make_row(bill_code, contract, month_date, spot_value):
        """Build a 30-element tuple matching EXCEL_COLUMN_POSITIONS layout."""
        from datetime import datetime
        row = [None] * 30
        row[0] = bill_code       # bill_code
        row[17] = spot_value     # spot_value
        row[27] = contract       # contract
        # broadcast_month (col 18) — simulate datetime like openpyxl returns
        if month_date:
            row[18] = datetime.strptime(month_date, "%Y-%m-%d")
        row[1] = row[18]         # air_date (needs a value to not skip)
        return tuple(row)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestBuildExcelFingerprints -v`
Expected: FAIL — `build_excel_fingerprints` not found

- [ ] **Step 3: Implement build_excel_fingerprints**

```python
# Add to src/services/import_diff.py

from src.services.import_integration_utilities import _parse_month_value


def _parse_month(value: Any) -> Optional[str]:
    """Parse a broadcast_month cell into 'Mmm-YY' display format.

    Delegates to _parse_month_value from import_integration_utilities
    to ensure parity with the existing import pipeline.
    """
    return _parse_month_value(value)


def _to_cents(value: Any) -> int:
    """Convert a spot_value cell to integer cents."""
    try:
        return round(float(value or 0) * 100)
    except (TypeError, ValueError):
        return 0


def build_excel_fingerprints(
    rows: List[Tuple],
) -> Tuple[
    Dict[GroupKey, Fingerprint],
    Dict[GroupKey, List[Tuple]],
    set,
]:
    """Build fingerprints from Excel rows (raw tuples from openpyxl).

    Args:
        rows: List of tuples matching EXCEL_COLUMN_POSITIONS layout.

    Returns:
        (fingerprints, grouped_rows, months_found)
        - fingerprints: {group_key: (sum_cents, row_count)}
        - grouped_rows: {group_key: [raw_row, ...]}
        - months_found: set of broadcast month strings
    """
    # Column indices from EXCEL_COLUMN_POSITIONS
    BILL_CODE = 0
    SPOT_VALUE = 17
    BROADCAST_MONTH = 18
    CONTRACT = 27
    AIR_DATE = 1

    fingerprints: Dict[GroupKey, Fingerprint] = {}
    grouped_rows: Dict[GroupKey, List[Tuple]] = {}
    months: set = set()
    cents_accum: Dict[GroupKey, int] = {}
    count_accum: Dict[GroupKey, int] = {}

    for raw_row in rows:
        # Rows may have a sheet-name tag appended — use only data columns for checks
        row = raw_row[:30] if len(raw_row) > 30 else raw_row
        if not any(row):
            continue

        # Must have broadcast_month and bill_code
        month_val = row[BROADCAST_MONTH] if BROADCAST_MONTH < len(row) else None
        month_display = _parse_month(month_val)
        if not month_display:
            continue

        bill_code = row[BILL_CODE] if BILL_CODE < len(row) else None
        if not bill_code:
            continue
        bill_code = str(bill_code).strip()

        air_date = row[AIR_DATE] if AIR_DATE < len(row) else None
        if not air_date:
            continue

        contract = row[CONTRACT] if CONTRACT < len(row) else None
        contract = str(contract).strip() if contract else ""

        spot_value = row[SPOT_VALUE] if SPOT_VALUE < len(row) else None

        key = (bill_code, contract, month_display)
        months.add(month_display)

        cents_accum[key] = cents_accum.get(key, 0) + _to_cents(spot_value)
        count_accum[key] = count_accum.get(key, 0) + 1
        grouped_rows.setdefault(key, []).append(raw_row)  # preserve full tagged row

    fingerprints = {
        key: (cents_accum[key], count_accum[key]) for key in cents_accum
    }

    return fingerprints, grouped_rows, months
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestBuildExcelFingerprints -v`
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/services/import_diff.py tests/services/test_import_diff.py
git commit -m "feat: add build_excel_fingerprints for contract-group diff"
```

---

### Task 3: Diff Comparison Logic

**Files:**
- Modify: `src/services/import_diff.py`
- Modify: `tests/services/test_import_diff.py`

- [ ] **Step 1: Write failing test for compare_fingerprints**

```python
# Add to tests/services/test_import_diff.py

from src.services.import_diff import GroupKey, Fingerprint


class TestCompareFingerprints:
    def test_unchanged_groups(self):
        from src.services.import_diff import compare_fingerprints

        excel_fps = {("A", "100", "Mar-26"): (10000, 5)}
        db_fps = {("A", "100", "Mar-26"): (10000, 5)}
        result = compare_fingerprints(excel_fps, db_fps)
        assert ("A", "100", "Mar-26") in result.unchanged
        assert len(result.changed) == 0

    def test_changed_value(self):
        from src.services.import_diff import compare_fingerprints

        excel_fps = {("A", "100", "Mar-26"): (15000, 5)}
        db_fps = {("A", "100", "Mar-26"): (10000, 5)}
        result = compare_fingerprints(excel_fps, db_fps)
        assert ("A", "100", "Mar-26") in result.changed

    def test_changed_count(self):
        from src.services.import_diff import compare_fingerprints

        excel_fps = {("A", "100", "Mar-26"): (10000, 6)}
        db_fps = {("A", "100", "Mar-26"): (10000, 5)}
        result = compare_fingerprints(excel_fps, db_fps)
        assert ("A", "100", "Mar-26") in result.changed

    def test_new_group(self):
        from src.services.import_diff import compare_fingerprints

        excel_fps = {("A", "100", "Mar-26"): (10000, 5)}
        db_fps = {}
        result = compare_fingerprints(excel_fps, db_fps)
        assert ("A", "100", "Mar-26") in result.added

    def test_removed_group(self):
        from src.services.import_diff import compare_fingerprints

        excel_fps = {}
        db_fps = {("A", "100", "Mar-26"): (10000, 5)}
        result = compare_fingerprints(excel_fps, db_fps)
        assert ("A", "100", "Mar-26") in result.removed

    def test_fallback_threshold(self):
        from src.services.import_diff import compare_fingerprints

        # 9 out of 10 groups changed = 90% > 80% threshold
        excel_fps = {(f"C{i}", "100", "Mar-26"): (i * 100, 1) for i in range(10)}
        db_fps = {(f"C{i}", "100", "Mar-26"): (i * 200, 1) for i in range(10)}
        # One group matches
        db_fps[("C0", "100", "Mar-26")] = (0, 1)
        result = compare_fingerprints(excel_fps, db_fps)
        assert result.should_fallback is True

    def test_no_fallback_below_threshold(self):
        from src.services.import_diff import compare_fingerprints

        # 1 out of 10 changed = 10%
        excel_fps = {(f"C{i}", "100", "Mar-26"): (i * 100, 1) for i in range(10)}
        db_fps = dict(excel_fps)
        db_fps[("C0", "100", "Mar-26")] = (99999, 1)  # one change
        result = compare_fingerprints(excel_fps, db_fps)
        assert result.should_fallback is False
```

- [ ] **Step 2: Run test to verify it fails**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestCompareFingerprints -v`
Expected: FAIL — `compare_fingerprints` not found

- [ ] **Step 3: Implement compare_fingerprints**

```python
# Add to src/services/import_diff.py
from dataclasses import dataclass, field


FALLBACK_THRESHOLD = 0.80  # Fall back to full flush if >80% of groups changed


@dataclass
class DiffResult:
    """Result of comparing Excel vs DB fingerprints."""
    unchanged: set = field(default_factory=set)   # GroupKeys that match
    changed: set = field(default_factory=set)      # GroupKeys with different fingerprint
    added: set = field(default_factory=set)        # GroupKeys only in Excel
    removed: set = field(default_factory=set)      # GroupKeys only in DB
    should_fallback: bool = False                  # True if too many groups changed

    @property
    def total_groups(self) -> int:
        return len(self.unchanged) + len(self.changed) + len(self.added) + len(self.removed)

    @property
    def groups_requiring_writes(self) -> int:
        return len(self.changed) + len(self.added) + len(self.removed)


def compare_fingerprints(
    excel_fps: Dict[GroupKey, Fingerprint],
    db_fps: Dict[GroupKey, Fingerprint],
    fallback_threshold: float = FALLBACK_THRESHOLD,
) -> DiffResult:
    """Compare Excel and DB fingerprints, returning which groups need action."""
    result = DiffResult()

    all_keys = set(excel_fps.keys()) | set(db_fps.keys())

    for key in all_keys:
        in_excel = key in excel_fps
        in_db = key in db_fps

        if in_excel and in_db:
            if excel_fps[key] == db_fps[key]:
                result.unchanged.add(key)
            else:
                result.changed.add(key)
        elif in_excel:
            result.added.add(key)
        else:
            result.removed.add(key)

    # Check fallback threshold
    total_comparable = len(result.unchanged) + len(result.changed)
    if total_comparable > 0:
        change_ratio = len(result.changed) / total_comparable
        if change_ratio > fallback_threshold:
            result.should_fallback = True

    return result
```

- [ ] **Step 4: Run test to verify it passes**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py::TestCompareFingerprints -v`
Expected: 7 PASSED

- [ ] **Step 5: Run all diff tests**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py -v`
Expected: 15 PASSED (4 + 4 + 7)

- [ ] **Step 6: Commit**

```bash
git add src/services/import_diff.py tests/services/test_import_diff.py
git commit -m "feat: add compare_fingerprints with fallback threshold"
```

---

### Task 4: Extract Row-Processing Helper

**Files:**
- Modify: `src/services/broadcast_month_import_service.py:870-1027` (extract from `_import_excel_data_with_progress`)

- [ ] **Step 1: Extract _process_single_row from the loop body**

Extract the row-processing logic from the inner loop of `_import_excel_data_with_progress` (lines 873-1022) into a new method. The method processes one Excel row and returns a dict ready for INSERT, or None to skip.

```python
# Add to BroadcastMonthImportService, before _import_excel_data_with_progress

def _process_single_row(
    self,
    row: tuple,
    current_sheet_name: str,
    filename: str,
    batch_id: str,
    allowed_months: List[str],
    conn: sqlite3.Connection,
    unmatched_customers: Set[str],
    unmatched_agencies: Set[str],
    sheet_source_stats: Dict[str, int],
) -> Optional[Dict[str, Any]]:
    """Process a single Excel row into a spot_data dict for INSERT.

    Returns None if the row should be skipped (empty, filtered, invalid).
    Mutates unmatched_customers, unmatched_agencies, sheet_source_stats as side effects.
    """
    if not any(row):
        return None

    month_col_index = [
        k for k, v in EXCEL_COLUMN_POSITIONS.items() if v == "broadcast_month"
    ]
    if not month_col_index:
        return None
    month_value = row[month_col_index[0]]
    if not month_value:
        return None

    broadcast_month_display = self._parse_month_value(month_value)
    if not broadcast_month_display:
        return None

    if broadcast_month_display not in allowed_months:
        return None  # filtered by month

    spot_data: Dict[str, Any] = {
        "import_batch_id": batch_id,
        "broadcast_month": broadcast_month_display,
    }

    sheet_source: Optional[str] = None

    for col_idx, field_name in EXCEL_COLUMN_POSITIONS.items():
        if field_name and col_idx < len(row):
            val = row[col_idx]
            if val is None or val == "":
                continue

            if field_name == "sheet_source":
                sheet_source = str(val).strip() if val else None
                continue

            if field_name == "bill_code":
                spot_data[field_name] = str(val).strip()
            elif field_name == "air_date":
                if hasattr(val, "date"):
                    spot_data[field_name] = val.date().isoformat()
                else:
                    spot_data[field_name] = str(val).strip()
            elif field_name in ["gross_rate", "station_net", "spot_value", "broker_fees"]:
                try:
                    spot_data[field_name] = float(val)
                except:
                    spot_data[field_name] = None
            elif field_name == "day_of_week":
                try:
                    spot_data[field_name] = normalize_broadcast_day(str(val).strip())
                except:
                    spot_data[field_name] = str(val).strip()
            elif field_name == "revenue_type":
                spot_data[field_name] = self._normalize_revenue_type(str(val).strip())
            elif field_name == "spot_type":
                spot_data[field_name] = self._normalize_spot_type(str(val).strip())
            elif field_name != "broadcast_month":
                spot_data[field_name] = str(val).strip()

    effective_source = sheet_source or current_sheet_name
    sheet_source_stats[effective_source] = sheet_source_stats.get(effective_source, 0) + 1
    spot_data["source_file"] = SourceFileFormatter.format_source_file(filename, effective_source)

    if "market_name" in spot_data:
        market_id = self._lookup_market_id(spot_data["market_name"], conn)
        if market_id:
            spot_data["market_id"] = market_id

    if "bill_code" in spot_data:
        entity_result = self.batch_resolver.lookup_entities_cached(spot_data["bill_code"], conn)
        if entity_result.customer_id:
            spot_data["customer_id"] = entity_result.customer_id
        else:
            unmatched_customers.add(spot_data["bill_code"])
        if entity_result.agency_id:
            spot_data["agency_id"] = entity_result.agency_id
        else:
            if ":" in spot_data["bill_code"]:
                unmatched_agencies.add(spot_data["bill_code"].split(":", 1)[0].strip())

    if "language_id" not in spot_data:
        spot_data["language_id"] = 1

    if not spot_data.get("bill_code") or not spot_data.get("air_date"):
        return None

    return spot_data
```

- [ ] **Step 2: Refactor _import_excel_data_with_progress to use the helper**

Replace the inner loop body (lines 873-1022) with a call to `_process_single_row`. The method should produce identical behavior — same SQL inserts, same stats tracking, same error handling. Keep the outer sheet loop, progress bar, and stats logging unchanged.

- [ ] **Step 3: Rebuild and verify existing import still works**

Run: `docker compose build --quiet && docker compose up -d`
Then: `docker compose exec -T spotops uv run python cli/daily_update.py "/app/data/raw/daily/Commercial Log 260324.xlsx" --auto-setup --unattended --verbose 2>&1 | tail -15`
Expected: Same 16,548 records imported, same sheet breakdown as before.

- [ ] **Step 4: Commit**

```bash
git add src/services/broadcast_month_import_service.py
git commit -m "refactor: extract _process_single_row from import loop"
```

---

### Task 5: Wire Diff Into Import Workflow

**Files:**
- Modify: `src/services/broadcast_month_import_service.py:286-361` (add `import_strategy` param)
- Modify: `src/services/broadcast_month_import_service.py:643-716` (`_execute_import_workflow`)

- [ ] **Step 1: Add import_strategy field to ImportContext and execute_month_replacement**

In `src/models/import_workflow.py` line 132, add `import_strategy: str = "diff"` field to `ImportContext` dataclass (after `dry_run`).

In `execute_month_replacement` (line 286), add `import_strategy: str = "diff"` parameter. Pass it into `ImportContext` construction at line 313.

- [ ] **Step 2: Add _execute_diff_workflow method**

```python
# Add to BroadcastMonthImportService, after _execute_import_workflow

def _execute_diff_workflow(
    self, context: ImportContext, result: ImportResult
) -> ImportResult:
    """Execute import using contract-group fingerprint diff.

    Only deletes and reinserts groups where (bill_code, contract, broadcast_month)
    fingerprint (sum_cents, row_count) differs between Excel and DB.
    """
    from src.services.import_diff import (
        build_db_fingerprints,
        build_excel_fingerprints,
        compare_fingerprints,
    )

    tqdm.write("Phase 1: Building entity cache...")
    self.batch_resolver.build_entity_cache_from_excel(
        context.excel_analysis.file_path
    )

    self._create_import_batch(
        context.batch_id,
        context.import_mode,
        context.excel_analysis.file_path,
        context.months_to_process,
    )

    try:
        # Build Excel fingerprints from all sheets
        # Tag each row with its sheet name (appended as extra element)
        tqdm.write("Phase 2: Building Excel fingerprints...")
        all_rows = []
        sheets, workbook = get_all_import_worksheets(context.excel_analysis.file_path)
        for worksheet, sheet_name in sheets:
            for row in worksheet.iter_rows(min_row=2, values_only=True):
                all_rows.append(tuple(row) + (sheet_name,))
        workbook.close()

        excel_fps, grouped_rows, _ = build_excel_fingerprints(all_rows)
        tqdm.write(f"  Excel: {len(excel_fps)} contract groups")

        with self.safe_transaction() as conn:
            # Build DB fingerprints
            tqdm.write("Phase 3: Building DB fingerprints...")
            db_fps = build_db_fingerprints(context.months_to_process, conn)
            tqdm.write(f"  DB: {len(db_fps)} contract groups")

            # Compare
            tqdm.write("Phase 4: Comparing fingerprints...")
            diff = compare_fingerprints(excel_fps, db_fps)

            tqdm.write(
                f"  Unchanged: {len(diff.unchanged)} | "
                f"Changed: {len(diff.changed)} | "
                f"Added: {len(diff.added)} | "
                f"Removed: {len(diff.removed)}"
            )

            # Check fallback
            if diff.should_fallback:
                tqdm.write(
                    f"WARNING: {diff.groups_requiring_writes}/{diff.total_groups} groups "
                    f"require writes — falling back to full flush"
                )
                logger.warning(
                    f"Diff fallback triggered: {diff.groups_requiring_writes}/{diff.total_groups} "
                    f"groups changed (threshold: 80%)"
                )
                # Fall back to full flush
                result.records_deleted = self._delete_months_with_progress(
                    context.months_to_process, conn
                )
                result.records_imported = self._import_excel_data_with_progress(
                    context.excel_analysis.file_path,
                    context.batch_id,
                    conn,
                    context.months_to_process,
                )
                # Send ntfy alert
                self._send_fallback_alert(context, diff)
            else:
                # Surgical diff apply
                tqdm.write("Phase 5: Applying diff...")
                deleted, imported = self._apply_diff(
                    diff, grouped_rows, context, conn
                )
                result.records_deleted = deleted
                result.records_imported = imported

            self._validate_and_correct_customers(context.batch_id, conn)

            if context.is_historical_mode:
                result.closed_months = self._close_months(
                    context.months_to_process, context.closed_by, conn
                )

            self._complete_import_batch(context.batch_id, result, conn)
            result.success = True
            tqdm.write("Import committed successfully")

    except Exception as e:
        error_msg = f"Diff import failed: {str(e)}"
        tqdm.write(f"ERROR: {error_msg}")
        result.add_error(error_msg)
        self._fail_import_batch(context.batch_id, str(e))
        raise BroadcastMonthImportError(error_msg)

    self._refresh_cache_tables()
    return result
```

- [ ] **Step 3: Add _apply_diff method**

```python
def _apply_diff(
    self,
    diff,
    grouped_rows: dict,
    context,
    conn: sqlite3.Connection,
) -> Tuple[int, int]:
    """Delete changed/removed groups and insert changed/new groups.

    Returns (total_deleted, total_imported).
    """
    filename = SourceFileFormatter.extract_filename_from_path(
        context.excel_analysis.file_path
    )
    total_deleted = 0
    total_imported = 0
    unmatched_customers: Set[str] = set()
    unmatched_agencies: Set[str] = set()
    sheet_source_stats: Dict[str, int] = {}

    # Delete changed + removed groups
    groups_to_delete = diff.changed | diff.removed
    for key in groups_to_delete:
        bill_code, contract, month = key
        if contract:
            deleted = conn.execute(
                "DELETE FROM spots WHERE bill_code = ? AND contract = ? AND broadcast_month = ?",
                (bill_code, contract, month),
            ).rowcount
        else:
            deleted = conn.execute(
                "DELETE FROM spots WHERE bill_code = ? AND (contract IS NULL OR contract = '') AND broadcast_month = ?",
                (bill_code, month),
            ).rowcount
        total_deleted += deleted

    # Insert changed + new groups
    groups_to_insert = diff.changed | diff.added
    for key in groups_to_insert:
        rows = grouped_rows.get(key, [])
        for tagged_row in rows:
            # Last element is the sheet name tag; rest is the original row
            row = tagged_row[:-1]
            sheet_name = tagged_row[-1] if isinstance(tagged_row[-1], str) else ""
            spot_data = self._process_single_row(
                row, sheet_name, filename, context.batch_id,
                context.months_to_process, conn,
                unmatched_customers, unmatched_agencies,
                sheet_source_stats,
            )
            if spot_data is None:
                continue

            fields = list(spot_data.keys())
            placeholders = ", ".join(["?"] * len(fields))
            field_names = ", ".join(fields)
            values = [spot_data[f] for f in fields]
            conn.execute(
                f"INSERT INTO spots ({field_names}) VALUES ({placeholders})",
                values,
            )
            total_imported += 1

    if unmatched_customers:
        tqdm.write(f"Unmatched customers: {len(unmatched_customers)}")
    if unmatched_agencies:
        tqdm.write(f"Unmatched agencies: {len(unmatched_agencies)}")
    if sheet_source_stats:
        tqdm.write("Sheet breakdown (changed groups):")
        for sheet, count in sorted(sheet_source_stats.items()):
            tqdm.write(f"   {sheet}: {count:,} records")

    return total_deleted, total_imported
```

- [ ] **Step 4: Add _send_fallback_alert method**

```python
def _send_fallback_alert(self, context, diff):
    """Send ntfy alert when diff fallback to full flush is triggered."""
    try:
        import urllib.request
        import os

        topic = os.environ.get("NTFY_TOPIC", "")
        if not topic:
            return

        msg = (
            f"Diff import fell back to full flush.\n"
            f"Groups changed: {len(diff.changed)}/{diff.total_groups}\n"
            f"File: {context.excel_analysis.file_path}"
        )
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=msg.encode(),
            headers={
                "Title": "CTV Import: Diff Fallback",
                "Priority": "4",
                "Tags": "warning",
            },
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.warning(f"Failed to send fallback alert: {e}")
```

- [ ] **Step 5: Update _execute_import_workflow to dispatch by strategy**

Modify `_execute_import_workflow` (line 643) to check `context.import_strategy`. If `"diff"`, call `_execute_diff_workflow`. If `"full"`, use existing logic.

```python
def _execute_import_workflow(
    self, context: ImportContext, result: ImportResult
) -> ImportResult:
    """Execute the actual import — dispatches to diff or full strategy."""
    if context.import_strategy == 'diff':
        return self._execute_diff_workflow(context, result)

    # Original full-flush implementation follows...
    # (existing code unchanged)
```

- [ ] **Step 6: Commit**

```bash
git add src/services/broadcast_month_import_service.py
git commit -m "feat: wire diff workflow into import pipeline with fallback"
```

---

### Task 6: Integration Test

**Files:**
- Create: `tests/services/test_diff_integration.py`

- [ ] **Step 1: Write integration test for no-change import**

```python
# tests/services/test_diff_integration.py
"""Integration tests for diff-based import against a real temp database."""
import pytest
import sqlite3
import tempfile
import os
from unittest.mock import MagicMock

SCHEMA = """
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    bill_code TEXT, air_date DATE, end_date DATE, day_of_week TEXT,
    time_in TEXT, time_out TEXT, length_seconds TEXT, media TEXT,
    comments TEXT, language_code TEXT, format TEXT, sequence_number INTEGER,
    line_number INTEGER, spot_type TEXT, estimate TEXT,
    gross_rate DECIMAL(12,2), make_good TEXT, spot_value DECIMAL(12,2),
    broadcast_month TEXT, broker_fees DECIMAL(12,2), priority INTEGER,
    station_net DECIMAL(12,2), sales_person TEXT, revenue_type TEXT,
    billing_type TEXT, agency_flag TEXT, affidavit_flag TEXT,
    contract TEXT, market_name TEXT, customer_id INTEGER,
    agency_id INTEGER, market_id INTEGER, language_id INTEGER,
    load_date TIMESTAMP, source_file TEXT, is_historical BOOLEAN,
    effective_date DATE, import_batch_id TEXT, spot_category TEXT
);
CREATE TABLE month_closures (broadcast_month TEXT PRIMARY KEY);
CREATE TABLE import_batches (
    batch_id TEXT PRIMARY KEY, import_mode TEXT, source_file TEXT,
    broadcast_months_affected TEXT, records_imported INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0, status TEXT DEFAULT 'RUNNING',
    import_date TIMESTAMP, completed_date TIMESTAMP, error_message TEXT
);
CREATE TABLE raw_customer_inputs (raw_text TEXT PRIMARY KEY, created_at TEXT);
CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, normalized_name TEXT, is_active BOOLEAN DEFAULT 1);
CREATE TABLE agencies (agency_id INTEGER PRIMARY KEY, agency_name TEXT, is_active BOOLEAN DEFAULT 1);
CREATE TABLE entity_aliases (alias_id INTEGER PRIMARY KEY, alias_name TEXT, entity_type TEXT, target_entity_id INTEGER, is_active BOOLEAN DEFAULT 1);
CREATE TABLE markets (market_id INTEGER PRIMARY KEY, market_code TEXT, market_name TEXT);
"""


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.close()
    yield path
    os.unlink(path)


class TestNoChangeImport:
    def test_unchanged_groups_keep_spot_ids(self, db_path):
        """When fingerprints match, spot_ids must be preserved."""
        from src.services.import_diff import (
            build_db_fingerprints,
            build_excel_fingerprints,
            compare_fingerprints,
        )

        conn = sqlite3.connect(db_path)

        # Seed DB with spots
        conn.executemany(
            """INSERT INTO spots (bill_code, contract, broadcast_month, spot_value, air_date)
               VALUES (?,?,?,?,?)""",
            [
                ("Acme:Widget", "100", "Mar-26", 150.00, "2026-03-01"),
                ("Acme:Widget", "100", "Mar-26", 250.00, "2026-03-02"),
                ("Beta:Gizmo", "200", "Mar-26", 500.00, "2026-03-01"),
            ],
        )
        conn.commit()

        original_ids = {
            row[0]
            for row in conn.execute("SELECT spot_id FROM spots").fetchall()
        }

        # Build matching Excel rows
        from tests.services.test_import_diff import TestBuildExcelFingerprints
        make_row = TestBuildExcelFingerprints._make_row
        excel_rows = [
            make_row("Acme:Widget", "100", "2026-03-01", 150.00),
            make_row("Acme:Widget", "100", "2026-03-01", 250.00),
            make_row("Beta:Gizmo", "200", "2026-03-01", 500.00),
        ]

        excel_fps, grouped, months = build_excel_fingerprints(excel_rows)
        db_fps = build_db_fingerprints(["Mar-26"], conn)
        diff = compare_fingerprints(excel_fps, db_fps)

        assert len(diff.unchanged) == 2
        assert len(diff.changed) == 0
        assert len(diff.added) == 0
        assert len(diff.removed) == 0

        # Verify spot_ids unchanged
        current_ids = {
            row[0]
            for row in conn.execute("SELECT spot_id FROM spots").fetchall()
        }
        assert current_ids == original_ids

        conn.close()
```

- [ ] **Step 2: Run integration test**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_diff_integration.py -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add tests/services/test_diff_integration.py
git commit -m "test: add integration test for diff-based import"
```

---

### Task 7: End-to-End Verification

**Files:** No new files — testing against live container

- [ ] **Step 1: Rebuild and deploy**

```bash
docker compose build --quiet && docker compose up -d
```

Wait for health check: `docker compose ps` should show `(healthy)`.

- [ ] **Step 2: Run all unit tests**

Run: `docker compose exec -T spotops uv run pytest tests/services/test_import_diff.py tests/services/test_diff_integration.py -v`
Expected: All tests PASS

- [ ] **Step 3: Record current spot_ids for comparison**

```bash
docker compose exec -T spotops uv run python3 -c "
import sqlite3
conn = sqlite3.connect('/srv/spotops/db/production.db')
c = conn.cursor()
c.execute('SELECT spot_id, bill_code, contract, broadcast_month FROM spots WHERE broadcast_month = \"Mar-26\" ORDER BY spot_id LIMIT 10')
for r in c.fetchall(): print(r)
print()
c.execute('SELECT COUNT(*) FROM spots WHERE broadcast_month NOT IN (SELECT broadcast_month FROM month_closures)')
print(f'Total open-month spots: {c.fetchone()[0]}')
"
```

- [ ] **Step 4: Run diff import against production data**

```bash
docker compose exec -T spotops uv run python cli/daily_update.py \
    "/app/data/raw/daily/Commercial Log 260324.xlsx" \
    --auto-setup --unattended --verbose 2>&1 | tail -30
```

Expected output should show:
- "Building Excel fingerprints..." / "Building DB fingerprints..."
- "Unchanged: ~211 | Changed: 0 | Added: 0 | Removed: 0"
- `records_imported=0, records_deleted=0`
- Same spot_ids preserved

- [ ] **Step 5: Verify spot_ids were preserved**

Run the same query from Step 3 — spot_ids should be identical.

- [ ] **Step 6: Verify import batch was recorded**

```bash
docker compose exec -T spotops uv run python3 -c "
import sqlite3
conn = sqlite3.connect('/srv/spotops/db/production.db')
c = conn.cursor()
c.execute('SELECT batch_id, records_imported, records_deleted, status FROM import_batches ORDER BY import_date DESC LIMIT 3')
for r in c.fetchall(): print(r)
"
```

Expected: Latest batch shows `records_imported=0, records_deleted=0, status=COMPLETED`

- [ ] **Step 7: Commit any final adjustments**

```bash
git add src/services/import_diff.py src/services/broadcast_month_import_service.py src/models/import_workflow.py tests/services/test_import_diff.py tests/services/test_diff_integration.py
git commit -m "chore: finalize diff-based import — verified on production data"
```
