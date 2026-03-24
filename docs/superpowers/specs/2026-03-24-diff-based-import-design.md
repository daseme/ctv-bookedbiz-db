# Diff-Based Commercial Log Import

**Date**: 2026-03-24
**Status**: Approved

## Problem

The 4x daily import deletes and reinserts ~16,500 spots across all open months every run, even when nothing changed. This causes:

1. **WAL bloat** — continuous Litestream backups to Backblaze capture 33K row writes per import (delete + insert) x 4/day = ~132K unnecessary row writes daily
2. **Unstable spot_ids** — every import destroys and recreates all spot_ids, preventing any future reference stability
3. **No change visibility** — impossible to tell what actually changed between imports

## Design

### Core Concept

Diff at the **contract group** level: `(bill_code, contract, broadcast_month)`. If the fingerprint for a group matches between Excel and DB, skip it entirely. Only delete and reinsert groups where the fingerprint differs.

This matches the business model: the planning horizon cares about dollars per contract per month. Individual spot details are implementation that gets locked in at month close.

### Fingerprint Comparison

Compare integer cents and row count to avoid floating-point mismatch. SQLite stores DECIMAL(12,2) as REAL (IEEE 754 float), and summation order between Python and SQL can produce tiny discrepancies. Comparing in integer cents eliminates this.

**Excel side (Python):**
```python
# For each group: sum spot_values as integer cents
cents = sum(round(float(v or 0) * 100) for v in spot_values)
fingerprint = (cents, row_count)
```

**DB side (SQL):**
```sql
SELECT bill_code, COALESCE(contract, ''), broadcast_month,
       CAST(ROUND(SUM(COALESCE(spot_value, 0)) * 100, 0) AS INTEGER),
       COUNT(*)
FROM spots
WHERE broadcast_month IN (/* open months */)
GROUP BY bill_code, COALESCE(contract, ''), broadcast_month
```

NULL/empty spot_value is treated as 0 on both sides.

### Algorithm

**Step 1 — Build Excel fingerprints.** Single pass over all importable sheets. Group rows by `(bill_code, COALESCE(contract, ''), broadcast_month)`. For each group, compute `(sum_cents, row_count)`. Bucket raw rows by group key in memory. This pass subsumes the current month-analysis read — months are extracted as a side effect, reducing total Excel file reads from 3 to 2 (analysis + fingerprint combined, then selective row processing).

**Step 2 — Build DB fingerprints.** One query returning the same `(group_key) -> (sum_cents, row_count)` structure.

**Step 3 — Compare.**

| Excel | DB | Action |
|-------|-----|--------|
| Fingerprint matches | Fingerprint matches | Skip — zero writes |
| Fingerprint differs | Fingerprint exists | Delete group rows, insert new rows |
| Group exists | Group missing | Insert new rows |
| Group missing | Group exists | Delete group rows |

**Step 4 — Log summary.** Report groups unchanged (spots preserved), groups changed, groups added, groups removed, total writes vs what full flush would have done.

### Integration Point

The change lives inside `_execute_import_workflow` in `broadcast_month_import_service.py`. The surrounding code stays unchanged:

**Before (unchanged):**
- `batch_resolver.build_entity_cache_from_excel()` — entity cache
- `_create_import_batch()` — audit record
- `safe_transaction()` begins

**Replaced (inside the transaction):**
- ~~`_delete_months_with_progress()`~~ and ~~`_import_excel_data_with_progress()`~~
- New: `_build_excel_fingerprints()`, `_build_db_fingerprints()`, `_diff_and_apply()`

**After (unchanged):**
- `_validate_and_correct_customers()` — customer alignment
- `_close_months()` — for HISTORICAL mode
- `_complete_import_batch()` — finalize audit record
- `_refresh_cache_tables()` — outside transaction

### Row Processing Helper

The current row-processing logic (~160 lines embedded in a nested loop) is extracted into a helper:

```python
def _process_excel_row(
    self, row: tuple, current_sheet_name: str, filename: str,
    batch_id: str, allowed_months: List[str], conn: sqlite3.Connection
) -> Optional[Dict[str, Any]]:
    """Process a single Excel row into a spot_data dict ready for INSERT.
    Returns None if the row should be skipped (empty, filtered, invalid).
    Side effect: populates unmatched_customers/agencies sets for reporting."""
```

This helper is used by both the diff path (inserting only changed groups) and the full-flush fallback path. Column mapping, normalization, entity resolution, and source tracking are unchanged — just moved into a callable unit.

### Fallback

If >80% of groups changed, fall back to full flush. This is a safety net — if it fires, it means something unexpected happened. On fallback:

- Log at WARNING level with full details (groups changed, total groups, threshold)
- Send ntfy notification so it can be investigated
- Proceed with full flush (current behavior)

A full flush should effectively never happen under normal operation. If it does, that's a signal to investigate.

### Import Strategy Flag

Add `import_strategy` parameter to `execute_month_replacement` (default `"diff"`, option `"full"`). This preserves the old behavior behind a flag for rollback without code changes.

### Schema

No schema changes. The `spots`, `import_batches`, and all existing tables stay as-is.

**Behavioral changes:**
- `import_batches.records_imported` and `records_deleted` reflect actual writes. A no-change import records `0/0` instead of `16548/16548`. The batch record is always created for audit trail.
- Unchanged spots retain their original `import_batch_id` from when they were last written. A batch only tracks the spots it actually touched, which is better for auditing.

### Edge Cases

- **NULL contract**: Coalesced to `''` on both sides so grouping matches
- **NULL/empty spot_value**: Treated as 0 on both sides (Python: `float(v or 0)`, SQL: `COALESCE(spot_value, 0)`)
- **Empty sheets**: Sheets with only a header row contribute zero rows to fingerprinting — handled gracefully
- **New month appears in Excel**: All groups are "new" — pure inserts, no deletes
- **Month disappears from Excel** (e.g., closed externally): All groups are "removed" — pure deletes
- **Worldlink Lines / Pending / Add to booked business sheets**: Handled identically — the multi-sheet reader feeds all rows into the same fingerprint grouping
- **Concurrent imports**: Inherited `BEGIN IMMEDIATE` lock prevents data corruption. Second import blocks until first completes.

### Error Handling

- Excel fingerprint failure -> fall back to full flush with ntfy alert (no regression from today)
- Transaction failure -> `safe_transaction()` rolls back ALL changes atomically; no group is partially modified
- Fallback trigger -> logged at WARNING + ntfy notification for audit

### Testing

- Unit test fingerprint comparison logic (pure function, no DB)
- Unit test integer-cents conversion with known float edge cases
- Integration test: run diff import, verify unchanged groups keep their spot_ids
- Verify no-change import produces zero deletes and zero inserts
- Verify WAL file size stays flat on no-change import
- Verify fallback fires and alerts when >80% groups differ (simulated)

### Expected Impact

On a typical no-change import (majority of the 4x daily runs):
- **Before**: ~33K row writes (16.5K delete + 16.5K insert), ~50s
- **After**: 0 row writes, <5s (fingerprint comparison only)
- **WAL reduction**: ~132K unnecessary writes/day eliminated
