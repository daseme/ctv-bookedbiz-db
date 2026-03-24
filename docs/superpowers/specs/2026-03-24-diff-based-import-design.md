# Diff-Based Commercial Log Import

**Date**: 2026-03-24
**Status**: Approved

## Problem

The 4x daily import deletes and reinserts ~16,500 spots across all open months every run, even when nothing changed. This causes:

1. **WAL bloat** — continuous Litestream backups to Backblaze capture 33K row writes per import (delete + insert) × 4/day = ~132K unnecessary row writes daily
2. **Unstable spot_ids** — every import destroys and recreates all spot_ids, preventing any future reference stability
3. **No change visibility** — impossible to tell what actually changed between imports

## Design

### Core Concept

Diff at the **contract group** level: `(bill_code, contract, broadcast_month)`. If the `SUM(spot_value)` and `COUNT(*)` for a group match between Excel and DB, skip it entirely. Only delete and reinsert groups where the fingerprint differs.

This matches the business model: the planning horizon cares about dollars per contract per month. Individual spot details are implementation that gets locked in at month close.

### Algorithm

**Step 1 — Build Excel fingerprints.** Single pass over all importable sheets. Group rows by `(bill_code, contract, broadcast_month)`. For each group, compute `(sum(spot_value), row_count)`. Bucket raw rows by group key in memory.

**Step 2 — Build DB fingerprints.** One query:

```sql
SELECT bill_code, COALESCE(contract, ''), broadcast_month,
       SUM(COALESCE(spot_value, 0)), COUNT(*)
FROM spots
WHERE broadcast_month IN (/* open months */)
GROUP BY bill_code, COALESCE(contract, ''), broadcast_month
```

**Step 3 — Compare.**

| Excel | DB | Action |
|-------|-----|--------|
| Fingerprint matches | Fingerprint matches | Skip — zero writes |
| Fingerprint differs | Fingerprint exists | Delete group rows, insert new rows |
| Group exists | Group missing | Insert new rows |
| Group missing | Group exists | Delete group rows |

**Step 4 — Log summary.** Report groups unchanged (spots preserved), groups changed, groups added, groups removed, total writes vs what full flush would have done.

### Integration Point

The change lives inside `_execute_import_workflow` in `broadcast_month_import_service.py`. Today it calls:

1. `_delete_months_with_progress()` — bulk delete all open months
2. `_import_excel_data_with_progress()` — reinsert everything

The new flow replaces those two calls:

1. `_build_excel_fingerprints(file, months)` — returns dict of fingerprints + grouped raw rows
2. `_build_db_fingerprints(months, conn)` — one SQL query
3. `_diff_and_apply(excel_fps, db_fps, excel_rows, batch_id, conn)` — targeted delete/insert

The row-processing logic (column mapping, normalization, entity resolution) is extracted into a helper used by the diff path. No changes to analysis phase, month classification, entity cache building, or post-import validation.

### Fallback

If >80% of groups changed, fall back to full flush. This is a safety net — if it fires, it means something unexpected happened. On fallback:

- Log at WARNING level with details (how many groups changed, threshold)
- Send ntfy notification so it can be investigated
- Proceed with full flush (current behavior)

A full flush should effectively never happen under normal operation.

### Import Strategy Flag

Add `import_strategy` parameter to `execute_month_replacement` (default `"diff"`, option `"full"`). This preserves the old behavior behind a flag for rollback without code changes.

### Schema

No schema changes. The `spots`, `import_batches`, and all existing tables stay as-is.

Behavioral change: `import_batches.records_imported` and `records_deleted` reflect actual writes. A no-change import records `0/0` instead of `16548/16548`. The batch record is always created for audit trail.

### Edge Cases

- **NULL contract**: Coalesced to `''` on both sides so grouping matches
- **New month appears in Excel**: All groups are "new" — pure inserts, no deletes
- **Month disappears from Excel** (e.g., closed externally): All groups are "removed" — pure deletes
- **Worldlink Lines / Pending / Add to booked business sheets**: Handled identically — the multi-sheet reader feeds all rows into the same fingerprint grouping

### Error Handling

- Excel fingerprint failure → fall back to full flush (no regression from today)
- Partial apply failure → `safe_transaction()` rolls back everything; unchanged groups stay untouched
- Fallback trigger → logged + ntfy notification for audit

### Testing

- Unit test fingerprint comparison logic (pure function, no DB)
- Integration test: run diff import, verify unchanged groups keep their spot_ids
- Verify no-change import produces zero deletes and zero inserts
- Verify WAL file size stays flat on no-change import
- Verify fallback fires when >80% groups differ (simulated)

### Expected Impact

On a typical no-change import (majority of the 4x daily runs):
- **Before**: ~33K row writes (16.5K delete + 16.5K insert)
- **After**: 0 row writes
- **WAL reduction**: ~132K unnecessary writes/day eliminated
- **Import time**: Faster (fingerprint comparison is instant vs 50s full import)
