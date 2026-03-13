# CRM Phase 1: Signal Queue + Renewal Gaps — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn passive entity signals into an actionable work queue with renewal gap detection and revenue-at-risk visibility.

**Architecture:** New `signal_actions` table tracks signal lifecycle (new → acknowledged/snoozed/dismissed). `SignalActionService` manages the queue. Renewal gaps are computed as a new signal type during `refresh_signals()`. Auto-acknowledge hook in `ActivityService.create_activity()` closes the loop when AEs log activity. UI updates to My Accounts page replace the action items section with a signal queue.

**Tech Stack:** Python/Flask, SQLite, vanilla JS (no new dependencies)

**Spec:** `docs/superpowers/specs/2026-03-12-crm-enhancements-design.md` — Phase 1 sections 1a-1d.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `sql/migrations/025_signal_actions.sql` | Create | Migration: signal_actions table |
| `src/services/signal_action_service.py` | Create | Signal queue CRUD, sync logic, snooze/dismiss |
| `tests/services/test_signal_action_service.py` | Create | Tests for SignalActionService |
| `src/services/entity_metrics_service.py` | Modify | Add renewal_gap signal type + snapshot-based sync hook |
| `tests/services/test_entity_metrics_service.py` | Modify | Add tests for renewal_gap computation |
| `src/services/activity_service.py` | Modify | Auto-acknowledge hook in create_activity |
| `tests/services/test_activity_service.py` | Modify | Add tests for auto-acknowledge |
| `src/services/ae_crm_service.py` | Modify | Add revenue_at_risk to get_stats |
| `tests/services/test_ae_crm_service.py` | Modify | Test revenue_at_risk stat |
| `src/services/factory.py` | Modify | Register signal_action_service |
| `src/web/routes/ae_crm.py` | Modify | Add signal queue API endpoints |
| `src/web/templates/ae_my_accounts.html` | Modify | Signal queue UI section |
| `src/web/static/js/ae_my_accounts.js` | Modify | Signal queue fetch/render/actions |

---

## Context for Implementers

### Key patterns to follow

**Database connection:** Services extend `BaseService` and accept `db_connection` in `__init__`. Routes get connections via `get_container().get("database_connection")` then `with db.connection_ro() as conn:` for reads, `with db.connection() as conn:` for writes.

**Broadcast month format:** Stored as `'Jan-25'` (mmm-yy). Convert to ISO for comparison:
```sql
'20' || SUBSTR(broadcast_month, 5, 2) || '-' ||
CASE SUBSTR(broadcast_month, 1, 3)
    WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
    WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
    WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
    WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
END
```

**Revenue exclusions:** Always exclude Trade: `WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)`

**entity_signals PK:** Composite `(entity_type, entity_id, signal_type)` — no signal_id column.

**Test runner:** `/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/path -v`

**Service registration:** Factory function at bottom of `src/services/factory.py`, singleton registration in `initialize_services()` after the ae_crm_service block (~line 332).

---

## Chunk 1: Data Layer + SignalActionService

### Task 1: Create signal_actions migration

**Files:**
- Create: `sql/migrations/025_signal_actions.sql`

- [ ] **Step 1: Write migration file**

```sql
-- 025_signal_actions.sql
-- Signal action queue: tracks lifecycle of entity signals as work items

CREATE TABLE IF NOT EXISTS signal_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL CHECK (entity_type IN ('customer', 'agency')),
    entity_id INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    assigned_ae TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'acknowledged', 'snoozed', 'dismissed')),
    reason TEXT,
    snooze_until DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_signal_actions_ae_status
    ON signal_actions(assigned_ae, status);
CREATE INDEX IF NOT EXISTS idx_signal_actions_entity
    ON signal_actions(entity_type, entity_id, signal_type);
```

- [ ] **Step 2: Run migration against production DB**

```bash
sqlite3 /var/lib/ctv-bookedbiz-db/production.db < sql/migrations/025_signal_actions.sql
```

Verify: `sqlite3 /var/lib/ctv-bookedbiz-db/production.db ".schema signal_actions"` — should show the table.

- [ ] **Step 3: Commit**

```bash
git add sql/migrations/025_signal_actions.sql
git commit -m "feat: add signal_actions migration (025)"
```

---

### Task 2: Create SignalActionService with queue queries

**Files:**
- Create: `src/services/signal_action_service.py`
- Create: `tests/services/test_signal_action_service.py`

- [ ] **Step 1: Write tests for core signal action operations**

Create `tests/services/test_signal_action_service.py` with a test fixture that creates an in-memory SQLite database with the required schema (entity_signals, signal_actions, agencies, customers, entity_activity). Seed it with test data.

Test classes and cases:

**`TestGetQueue`** — tests for `get_queue(conn, ae_name)`
1. `test_returns_new_actions_for_ae` — seed 2 actions for AE "Alice", 1 for "Bob". Call with ae_name="Alice", assert 2 returned.
2. `test_excludes_acknowledged_and_dismissed` — seed actions in all 4 statuses for same AE. Assert only "new" returned.
3. `test_includes_expired_snooze_as_new` — seed a snoozed action with snooze_until = yesterday. Assert it appears in queue (status shown as "new").
4. `test_excludes_active_snooze` — seed snoozed action with snooze_until = tomorrow. Assert not in queue.
5. `test_sorted_by_priority_then_age` — seed actions with different signal_priority and created_date. Assert order: lowest priority number first, then oldest first within same priority.
6. `test_includes_signal_label_and_revenue` — seed matching entity_signals row. Assert returned dict includes signal_label and trailing_revenue from the joined entity_signals row.

**`TestSnooze`** — tests for `snooze_action(conn, action_id, reason, snooze_until, updated_by)`
1. `test_snooze_sets_status_and_date` — snooze an action, verify status = "snoozed", snooze_until and reason set.
2. `test_snooze_requires_reason` — call with empty reason, assert error returned.
3. `test_snooze_nonexistent_returns_error` — call with bad action_id, assert error.

**`TestDismiss`** — tests for `dismiss_action(conn, action_id, reason, updated_by)`
1. `test_dismiss_sets_status` — dismiss an action, verify status = "dismissed" and reason stored.
2. `test_dismiss_requires_reason` — call with empty reason, assert error.

**`TestAcknowledgeForEntity`** — tests for `acknowledge_for_entity(conn, entity_type, entity_id, updated_by)`
1. `test_acknowledges_all_new_actions_for_entity` — seed 2 new actions for same entity. Call acknowledge. Assert both status = "acknowledged".
2. `test_does_not_touch_dismissed_actions` — seed 1 dismissed action. Call acknowledge. Assert still "dismissed".
3. `test_acknowledges_expired_snoozed_actions` — seed snoozed action past snooze_until. Call acknowledge. Assert "acknowledged".

**`TestSyncFromSignals`** — tests for `sync_from_signals(conn, before_snapshot, ae_lookup)`
1. `test_creates_actions_for_new_signals` — pass empty before_snapshot and entity_signals with 2 rows. Assert 2 new signal_actions created.
2. `test_acknowledges_actions_for_removed_signals` — seed signal_action for a signal that's in before_snapshot but not in current entity_signals. Assert status = "acknowledged".
3. `test_does_not_duplicate_existing_open_actions` — seed an existing "new" signal_action. Pass same signal in current entity_signals. Assert no duplicate created.
4. `test_reverts_expired_snoozes_before_sync` — seed expired snoozed action. Run sync. Assert status reverted to "new".

- [ ] **Step 2: Run tests to verify they fail**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_signal_action_service.py -v
```

Expected: ImportError (module doesn't exist yet).

- [ ] **Step 3: Implement SignalActionService**

Create `src/services/signal_action_service.py`:

```python
"""Service for signal action queue management.

Tracks the lifecycle of entity signals as work items that AEs
must acknowledge, snooze, or dismiss.
"""

from src.services.base_service import BaseService


class SignalActionService(BaseService):
    """Manages signal action queue for AE workflow."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_queue(self, conn, ae_name):
        """Get unworked signal actions for an AE.

        Returns signal_actions with status='new' plus expired snoozes,
        joined with entity_signals for label/revenue data.
        Sorted by signal_priority ASC then age DESC.

        Args:
            conn: Database connection (writable — reverts expired snoozes).
            ae_name: AE name to filter by.

        Returns:
            List of action dicts with signal metadata.
        """
        # First, revert expired snoozes (write operation)
        conn.execute("""
            UPDATE signal_actions
            SET status = 'new', updated_date = CURRENT_TIMESTAMP
            WHERE status = 'snoozed'
              AND snooze_until < date('now')
        """)

        rows = conn.execute("""
            SELECT
                sa.action_id,
                sa.entity_type,
                sa.entity_id,
                sa.signal_type,
                sa.status,
                sa.created_date,
                sa.snooze_until,
                sa.reason,
                es.signal_label,
                es.signal_priority,
                es.trailing_revenue,
                CASE sa.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = sa.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = sa.entity_id)
                END AS entity_name,
                CAST(julianday('now') - julianday(sa.created_date)
                     AS INTEGER) AS days_aging
            FROM signal_actions sa
            LEFT JOIN entity_signals es
                ON es.entity_type = sa.entity_type
               AND es.entity_id = sa.entity_id
               AND es.signal_type = sa.signal_type
            WHERE sa.assigned_ae = ?
              AND sa.status = 'new'
            ORDER BY
                COALESCE(es.signal_priority, 99) ASC,
                sa.created_date ASC
        """, [ae_name]).fetchall()

        return [dict(r) for r in rows]

    def snooze_action(self, conn, action_id, reason, snooze_until,
                      updated_by):
        """Snooze a signal action until a future date.

        Args:
            conn: Database connection (writable).
            action_id: ID of the signal_action.
            reason: Why the AE is deferring (required).
            snooze_until: ISO date string for wake-up.
            updated_by: Who performed the action.

        Returns:
            Dict with success or error key.
        """
        if not reason or not reason.strip():
            return {"error": "Reason is required to snooze a signal"}

        action = conn.execute(
            "SELECT action_id FROM signal_actions WHERE action_id = ?",
            [action_id],
        ).fetchone()
        if not action:
            return {"error": "Signal action not found", "status": 404}

        conn.execute("""
            UPDATE signal_actions
            SET status = 'snoozed',
                reason = ?,
                snooze_until = ?,
                updated_by = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE action_id = ?
        """, [reason.strip(), snooze_until, updated_by, action_id])

        return {"success": True, "action_id": action_id}

    def dismiss_action(self, conn, action_id, reason, updated_by):
        """Dismiss a signal action as not actionable.

        Args:
            conn: Database connection (writable).
            action_id: ID of the signal_action.
            reason: Why the AE is dismissing (required).
            updated_by: Who performed the action.

        Returns:
            Dict with success or error key.
        """
        if not reason or not reason.strip():
            return {"error": "Reason is required to dismiss a signal"}

        action = conn.execute(
            "SELECT action_id FROM signal_actions WHERE action_id = ?",
            [action_id],
        ).fetchone()
        if not action:
            return {"error": "Signal action not found", "status": 404}

        conn.execute("""
            UPDATE signal_actions
            SET status = 'dismissed',
                reason = ?,
                updated_by = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE action_id = ?
        """, [reason.strip(), updated_by, action_id])

        return {"success": True, "action_id": action_id}

    def acknowledge_for_entity(self, conn, entity_type, entity_id,
                               updated_by):
        """Acknowledge all open signal actions for an entity.

        Called automatically when an AE logs activity. Reverts
        expired snoozes first so they get acknowledged too.

        Args:
            conn: Database connection (writable).
            entity_type: 'customer' or 'agency'.
            entity_id: Entity ID.
            updated_by: Who performed the action.
        """
        # Revert expired snoozes for this entity first
        conn.execute("""
            UPDATE signal_actions
            SET status = 'new', updated_date = CURRENT_TIMESTAMP
            WHERE entity_type = ? AND entity_id = ?
              AND status = 'snoozed'
              AND snooze_until < date('now')
        """, [entity_type, entity_id])

        conn.execute("""
            UPDATE signal_actions
            SET status = 'acknowledged',
                updated_by = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE entity_type = ? AND entity_id = ?
              AND status = 'new'
        """, [updated_by, entity_type, entity_id])

    def sync_from_signals(self, conn, before_snapshot, ae_lookup):
        """Sync signal_actions with current entity_signals state.

        Called after refresh_signals() completes. Uses a diff between
        the before-snapshot and current entity_signals to create new
        actions and acknowledge removed ones.

        Args:
            conn: Database connection (writable).
            before_snapshot: Set of (entity_type, entity_id, signal_type)
                tuples captured before refresh_signals ran.
            ae_lookup: Dict mapping (entity_type, entity_id) to
                assigned_ae string.
        """
        # Step 1: Revert expired snoozes
        conn.execute("""
            UPDATE signal_actions
            SET status = 'new', updated_date = CURRENT_TIMESTAMP
            WHERE status = 'snoozed'
              AND snooze_until < date('now')
        """)

        # Step 2: Get current signals
        current_rows = conn.execute("""
            SELECT entity_type, entity_id, signal_type
            FROM entity_signals
        """).fetchall()
        current_signals = {
            (r["entity_type"], r["entity_id"], r["signal_type"])
            for r in current_rows
        }

        # Step 3: New signals — create actions if no open action exists
        new_signals = current_signals - before_snapshot
        for entity_type, entity_id, signal_type in new_signals:
            ae = ae_lookup.get((entity_type, entity_id))
            if not ae:
                continue
            existing = conn.execute("""
                SELECT 1 FROM signal_actions
                WHERE entity_type = ? AND entity_id = ?
                  AND signal_type = ?
                  AND status IN ('new', 'snoozed')
            """, [entity_type, entity_id, signal_type]).fetchone()
            if not existing:
                conn.execute("""
                    INSERT INTO signal_actions
                        (entity_type, entity_id, signal_type,
                         assigned_ae, status)
                    VALUES (?, ?, ?, ?, 'new')
                """, [entity_type, entity_id, signal_type, ae])

        # Step 4: Removed signals — acknowledge open actions
        removed_signals = before_snapshot - current_signals
        for entity_type, entity_id, signal_type in removed_signals:
            conn.execute("""
                UPDATE signal_actions
                SET status = 'acknowledged',
                    updated_by = 'system:signal_recovered',
                    updated_date = CURRENT_TIMESTAMP
                WHERE entity_type = ? AND entity_id = ?
                  AND signal_type = ?
                  AND status IN ('new', 'snoozed')
            """, [entity_type, entity_id, signal_type])
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_signal_action_service.py -v
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/services/signal_action_service.py tests/services/test_signal_action_service.py
git commit -m "feat: add SignalActionService with queue, snooze, dismiss, sync"
```

---

### Task 3: Register SignalActionService in factory

**Files:**
- Modify: `src/services/factory.py`

- [ ] **Step 1: Add factory function**

Add at the bottom of factory.py, after the `create_ae_crm_service` function:

```python
def create_signal_action_service():
    """Factory function for SignalActionService."""
    from src.services.signal_action_service import SignalActionService

    container = get_container()
    db_connection = container.get("database_connection")
    return SignalActionService(db_connection)
```

- [ ] **Step 2: Register in initialize_services()**

Add after the ae_crm_service registration block (~line 332):

```python
print("🔧 Registering signal_action_service...")
container.register_singleton("signal_action_service", create_signal_action_service)
print("✅ signal_action_service registered")
```

- [ ] **Step 3: Verify import works**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -c "from src.services.signal_action_service import SignalActionService; print('OK')"
```

- [ ] **Step 4: Commit**

```bash
git add src/services/factory.py
git commit -m "feat: register SignalActionService in factory"
```

---

## Chunk 2: Renewal Gap Detection + Auto-Acknowledge Hook

### Task 4: Add renewal_gap signal to refresh_signals

**Files:**
- Modify: `src/services/entity_metrics_service.py`
- Modify: `tests/services/test_entity_metrics_service.py`

- [ ] **Step 1: Write tests for renewal_gap detection**

Add a new test class `TestRenewalGapSignal` to `tests/services/test_entity_metrics_service.py`.

Test cases:
1. `test_renewal_gap_detected` — customer has $10K trailing 3 months, $0 forward 3 months. After refresh_signals, entity_signals contains a row with signal_type='renewal_gap', signal_priority=1.
2. `test_no_gap_when_forward_sufficient` — customer has $10K trailing, $8K forward. No renewal_gap signal.
3. `test_gap_when_forward_below_25pct` — customer has $10K trailing, $2K forward (20%). renewal_gap signal created.
4. `test_no_gap_for_zero_trailing` — customer has $0 trailing, $0 forward. No renewal_gap (nothing to lose).
5. `test_renewal_gap_label_includes_dollars` — verify signal_label contains dollar amounts (e.g., "Renewal gap: $10,000 trailing, $0 forward").
6. `test_renewal_gap_for_agency` — agency customer has trailing revenue but no forward. Gap signal created for agency entity.

The test fixture needs to seed spots in specific broadcast months relative to "now" to test trailing vs forward windows. Use broadcast months that are 1-3 months ago for trailing and 1-3 months ahead for forward.

- [ ] **Step 2: Run tests to verify they fail**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_entity_metrics_service.py::TestRenewalGapSignal -v
```

Expected: FAIL (renewal_gap not computed yet).

- [ ] **Step 3: Implement renewal_gap in refresh_signals**

Add a new section to `refresh_signals()` in `entity_metrics_service.py`, after the existing signal computation block. The renewal gap query runs separately from the existing 5 signal types because it uses a different time window (3-month vs 12-month).

```python
# --- Renewal gap detection ---
# Trailing 3 broadcast months vs forward 3 broadcast months
# An account has a renewal gap when trailing > $0 and forward < 25% of trailing

bm_to_iso = """
    '20' || SUBSTR(s.broadcast_month, 5, 2) || '-' ||
    CASE SUBSTR(s.broadcast_month, 1, 3)
        WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
        WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
        WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
        WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
        WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
    END
"""

for entity_col, entity_type, entity_table, join_clause in [
    ("customer_id", "customer", "customers",
     "s.customer_id = e.customer_id"),
    ("agency_id", "agency", "agencies",
     "s.customer_id IN (SELECT customer_id FROM customers WHERE agency_id = e.agency_id)"),
]:
    gap_rows = conn.execute(f"""
        SELECT
            sub.entity_id,
            sub.trailing_3m,
            sub.forward_3m
        FROM (
            SELECT
                e.{entity_col} AS entity_id,
                COALESCE(SUM(CASE
                    WHEN {bm_to_iso} >= strftime('%Y-%m', 'now', '-3 months')
                     AND {bm_to_iso} < strftime('%Y-%m', 'now')
                    THEN s.gross_rate ELSE 0 END), 0) AS trailing_3m,
                COALESCE(SUM(CASE
                    WHEN {bm_to_iso} >= strftime('%Y-%m', 'now')
                     AND {bm_to_iso} < strftime('%Y-%m', 'now', '+3 months')
                    THEN s.gross_rate ELSE 0 END), 0) AS forward_3m
            FROM {entity_table} e
            LEFT JOIN spots s ON {join_clause}
                AND s.is_historical = 0
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            WHERE e.is_active = 1
            GROUP BY e.{entity_col}
        ) sub
        WHERE sub.trailing_3m > 0
          AND sub.forward_3m < sub.trailing_3m * 0.25
    """).fetchall()

    for row in gap_rows:
        trailing = row["trailing_3m"]
        forward = row["forward_3m"]
        label = (
            f"Renewal gap: ${trailing:,.0f} trailing, "
            f"${forward:,.0f} forward"
        )
        conn.execute("""
            INSERT OR REPLACE INTO entity_signals
                (entity_type, entity_id, signal_type, signal_label,
                 signal_priority, trailing_revenue, prior_revenue)
            VALUES (?, ?, 'renewal_gap', ?, 1, ?, ?)
        """, [entity_type, row["entity_id"], label, trailing, forward])
```

Note: The `LEFT JOIN` and entity_clause patterns must be adapted to match the existing query style in `refresh_signals()`. The implementer should read the existing method first and follow its conventions.

- [ ] **Step 4: Run tests to verify they pass**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_entity_metrics_service.py::TestRenewalGapSignal -v
```

Expected: All pass.

- [ ] **Step 5: Run existing metric tests to verify no regression**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_entity_metrics_service.py -v
```

Expected: All existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/services/entity_metrics_service.py tests/services/test_entity_metrics_service.py
git commit -m "feat: add renewal_gap signal type to refresh_signals"
```

---

### Task 5: Add snapshot + sync hook to refresh_signals

**Files:**
- Modify: `src/services/entity_metrics_service.py`
- Create or modify: `tests/services/test_signal_sync.py`

- [ ] **Step 1: Write tests for signal sync integration**

Create `tests/services/test_signal_sync.py` with tests for the snapshot-based sync that runs around refresh_signals.

Test cases:
1. `test_sync_creates_actions_after_refresh` — set up entity_signals with 2 signals, run refresh (which deletes + reinserts them). After sync, signal_actions has 2 "new" rows.
2. `test_sync_acknowledges_recovered_entities` — set up a signal_action for a signal. Run refresh where that signal is no longer computed. After sync, action status = "acknowledged".
3. `test_sync_uses_ae_from_entity` — seed customer with assigned_ae="Alice". After sync, signal_action.assigned_ae = "Alice".
4. `test_sync_skips_entities_without_ae` — entity with assigned_ae = NULL. No signal_action created.

- [ ] **Step 2: Run tests to verify they fail**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_signal_sync.py -v
```

- [ ] **Step 3: Implement snapshot + sync in refresh_signals**

Modify `refresh_signals()` in `entity_metrics_service.py` to:

1. **Before the DELETE:** Capture snapshot of current signals.
2. **After all INSERT logic:** Build ae_lookup from agencies + customers, then call `SignalActionService.sync_from_signals()`.

Add inside `refresh_signals()`, after `self.ensure_cache_tables(conn)` and **before** `conn.execute("DELETE FROM entity_signals")` (the DELETE is on line ~88 — the snapshot MUST come before it or the diff will always be empty):
```python
# Snapshot current signals before delete
before_snapshot = {
    (r["entity_type"], r["entity_id"], r["signal_type"])
    for r in conn.execute(
        "SELECT entity_type, entity_id, signal_type FROM entity_signals"
    ).fetchall()
}
```

Add at the end of `refresh_signals`, after all signal inserts:
```python
# Build AE lookup for signal action assignment
ae_rows = conn.execute("""
    SELECT 'agency' AS entity_type, agency_id AS entity_id,
           assigned_ae
    FROM agencies WHERE is_active = 1 AND assigned_ae IS NOT NULL
    UNION ALL
    SELECT 'customer' AS entity_type, customer_id AS entity_id,
           assigned_ae
    FROM customers WHERE is_active = 1 AND assigned_ae IS NOT NULL
""").fetchall()
ae_lookup = {
    (r["entity_type"], r["entity_id"]): r["assigned_ae"]
    for r in ae_rows
}

# Sync signal actions with refreshed signals
from src.services.signal_action_service import SignalActionService
svc = SignalActionService(self.db_connection)
svc.sync_from_signals(conn, before_snapshot, ae_lookup)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_signal_sync.py -v
```

- [ ] **Step 5: Run all metric + signal action tests**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_entity_metrics_service.py tests/services/test_signal_action_service.py tests/services/test_signal_sync.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/services/entity_metrics_service.py tests/services/test_signal_sync.py
git commit -m "feat: add snapshot-based signal sync to refresh_signals"
```

---

### Task 6: Add auto-acknowledge hook to ActivityService

**Files:**
- Modify: `src/services/activity_service.py`
- Modify: `tests/services/test_activity_service.py`

- [ ] **Step 1: Write tests for auto-acknowledge**

Add a new test class `TestAutoAcknowledgeOnActivity` to `tests/services/test_activity_service.py`.

The test fixture needs the signal_actions table in its schema. Add it to the `activity_db` fixture's `executescript` call.

Test cases:
1. `test_logging_call_acknowledges_signal` — seed a "new" signal_action for a customer. Log a "call" activity. Assert signal_action status = "acknowledged".
2. `test_logging_note_acknowledges_signal` — same but with "note" activity type.
3. `test_status_change_does_not_acknowledge` — seed a "new" signal_action. Log a "status_change" activity. Assert signal_action status still "new".
4. `test_follow_up_does_not_acknowledge` — seed a "new" signal_action. Log a "follow_up" activity. Assert status still "new".
5. `test_acknowledge_does_not_touch_dismissed` — seed a "dismissed" signal_action. Log a "call". Assert still "dismissed".
6. `test_no_signal_actions_no_error` — log activity for entity with no signal_actions. Assert no error (graceful no-op).

- [ ] **Step 2: Run tests to verify they fail**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_activity_service.py::TestAutoAcknowledgeOnActivity -v
```

- [ ] **Step 3: Implement the hook**

In `activity_service.py`, modify `create_activity()`. After the successful INSERT and before the return statement (around line 100), add:

```python
# Auto-acknowledge signal actions for qualifying activity types
if activity_type in ("note", "call", "email", "meeting"):
    conn.execute("""
        UPDATE signal_actions
        SET status = 'acknowledged',
            updated_by = ?,
            updated_date = CURRENT_TIMESTAMP
        WHERE entity_type = ? AND entity_id = ?
          AND status = 'new'
    """, [created_by, entity_type, entity_id])
    # Also catch expired snoozes
    conn.execute("""
        UPDATE signal_actions
        SET status = 'acknowledged',
            updated_by = ?,
            updated_date = CURRENT_TIMESTAMP
        WHERE entity_type = ? AND entity_id = ?
          AND status = 'snoozed'
          AND snooze_until < date('now')
    """, [created_by, entity_type, entity_id])
```

Note: This directly updates signal_actions without going through SignalActionService to avoid circular imports. The table is simple enough that raw SQL is cleaner here.

- [ ] **Step 4: Run tests to verify they pass**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_activity_service.py -v
```

Expected: All tests pass (both new and existing).

- [ ] **Step 5: Commit**

```bash
git add src/services/activity_service.py tests/services/test_activity_service.py
git commit -m "feat: auto-acknowledge signal actions on activity logging"
```

---

## Chunk 3: Revenue At Risk + API Routes + UI

### Task 7: Add revenue_at_risk to AeCrmService stats

**Files:**
- Modify: `src/services/ae_crm_service.py`
- Modify: `tests/services/test_ae_crm_service.py`

- [ ] **Step 1: Write test for revenue_at_risk stat**

Add to `TestGetStats` in `tests/services/test_ae_crm_service.py`:

1. `test_revenue_at_risk_sums_renewal_gaps` — seed entity_signals with signal_type='renewal_gap' and trailing_revenue values. Call get_stats(). Assert `stats["revenue_at_risk"]` equals the sum.
2. `test_revenue_at_risk_zero_when_no_gaps` — no renewal_gap signals. Assert `stats["revenue_at_risk"] == 0`.

- [ ] **Step 2: Run tests to verify they fail**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_ae_crm_service.py::TestGetStats -v
```

- [ ] **Step 3: Implement revenue_at_risk in get_stats**

In `ae_crm_service.py`, in the `get_stats()` method, add a query after the follow_up_row query:

```python
ae_signal_filter = ""
signal_params = []
if ae_name:
    ae_signal_filter = """
        AND (
            (es.entity_type = 'customer' AND es.entity_id IN (
                SELECT customer_id FROM customers
                WHERE assigned_ae = ?))
            OR
            (es.entity_type = 'agency' AND es.entity_id IN (
                SELECT agency_id FROM agencies
                WHERE assigned_ae = ?))
        )
    """
    signal_params = [ae_name, ae_name]

at_risk_row = conn.execute(f"""
    SELECT COALESCE(SUM(es.trailing_revenue), 0) AS at_risk
    FROM entity_signals es
    WHERE es.signal_type = 'renewal_gap'
      {ae_signal_filter}
""", signal_params).fetchone()
```

Add to the return dict:
```python
"revenue_at_risk": at_risk_row["at_risk"],
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/test_ae_crm_service.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/services/ae_crm_service.py tests/services/test_ae_crm_service.py
git commit -m "feat: add revenue_at_risk stat from renewal_gap signals"
```

---

### Task 8: Add signal queue API routes

**Files:**
- Modify: `src/web/routes/ae_crm.py`

- [ ] **Step 1: Add three new endpoints to ae_crm.py**

Add before the revenue-trend route (to keep literal routes before parameterized ones):

```python
@ae_crm_bp.route("/api/ae/my-accounts/signal-queue")
@role_required(UserRole.AE)
def api_signal_queue():
    """Return signal action queue for the current AE."""
    ae_name, _, _, _ = _resolve_ae_name()
    if not ae_name:
        return jsonify([])
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        return jsonify(signal_svc.get_queue(conn, ae_name=ae_name))


@ae_crm_bp.route(
    "/api/ae/my-accounts/signal-queue/<int:action_id>/snooze",
    methods=["POST"],
)
@role_required(UserRole.AE)
def api_snooze_signal(action_id):
    """Snooze a signal action."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "")
    snooze_until = data.get("snooze_until", "")
    if not snooze_until:
        return jsonify({"error": "snooze_until date is required"}), 400
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        result = signal_svc.snooze_action(
            conn, action_id, reason, snooze_until,
            updated_by=current_user.full_name,
        )
        if "error" in result:
            return jsonify(result), result.get("status", 400)
        return jsonify(result)


@ae_crm_bp.route(
    "/api/ae/my-accounts/signal-queue/<int:action_id>/dismiss",
    methods=["POST"],
)
@role_required(UserRole.AE)
def api_dismiss_signal(action_id):
    """Dismiss a signal action."""
    data = request.get_json(silent=True) or {}
    reason = data.get("reason", "")
    signal_svc = _svc("signal_action_service")
    with _db().connection() as conn:
        result = signal_svc.dismiss_action(
            conn, action_id, reason,
            updated_by=current_user.full_name,
        )
        if "error" in result:
            return jsonify(result), result.get("status", 400)
        return jsonify(result)
```

Note: `api_signal_queue` uses `connection()` (writable) because `get_queue` runs an UPDATE to revert expired snoozes.

- [ ] **Step 2: Verify routes register**

```bash
DB_PATH=/opt/apps/ctv-bookedbiz-db/.data/dev.db /opt/apps/ctv-bookedbiz-db/.venv/bin/python -c "
from src.web.app import create_app
app = create_app()
routes = [r.rule for r in app.url_map.iter_rules() if 'signal-queue' in r.rule]
for r in sorted(routes):
    print(r)
" 2>&1 | grep -v '^[🔧✅📋📦🏭]'
```

Expected output:
```
/api/ae/my-accounts/signal-queue
/api/ae/my-accounts/signal-queue/<int:action_id>/dismiss
/api/ae/my-accounts/signal-queue/<int:action_id>/snooze
```

- [ ] **Step 3: Commit**

```bash
git add src/web/routes/ae_crm.py
git commit -m "feat: add signal queue API endpoints (list, snooze, dismiss)"
```

---

### Task 9: Update My Accounts UI with signal queue

**Files:**
- Modify: `src/web/templates/ae_my_accounts.html`
- Modify: `src/web/static/js/ae_my_accounts.js`

- [ ] **Step 1: Add signal queue section to template**

In `ae_my_accounts.html`, add a new section between the summary bar and the existing action items section. Add CSS for the signal queue:

```css
/* Signal Queue */
.signal-queue {
    background: #fef2f2; border: 1px solid #fecaca; border-radius: 8px;
    padding: 16px 20px; margin-bottom: 20px;
}
.signal-queue h3 { margin: 0 0 12px; font-size: 15px; color: #991b1b; }
.signal-queue-empty { display: none; }
.signal-item {
    display: flex; align-items: center; gap: 12px; padding: 10px 0;
    border-bottom: 1px solid #fee2e2;
}
.signal-item:last-child { border-bottom: none; }
.signal-aging { font-size: 12px; font-weight: 600; min-width: 40px; }
.signal-aging.normal { color: #6b7280; }
.signal-aging.warning { color: #d97706; }
.signal-aging.critical { color: #dc2626; }
.signal-actions { display: flex; gap: 6px; margin-left: auto; }
.signal-actions button {
    padding: 4px 10px; border-radius: 4px; border: 1px solid #d1d5db;
    background: #fff; font-size: 12px; cursor: pointer;
}
.signal-actions button:hover { background: #f3f4f6; }
.snooze-form {
    display: none; padding: 10px; background: #fffbeb;
    border: 1px solid #fde68a; border-radius: 6px; margin-top: 8px;
}
.snooze-form input, .snooze-form textarea {
    padding: 6px 8px; border: 1px solid #d1d5db; border-radius: 4px;
    font-size: 13px; width: 100%;
}
.snooze-form textarea { height: 40px; resize: vertical; }
```

Add the HTML section after the summary bar:

```html
<!-- Signal Queue -->
<div class="signal-queue" id="signal-queue" style="display:none;">
    <h3>Signals Requiring Action
        <span id="signal-queue-count"
              style="font-weight:400; color:#991b1b;"></span>
    </h3>
    <div id="signal-queue-list"></div>
</div>
```

- [ ] **Step 2: Add revenue_at_risk to summary bar**

Add a fifth stat card after the follow-ups card in the summary bar HTML:

```html
<div class="stat-card" id="card-at-risk">
    <div class="stat-value" id="stat-at-risk"
         style="color:#dc2626;">-</div>
    <div class="stat-label">Revenue At Risk</div>
</div>
```

Update the `.summary-bar` CSS grid to 5 columns:
```css
.summary-bar {
    display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px;
    margin-bottom: 20px;
}
```

And update the responsive rule:
```css
@media (max-width: 768px) {
    .summary-bar { grid-template-columns: repeat(2, 1fr); }
```

- [ ] **Step 3: Add JS functions for signal queue**

Add to `ae_my_accounts.js`:

1. **`loadSignalQueue()`** — fetch `/api/ae/my-accounts/signal-queue` + query string, render items. Show section if items > 0.
2. **`renderSignalItem(item)`** — returns HTML for one signal queue item with aging badge, signal badge, account name link, and snooze/dismiss buttons.
3. **`snoozeSignal(actionId)`** — toggles a snooze form inline, POSTs to snooze endpoint, reloads queue.
4. **`dismissSignal(actionId)`** — prompts for reason, POSTs to dismiss endpoint, reloads queue.
5. Update `loadStats()` to also set `stat-at-risk` from `stats.revenue_at_risk`.
6. Call `loadSignalQueue()` from the DOMContentLoaded handler.

Signal aging logic (computed from `days_aging` in response):
```javascript
function agingClass(days) {
    if (days >= 8) return 'critical';
    if (days >= 4) return 'warning';
    return 'normal';
}
```

- [ ] **Step 4: Update the guide modal**

Add a section to the guide modal in `ae_my_accounts.html` explaining the signal queue — what the signals mean, how to snooze vs dismiss, and that logging activity auto-acknowledges.

- [ ] **Step 5: Test manually**

Restart service, navigate to `/ae/my-accounts`. If no signal_actions exist yet, the queue should be hidden. Run `refresh_signals` against the production DB to generate signal_actions, then verify the queue populates.

- [ ] **Step 6: Commit**

```bash
git add src/web/templates/ae_my_accounts.html src/web/static/js/ae_my_accounts.js
git commit -m "feat: add signal queue UI to My Accounts page"
```

---

### Task 10: Verify end-to-end and final cleanup

**Files:**
- Various

- [ ] **Step 1: Run full test suite**

```bash
/opt/apps/ctv-bookedbiz-db/.venv/bin/python -m pytest tests/services/ --ignore=tests/services/test_language_block_service_basic.py -v
```

All new and existing tests should pass. Note: `test_export_service.py` has 2 pre-existing failures (edi_code column) — these are unrelated.

- [ ] **Step 2: Restart service and manual verification**

Restart the service. Verify:
1. Page loads with signal queue section (hidden if empty)
2. Revenue At Risk card shows in summary bar
3. Signal queue populates after running refresh_signals
4. Snooze and dismiss buttons work
5. Logging an activity acknowledges signals for that account
6. Guide modal includes signal queue documentation

- [ ] **Step 3: Verify git log is clean**

```bash
git log --oneline -15
```

Should show atomic commits for each task.

- [ ] **Step 4: Commit any fixes**

If manual testing reveals issues, fix and commit with descriptive messages.
