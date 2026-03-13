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
        # Revert expired snoozes so they appear as 'new' in the queue
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
                sa.assigned_ae,
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

    def snooze_action(self, conn, action_id, reason, snooze_until, updated_by):
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

    def acknowledge_for_entity(self, conn, entity_type, entity_id, updated_by):
        """Acknowledge all open signal actions for an entity.

        Called automatically when an AE logs activity. Reverts expired
        snoozes first so they get acknowledged too.

        Args:
            conn: Database connection (writable).
            entity_type: 'customer' or 'agency'.
            entity_id: Entity ID.
            updated_by: Who performed the action.
        """
        # Revert expired snoozes for this entity so they get acknowledged
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
        # Step 1: Revert expired snoozes globally
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
