"""Service for entity activity log operations.

Extracted from address_book.py routes. All methods accept a conn parameter
(sqlite3.Connection with Row factory) and return plain dicts.
"""

import logging
from datetime import date, timedelta

from src.services.base_service import BaseService

logger = logging.getLogger(__name__)

VALID_ACTIVITY_TYPES = [
    "note", "call", "email", "meeting", "status_change", "follow_up",
]


class ActivityService(BaseService):
    """Manages entity activity logs and follow-ups."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_activities(self, conn, entity_type, entity_id, limit=50):
        """Get activity log for an entity.

        Returns list of activity dicts with joined contact_name.
        """
        rows = conn.execute("""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.activity_type,
                ea.activity_date,
                ea.description,
                ea.created_by,
                ea.created_date,
                ea.contact_id,
                ea.due_date,
                ea.is_completed,
                ea.completed_date,
                ec.contact_name
            FROM entity_activity ea
            LEFT JOIN entity_contacts ec ON ea.contact_id = ec.contact_id
            WHERE ea.entity_type = ? AND ea.entity_id = ?
            ORDER BY ea.activity_date DESC
            LIMIT ?
        """, [entity_type, entity_id, limit]).fetchall()
        return [dict(r) for r in rows]

    def create_activity(
        self, conn, entity_type, entity_id, activity_type,
        description, created_by, contact_id=None, due_date=None,
    ):
        """Create a new activity log entry.

        Returns dict with activity_id and activity_type on success,
        or dict with 'error' key on validation failure.
        """
        if activity_type not in VALID_ACTIVITY_TYPES:
            return {
                "error": f"Invalid activity_type. Must be one of: "
                         f"{VALID_ACTIVITY_TYPES}",
            }

        if activity_type == "follow_up" and not due_date:
            return {"error": "due_date is required for follow_up activities"}

        # Verify entity exists
        if entity_type == "agency":
            exists = conn.execute(
                "SELECT 1 FROM agencies "
                "WHERE agency_id = ? AND is_active = 1",
                [entity_id],
            ).fetchone()
        else:
            exists = conn.execute(
                "SELECT 1 FROM customers "
                "WHERE customer_id = ? AND is_active = 1",
                [entity_id],
            ).fetchone()

        if not exists:
            return {"error": f"{entity_type} not found", "status": 404}

        conn.execute("""
            INSERT INTO entity_activity
                (entity_type, entity_id, activity_type, description,
                 created_by, contact_id, due_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            entity_type, entity_id, activity_type,
            description or None, created_by, contact_id, due_date,
        ])
        activity_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]

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
            conn.execute("""
                UPDATE signal_actions
                SET status = 'acknowledged',
                    updated_by = ?,
                    updated_date = CURRENT_TIMESTAMP
                WHERE entity_type = ? AND entity_id = ?
                  AND status = 'snoozed'
                  AND snooze_until < date('now')
            """, [created_by, entity_type, entity_id])

        return {
            "success": True,
            "activity_id": activity_id,
            "activity_type": activity_type,
        }

    def toggle_completion(self, conn, activity_id):
        """Toggle completion status of a follow-up activity.

        Returns dict with success/is_completed or error key.
        """
        activity = conn.execute(
            "SELECT activity_type, is_completed "
            "FROM entity_activity WHERE activity_id = ?",
            [activity_id],
        ).fetchone()

        if not activity:
            return {"error": "Activity not found", "status": 404}

        if activity["activity_type"] != "follow_up":
            return {
                "error": "Only follow_up activities can be completed",
            }

        new_status = 0 if activity["is_completed"] else 1
        conn.execute("""
            UPDATE entity_activity
            SET is_completed = ?,
                completed_date = CASE
                    WHEN ? = 1 THEN CURRENT_TIMESTAMP
                    ELSE NULL END
            WHERE activity_id = ?
        """, [new_status, new_status, activity_id])

        return {"success": True, "is_completed": new_status}

    def get_follow_ups(self, conn, ae_name=None):
        """Get incomplete follow-ups plus recently completed ones.

        Args:
            ae_name: If provided, only return follow-ups for entities
                assigned to this AE. None returns all.

        Returns list of follow-up dicts with urgency classification.
        """
        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = """
              AND (
                  (ea.entity_type = 'customer' AND ea.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ?))
                  OR
                  (ea.entity_type = 'agency' AND ea.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ?))
              )
            """
            params = [ae_name, ae_name]

        rows = conn.execute(f"""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.description,
                ea.due_date,
                ea.is_completed,
                ea.completed_date,
                ea.activity_date,
                CASE ea.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = ea.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = ea.entity_id)
                END AS entity_name
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND (ea.is_completed = 0
                   OR ea.completed_date >= datetime('now', '-7 days'))
              {ae_filter}
            ORDER BY ea.is_completed ASC, ea.due_date ASC
        """, params).fetchall()

        today = date.today().isoformat()
        results = []
        for row in rows:
            d = dict(row)
            d["urgency"] = self._classify_urgency(d, today)
            results.append(d)
        return results

    def get_recent_activity_for_ae(self, conn, ae_name=None, limit=15):
        """Get recent activities across all entities assigned to an AE.

        Args:
            conn: Database connection (read-only preferred).
            ae_name: Filter to this AE. None returns all.
            limit: Max results to return.

        Returns list of activity dicts with entity_name, ordered by
        activity_date descending.
        """
        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = """
              AND (
                  (ea.entity_type = 'customer' AND ea.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ?))
                  OR
                  (ea.entity_type = 'agency' AND ea.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ?))
              )
            """
            params = [ae_name, ae_name]

        params.append(limit)
        rows = conn.execute(f"""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.activity_type,
                ea.activity_date,
                ea.description,
                ea.created_by,
                ea.due_date,
                ea.is_completed,
                CASE ea.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = ea.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = ea.entity_id)
                END AS entity_name
            FROM entity_activity ea
            WHERE 1=1 {ae_filter}
            ORDER BY ea.activity_date DESC
            LIMIT ?
        """, params).fetchall()
        return [dict(r) for r in rows]

    def _classify_urgency(self, follow_up, today_iso):
        """Classify follow-up urgency based on due date."""
        if follow_up["is_completed"]:
            return "completed"
        due = follow_up.get("due_date")
        if not due:
            return "upcoming"
        if due < today_iso:
            return "overdue"
        if due == today_iso:
            return "due-today"
        due_date = date.fromisoformat(due)
        if due_date <= date.today() + timedelta(days=3):
            return "due-soon"
        return "upcoming"
