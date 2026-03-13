"""Service for manager dashboard -- composes metrics across all AEs."""

from src.services.base_service import BaseService


class ManagerDashboardService(BaseService):
    """Composes cross-AE metrics for the manager dashboard."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_scoreboard(self, conn, ae_names):
        """Per-AE fast stats for the scoreboard comparison table.

        Args:
            conn: Database connection (read-only preferred).
            ae_names: List of AE name strings.

        Returns:
            Dict mapping ae_name -> stats dict with account_count,
            revenue_at_risk, unworked_signals_7d, open_followups,
            overdue_followups.
        """
        if not ae_names:
            return {}

        result = {}
        for ae in ae_names:
            result[ae] = {
                "account_count": self._count_accounts(conn, ae),
                "revenue_at_risk": self._revenue_at_risk(conn, ae),
                "unworked_signals_7d": self._unworked_signals(conn, ae),
                **self._followup_counts(conn, ae),
            }
        return result

    def _count_accounts(self, conn, ae_name):
        row = conn.execute("""
            SELECT COUNT(*) AS n FROM (
                SELECT customer_id FROM customers
                WHERE assigned_ae = ? AND is_active = 1
                UNION ALL
                SELECT agency_id FROM agencies
                WHERE assigned_ae = ? AND is_active = 1
            )
        """, [ae_name, ae_name]).fetchone()
        return row["n"]

    def _revenue_at_risk(self, conn, ae_name):
        row = conn.execute("""
            SELECT COALESCE(SUM(es.trailing_revenue), 0) AS at_risk
            FROM entity_signals es
            WHERE es.signal_type = 'renewal_gap'
              AND (
                  (es.entity_type = 'customer' AND es.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ? AND is_active = 1))
                  OR
                  (es.entity_type = 'agency' AND es.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ? AND is_active = 1))
              )
        """, [ae_name, ae_name]).fetchone()
        return row["at_risk"]

    def _unworked_signals(self, conn, ae_name):
        row = conn.execute("""
            SELECT COUNT(*) AS n
            FROM signal_actions
            WHERE assigned_ae = ?
              AND status = 'new'
              AND julianday('now') - julianday(created_date) > 7
        """, [ae_name]).fetchone()
        return row["n"]

    def _followup_counts(self, conn, ae_name):
        row = conn.execute("""
            SELECT
                COUNT(*) AS open_followups,
                SUM(CASE WHEN ea.due_date < date('now')
                    THEN 1 ELSE 0 END) AS overdue_followups
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND ea.is_completed = 0
              AND (
                  (ea.entity_type = 'customer' AND ea.entity_id IN (
                      SELECT customer_id FROM customers
                      WHERE assigned_ae = ? AND is_active = 1))
                  OR
                  (ea.entity_type = 'agency' AND ea.entity_id IN (
                      SELECT agency_id FROM agencies
                      WHERE assigned_ae = ? AND is_active = 1))
              )
        """, [ae_name, ae_name]).fetchone()
        return {
            "open_followups": row["open_followups"] or 0,
            "overdue_followups": row["overdue_followups"] or 0,
        }

    def get_attention_items(self, conn):
        """Items needing manager awareness, sorted by dollar impact.

        Returns two categories:
        - unworked_signal: signal actions unworked >7 days
        - renewal_gap_stale: renewal_gap signals with no touch in 14 days

        Args:
            conn: Database connection (read-only preferred).

        Returns:
            List of attention item dicts sorted by trailing_revenue DESC.
        """
        items = []
        items.extend(self._unworked_signal_items(conn))
        items.extend(self._renewal_gap_stale_items(conn))
        items.sort(key=lambda x: x.get("trailing_revenue", 0), reverse=True)
        return items

    def _unworked_signal_items(self, conn):
        rows = conn.execute("""
            SELECT
                sa.entity_type,
                sa.entity_id,
                sa.signal_type,
                sa.assigned_ae,
                CAST(julianday('now') - julianday(sa.created_date)
                     AS INTEGER) AS days_aging,
                COALESCE(es.trailing_revenue, 0) AS trailing_revenue,
                es.signal_label,
                CASE sa.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = sa.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = sa.entity_id)
                END AS entity_name
            FROM signal_actions sa
            LEFT JOIN entity_signals es
                ON es.entity_type = sa.entity_type
               AND es.entity_id = sa.entity_id
               AND es.signal_type = sa.signal_type
            WHERE sa.status = 'new'
              AND julianday('now') - julianday(sa.created_date) > 7
            ORDER BY COALESCE(es.trailing_revenue, 0) DESC
        """).fetchall()
        return [{"item_type": "unworked_signal", **dict(r)} for r in rows]

    def _renewal_gap_stale_items(self, conn):
        rows = conn.execute("""
            SELECT
                es.entity_type,
                es.entity_id,
                es.trailing_revenue,
                es.signal_label,
                CASE es.entity_type
                    WHEN 'agency' THEN (
                        SELECT agency_name FROM agencies
                        WHERE agency_id = es.entity_id)
                    WHEN 'customer' THEN (
                        SELECT normalized_name FROM customers
                        WHERE customer_id = es.entity_id)
                END AS entity_name,
                CASE es.entity_type
                    WHEN 'agency' THEN (
                        SELECT assigned_ae FROM agencies
                        WHERE agency_id = es.entity_id)
                    WHEN 'customer' THEN (
                        SELECT assigned_ae FROM customers
                        WHERE customer_id = es.entity_id)
                END AS assigned_ae
            FROM entity_signals es
            WHERE es.signal_type = 'renewal_gap'
              AND NOT EXISTS (
                  SELECT 1 FROM entity_activity ea
                  WHERE ea.entity_type = es.entity_type
                    AND ea.entity_id = es.entity_id
                    AND ea.activity_type IN ('note', 'call', 'email', 'meeting')
                    AND ea.activity_date >= date('now', '-14 days')
              )
        """).fetchall()
        return [{"item_type": "renewal_gap_stale", **dict(r)} for r in rows]

    def get_weekly_activity(self, conn, ae_names):
        """Activity counts by type per AE for the last 7 days.

        Counts note, call, email, meeting, and completed follow_ups.
        Excludes status_change and incomplete follow_ups.

        Args:
            conn: Database connection (read-only preferred).
            ae_names: List of AE name strings.

        Returns:
            Dict mapping ae_name -> {note, call, email, meeting,
            follow_up, total} counts.
        """
        base = {
            "note": 0, "call": 0, "email": 0,
            "meeting": 0, "follow_up": 0, "total": 0,
        }
        if not ae_names:
            return {}

        result = {ae: dict(base) for ae in ae_names}

        for ae in ae_names:
            rows = conn.execute("""
                SELECT ea.activity_type, COUNT(*) AS n
                FROM entity_activity ea
                WHERE ea.activity_date >= date('now', '-7 days')
                  AND ea.activity_type IN (
                      'note', 'call', 'email', 'meeting', 'follow_up')
                  AND (ea.activity_type != 'follow_up'
                       OR ea.is_completed = 1)
                  AND (
                      (ea.entity_type = 'customer' AND ea.entity_id IN (
                          SELECT customer_id FROM customers
                          WHERE assigned_ae = ?))
                      OR
                      (ea.entity_type = 'agency' AND ea.entity_id IN (
                          SELECT agency_id FROM agencies
                          WHERE assigned_ae = ?))
                  )
                GROUP BY ea.activity_type
            """, [ae, ae]).fetchall()
            total = 0
            for r in rows:
                result[ae][r["activity_type"]] = r["n"]
                total += r["n"]
            result[ae]["total"] = total

        return result
