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
