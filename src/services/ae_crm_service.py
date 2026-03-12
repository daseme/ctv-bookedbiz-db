"""Service for AE CRM page -- scoped account queries and stats."""

import logging
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class AeCrmService(BaseService):
    """AE-scoped account queries for the My Accounts page."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_accounts(self, conn, ae_name=None):
        """Get accounts assigned to an AE with signals and activity.

        Args:
            conn: Database connection (read-only preferred).
            ae_name: Filter to this AE. None returns all active.

        Returns:
            List of account dicts sorted by signal priority then name.
        """
        ae_filter = ""
        params = []
        if ae_name:
            ae_filter = "AND e.assigned_ae = ?"
            params = [ae_name]

        rows = conn.execute(f"""
            SELECT
                e.entity_type,
                e.entity_id,
                e.entity_name,
                e.assigned_ae,
                es.signal_type,
                es.signal_label,
                es.signal_priority,
                COALESCE(es.trailing_revenue, 0) AS trailing_revenue,
                (SELECT MAX(ea.activity_date)
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                ) AS last_activity_date,
                (SELECT ea.due_date
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                   AND ea.activity_type = 'follow_up'
                   AND ea.is_completed = 0
                 ORDER BY ea.due_date ASC
                 LIMIT 1
                ) AS next_follow_up_date,
                (SELECT ea.description
                 FROM entity_activity ea
                 WHERE ea.entity_type = e.entity_type
                   AND ea.entity_id = e.entity_id
                   AND ea.activity_type = 'follow_up'
                   AND ea.is_completed = 0
                 ORDER BY ea.due_date ASC
                 LIMIT 1
                ) AS next_follow_up_desc
            FROM (
                SELECT 'agency' AS entity_type,
                       agency_id AS entity_id,
                       agency_name AS entity_name,
                       assigned_ae
                FROM agencies WHERE is_active = 1
                UNION ALL
                SELECT 'customer' AS entity_type,
                       customer_id AS entity_id,
                       normalized_name AS entity_name,
                       assigned_ae
                FROM customers WHERE is_active = 1
            ) e
            LEFT JOIN entity_signals es
                ON es.entity_type = e.entity_type
               AND es.entity_id = e.entity_id
            WHERE 1=1 {ae_filter}
            ORDER BY
                CASE WHEN es.signal_priority IS NOT NULL
                     THEN 0 ELSE 1 END,
                es.signal_priority ASC,
                e.entity_name ASC
        """, params).fetchall()

        return [dict(r) for r in rows]

    def get_stats(self, conn, ae_name=None):
        """Get summary stats for the AE's book of business.

        Args:
            conn: Database connection (read-only preferred).
            ae_name: Filter to this AE. None returns all.

        Returns:
            Dict with account_count, trailing_revenue,
            signal_count, follow_up_count, overdue_count.
        """
        accounts = self.get_accounts(conn, ae_name)
        signal_count = sum(
            1 for a in accounts if a.get("signal_type")
        )
        trailing_revenue = sum(
            a.get("trailing_revenue", 0) for a in accounts
        )

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

        follow_up_row = conn.execute(f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN ea.due_date < date('now')
                    THEN 1 ELSE 0 END) AS overdue
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND ea.is_completed = 0
              {ae_filter}
        """, params).fetchone()

        return {
            "account_count": len(accounts),
            "trailing_revenue": trailing_revenue,
            "signal_count": signal_count,
            "follow_up_count": follow_up_row["total"] or 0,
            "overdue_count": follow_up_row["overdue"] or 0,
        }

    def get_revenue_trend(self, conn, entity_type, entity_id):
        """Get monthly revenue for an entity over the last 12 months.

        Args:
            conn: Database connection (read-only preferred).
            entity_type: 'customer' or 'agency'.
            entity_id: ID of the entity.

        Returns:
            List of {broadcast_month, revenue} dicts, chronological.
        """
        if entity_type == "agency":
            entity_clause = """
                s.customer_id IN (
                    SELECT customer_id FROM customers
                    WHERE agency_id = ?)
            """
        else:
            entity_clause = "s.customer_id = ?"

        rows = conn.execute(f"""
            SELECT
                s.broadcast_month,
                COALESCE(SUM(s.gross_rate), 0) AS revenue
            FROM spots s
            WHERE {entity_clause}
              AND s.is_historical = 0
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
              AND ('20' || SUBSTR(s.broadcast_month, 5, 2)
                   || '-' ||
                   CASE SUBSTR(s.broadcast_month, 1, 3)
                       WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
                       WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
                       WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                       WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
                       WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
                       WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                   END) >= strftime('%Y-%m', 'now', '-12 months')
            GROUP BY s.broadcast_month
            ORDER BY
                '20' || SUBSTR(s.broadcast_month, 5, 2)
                || '-' ||
                CASE SUBSTR(s.broadcast_month, 1, 3)
                    WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02'
                    WHEN 'Mar' THEN '03' WHEN 'Apr' THEN '04'
                    WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
                    WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08'
                    WHEN 'Sep' THEN '09' WHEN 'Oct' THEN '10'
                    WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
                END ASC
        """, [entity_id]).fetchall()

        return [dict(r) for r in rows]
