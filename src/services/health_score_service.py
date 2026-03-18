"""Service for computing account health scores, tiers, and touch cadence."""

from src.services.base_service import BaseService

SIGNAL_SCORES = {
    "growing": 100,
    "new_account": 80,
    "gone_quiet": 40,
    "declining": 25,
    "churned": 0,
    "renewal_gap": 0,
}

TOUCH_THRESHOLDS = [
    (6, 100),
    (14, 75),
    (30, 50),
    (60, 25),
]

BM_TO_ISO = """
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

TIER_CADENCE = {"A": 7, "B": 14, "C": 30}


class HealthScoreService(BaseService):
    """Computes live health scores for AE accounts."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_health_scores(self, conn, ae_name=None):
        """Compute health scores for all active accounts.

        Args:
            conn: Database connection (read-only preferred).
            ae_name: Filter to this AE. None returns all active.

        Returns:
            List of dicts with entity_type, entity_id, health_score,
            health_color, and individual factor scores.
        """
        entities = self._load_entities(conn, ae_name)
        signals = self._load_signals(conn)
        touches = self._load_last_touches(conn)
        overdue = self._load_overdue_set(conn)
        revenue = self._load_revenue_trends(conn)

        results = []
        for e in entities:
            key = (e["entity_type"], e["entity_id"])

            rev_score = self._score_revenue_trend(revenue.get(key))
            sig_score = self._score_signal(signals.get(key))
            touch_score = self._score_last_touch(touches.get(key))
            fu_score = 0 if key in overdue else 100

            composite = round(
                rev_score * 0.30
                + sig_score * 0.25
                + touch_score * 0.25
                + fu_score * 0.20
            )
            composite = max(0, min(100, composite))

            results.append({
                "entity_type": e["entity_type"],
                "entity_id": e["entity_id"],
                "health_score": composite,
                "health_color": self._color(composite),
                "revenue_trend_score": rev_score,
                "signal_state_score": sig_score,
                "last_touch_score": touch_score,
                "followup_compliance_score": fu_score,
            })

        return results

    def _load_entities(self, conn, ae_name):
        params = []
        if ae_name:
            params = [ae_name]

        rows = conn.execute(f"""
            SELECT 'customer' AS entity_type,
                   customer_id AS entity_id, assigned_ae
            FROM customers WHERE is_active = 1
            {'AND assigned_ae = ?' if ae_name else ''}
            UNION ALL
            SELECT 'agency' AS entity_type,
                   agency_id AS entity_id, assigned_ae
            FROM agencies WHERE is_active = 1
            {'AND assigned_ae = ?' if ae_name else ''}
        """, params * 2 if ae_name else []).fetchall()
        return [dict(r) for r in rows]

    def _load_signals(self, conn):
        rows = conn.execute("""
            SELECT entity_type, entity_id, signal_type
            FROM entity_signals
            ORDER BY signal_priority ASC
        """).fetchall()
        result = {}
        for r in rows:
            key = (r["entity_type"], r["entity_id"])
            if key not in result:
                result[key] = r["signal_type"]
        return result

    def _load_last_touches(self, conn):
        rows = conn.execute("""
            SELECT entity_type, entity_id,
                   CAST(julianday('now') - julianday(
                       MAX(activity_date)) AS INTEGER) AS days_ago
            FROM entity_activity
            WHERE activity_type IN ('note', 'call', 'email', 'meeting')
            GROUP BY entity_type, entity_id
        """).fetchall()
        return {
            (r["entity_type"], r["entity_id"]): r["days_ago"]
            for r in rows
        }

    def _load_overdue_set(self, conn):
        rows = conn.execute("""
            SELECT DISTINCT entity_type, entity_id
            FROM entity_activity
            WHERE activity_type = 'follow_up'
              AND is_completed = 0
              AND due_date < date('now')
        """).fetchall()
        return {(r["entity_type"], r["entity_id"]) for r in rows}

    def _load_revenue_trends(self, conn):
        result = {}
        for entity_col, entity_type, entity_table, join_clause in [
            ("customer_id", "customer", "customers",
             "s.customer_id = e.customer_id"),
            ("agency_id", "agency", "agencies",
             "s.customer_id IN "
             "(SELECT customer_id FROM customers "
             "WHERE agency_id = e.agency_id)"),
        ]:
            rows = conn.execute(f"""
                SELECT
                    e.{entity_col} AS entity_id,
                    COALESCE(SUM(CASE
                        WHEN {BM_TO_ISO}
                            >= strftime('%Y-%m', 'now', '-3 months')
                         AND {BM_TO_ISO}
                            < strftime('%Y-%m', 'now')
                        THEN s.gross_rate ELSE 0 END), 0)
                        AS trailing_3m,
                    COALESCE(SUM(CASE
                        WHEN {BM_TO_ISO}
                            >= strftime('%Y-%m', 'now', '-6 months')
                         AND {BM_TO_ISO}
                            < strftime('%Y-%m', 'now', '-3 months')
                        THEN s.gross_rate ELSE 0 END), 0)
                        AS prior_3m
                FROM {entity_table} e
                LEFT JOIN spots s ON {join_clause}
                    AND s.is_historical = 0
                    AND (s.revenue_type != 'Trade'
                         OR s.revenue_type IS NULL)
                WHERE e.is_active = 1
                GROUP BY e.{entity_col}
            """).fetchall()
            for r in rows:
                key = (entity_type, r["entity_id"])
                result[key] = {
                    "trailing": r["trailing_3m"],
                    "prior": r["prior_3m"],
                }
        return result

    def _score_revenue_trend(self, rev_data):
        if not rev_data:
            return 60
        trailing = rev_data["trailing"]
        prior = rev_data["prior"]
        if prior == 0:
            return 60
        pct_change = (trailing - prior) / prior
        if pct_change >= 0.10:
            return 100
        if pct_change >= -0.10:
            return 60
        if pct_change >= -0.25:
            return 40
        if pct_change >= -0.50:
            return 20
        return 0

    def _score_signal(self, signal_type):
        if signal_type is None:
            return 100
        return SIGNAL_SCORES.get(signal_type, 50)

    def _score_last_touch(self, days_ago):
        if days_ago is None:
            return 0
        for threshold, score in TOUCH_THRESHOLDS:
            if days_ago <= threshold:
                return score
        return 0

    def _color(self, score):
        if score >= 70:
            return "green"
        if score >= 40:
            return "yellow"
        return "red"

    # ------------------------------------------------------------------
    # Tiering and touch cadence
    # ------------------------------------------------------------------

    def get_health_with_tiers(self, conn, ae_name=None):
        """Health scores plus tier, cadence, and touch status.

        Args:
            conn: Database connection.
            ae_name: Filter to this AE. None returns all active.

        Returns:
            List of health score dicts enriched with tier,
            tier_cadence_days, days_since_touch, and touch_status.
        """
        scores = self.get_health_scores(conn, ae_name)
        touches = self._load_last_touches(conn)
        trailing_12m = self._load_trailing_12m(conn)
        entities = self._load_entities(conn, ae_name)

        # Build ae -> list of (key, revenue) for ranking within each AE
        ae_buckets: dict[str, list[tuple[tuple, float]]] = {}
        for e in entities:
            key = (e["entity_type"], e["entity_id"])
            ae = e["assigned_ae"] or ""
            revenue = trailing_12m.get(key, 0.0)
            ae_buckets.setdefault(ae, []).append((key, revenue))

        # Assign tiers per AE by revenue rank (descending)
        tiers: dict[tuple, str] = {}
        for ae, items in ae_buckets.items():
            items.sort(key=lambda x: x[1], reverse=True)
            n = len(items)
            for rank, (key, _) in enumerate(items):
                percentile = rank / n
                if percentile < 0.20:
                    tiers[key] = "A"
                elif percentile < 0.60:
                    tiers[key] = "B"
                else:
                    tiers[key] = "C"

        # Enrich each score dict
        for score in scores:
            key = (score["entity_type"], score["entity_id"])
            tier = tiers.get(key, "C")
            cadence_days = TIER_CADENCE[tier]
            days_since_touch = touches.get(key)
            score["tier"] = tier
            score["tier_cadence_days"] = cadence_days
            score["days_since_touch"] = days_since_touch
            score["touch_status"] = self._touch_status(days_since_touch, cadence_days)

        return scores

    def _load_trailing_12m(self, conn):
        """Load trailing 12-month revenue per entity for tier ranking.

        Returns:
            Dict mapping (entity_type, entity_id) -> float revenue.
        """
        result = {}
        for entity_col, entity_type, entity_table, join_clause in [
            ("customer_id", "customer", "customers",
             "s.customer_id = e.customer_id"),
            ("agency_id", "agency", "agencies",
             "s.customer_id IN "
             "(SELECT customer_id FROM customers "
             "WHERE agency_id = e.agency_id)"),
        ]:
            rows = conn.execute(f"""
                SELECT e.{entity_col} AS entity_id,
                       COALESCE(SUM(CASE
                           WHEN {BM_TO_ISO}
                               >= strftime('%Y-%m', 'now', '-12 months')
                            AND {BM_TO_ISO}
                               < strftime('%Y-%m', 'now')
                            AND (s.revenue_type != 'Trade'
                                 OR s.revenue_type IS NULL)
                           THEN s.gross_rate ELSE 0 END), 0) AS trailing_12m
                FROM {entity_table} e
                LEFT JOIN spots s ON {join_clause}
                    AND s.is_historical = 0
                WHERE e.is_active = 1
                GROUP BY e.{entity_col}
            """).fetchall()
            for r in rows:
                key = (entity_type, r["entity_id"])
                result[key] = r["trailing_12m"]
        return result

    def touch_compliance(self, health_results):
        """Percentage of accounts where touch is within cadence.

        Args:
            health_results: Output from get_health_with_tiers().

        Returns:
            Integer 0-100 representing compliance percentage.
        """
        if not health_results:
            return 100
        within = sum(
            1 for r in health_results
            if r.get("touch_status") in ("green", "yellow")
        )
        return round(within / len(health_results) * 100)

    def _touch_status(self, days_since_touch, cadence_days):
        """Compute touch status color relative to cadence window.

        Args:
            days_since_touch: Days since last touch, or None if never touched.
            cadence_days: Target cadence in days for this tier.

        Returns:
            'green', 'yellow', or 'red'.
        """
        if days_since_touch is None:
            return "red"
        if days_since_touch > cadence_days:
            return "red"
        if days_since_touch > cadence_days * 0.75:
            return "yellow"
        return "green"
