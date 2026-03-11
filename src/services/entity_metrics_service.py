"""Service for computing and caching entity metrics and signals."""

import logging
from datetime import date

from src.services.base_service import BaseService
from src.utils.formatting import fmt_revenue

logger = logging.getLogger(__name__)


class EntityMetricsService(BaseService):
    """Computes and caches entity-level metrics and health signals."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def ensure_cache_tables(self, conn):
        """Create cache tables if they don't exist."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_metrics (
                entity_type TEXT,
                entity_id INTEGER,
                markets TEXT,
                last_active TEXT,
                total_revenue REAL DEFAULT 0,
                spot_count INTEGER DEFAULT 0,
                agency_spot_count INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (entity_type, entity_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entity_signals (
                entity_type TEXT,
                entity_id INTEGER,
                signal_type TEXT,
                signal_label TEXT,
                signal_priority INTEGER,
                trailing_revenue REAL,
                prior_revenue REAL,
                computed_at TEXT DEFAULT (datetime('now')),
                PRIMARY KEY (entity_type, entity_id, signal_type)
            )
        """)

    def refresh_metrics(self, conn):
        """Delete and recompute all entity metrics from spots."""
        self.ensure_cache_tables(conn)
        conn.execute("DELETE FROM entity_metrics")
        conn.execute("""
            INSERT INTO entity_metrics
                (entity_type, entity_id, markets, last_active,
                 total_revenue, spot_count)
            SELECT
                'agency', agency_id,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN market_name != '' THEN market_name END),
                MAX(air_date),
                SUM(CASE
                    WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END),
                COUNT(*)
            FROM spots WHERE agency_id IS NOT NULL
            GROUP BY agency_id
        """)
        conn.execute("""
            INSERT INTO entity_metrics
                (entity_type, entity_id, markets, last_active,
                 total_revenue, spot_count, agency_spot_count)
            SELECT
                'customer', customer_id,
                GROUP_CONCAT(DISTINCT CASE
                    WHEN market_name != '' THEN market_name END),
                MAX(air_date),
                SUM(CASE
                    WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END),
                COUNT(*),
                COUNT(agency_id)
            FROM spots WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        """)

    def refresh_signals(self, conn):
        """Delete and recompute all entity signals from spots."""
        self.ensure_cache_tables(conn)
        conn.execute("DELETE FROM entity_signals")

        signal_query = """
            SELECT {id_col} as entity_id,
              SUM(CASE
                  WHEN air_date >= date('now','-12 months')
                       AND air_date <= date('now')
                       AND (revenue_type != 'Trade'
                            OR revenue_type IS NULL)
                  THEN gross_rate ELSE 0 END) as trailing_12m,
              SUM(CASE
                  WHEN air_date >= date('now','-24 months')
                       AND air_date < date('now','-12 months')
                       AND (revenue_type != 'Trade'
                            OR revenue_type IS NULL)
                  THEN gross_rate ELSE 0 END) as prior_12m,
              SUM(CASE
                  WHEN air_date > date('now')
                       AND (revenue_type != 'Trade'
                            OR revenue_type IS NULL)
                  THEN gross_rate ELSE 0 END) as future_rev,
              SUM(CASE
                  WHEN air_date > date('now') THEN 1
                  ELSE 0 END) as future_spots,
              SUM(CASE
                  WHEN revenue_type != 'Trade'
                       OR revenue_type IS NULL
                  THEN gross_rate ELSE 0 END) as lifetime_rev,
              MIN(air_date) as first_spot,
              MAX(CASE
                  WHEN air_date <= date('now')
                  THEN air_date END) as last_past_spot,
              COUNT(DISTINCT CASE
                  WHEN air_date >= date('now','-24 months')
                       AND air_date <= date('now')
                  THEN strftime('%Y-%m', air_date) END)
                  as active_months_24m
            FROM spots
            WHERE {id_col} IS NOT NULL
            GROUP BY {id_col}
        """

        rows_to_insert = []

        for entity_type, id_col in [
            ("agency", "agency_id"),
            ("customer", "customer_id"),
        ]:
            query = signal_query.format(id_col=id_col)
            for row in conn.execute(query).fetchall():
                self._compute_signals_for_row(
                    entity_type, row, rows_to_insert
                )

        if rows_to_insert:
            conn.executemany("""
                INSERT INTO entity_signals
                    (entity_type, entity_id, signal_type, signal_label,
                     signal_priority, trailing_revenue, prior_revenue)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows_to_insert)

    def _compute_signals_for_row(self, entity_type, row, rows_to_insert):
        """Evaluate all signal rules for one entity row."""
        eid = row["entity_id"]
        trailing = row["trailing_12m"] or 0
        prior = row["prior_12m"] or 0
        future_rev = row["future_rev"] or 0
        future_spots = row["future_spots"] or 0
        lifetime = row["lifetime_rev"] or 0
        first_spot = row["first_spot"]
        last_past = row["last_past_spot"]
        active_months = row["active_months_24m"] or 0

        # Signal 1: Churned
        if (prior >= 10_000
                and (trailing + future_rev) == 0
                and future_spots == 0):
            priority = 1
            label = f"{fmt_revenue(prior)} prior year → $0"
            rows_to_insert.append(
                (entity_type, eid, "churned", label,
                 priority, trailing, prior)
            )

        # Signal 2: Declining
        elif (prior >= 10_000
              and trailing > 0
              and trailing < prior * 0.70):
            gap = prior - trailing
            if future_rev < gap * 0.50:
                pct = round((1 - trailing / prior) * 100)
                priority = 2
                label = (
                    f"{fmt_revenue(prior)} → "
                    f"{fmt_revenue(trailing)} (-{pct}%)"
                )
                rows_to_insert.append(
                    (entity_type, eid, "declining", label,
                     priority, trailing, prior)
                )

        # Signal 3: Gone Quiet
        if lifetime >= 10_000 and last_past and future_spots == 0:
            try:
                last_date = date.fromisoformat(last_past)
                days_quiet = (date.today() - last_date).days
            except (ValueError, TypeError):
                days_quiet = 0

            if active_months >= 20:
                threshold = 90
                cadence = "books monthly"
            elif active_months >= 12:
                threshold = 120
                cadence = "books regularly"
            elif active_months >= 6:
                threshold = 240
                cadence = "books seasonally"
            else:
                threshold = None
                cadence = ""

            if threshold and days_quiet > threshold:
                already_churned = any(
                    r[0] == entity_type
                    and r[1] == eid
                    and r[2] == "churned"
                    for r in rows_to_insert
                )
                if not already_churned:
                    priority = 3
                    label = f"Quiet {days_quiet}d · {cadence}"
                    rows_to_insert.append(
                        (entity_type, eid, "gone_quiet", label,
                         priority, trailing, prior)
                    )

        # Signal 4: New Account
        if first_spot and lifetime >= 5_000:
            try:
                first_date = date.fromisoformat(first_spot)
                months_since_first = (
                    (date.today().year - first_date.year) * 12
                    + (date.today().month - first_date.month)
                )
            except (ValueError, TypeError):
                months_since_first = 999

            if months_since_first <= 12:
                priority = 4
                month_str = first_date.strftime("%b %Y")
                label = (
                    f"New · first booked {month_str} · "
                    f"{fmt_revenue(lifetime)}"
                )
                rows_to_insert.append(
                    (entity_type, eid, "new_account", label,
                     priority, trailing, prior)
                )

        # Signal 5: Growing
        if (trailing >= 10_000
                and prior > 0
                and trailing > prior * 1.30):
            pct = round((trailing / prior - 1) * 100)
            priority = 5
            label = (
                f"{fmt_revenue(prior)} → "
                f"{fmt_revenue(trailing)} (+{pct}%)"
            )
            rows_to_insert.append(
                (entity_type, eid, "growing", label,
                 priority, trailing, prior)
            )

    def refresh_metrics_for_ids(
        self, conn, customer_ids=None, agency_ids=None
    ):
        """Targeted refresh for specific entity IDs only."""
        self.ensure_cache_tables(conn)

        if agency_ids:
            placeholders = ",".join("?" * len(agency_ids))
            conn.execute(
                "DELETE FROM entity_metrics "
                "WHERE entity_type='agency' "
                f"AND entity_id IN ({placeholders})",
                agency_ids,
            )
            conn.execute(f"""
                INSERT INTO entity_metrics
                    (entity_type, entity_id, markets, last_active,
                     total_revenue, spot_count)
                SELECT
                    'agency', agency_id,
                    GROUP_CONCAT(DISTINCT CASE
                        WHEN market_name != '' THEN market_name END),
                    MAX(air_date),
                    SUM(CASE
                        WHEN revenue_type != 'Trade'
                             OR revenue_type IS NULL
                        THEN gross_rate ELSE 0 END),
                    COUNT(*)
                FROM spots
                WHERE agency_id IN ({placeholders})
                GROUP BY agency_id
            """, agency_ids)

        if customer_ids:
            placeholders = ",".join("?" * len(customer_ids))
            conn.execute(
                "DELETE FROM entity_metrics "
                "WHERE entity_type='customer' "
                f"AND entity_id IN ({placeholders})",
                customer_ids,
            )
            conn.execute(f"""
                INSERT INTO entity_metrics
                    (entity_type, entity_id, markets, last_active,
                     total_revenue, spot_count, agency_spot_count)
                SELECT
                    'customer', customer_id,
                    GROUP_CONCAT(DISTINCT CASE
                        WHEN market_name != '' THEN market_name END),
                    MAX(air_date),
                    SUM(CASE
                        WHEN revenue_type != 'Trade'
                             OR revenue_type IS NULL
                        THEN gross_rate ELSE 0 END),
                    COUNT(*),
                    COUNT(agency_id)
                FROM spots
                WHERE customer_id IN ({placeholders})
                GROUP BY customer_id
            """, customer_ids)

    def get_metrics_map(self, conn):
        """Load all metrics as dict keyed by (entity_type, entity_id)."""
        rows = conn.execute("SELECT * FROM entity_metrics").fetchall()
        return {
            (row["entity_type"], row["entity_id"]): dict(row)
            for row in rows
        }

    def get_signals_map(self, conn):
        """Load first signal per entity, keyed by (entity_type, entity_id)."""
        rows = conn.execute(
            "SELECT * FROM entity_signals "
            "ORDER BY signal_priority ASC"
        ).fetchall()
        result = {}
        for row in rows:
            key = (row["entity_type"], row["entity_id"])
            if key not in result:
                result[key] = dict(row)
        return result

    def get_entity_signals(self, conn, entity_type, entity_id):
        """Get all signals for one entity."""
        rows = conn.execute(
            "SELECT * FROM entity_signals "
            "WHERE entity_type = ? AND entity_id = ? "
            "ORDER BY signal_priority ASC",
            (entity_type, entity_id),
        ).fetchall()
        return [dict(row) for row in rows]

    def auto_refresh_if_empty(self, conn):
        """Refresh both caches if entity_metrics is empty."""
        self.ensure_cache_tables(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM entity_metrics"
        ).fetchone()[0]
        if count == 0:
            self.refresh_metrics(conn)
            self.refresh_signals(conn)
