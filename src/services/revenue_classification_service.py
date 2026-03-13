"""Service for revenue classification (regular/irregular) analysis."""

from src.services.base_service import BaseService

MONTH_ABBREVS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

YEAR_EXTRACT = "CAST('20' || SUBSTR(s.broadcast_month, 5, 2) AS INTEGER)"

TRADE_FILTER = "(s.revenue_type != 'Trade' OR s.revenue_type IS NULL)"

VALID_CLASSES = ("regular", "irregular")

WORLDLINK_AGENCY_ID = 5

WORLDLINK_NAME_PREFIXES = (
    "worldlink",
    "marketing architect",
    "direct donor",
)

WORLDLINK_NAME_CONTAINS = (
    "(marketing architects)",
    "(direct donor)",
)


def _is_worldlink(agency_id, name):
    """Check if a customer belongs to the WorldLink group."""
    if agency_id == WORLDLINK_AGENCY_ID:
        return True
    lower = (name or "").lower()
    for prefix in WORLDLINK_NAME_PREFIXES:
        if lower.startswith(prefix):
            return True
    for substr in WORLDLINK_NAME_CONTAINS:
        if substr in lower:
            return True
    return False


class RevenueClassificationService(BaseService):
    """Analyze and manage customer revenue classification."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_summary(self, conn, year, filters=None):
        """Return summary stats, monthly breakdown, and available years.

        Args:
            conn: sqlite3.Connection with Row factory.
            year: Integer year (e.g. 2025).
            filters: Optional dict with sector_id, ae, classification keys.

        Returns:
            Dict with regular_total, irregular_total, regular_pct,
            unclassified_count, monthly list, available_years list.
        """
        filters = filters or {}
        where_clauses = [
            "s.customer_id IS NOT NULL",
            TRADE_FILTER,
            f"{YEAR_EXTRACT} = :year",
        ]
        params = {"year": year}

        if filters.get("sector_id"):
            where_clauses.append("c.sector_id = :sector_id")
            params["sector_id"] = filters["sector_id"]
        if filters.get("ae"):
            where_clauses.append("c.assigned_ae = :ae")
            params["ae"] = filters["ae"]
        if filters.get("classification"):
            where_clauses.append("c.revenue_class = :classification")
            params["classification"] = filters["classification"]

        where = " AND ".join(where_clauses)

        rows = conn.execute(f"""
            SELECT
                SUBSTR(s.broadcast_month, 1, 3) AS month_abbr,
                c.revenue_class,
                SUM(s.gross_rate) AS total
            FROM spots s
            JOIN customers c ON s.customer_id = c.customer_id
            WHERE {where}
            GROUP BY month_abbr, c.revenue_class
        """, params).fetchall()

        regular_total = 0.0
        irregular_total = 0.0
        monthly_data = {}

        for row in rows:
            abbr = row["month_abbr"]
            cls = row["revenue_class"] or "regular"
            amount = row["total"] or 0.0

            if cls == "regular":
                regular_total += amount
            else:
                irregular_total += amount

            if abbr not in monthly_data:
                monthly_data[abbr] = {"month": abbr, "regular": 0, "irregular": 0}
            monthly_data[abbr][cls] += amount

        grand_total = regular_total + irregular_total
        regular_pct = (
            (regular_total / grand_total * 100) if grand_total > 0 else 0
        )

        monthly = []
        for abbr in MONTH_ABBREVS:
            monthly.append(
                monthly_data.get(abbr, {"month": abbr, "regular": 0, "irregular": 0})
            )

        unclassified = conn.execute(
            "SELECT COUNT(*) AS cnt FROM customers "
            "WHERE is_active = 1 AND revenue_class IS NULL"
        ).fetchone()["cnt"]

        year_rows = conn.execute(f"""
            SELECT DISTINCT {YEAR_EXTRACT} AS yr
            FROM spots s
            WHERE s.customer_id IS NOT NULL AND {TRADE_FILTER}
            ORDER BY yr
        """).fetchall()
        available_years = [r["yr"] for r in year_rows]

        sectors = self.get_sectors(conn)

        return {
            "regular_total": regular_total,
            "irregular_total": irregular_total,
            "regular_pct": round(regular_pct, 1),
            "unclassified_count": unclassified,
            "monthly": monthly,
            "available_years": available_years,
            "sectors": sectors,
        }

    def get_customers(self, conn, year, filters=None):
        """Return customer list with current and prior year revenue.

        Args:
            conn: sqlite3.Connection with Row factory.
            year: Integer year (e.g. 2025).
            filters: Optional dict with sector_id, ae, classification keys.

        Returns:
            List of dicts with customer_id, name, sector_id, sector_name,
            revenue_class, assigned_ae, current_year_revenue,
            prior_year_revenue, yoy_dollar, yoy_pct.
        """
        filters = filters or {}
        cust_where = ["c.is_active = 1"]
        params = {"year": year, "prior_year": year - 1}

        if filters.get("sector_id"):
            cust_where.append("c.sector_id = :sector_id")
            params["sector_id"] = filters["sector_id"]
        if filters.get("ae"):
            cust_where.append("c.assigned_ae = :ae")
            params["ae"] = filters["ae"]
        if filters.get("classification"):
            cust_where.append("c.revenue_class = :classification")
            params["classification"] = filters["classification"]

        cust_filter = " AND ".join(cust_where)

        spot_filter = (
            "s.customer_id IS NOT NULL AND "
            f"{TRADE_FILTER}"
        )

        rows = conn.execute(f"""
            SELECT
                c.customer_id,
                c.normalized_name AS name,
                c.agency_id,
                c.sector_id,
                COALESCE(sec.sector_name, '') AS sector_name,
                COALESCE(c.revenue_class, 'regular') AS revenue_class,
                COALESCE(c.assigned_ae, '') AS assigned_ae,
                COALESCE(cy.rev, 0) AS current_year_revenue,
                COALESCE(py.rev, 0) AS prior_year_revenue
            FROM customers c
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            LEFT JOIN (
                SELECT s.customer_id, SUM(s.gross_rate) AS rev
                FROM spots s
                WHERE {spot_filter}
                  AND {YEAR_EXTRACT} = :year
                GROUP BY s.customer_id
            ) cy ON c.customer_id = cy.customer_id
            LEFT JOIN (
                SELECT s.customer_id, SUM(s.gross_rate) AS rev
                FROM spots s
                WHERE {spot_filter}
                  AND {YEAR_EXTRACT} = :prior_year
                GROUP BY s.customer_id
            ) py ON c.customer_id = py.customer_id
            WHERE {cust_filter}
            ORDER BY current_year_revenue DESC
        """, params).fetchall()

        result = []
        wl_cur = 0.0
        wl_prior = 0.0
        wl_class = "regular"
        non_wl = []

        for r in rows:
            name = r["name"] or ""
            is_worldlink = _is_worldlink(r["agency_id"], name)
            cur = r["current_year_revenue"]
            prior = r["prior_year_revenue"]

            if is_worldlink:
                wl_cur += cur
                wl_prior += prior
                if r["revenue_class"] == "irregular":
                    wl_class = "irregular"
            else:
                yoy_dollar = cur - prior
                yoy_pct = (
                    round(yoy_dollar / prior * 100, 1)
                    if prior > 0 else None
                )
                non_wl.append({
                    "customer_id": r["customer_id"],
                    "name": name,
                    "sector_id": r["sector_id"],
                    "sector_name": r["sector_name"],
                    "revenue_class": r["revenue_class"],
                    "assigned_ae": r["assigned_ae"],
                    "current_year_revenue": cur,
                    "prior_year_revenue": prior,
                    "yoy_dollar": yoy_dollar,
                    "yoy_pct": yoy_pct,
                })

        if wl_cur > 0 or wl_prior > 0:
            wl_yoy = wl_cur - wl_prior
            non_wl.append({
                "customer_id": WORLDLINK_AGENCY_ID,
                "name": "WorldLink (Agency)",
                "sector_id": None,
                "sector_name": "",
                "revenue_class": wl_class,
                "assigned_ae": "",
                "current_year_revenue": wl_cur,
                "prior_year_revenue": wl_prior,
                "yoy_dollar": wl_yoy,
                "yoy_pct": (
                    round(wl_yoy / wl_prior * 100, 1)
                    if wl_prior > 0 else None
                ),
            })

        non_wl.sort(key=lambda c: c["current_year_revenue"], reverse=True)
        return non_wl

    def update_classification(self, conn, customer_id, revenue_class):
        """Update a customer's revenue classification.

        Args:
            conn: sqlite3.Connection.
            customer_id: Integer customer ID.
            revenue_class: 'regular' or 'irregular'.

        Raises:
            ValueError: If revenue_class is invalid or customer not found.
        """
        if revenue_class not in VALID_CLASSES:
            raise ValueError(
                f"Invalid revenue_class '{revenue_class}'. "
                f"Must be one of: {VALID_CLASSES}"
            )

        if customer_id == WORLDLINK_AGENCY_ID:
            conn.execute(
                "UPDATE customers SET revenue_class = ? "
                "WHERE agency_id = ? "
                "OR LOWER(normalized_name) LIKE 'worldlink%' "
                "OR LOWER(normalized_name) LIKE 'marketing architect%' "
                "OR LOWER(normalized_name) LIKE 'direct donor%' "
                "OR LOWER(normalized_name) LIKE '%(marketing architects)%' "
                "OR LOWER(normalized_name) LIKE '%(direct donor)%'",
                (revenue_class, WORLDLINK_AGENCY_ID),
            )
            return

        row = conn.execute(
            "SELECT customer_id FROM customers WHERE customer_id = ?",
            (customer_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Customer {customer_id} not found")

        conn.execute(
            "UPDATE customers SET revenue_class = ? WHERE customer_id = ?",
            (revenue_class, customer_id),
        )

    def update_sector(self, conn, customer_id, sector_id):
        """Update a customer's sector assignment via junction table.

        Uses the customer_sectors junction table so that the sync trigger
        keeps customers.sector_id in sync automatically.

        Args:
            conn: sqlite3.Connection.
            customer_id: Integer customer ID.
            sector_id: Integer sector ID, or None to clear.

        Raises:
            ValueError: If customer not found or sector invalid.
        """
        row = conn.execute(
            "SELECT customer_id FROM customers WHERE customer_id = ?",
            (customer_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Customer {customer_id} not found")

        if sector_id is not None:
            sec = conn.execute(
                "SELECT sector_id FROM sectors WHERE sector_id = ?",
                (sector_id,),
            ).fetchone()
            if not sec:
                raise ValueError(f"Sector {sector_id} not found")

            conn.execute(
                "INSERT INTO customer_sectors "
                "(customer_id, sector_id, is_primary, assigned_by) "
                "VALUES (?, ?, 1, 'rcm_web') "
                "ON CONFLICT(customer_id, sector_id) "
                "DO UPDATE SET is_primary = 1",
                (customer_id, sector_id),
            )
        else:
            conn.execute(
                "DELETE FROM customer_sectors "
                "WHERE customer_id = ? AND is_primary = 1",
                (customer_id,),
            )
            conn.execute(
                "UPDATE customers SET sector_id = NULL "
                "WHERE customer_id = ?",
                (customer_id,),
            )

    def get_sectors(self, conn):
        """Return active sectors with group info for optgroup rendering."""
        rows = conn.execute(
            "SELECT sector_id, sector_name, "
            "COALESCE(sector_group, 'Other') AS sector_group "
            "FROM sectors WHERE is_active = 1 "
            "ORDER BY sector_group, sector_name"
        ).fetchall()
        return [
            {
                "sector_id": r["sector_id"],
                "sector_name": r["sector_name"],
                "sector_group": r["sector_group"],
            }
            for r in rows
        ]
