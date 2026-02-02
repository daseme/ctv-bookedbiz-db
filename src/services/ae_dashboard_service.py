"""
AE Account Management Dashboard Service

Provides year-over-year customer performance analysis for Account Executives.
Includes retention tracking, lost customer alerts, and portfolio health metrics.
"""

import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from src.services.base_service import BaseService
from src.utils.query_builders import CustomerNormalizationQueryBuilder

logger = logging.getLogger(__name__)


@dataclass
class CustomerYoYPerformance:
    """Year-over-year performance for a single customer"""

    customer: str
    customer_id: int
    sector: Optional[str]
    ae: str
    ytd_2024: float
    ytd_2023: float
    variance_dollars: float
    variance_pct: float
    status: str  # 'risk', 'growth', 'new', 'stable', 'lost'
    pattern: str  # 'Q4 Heavy', 'Seasonal', 'Growing', 'Steady', 'Lost'
    is_new_2024: bool
    q4_2024: float
    q4_2023: float
    q4_behind: bool

    def to_dict(self) -> dict:
        return {
            "customer": self.customer,
            "customer_id": self.customer_id,
            "sector": self.sector,
            "ae": self.ae,
            "ytd2024": self.ytd_2024,
            "ytd2023": self.ytd_2023,
            "variance": self.variance_dollars,
            "pct": self.variance_pct,
            "status": self.status,
            "pattern": self.pattern,
            "new2024": self.is_new_2024,
            "q42024": self.q4_2024,
            "q42023": self.q4_2023,
            "q4Behind": self.q4_behind,
        }


@dataclass
class SectorPerformance:
    """Sector-level performance summary"""

    sector: str
    ytd_2024: float
    ytd_2023: float
    variance_dollars: float
    variance_pct: float
    customer_count: int
    status: str  # 'declining', 'growing', 'stable'

    def to_dict(self) -> dict:
        return {
            "sector": self.sector,
            "ytd2024": self.ytd_2024,
            "ytd2023": self.ytd_2023,
            "variance": self.variance_dollars,
            "pct": self.variance_pct,
            "customers": self.customer_count,
            "status": self.status,
        }


@dataclass
class AEDashboardData:
    """Complete dashboard data for an AE"""

    selected_ae: Optional[str]  # None means "Everyone"
    selected_year: int

    # Customer details
    customers: List[CustomerYoYPerformance]

    # Sector breakdown
    sectors: List[SectorPerformance]

    # Overall metrics
    total_ytd_2024: float
    total_ytd_2023: float
    total_variance_dollars: float
    total_variance_pct: float

    active_customers_2024: int
    active_customers_2023: int

    lost_customers: List[CustomerYoYPerformance]
    lost_revenue: float
    retention_rate: float

    avg_per_customer: float

    new_customers: List[CustomerYoYPerformance]
    new_customer_revenue: float
    new_customer_pct: float

    top5_concentration_pct: float
    available_years: List[int]

    # Filter lists
    ae_list: List[str]
    sector_list: List[str]

    def to_dict(self) -> dict:
        return {
            "selected_ae": self.selected_ae,
            "selected_year": self.selected_year,
            "customers": [c.to_dict() for c in self.customers],
            "sectors": [s.to_dict() for s in self.sectors],
            "total_ytd_2024": self.total_ytd_2024,
            "total_ytd_2023": self.total_ytd_2023,
            "total_variance_dollars": self.total_variance_dollars,
            "total_variance_pct": self.total_variance_pct,
            "active_customers_2024": self.active_customers_2024,
            "active_customers_2023": self.active_customers_2023,
            "lost_customers": [c.to_dict() for c in self.lost_customers],
            "lost_revenue": self.lost_revenue,
            "retention_rate": self.retention_rate,
            "avg_per_customer": self.avg_per_customer,
            "new_customers": [c.to_dict() for c in self.new_customers],
            "new_customer_revenue": self.new_customer_revenue,
            "new_customer_pct": self.new_customer_pct,
            "top5_concentration_pct": self.top5_concentration_pct,
            "ae_list": self.ae_list,
            "sector_list": self.sector_list,
            "available_years": self.available_years,
        }


class AEDashboardService(BaseService):
    """Service for AE account management dashboard"""

    def get_dashboard_data(
        self, year: int, ae_filter: Optional[str] = None
    ) -> AEDashboardData:
        """
        Get complete dashboard data for specified year and optional AE filter.

        Args:
            year: Year to analyze (e.g., 2024)
            ae_filter: Optional AE name filter. None = "Everyone"

        Returns:
            Complete dashboard data structure
        """
        with self.safe_connection() as conn:
            # Get customer YoY performance
            customers = self._get_customer_yoy_performance(conn, year, ae_filter)

            # Calculate sector summaries
            sectors = self._calculate_sector_performance(customers)

            # Calculate overall metrics
            metrics = self._calculate_overall_metrics(customers)

            # Get filter lists
            ae_list = self._get_ae_list(conn)
            sector_list = self._get_sector_list(conn)
            available_years = self._get_available_years(conn)

            return AEDashboardData(
                selected_ae=ae_filter,
                selected_year=year,
                customers=customers,
                sectors=sectors,
                ae_list=ae_list,
                sector_list=sector_list,
                available_years=available_years,
                **metrics,
            )

    def _get_customer_yoy_performance(
        self, conn, year: int, ae_filter: Optional[str]
    ) -> List[CustomerYoYPerformance]:
        """Get customer-level YoY performance data"""

        prior_year = year - 1

        # Convert years to 2-digit format for broadcast_month comparison
        # Year 2024 -> '24', Year 2023 -> '23'
        year_suffix = str(year)[-2:]
        prior_year_suffix = str(prior_year)[-2:]

        # Build query with optional AE filter
        ae_condition = ""
        params = [
            year_suffix,
            prior_year_suffix,
            year_suffix,
            prior_year_suffix,
            year_suffix,
            prior_year_suffix,
        ]
        if ae_filter:
            ae_condition = "AND COALESCE(s.sales_person, 'Unknown') = ?"
            params.append(ae_filter)

        query = f"""
        WITH customer_revenue AS (
            SELECT
                COALESCE(audit.customer_id, s.customer_id, 0) as customer_id,
                COALESCE(audit.normalized_name, c.normalized_name, s.bill_code, 'Unknown') as customer_name,
                COALESCE(sec.sector_name, 'Unknown') as sector,
                COALESCE(s.sales_person, 'Unknown') as ae,
                -- Selected year revenue (broadcast_month format is 'mmm-yy' like 'Nov-24')
                SUM(CASE WHEN SUBSTR(s.broadcast_month, -2) = ?
                    THEN COALESCE(s.gross_rate, 0) ELSE 0 END) as ytd_2024,
                -- Prior year revenue
                SUM(CASE WHEN SUBSTR(s.broadcast_month, -2) = ?
                    THEN COALESCE(s.gross_rate, 0) ELSE 0 END) as ytd_2023,
                -- Q4 current year (Oct, Nov, Dec)
                SUM(CASE WHEN SUBSTR(s.broadcast_month, -2) = ?
                    AND SUBSTR(s.broadcast_month, 1, 3) IN ('Oct', 'Nov', 'Dec')
                    THEN COALESCE(s.gross_rate, 0) ELSE 0 END) as q4_2024,
                -- Q4 prior year
                SUM(CASE WHEN SUBSTR(s.broadcast_month, -2) = ?
                    AND SUBSTR(s.broadcast_month, 1, 3) IN ('Oct', 'Nov', 'Dec')
                    THEN COALESCE(s.gross_rate, 0) ELSE 0 END) as q4_2023,
                -- Check if new - find first year with revenue
                MIN(CASE WHEN COALESCE(s.gross_rate, 0) > 0
                    THEN '20' || SUBSTR(s.broadcast_month, -2) END) as first_year
            FROM spots s
            {CustomerNormalizationQueryBuilder.build_customer_join()}
            LEFT JOIN customers c ON COALESCE(audit.customer_id, s.customer_id) = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE SUBSTR(s.broadcast_month, -2) IN (?, ?)
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                {ae_condition}
            GROUP BY COALESCE(audit.customer_id, s.customer_id, 0), customer_name, sec.sector_name, s.sales_person
            HAVING ytd_2024 > 0 OR ytd_2023 > 0
        )
        SELECT
            customer_id,
            customer_name,
            sector,
            ae,
            ytd_2024,
            ytd_2023,
            q4_2024,
            q4_2023,
            first_year
        FROM customer_revenue
        ORDER BY ytd_2024 DESC
        """

        # Add year parameters for Q4 calculations (already in params list above)

        cursor = conn.execute(query, params)

        customers = []
        for row in cursor.fetchall():
            (
                customer_id,
                customer_name,
                sector,
                ae,
                ytd_2024,
                ytd_2023,
                q4_2024,
                q4_2023,
                first_year,
            ) = row

            # Calculate variance
            variance_dollars = ytd_2024 - ytd_2023
            variance_pct = (
                (variance_dollars / ytd_2023 * 100)
                if ytd_2023 > 0
                else (100 if ytd_2024 > 0 else 0)
            )

            # Determine status
            is_new = str(first_year) == str(year)
            if ytd_2024 == 0 and ytd_2023 > 0:
                status = "lost"
                pattern = "Lost"
            elif is_new:
                status = "new"
                pattern = "New Customer"
            elif variance_pct < -20:
                status = "risk"
                pattern = self._determine_pattern(ytd_2024, ytd_2023, q4_2024, q4_2023)
            elif variance_pct > 20:
                status = "growth"
                pattern = "Growing"
            else:
                status = "stable"
                pattern = "Steady"

            # Q4 behind check
            q4_expected = q4_2023
            q4_behind = q4_2024 < (q4_expected * 0.7) if q4_expected > 0 else False

            customers.append(
                CustomerYoYPerformance(
                    customer=customer_name,
                    customer_id=customer_id,
                    sector=sector or "Unknown",
                    ae=ae,
                    ytd_2024=ytd_2024,
                    ytd_2023=ytd_2023,
                    variance_dollars=variance_dollars,
                    variance_pct=variance_pct,
                    status=status,
                    pattern=pattern,
                    is_new_2024=is_new,
                    q4_2024=q4_2024,
                    q4_2023=q4_2023,
                    q4_behind=q4_behind,
                )
            )

        return customers

    def _determine_pattern(
        self, ytd_2024: float, ytd_2023: float, q4_2024: float, q4_2023: float
    ) -> str:
        """Determine customer spending pattern"""
        if q4_2023 > (ytd_2023 * 0.4):  # Q4 was >40% of annual
            return "Q4 Heavy"
        elif abs(ytd_2024 - ytd_2023) / ytd_2023 > 0.3:  # Large variance
            return "Seasonal"
        else:
            return "Stable"

    def _calculate_sector_performance(
        self, customers: List[CustomerYoYPerformance]
    ) -> List[SectorPerformance]:
        """Aggregate customer data into sector summaries"""

        sectors_dict = {}

        for customer in customers:
            sector = customer.sector or "Unknown"

            if sector not in sectors_dict:
                sectors_dict[sector] = {"ytd_2024": 0, "ytd_2023": 0, "count": 0}

            sectors_dict[sector]["ytd_2024"] += customer.ytd_2024
            sectors_dict[sector]["ytd_2023"] += customer.ytd_2023
            sectors_dict[sector]["count"] += 1

        sectors = []
        for sector_name, data in sectors_dict.items():
            variance = data["ytd_2024"] - data["ytd_2023"]
            pct = (variance / data["ytd_2023"] * 100) if data["ytd_2023"] > 0 else 0

            if pct < -10:
                status = "declining"
            elif pct > 10:
                status = "growing"
            else:
                status = "stable"

            sectors.append(
                SectorPerformance(
                    sector=sector_name,
                    ytd_2024=data["ytd_2024"],
                    ytd_2023=data["ytd_2023"],
                    variance_dollars=variance,
                    variance_pct=pct,
                    customer_count=data["count"],
                    status=status,
                )
            )

        # Sort by 2024 revenue descending
        sectors.sort(key=lambda s: s.ytd_2024, reverse=True)

        return sectors

    def _calculate_overall_metrics(
        self, customers: List[CustomerYoYPerformance]
    ) -> Dict[str, Any]:
        """Calculate dashboard-level aggregate metrics"""

        # Total revenue
        total_ytd_2024 = sum(c.ytd_2024 for c in customers)
        total_ytd_2023 = sum(c.ytd_2023 for c in customers)
        total_variance = total_ytd_2024 - total_ytd_2023
        total_variance_pct = (
            (total_variance / total_ytd_2023 * 100) if total_ytd_2023 > 0 else 0
        )

        # Active customers
        active_2024 = len([c for c in customers if c.ytd_2024 > 0])
        active_2023 = len([c for c in customers if c.ytd_2023 > 0])

        # Lost customers
        lost_customers = [c for c in customers if c.status == "lost"]
        lost_revenue = sum(c.ytd_2023 for c in lost_customers)

        # Retention rate
        retention_rate = (
            ((active_2023 - len(lost_customers)) / active_2023 * 100)
            if active_2023 > 0
            else 0
        )

        # Average per customer
        avg_per_customer = (total_ytd_2024 / active_2024) if active_2024 > 0 else 0

        # New customers
        new_customers = [c for c in customers if c.is_new_2024]
        new_customer_revenue = sum(c.ytd_2024 for c in new_customers)
        new_customer_pct = (
            (new_customer_revenue / total_ytd_2024 * 100) if total_ytd_2024 > 0 else 0
        )

        # Top 5 concentration
        active_customers_sorted = sorted(
            [c for c in customers if c.ytd_2024 > 0],
            key=lambda c: c.ytd_2024,
            reverse=True,
        )
        top5_revenue = sum(c.ytd_2024 for c in active_customers_sorted[:5])
        top5_pct = (top5_revenue / total_ytd_2024 * 100) if total_ytd_2024 > 0 else 0

        return {
            "total_ytd_2024": total_ytd_2024,
            "total_ytd_2023": total_ytd_2023,
            "total_variance_dollars": total_variance,
            "total_variance_pct": total_variance_pct,
            "active_customers_2024": active_2024,
            "active_customers_2023": active_2023,
            "lost_customers": lost_customers,
            "lost_revenue": lost_revenue,
            "retention_rate": retention_rate,
            "avg_per_customer": avg_per_customer,
            "new_customers": new_customers,
            "new_customer_revenue": new_customer_revenue,
            "new_customer_pct": new_customer_pct,
            "top5_concentration_pct": top5_pct,
        }

    def _get_ae_list(self, conn) -> List[str]:
        """Get list of all AEs with revenue"""
        cursor = conn.execute("""
            SELECT DISTINCT COALESCE(sales_person, 'Unknown') as ae
            FROM spots
            WHERE sales_person IS NOT NULL
            ORDER BY ae
        """)

        return [row[0] for row in cursor.fetchall()]

    def _get_sector_list(self, conn) -> List[str]:
        """Get list of all sectors"""
        cursor = conn.execute("""
            SELECT DISTINCT sector_name
            FROM sectors
            WHERE is_active = 1
            ORDER BY sector_name
        """)

        return [row[0] for row in cursor.fetchall()]

    def _get_available_years(self, conn) -> List[int]:
        """Get list of years that have data, sorted descending"""
        cursor = conn.execute("""
            SELECT DISTINCT CAST('20' || SUBSTR(broadcast_month, -2) AS INTEGER) as year
            FROM spots
            ORDER BY year DESC
        """)
        return [row[0] for row in cursor.fetchall()]
