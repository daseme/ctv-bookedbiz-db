"""Customer detail service - business logic for customer report page."""

from dataclasses import dataclass, field
from typing import Optional
from decimal import Decimal


@dataclass
class CustomerSummary:
    """Core customer identity and lifetime metrics."""
    customer_id: int
    normalized_name: str
    customer_type: Optional[str]
    is_active: bool
    created_date: str
    notes: Optional[str]
    sector_code: Optional[str]
    sector_name: Optional[str]
    agency_name: Optional[str]
    primary_ae: Optional[str]
    lifetime_gross: Decimal = Decimal("0")
    lifetime_net: Decimal = Decimal("0")
    lifetime_spots: int = 0
    first_air_date: Optional[str] = None
    last_air_date: Optional[str] = None
    avg_spot_rate: Decimal = Decimal("0")


@dataclass
class PeriodComparison:
    """Year-over-year comparison metrics."""
    current_year: int
    current_year_gross: Decimal = Decimal("0")
    current_year_net: Decimal = Decimal("0")
    current_year_spots: int = 0
    prior_year_gross: Decimal = Decimal("0")
    prior_year_net: Decimal = Decimal("0")
    prior_year_spots: int = 0
    
    @property
    def gross_change_pct(self) -> Optional[float]:
        if self.prior_year_gross and self.prior_year_gross > 0:
            return float((self.current_year_gross - self.prior_year_gross) / self.prior_year_gross * 100)
        return None


@dataclass
class MonthlyRevenue:
    """Single month revenue data point."""
    broadcast_month: str
    gross_revenue: Decimal
    net_revenue: Decimal
    spot_count: int


@dataclass
class LanguageBreakdown:
    """Revenue breakdown by language."""
    language_code: str
    language_name: Optional[str]
    gross_revenue: Decimal
    net_revenue: Decimal
    spot_count: int
    pct_of_total: float = 0.0


@dataclass
class AEBreakdown:
    """Revenue breakdown by account executive."""
    ae_name: str
    gross_revenue: Decimal
    net_revenue: Decimal
    spot_count: int
    pct_of_total: float = 0.0


@dataclass
class MarketBreakdown:
    """Revenue breakdown by market."""
    market_code: str
    market_name: str
    gross_revenue: Decimal
    net_revenue: Decimal
    spot_count: int
    pct_of_total: float = 0.0


@dataclass
class RecentSpot:
    """Recent spot activity record."""
    spot_id: int
    air_date: str
    broadcast_month: str
    time_in: Optional[str]
    length_seconds: Optional[str]
    gross_rate: Decimal
    station_net: Decimal
    sales_person: Optional[str]
    language_code: Optional[str]
    market_code: Optional[str]
    revenue_type: Optional[str]


@dataclass
class BillCodeAlias:
    """Bill code variation that resolves to this customer."""
    alias_name: str
    confidence_score: int
    created_date: str


@dataclass
class CustomerDetailReport:
    """Complete customer detail report."""
    summary: CustomerSummary
    period_comparison: PeriodComparison
    monthly_trend: list[MonthlyRevenue] = field(default_factory=list)
    language_breakdown: list[LanguageBreakdown] = field(default_factory=list)
    ae_breakdown: list[AEBreakdown] = field(default_factory=list)
    market_breakdown: list[MarketBreakdown] = field(default_factory=list)
    recent_spots: list[RecentSpot] = field(default_factory=list)
    bill_code_aliases: list[BillCodeAlias] = field(default_factory=list)
    date_range_label: str = ""
    has_date_filter: bool = False


class CustomerDetailService:
    """Service for retrieving customer detail report data."""

    def __init__(self, db_connection):
        self.conn = db_connection
        self.start_date = ""
        self.end_date = ""

    def _date_filter_sql(self, alias="s"):
        """Build date filter clause and params for air_date."""
        if not self.start_date and not self.end_date:
            return "", []
        clauses = []
        params = []
        if self.start_date:
            clauses.append(f"{alias}.air_date >= ?")
            params.append(self.start_date)
        if self.end_date:
            clauses.append(f"{alias}.air_date <= ?")
            params.append(self.end_date)
        return " AND " + " AND ".join(clauses), params

    @staticmethod
    def _format_date_label(start_date, end_date):
        """Format date range as human-readable label."""
        from datetime import datetime
        fmt = "%b %d, %Y"
        parts = []
        if start_date:
            parts.append(
                datetime.strptime(start_date, "%Y-%m-%d").strftime(fmt)
            )
        if end_date:
            parts.append(
                datetime.strptime(end_date, "%Y-%m-%d").strftime(fmt)
            )
        return " – ".join(parts)

    def get_customer_detail(
        self,
        customer_id: int,
        start_date: str = "",
        end_date: str = "",
    ) -> Optional[CustomerDetailReport]:
        """Build complete customer detail report."""
        self.start_date = start_date
        self.end_date = end_date
        has_filter = bool(start_date or end_date)

        summary = self._get_summary(customer_id)
        if not summary:
            return None

        period_comparison = (
            PeriodComparison(current_year=2025)
            if has_filter
            else self._get_period_comparison(customer_id)
        )

        return CustomerDetailReport(
            summary=summary,
            period_comparison=period_comparison,
            monthly_trend=self._get_monthly_trend(customer_id),
            language_breakdown=self._get_language_breakdown(customer_id),
            ae_breakdown=self._get_ae_breakdown(customer_id),
            market_breakdown=self._get_market_breakdown(customer_id),
            recent_spots=self._get_recent_spots(customer_id),
            bill_code_aliases=self._get_aliases(customer_id),
            date_range_label=self._format_date_label(
                start_date, end_date
            ) if has_filter else "",
            has_date_filter=has_filter,
        )
    
    def _get_summary(self, customer_id: int) -> Optional[CustomerSummary]:
        """Get customer identity and lifetime metrics."""
        date_sql, date_params = self._date_filter_sql("s")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                c.customer_id,
                c.normalized_name,
                c.customer_type,
                c.is_active,
                c.created_date,
                c.notes,
                sec.sector_code,
                sec.sector_name,
                a.agency_name,
                COALESCE(SUM(s.gross_rate), 0) AS lifetime_gross,
                COALESCE(SUM(s.station_net), 0) AS lifetime_net,
                COUNT(s.spot_id) AS lifetime_spots,
                MIN(s.air_date) AS first_air_date,
                MAX(s.air_date) AS last_air_date
            FROM customers c
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            LEFT JOIN agencies a ON c.agency_id = a.agency_id
            LEFT JOIN spots s ON c.customer_id = s.customer_id
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                {date_sql}
            WHERE c.customer_id = ?
            GROUP BY c.customer_id
        """, date_params + [customer_id])

        row = cursor.fetchone()
        if not row:
            return None

        ae_date_sql, ae_date_params = self._date_filter_sql("")
        ae_date_sql_clean = ae_date_sql.replace(".air_date", "air_date")
        cursor.execute(f"""
            SELECT sales_person
            FROM spots
            WHERE customer_id = ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND sales_person IS NOT NULL
                {ae_date_sql_clean}
            GROUP BY sales_person
            ORDER BY SUM(gross_rate) DESC
            LIMIT 1
        """, [customer_id] + ae_date_params)
        ae_row = cursor.fetchone()
        primary_ae = ae_row[0] if ae_row else None
        
        lifetime_gross = Decimal(str(row[9])) if row[9] else Decimal("0")
        lifetime_spots = row[11] or 0
        
        return CustomerSummary(
            customer_id=row[0],
            normalized_name=row[1],
            customer_type=row[2],
            is_active=bool(row[3]),
            created_date=row[4],
            notes=row[5],
            sector_code=row[6],
            sector_name=row[7],
            agency_name=row[8],
            lifetime_gross=lifetime_gross,
            lifetime_net=Decimal(str(row[10])) if row[10] else Decimal("0"),
            lifetime_spots=lifetime_spots,
            first_air_date=row[12],
            last_air_date=row[13],
            avg_spot_rate=lifetime_gross / lifetime_spots if lifetime_spots > 0 else Decimal("0"),
            primary_ae=primary_ae
        )
    
    def _get_period_comparison(self, customer_id: int) -> PeriodComparison:
        """Get current year vs prior year comparison."""
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH yearly AS (
                SELECT 
                    CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER) AS year,
                    SUM(gross_rate) AS gross,
                    SUM(station_net) AS net,
                    COUNT(*) AS spots
                FROM spots
                WHERE customer_id = ?
                    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                GROUP BY CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER)
            )
            SELECT 
                year, gross, net, spots
            FROM yearly
            WHERE year >= (SELECT MAX(year) - 1 FROM yearly)
            ORDER BY year DESC
        """, [customer_id])
        
        rows = cursor.fetchall()
        
        current_year = 2025  # Default
        current = {"gross": Decimal("0"), "net": Decimal("0"), "spots": 0}
        prior = {"gross": Decimal("0"), "net": Decimal("0"), "spots": 0}
        
        for row in rows:
            year, gross, net, spots = row
            if not current_year or year > current_year - 1:
                current_year = year
                current = {"gross": Decimal(str(gross or 0)), "net": Decimal(str(net or 0)), "spots": spots or 0}
            else:
                prior = {"gross": Decimal(str(gross or 0)), "net": Decimal(str(net or 0)), "spots": spots or 0}
        
        return PeriodComparison(
            current_year=current_year,
            current_year_gross=current["gross"],
            current_year_net=current["net"],
            current_year_spots=current["spots"],
            prior_year_gross=prior["gross"],
            prior_year_net=prior["net"],
            prior_year_spots=prior["spots"]
        )
    
    def _get_monthly_trend(self, customer_id: int, months: int = 24) -> list[MonthlyRevenue]:
        """Get monthly revenue trend for charting."""
        date_sql, date_params = self._date_filter_sql("")
        date_sql_clean = date_sql.replace(".air_date", "air_date")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                broadcast_month,
                SUM(gross_rate) AS gross,
                SUM(station_net) AS net,
                COUNT(*) AS spots
            FROM spots
            WHERE customer_id = ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                {date_sql_clean}
            GROUP BY broadcast_month
            ORDER BY
                CAST('20' || SUBSTR(broadcast_month, 5, 2) AS INTEGER),
                CASE SUBSTR(broadcast_month, 1, 3)
                    WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                    WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                    WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                    WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                END
        """, [customer_id] + date_params)
        
        results = []
        for row in cursor.fetchall():
            results.append(MonthlyRevenue(
                broadcast_month=row[0],
                gross_revenue=Decimal(str(row[1] or 0)),
                net_revenue=Decimal(str(row[2] or 0)),
                spot_count=row[3] or 0
            ))
        
        # Return last N months
        return results[-months:] if len(results) > months else results
    
    def _get_language_breakdown(self, customer_id: int) -> list[LanguageBreakdown]:
        """Get revenue breakdown by language."""
        date_sql, date_params = self._date_filter_sql("s")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                COALESCE(s.language_code, 'Unknown') AS lang_code,
                l.language_name,
                SUM(s.gross_rate) AS gross,
                SUM(s.station_net) AS net,
                COUNT(*) AS spots
            FROM spots s
            LEFT JOIN languages l ON s.language_code = l.language_code
            WHERE s.customer_id = ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                {date_sql}
            GROUP BY COALESCE(s.language_code, 'Unknown'), l.language_name
            ORDER BY gross DESC
        """, [customer_id] + date_params)
        
        results = []
        total_gross = Decimal("0")
        
        for row in cursor.fetchall():
            gross = Decimal(str(row[2] or 0))
            total_gross += gross
            results.append(LanguageBreakdown(
                language_code=row[0],
                language_name=row[1],
                gross_revenue=gross,
                net_revenue=Decimal(str(row[3] or 0)),
                spot_count=row[4] or 0
            ))
        
        # Calculate percentages
        for item in results:
            if total_gross > 0:
                item.pct_of_total = float(item.gross_revenue / total_gross * 100)
        
        return results
    
    def _get_ae_breakdown(self, customer_id: int) -> list[AEBreakdown]:
        """Get revenue breakdown by account executive."""
        date_sql, date_params = self._date_filter_sql("")
        date_sql_clean = date_sql.replace(".air_date", "air_date")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                COALESCE(sales_person, 'Unassigned') AS ae,
                SUM(gross_rate) AS gross,
                SUM(station_net) AS net,
                COUNT(*) AS spots
            FROM spots
            WHERE customer_id = ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                {date_sql_clean}
            GROUP BY COALESCE(sales_person, 'Unassigned')
            ORDER BY gross DESC
        """, [customer_id] + date_params)
        
        results = []
        total_gross = Decimal("0")
        
        for row in cursor.fetchall():
            gross = Decimal(str(row[1] or 0))
            total_gross += gross
            results.append(AEBreakdown(
                ae_name=row[0],
                gross_revenue=gross,
                net_revenue=Decimal(str(row[2] or 0)),
                spot_count=row[3] or 0
            ))
        
        for item in results:
            if total_gross > 0:
                item.pct_of_total = float(item.gross_revenue / total_gross * 100)
        
        return results
    
    def _get_market_breakdown(self, customer_id: int) -> list[MarketBreakdown]:
        """Get revenue breakdown by market."""
        date_sql, date_params = self._date_filter_sql("s")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                COALESCE(m.market_code, 'Unknown') AS mkt_code,
                COALESCE(m.market_name, 'Unknown') AS mkt_name,
                SUM(s.gross_rate) AS gross,
                SUM(s.station_net) AS net,
                COUNT(*) AS spots
            FROM spots s
            LEFT JOIN markets m ON s.market_id = m.market_id
            WHERE s.customer_id = ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                {date_sql}
            GROUP BY COALESCE(m.market_code, 'Unknown'), COALESCE(m.market_name, 'Unknown')
            ORDER BY gross DESC
        """, [customer_id] + date_params)
        
        results = []
        total_gross = Decimal("0")
        
        for row in cursor.fetchall():
            gross = Decimal(str(row[2] or 0))
            total_gross += gross
            results.append(MarketBreakdown(
                market_code=row[0],
                market_name=row[1],
                gross_revenue=gross,
                net_revenue=Decimal(str(row[3] or 0)),
                spot_count=row[4] or 0
            ))
        
        for item in results:
            if total_gross > 0:
                item.pct_of_total = float(item.gross_revenue / total_gross * 100)
        
        return results
    
    def _get_recent_spots(self, customer_id: int, limit: int = 15) -> list[RecentSpot]:
        """Get most recent spot activity."""
        date_sql, date_params = self._date_filter_sql("s")
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT
                s.spot_id,
                s.air_date,
                s.broadcast_month,
                s.time_in,
                s.length_seconds,
                s.gross_rate,
                s.station_net,
                s.sales_person,
                s.language_code,
                m.market_code,
                s.revenue_type
            FROM spots s
            LEFT JOIN markets m ON s.market_id = m.market_id
            WHERE s.customer_id = ?
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                {date_sql}
            ORDER BY s.air_date DESC, s.time_in DESC
            LIMIT ?
        """, [customer_id] + date_params + [limit])
        
        results = []
        for row in cursor.fetchall():
            results.append(RecentSpot(
                spot_id=row[0],
                air_date=row[1],
                broadcast_month=row[2],
                time_in=row[3],
                length_seconds=row[4],
                gross_rate=Decimal(str(row[5] or 0)),
                station_net=Decimal(str(row[6] or 0)),
                sales_person=row[7],
                language_code=row[8],
                market_code=row[9],
                revenue_type=row[10]
            ))
        
        return results
    
    def _get_aliases(self, customer_id: int) -> list[BillCodeAlias]:
        """Get bill code aliases that resolve to this customer."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                alias_name,
                confidence_score,
                created_date
            FROM entity_aliases
            WHERE entity_type = 'customer'
                AND target_entity_id = ?
                AND is_active = 1
            ORDER BY created_date DESC
        """, [customer_id])
        
        results = []
        for row in cursor.fetchall():
            results.append(BillCodeAlias(
                alias_name=row[0],
                confidence_score=row[1] or 100,
                created_date=row[2]
            ))
        
        return results