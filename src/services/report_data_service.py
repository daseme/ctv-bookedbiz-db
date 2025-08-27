#!/usr/bin/env python3
# src/services/report_data_service.py
"""
Report Data Service (Broadcast-Month–centric, AE-normalized)

Changes:
- Scope all queries by broadcast_month (mmm-yy). No air_date coupling.
- AE filtering normalized (UPPER/TRIM) with explicit 'Unknown' handling.
- COUNT(*) + COALESCE fixes.
- Removed lru_cache on methods that take db_connection (avoid unhashable-arg cache errors).
"""

import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal

from src.services.container import get_container
from src.models.report_data import (
    ReportMetadata, ReportFilters, MonthlyRevenueReportData,
    AEPerformanceReportData, QuarterlyPerformanceReportData, SectorPerformanceReportData,
    CustomerMonthlyRow, AEPerformanceData, QuarterlyData, SectorData, CustomerSectorData,
    MonthStatus, create_month_status_from_closure_data
)
from src.utils.template_formatters import calculate_statistics

logger = logging.getLogger(__name__)

# ---------- Helpers ----------

def _bm_like(year: int) -> str:
    return f"%-{str(year)[-2:]}"

def _ae_where_clause(ae_filter: Optional[str], column: str = "s.sales_person") -> Tuple[str, List[Any]]:
    """
    Build predicate/params for AE filter.
    'Unknown' → NULL or empty sales_person on raw columns,
    OR 'UNKNOWN' when filtering on pre-normalized ae_key.
    """
    if not ae_filter or ae_filter.strip().lower() == "all":
        return "", []
    val = ae_filter.strip()
    if column == "ae_key":  # already normalized to uppercase or 'UNKNOWN'
        if val.lower() == "unknown":
            return "ae_key = 'UNKNOWN'", []
        return "ae_key = UPPER(TRIM(?))", [val]
    # raw column path
    if val.lower() == "unknown":
        return f"( {column} IS NULL OR TRIM({column}) = '' )", []
    return f"UPPER(TRIM({column})) = UPPER(TRIM(?))", [val]

def _bm_case_month(expr: str = "s.broadcast_month") -> str:
    return f"""
        CASE 
            WHEN {expr} LIKE 'Jan-%' THEN '01'
            WHEN {expr} LIKE 'Feb-%' THEN '02'
            WHEN {expr} LIKE 'Mar-%' THEN '03'
            WHEN {expr} LIKE 'Apr-%' THEN '04'
            WHEN {expr} LIKE 'May-%' THEN '05'
            WHEN {expr} LIKE 'Jun-%' THEN '06'
            WHEN {expr} LIKE 'Jul-%' THEN '07'
            WHEN {expr} LIKE 'Aug-%' THEN '08'
            WHEN {expr} LIKE 'Sep-%' THEN '09'
            WHEN {expr} LIKE 'Oct-%' THEN '10'
            WHEN {expr} LIKE 'Nov-%' THEN '11'
            WHEN {expr} LIKE 'Dec-%' THEN '12'
        END
    """.strip()

def _bm_case_year(expr: str = "broadcast_month") -> str:
    return f"""
        CASE 
            WHEN {expr} LIKE '%-21' THEN 2021
            WHEN {expr} LIKE '%-22' THEN 2022
            WHEN {expr} LIKE '%-23' THEN 2023
            WHEN {expr} LIKE '%-24' THEN 2024
            WHEN {expr} LIKE '%-25' THEN 2025
            WHEN {expr} LIKE '%-26' THEN 2026
            WHEN {expr} LIKE '%-27' THEN 2027
            WHEN {expr} LIKE '%-28' THEN 2028
            WHEN {expr} LIKE '%-29' THEN 2029
            WHEN {expr} LIKE '%-30' THEN 2030
        END
    """.strip()

# ---------- Service ----------

class ReportDataService:
    def __init__(self, container=None):
        self.container = container or get_container()
        self._cache_enabled = self.container.get_config('CACHE_ENABLED', True)
        self._cache_ttl = self.container.get_config('CACHE_TTL', 300)  # seconds

    # ----- Public API -----

    def get_monthly_revenue_report_data(
        self,
        year: int,
        filters: Optional[ReportFilters] = None
    ) -> MonthlyRevenueReportData:
        start_time = time.time()
        filters = filters or ReportFilters(year=year)
        logger.info("Monthly revenue report year=%s filters=%s", year, filters.to_dict())

        db = self.container.get('database_connection')

        revenue_data = self._get_customer_monthly_revenue(db, year, filters)
        available_years = self._get_available_years(db)
        ae_list = self._get_ae_list(db, year)  # year-scoped for correctness in older years
        revenue_types = self._get_revenue_types(db)
        month_status = self._get_month_status(db, year)

        # Stats based on chosen field
        revenue_field = (filters.revenue_field or 'gross').lower()
        rows_for_stats: List[Dict[str, Any]] = []
        for r in revenue_data:
            d = r.to_dict()
            d['total'] = float(r.total_net) if revenue_field == 'net' else float(r.total_gross)
            rows_for_stats.append(d)
        stats = calculate_statistics(rows_for_stats)

        data_last_updated = self._get_data_last_updated(db)
        processing_time = (time.time() - start_time) * 1000.0

        metadata = ReportMetadata(
            report_type="monthly_revenue",
            parameters={'year': year, 'filters': filters.to_dict()},
            row_count=len(revenue_data),
            processing_time_ms=processing_time,
            data_last_updated=data_last_updated
        )

        return MonthlyRevenueReportData(
            selected_year=year,
            available_years=available_years,
            total_customers=stats['total_customers'],
            active_customers=stats['active_customers'],
            total_revenue=Decimal(str(round(stats['total_revenue'], 2))),
            avg_monthly_revenue=Decimal(str(round(stats['avg_monthly_revenue'], 2))),
            revenue_data=revenue_data,
            ae_list=ae_list,
            revenue_types=revenue_types,
            month_status=month_status,
            filters=filters,
            metadata=metadata
        )

    def get_ae_performance_report_data(
        self,
        filters: Optional[ReportFilters] = None
    ) -> AEPerformanceReportData:
        start_time = time.time()
        filters = filters or ReportFilters()
        logger.info("AE performance report filters=%s", filters.to_dict())

        db = self.container.get('database_connection')
        ae_data = self._get_ae_performance_data(db, filters)

        total_revenue = sum(ae.total_revenue for ae in ae_data)
        pcts = [ae.performance_pct for ae in ae_data if getattr(ae, "performance_pct", None) is not None]
        avg_performance_pct = (sum(pcts) / len(pcts)) if pcts else None
        top_performer = max(ae_data, key=lambda x: x.total_revenue).ae_name if ae_data else None

        metadata = ReportMetadata(
            report_type="ae_performance",
            parameters={'filters': filters.to_dict()},
            row_count=len(ae_data),
            processing_time_ms=(time.time() - start_time) * 1000.0
        )

        return AEPerformanceReportData(
            ae_performance=ae_data,
            total_revenue=total_revenue,
            avg_performance_pct=avg_performance_pct,
            top_performer=top_performer,
            filters=filters,
            metadata=metadata
        )

    def get_quarterly_performance_data(
        self,
        filters: Optional[ReportFilters] = None
    ) -> QuarterlyPerformanceReportData:
        start_time = time.time()
        filters = filters or ReportFilters()
        logger.info("Quarterly performance report filters=%s", filters.to_dict())

        db = self.container.get('database_connection')
        quarterly_data = self._get_quarterly_data(db, filters)
        ae_data = self._get_ae_performance_data(db, filters)
        total_revenue = sum(q.total_revenue for q in quarterly_data)
        current_year = filters.year or date.today().year

        metadata = ReportMetadata(
            report_type="quarterly_performance",
            parameters={'filters': filters.to_dict()},
            row_count=len(quarterly_data),
            processing_time_ms=(time.time() - start_time) * 1000.0
        )

        return QuarterlyPerformanceReportData(
            current_year=current_year,
            quarterly_data=quarterly_data,
            ae_performance=ae_data,
            total_revenue=total_revenue,
            filters=filters,
            metadata=metadata
        )

    def get_sector_performance_data(
        self,
        filters: Optional[ReportFilters] = None
    ) -> SectorPerformanceReportData:
        start_time = time.time()
        filters = filters or ReportFilters()
        logger.info("Sector performance report filters=%s", filters.to_dict())

        db = self.container.get('database_connection')
        sectors = self._get_sector_data(db, filters)
        top_customers = self._get_top_customers_by_sector(db, filters)

        total_revenue = sum(sector.total_revenue for sector in sectors)

        metadata = ReportMetadata(
            report_type="sector_performance",
            parameters={'filters': filters.to_dict()},
            row_count=len(sectors),
            processing_time_ms=(time.time() - start_time) * 1000.0
        )

        return SectorPerformanceReportData(
            sectors=sectors,
            top_customers_by_sector=top_customers,
            total_revenue=total_revenue,
            sector_count=len(sectors),
            filters=filters,
            metadata=metadata
        )

    # ----- Internals -----

    def _get_customer_monthly_revenue(
        self,
        db_connection,
        year: int,
        filters: ReportFilters
    ) -> List[CustomerMonthlyRow]:
        month_expr = _bm_case_month("s.broadcast_month")
        query = f"""
            SELECT
                c.customer_id,
                COALESCE(a.agency_name || ' : ', '') || c.normalized_name AS customer,
                CASE 
                  WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'Unknown'
                  ELSE TRIM(s.sales_person)
                END AS ae,
                COALESCE(s.revenue_type, 'Regular') AS revenue_type,
                sect.sector_name AS sector,
                {month_expr} AS month,
                ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS gross_revenue,
                ROUND(SUM(COALESCE(s.station_net, 0)), 2) AS net_revenue
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN agencies  a ON s.agency_id = a.agency_id
            LEFT JOIN sectors  sect ON c.sector_id = sect.sector_id
            WHERE s.broadcast_month LIKE ?
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
              AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
        """
        params: List[Any] = [_bm_like(year)]

        if filters.customer_search:
            query += " AND LOWER(c.normalized_name) LIKE LOWER(?)"
            params.append(f"%{filters.customer_search}%")

        ae_sql, ae_params = _ae_where_clause(filters.ae_filter, "s.sales_person")
        if ae_sql:
            query += f" AND {ae_sql}"
            params.extend(ae_params)

        if filters.revenue_type and filters.revenue_type != 'all':
            query += " AND s.revenue_type = ?"
            params.append(filters.revenue_type)

        query += " GROUP BY c.customer_id, c.normalized_name, ae, revenue_type, sect.sector_name, month"

        conn = db_connection.connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()

        buckets: Dict[Any, Dict[str, Any]] = {}
        for customer_id, customer, ae, revenue_type, sector, month_txt, gross, net in rows:
            key = (customer_id, customer, ae, revenue_type)
            if key not in buckets:
                buckets[key] = {
                    'customer_id': customer_id,
                    'customer': customer,
                    'ae': ae,
                    'revenue_type': revenue_type,
                    'sector': sector,
                    'months_gross': {m: Decimal('0') for m in range(1, 13)},
                    'months_net':   {m: Decimal('0') for m in range(1, 13)},
                }
            try:
                mnum = int(month_txt)
                if 1 <= mnum <= 12:
                    buckets[key]['months_gross'][mnum] = Decimal(str(gross))
                    buckets[key]['months_net'][mnum]   = Decimal(str(net))
            except (TypeError, ValueError):
                pass

        result: List[CustomerMonthlyRow] = []
        for data in buckets.values():
            row = CustomerMonthlyRow(
                customer_id=data['customer_id'],
                customer=data['customer'],
                ae=data['ae'],
                revenue_type=data['revenue_type'],
                sector=data['sector']
            )
            for m in range(1, 13):
                row.set_month_value(m, data['months_gross'][m], data['months_net'][m])
            result.append(row)

        result.sort(key=lambda x: x.total, reverse=True)  # keep existing behavior
        return result

    def _get_available_years(self, db_connection) -> List[int]:
        query = f"""
            SELECT DISTINCT {_bm_case_year('broadcast_month')} AS year
            FROM spots
            WHERE broadcast_month IS NOT NULL
              AND broadcast_month <> ''
              AND broadcast_month LIKE '%-__'
            ORDER BY year DESC
        """
        conn = db_connection.connect()
        years = [int(r[0]) for r in conn.execute(query).fetchall() if r[0] is not None]
        conn.close()
        return sorted(list(set(years)), reverse=True)

    def _get_ae_list(self, db_connection, year: Optional[int] = None) -> List[str]:
        q = """
            SELECT MIN(TRIM(sales_person)) AS ae_display
            FROM spots
            WHERE sales_person IS NOT NULL AND TRIM(sales_person) <> ''
        """
        params: List[Any] = []
        if year:
            q += " AND broadcast_month LIKE ?"
            params.append(_bm_like(year))
        q += " GROUP BY UPPER(TRIM(sales_person)) ORDER BY ae_display"
        conn = db_connection.connect()
        names = [r[0] for r in conn.execute(q, params).fetchall()]
        conn.close()
        return names

    def _get_revenue_types(self, db_connection) -> List[str]:
        query = """
            SELECT DISTINCT COALESCE(revenue_type, 'Regular') AS revenue_type
            FROM spots
            WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
            ORDER BY revenue_type
        """
        conn = db_connection.connect()
        types_ = [r[0] for r in conn.execute(query).fetchall()]
        conn.close()
        return types_

    def _get_month_status(self, db_connection, year: int) -> List[MonthStatus]:
        query = "SELECT broadcast_month, closed_date, closed_by FROM month_closures WHERE broadcast_month LIKE ?"
        conn = db_connection.connect()
        rows = conn.execute(query, (_bm_like(year),)).fetchall()
        conn.close()
        closures = [{'broadcast_month': r[0], 'closed_date': r[1], 'closed_by': r[2]} for r in rows]
        return create_month_status_from_closure_data(closures, year)

    def _get_ae_performance_data(self, db_connection, filters: ReportFilters) -> List[AEPerformanceData]:
        query = """
            WITH norm AS (
                SELECT
                    CASE
                        WHEN s.sales_person IS NULL OR TRIM(s.sales_person) = '' THEN 'UNKNOWN'
                        ELSE UPPER(TRIM(s.sales_person))
                    END AS ae_key,
                    TRIM(COALESCE(s.sales_person, 'Unknown')) AS ae_display,
                    s.gross_rate,
                    s.air_date,
                    s.broadcast_month
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
            )
            SELECT
                MIN(ae_display) AS ae_name,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(gross_rate, 0)), 2) AS total_revenue,
                ROUND(AVG(COALESCE(gross_rate, 0)), 2) AS avg_rate,
                MIN(air_date) AS first_spot_date,
                MAX(air_date) AS last_spot_date
            FROM norm
            WHERE 1=1
        """
        params: List[Any] = []
        where: List[str] = []
        if filters.year:
            where.append("broadcast_month LIKE ?")
            params.append(_bm_like(filters.year))
        ae_sql, ae_params = _ae_where_clause(filters.ae_filter, "ae_key")
        if ae_sql:
            where.append(ae_sql)
            params.extend(ae_params)
        if where:
            query += " AND " + " AND ".join(where)
        query += " GROUP BY ae_key ORDER BY total_revenue DESC"

        conn = db_connection.connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()

        out: List[AEPerformanceData] = []
        for r in rows:
            out.append(AEPerformanceData(
                ae_name=r[0],
                spot_count=r[1],
                total_revenue=Decimal(str(r[2])),
                avg_rate=Decimal(str(r[3])),
                first_spot_date=datetime.strptime(r[4], '%Y-%m-%d').date() if r[4] else None,
                last_spot_date=datetime.strptime(r[5], '%Y-%m-%d').date() if r[5] else None
            ))
        return out

    def _get_quarterly_data(self, db_connection, filters: ReportFilters) -> List[QuarterlyData]:
        quarter_case = """
            CASE 
              WHEN s.broadcast_month LIKE 'Jan-%' OR s.broadcast_month LIKE 'Feb-%' OR s.broadcast_month LIKE 'Mar-%' THEN 'Q1'
              WHEN s.broadcast_month LIKE 'Apr-%' OR s.broadcast_month LIKE 'May-%' OR s.broadcast_month LIKE 'Jun-%' THEN 'Q2'
              WHEN s.broadcast_month LIKE 'Jul-%' OR s.broadcast_month LIKE 'Aug-%' OR s.broadcast_month LIKE 'Sep-%' THEN 'Q3'
              WHEN s.broadcast_month LIKE 'Oct-%' OR s.broadcast_month LIKE 'Nov-%' OR s.broadcast_month LIKE 'Dec-%' THEN 'Q4'
            END
        """.strip()
        year_case = _bm_case_year("s.broadcast_month")
        query = f"""
            SELECT 
                {quarter_case} AS quarter,
                {year_case}   AS year,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(s.gross_rate,0)), 2) AS total_revenue,
                ROUND(AVG(COALESCE(s.gross_rate,0)), 2) AS avg_rate
            FROM spots s
            WHERE s.broadcast_month IS NOT NULL
              AND s.gross_rate IS NOT NULL
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """
        params: List[Any] = []
        where: List[str] = []
        if filters.year:
            where.append("s.broadcast_month LIKE ?")
            params.append(_bm_like(filters.year))
        ae_sql, ae_params = _ae_where_clause(filters.ae_filter, "s.sales_person")
        if ae_sql:
            where.append(ae_sql)
            params.extend(ae_params)
        if where:
            query += " AND " + " AND ".join(where)
        query += " GROUP BY quarter, year ORDER BY year DESC, quarter"

        conn = db_connection.connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()

        out: List[QuarterlyData] = []
        for r in rows:
            out.append(QuarterlyData(
                quarter=r[0],
                year=int(r[1]),
                spot_count=r[2],
                total_revenue=Decimal(str(r[3])),
                avg_rate=Decimal(str(r[4]))
            ))
        return out

    def _get_sector_data(self, db_connection, filters: ReportFilters) -> List[SectorData]:
        query = """
            SELECT 
                sctr.sector_name,
                sctr.sector_code,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(sp.gross_rate,0)), 2) AS total_revenue,
                ROUND(AVG(COALESCE(sp.gross_rate,0)), 2) AS avg_rate
            FROM spots sp
            JOIN customers c   ON sp.customer_id = c.customer_id
            JOIN sectors  sctr ON c.sector_id = sctr.sector_id
            WHERE sp.gross_rate IS NOT NULL
              AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        """
        params: List[Any] = []
        where: List[str] = []
        if filters.year:
            where.append("sp.broadcast_month LIKE ?")
            params.append(_bm_like(filters.year))
        ae_sql, ae_params = _ae_where_clause(filters.ae_filter, "sp.sales_person")
        if ae_sql:
            where.append(ae_sql)
            params.extend(ae_params)
        if where:
            query += " AND " + " AND ".join(where)
        query += " GROUP BY sctr.sector_id, sctr.sector_name, sctr.sector_code ORDER BY total_revenue DESC"

        conn = db_connection.connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()

        out: List[SectorData] = []
        for r in rows:
            out.append(SectorData(
                sector_name=r[0],
                sector_code=r[1],
                spot_count=r[2],
                total_revenue=Decimal(str(r[3])),
                avg_rate=Decimal(str(r[4]))
            ))
        return out

    def _get_top_customers_by_sector(self, db_connection, filters: ReportFilters) -> List[CustomerSectorData]:
        query = """
            SELECT 
                sctr.sector_name,
                c.normalized_name AS customer_name,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(sp.gross_rate,0)), 2) AS total_revenue
            FROM spots sp
            JOIN customers c   ON sp.customer_id = c.customer_id
            JOIN sectors  sctr ON c.sector_id = sctr.sector_id
            WHERE sp.gross_rate IS NOT NULL
              AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        """
        params: List[Any] = []
        where: List[str] = []
        if filters.year:
            where.append("sp.broadcast_month LIKE ?")
            params.append(_bm_like(filters.year))
        ae_sql, ae_params = _ae_where_clause(filters.ae_filter, "sp.sales_person")
        if ae_sql:
            where.append(ae_sql)
            params.extend(ae_params)
        if where:
            query += " AND " + " AND ".join(where)
        query += " GROUP BY sctr.sector_id, c.customer_id ORDER BY sctr.sector_name, total_revenue DESC"

        conn = db_connection.connect()
        rows = conn.execute(query, params).fetchall()
        conn.close()

        out: List[CustomerSectorData] = []
        for r in rows:
            out.append(CustomerSectorData(
                sector_name=r[0],
                customer_name=r[1],
                spot_count=r[2],
                total_revenue=Decimal(str(r[3]))
            ))
        return out

    def _get_data_last_updated(self, db_connection) -> datetime:
        query = "SELECT MAX(load_date) AS last_update FROM spots WHERE load_date IS NOT NULL"
        conn = db_connection.connect()
        row = conn.execute(query).fetchone()
        conn.close()
        if row and row[0]:
            try:
                return datetime.fromisoformat(row[0])
            except Exception:
                pass
        return datetime.now()

# Factory
def create_report_data_service():
    return ReportDataService()
