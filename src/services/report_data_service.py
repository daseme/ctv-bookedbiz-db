# src/services/report_data_service.py
"""
Report Data Service for preparing structured report data.
Handles data aggregation, transformation, and caching for report generation.
"""
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, date
from decimal import Decimal
from functools import lru_cache
import calendar

from services.container import get_container
from models.report_data import (
    ReportMetadata, ReportFilters, MonthlyRevenueReportData,
    AEPerformanceReportData, QuarterlyPerformanceReportData, SectorPerformanceReportData,
    CustomerMonthlyRow, AEPerformanceData, QuarterlyData, SectorData, CustomerSectorData,
    MonthStatus, create_customer_monthly_row_from_dict, create_month_status_from_closure_data
)
from utils.template_formatters import calculate_statistics

logger = logging.getLogger(__name__)


class ReportDataService:
    """
    Service for preparing structured report data.
    Uses existing services and repositories to aggregate and format data for reports.
    """
    
    def __init__(self, container=None):
        """Initialize with service container."""
        self.container = container or get_container()
        self._cache_enabled = self.container.get_config('CACHE_ENABLED', True)
        self._cache_ttl = self.container.get_config('CACHE_TTL', 300)  # 5 minutes
        
    def get_monthly_revenue_report_data(
        self, 
        year: int, 
        filters: Optional[ReportFilters] = None
    ) -> MonthlyRevenueReportData:
        """
        Get complete monthly revenue report data for report5.html.
        
        Args:
            year: Year to generate report for
            filters: Optional filters to apply
            
        Returns:
            MonthlyRevenueReportData object ready for template consumption
        """
        start_time = time.time()
        
        if filters is None:
            filters = ReportFilters(year=year)
        
        logger.info(f"Generating monthly revenue report for year {year}")
        
        try:
            # Get database connection
            db_connection = self.container.get('database_connection')
            
            # Get customer monthly revenue data
            revenue_data = self._get_customer_monthly_revenue(db_connection, year, filters)
            
            # Get available years
            available_years = self._get_available_years(db_connection)
            
            # Get AE list
            ae_list = self._get_ae_list(db_connection)
            
            # Get revenue types
            revenue_types = self._get_revenue_types(db_connection)
            
            # Get month closure status
            month_status = self._get_month_status(db_connection, year)
            
            # Calculate statistics based on selected revenue field
            revenue_field = filters.revenue_field if filters.revenue_field else 'gross'
            revenue_data_dicts = []
            for row in revenue_data:
                row_dict = row.to_dict()
                # Override 'total' with the selected revenue field total
                if revenue_field == 'net':
                    row_dict['total'] = float(row.total_net)
                else:
                    row_dict['total'] = float(row.total_gross)
                revenue_data_dicts.append(row_dict)
            
            stats = calculate_statistics(revenue_data_dicts)
            
            # Create metadata
            data_last_updated = self._get_data_last_updated(db_connection)
            processing_time = (time.time() - start_time) * 1000
            metadata = ReportMetadata(
                report_type="monthly_revenue",
                parameters={'year': year, 'filters': filters.to_dict()},
                row_count=len(revenue_data),
                processing_time_ms=processing_time,
                data_last_updated=data_last_updated
            )
            
            # Build complete report data
            report_data = MonthlyRevenueReportData(
                selected_year=year,
                available_years=available_years,
                total_customers=stats['total_customers'],
                active_customers=stats['active_customers'],
                total_revenue=Decimal(str(round(stats['total_revenue'], 2))),  # Round here
                avg_monthly_revenue=Decimal(str(round(stats['avg_monthly_revenue'], 2))),  # Round here
                revenue_data=revenue_data,
                ae_list=ae_list,
                revenue_types=revenue_types,
                month_status=month_status,
                filters=filters,
                metadata=metadata
            )
            
            # After getting revenue_data, add debug logging:
            logger.info(f"DEBUG: Number of revenue records: {len(revenue_data)}")
            total_from_rows = sum(row.total for row in revenue_data)
            logger.info(f"DEBUG: Total from CustomerMonthlyRow objects: {total_from_rows}")
            logger.info(f"DEBUG: Type: {type(total_from_rows)}")
            
            # Calculate statistics based on selected revenue field
            revenue_field = filters.revenue_field if filters.revenue_field else 'gross'
            revenue_data_dicts = []
            for row in revenue_data:
                row_dict = row.to_dict()
                # Override 'total' with the selected revenue field total
                if revenue_field == 'net':
                    row_dict['total'] = float(row.total_net)
                else:
                    row_dict['total'] = float(row.total_gross)
                revenue_data_dicts.append(row_dict)
            
            # Debug the dict totals
            total_from_dicts = sum(row['total'] for row in revenue_data_dicts)
            logger.info(f"DEBUG: Total from dicts: {total_from_dicts}")
            
            #stats = calculate_statistics(revenue_data_dicts)
            logger.info(f"DEBUG: Stats total_revenue: {stats['total_revenue']}")

            # Log processing time
            logger.info(f"Monthly revenue report generated in {processing_time:.1f}ms")
            return report_data
        
        except Exception as e:
            logger.error(f"Error generating monthly revenue report: {e}")
            raise
    
    def get_ae_performance_report_data(
        self, 
        filters: Optional[ReportFilters] = None
    ) -> AEPerformanceReportData:
        """
        Get AE performance report data.
        
        Args:
            filters: Optional filters to apply
            
        Returns:
            AEPerformanceReportData object
        """
        start_time = time.time()
        
        if filters is None:
            filters = ReportFilters()
        
        logger.info("Generating AE performance report")
        
        try:
            db_connection = self.container.get('database_connection')
            
            # Get AE performance data
            ae_data = self._get_ae_performance_data(db_connection, filters)
            
            # Calculate summary statistics
            total_revenue = sum(ae.total_revenue for ae in ae_data)
            performance_pcts = [ae.performance_pct for ae in ae_data if ae.performance_pct is not None]
            avg_performance_pct = sum(performance_pcts) / len(performance_pcts) if performance_pcts else None
            top_performer = max(ae_data, key=lambda x: x.total_revenue).ae_name if ae_data else None
            
            # Create metadata
            processing_time = (time.time() - start_time) * 1000
            metadata = ReportMetadata(
                report_type="ae_performance",
                parameters={'filters': filters.to_dict()},
                row_count=len(ae_data),
                processing_time_ms=processing_time
            )
            
            return AEPerformanceReportData(
                ae_performance=ae_data,
                total_revenue=total_revenue,
                avg_performance_pct=avg_performance_pct,
                top_performer=top_performer,
                filters=filters,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating AE performance report: {e}")
            raise
    
    def get_quarterly_performance_data(
        self, 
        filters: Optional[ReportFilters] = None
    ) -> QuarterlyPerformanceReportData:
        """
        Get quarterly performance report data.
        
        Args:
            filters: Optional filters to apply
            
        Returns:
            QuarterlyPerformanceReportData object
        """
        start_time = time.time()
        
        if filters is None:
            filters = ReportFilters()
        
        logger.info("Generating quarterly performance report")
        
        try:
            db_connection = self.container.get('database_connection')
            
            # Get quarterly data
            quarterly_data = self._get_quarterly_data(db_connection, filters)
            
            # Get AE performance data
            ae_data = self._get_ae_performance_data(db_connection, filters)
            
            # Calculate total revenue
            total_revenue = sum(q.total_revenue for q in quarterly_data)
            
            # Determine current year
            current_year = filters.year or date.today().year
            
            # Create metadata
            processing_time = (time.time() - start_time) * 1000
            metadata = ReportMetadata(
                report_type="quarterly_performance",
                parameters={'filters': filters.to_dict()},
                row_count=len(quarterly_data),
                processing_time_ms=processing_time
            )
            
            return QuarterlyPerformanceReportData(
                current_year=current_year,
                quarterly_data=quarterly_data,
                ae_performance=ae_data,
                total_revenue=total_revenue,
                filters=filters,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating quarterly performance report: {e}")
            raise
    
    def get_sector_performance_data(
        self, 
        filters: Optional[ReportFilters] = None
    ) -> SectorPerformanceReportData:
        """
        Get sector performance report data.
        
        Args:
            filters: Optional filters to apply
            
        Returns:
            SectorPerformanceReportData object
        """
        start_time = time.time()
        
        if filters is None:
            filters = ReportFilters()
        
        logger.info("Generating sector performance report")
        
        try:
            db_connection = self.container.get('database_connection')
            
            # Get sector data
            sectors = self._get_sector_data(db_connection, filters)
            
            # Get top customers by sector
            top_customers = self._get_top_customers_by_sector(db_connection, filters)
            
            # Calculate totals
            total_revenue = sum(sector.total_revenue for sector in sectors)
            sector_count = len(sectors)
            
            # Create metadata
            processing_time = (time.time() - start_time) * 1000
            metadata = ReportMetadata(
                report_type="sector_performance",
                parameters={'filters': filters.to_dict()},
                row_count=len(sectors),
                processing_time_ms=processing_time
            )
            
            return SectorPerformanceReportData(
                sectors=sectors,
                top_customers_by_sector=top_customers,
                total_revenue=total_revenue,
                sector_count=sector_count,
                filters=filters,
                metadata=metadata
            )
            
        except Exception as e:
            logger.error(f"Error generating sector performance report: {e}")
            raise
    
    def _get_customer_monthly_revenue(
        self, 
        db_connection, 
        year: int, 
        filters: ReportFilters
    ) -> List[CustomerMonthlyRow]:
        """Get customer monthly revenue breakdown from database."""
        
        # Build base query with proper date handling using broadcast_month - fetch BOTH gross and net
        base_query = """
            SELECT
                c.customer_id,
                COALESCE(a.agency_name || ' : ', '') || c.normalized_name as customer,
                COALESCE(s.sales_person, 'Unknown') as ae,
                COALESCE(s.revenue_type, 'Regular') as revenue_type,
                sect.sector_name as sector,
                strftime('%m', s.broadcast_month) as month,
                ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) as gross_revenue,
                ROUND(SUM(COALESCE(s.station_net, 0)), 2) as net_revenue
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
            WHERE strftime('%Y', s.broadcast_month) = ?
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
            AND s.gross_rate > 0
        """
        
        params = [str(year)]
        
        # Add filters
        if filters.customer_search:
            base_query += " AND LOWER(c.normalized_name) LIKE LOWER(?)"
            params.append(f"%{filters.customer_search}%")
            
        if filters.ae_filter and filters.ae_filter != 'all':
            base_query += " AND s.sales_person = ?"
            params.append(filters.ae_filter)
            
        if filters.revenue_type and filters.revenue_type != 'all':
            base_query += " AND s.revenue_type = ?"
            params.append(filters.revenue_type)
        
        base_query += " GROUP BY c.customer_id, c.normalized_name, s.sales_person, s.revenue_type, sect.sector_name, month"
        
        # Execute query
        conn = db_connection.connect()
        cursor = conn.execute(base_query, params)
        raw_results = cursor.fetchall()
        conn.close()
        
        # Transform results into CustomerMonthlyRow objects
        customer_data = {}
        
        for row in raw_results:
            key = (row[0], row[1], row[2], row[3])  # customer_id, customer, ae, revenue_type
            
            if key not in customer_data:
                customer_data[key] = {
                    'customer_id': row[0],
                    'customer': row[1],
                    'ae': row[2],
                    'revenue_type': row[3],
                    'sector': row[4],
                    'months_gross': {},
                    'months_net': {}
                }
                # Initialize all months to 0
                for month in range(1, 13):
                    customer_data[key]['months_gross'][month] = Decimal('0')
                    customer_data[key]['months_net'][month] = Decimal('0')
            
            # Set the actual month values for both gross and net
            try:
                month_num = int(row[5])
                if 1 <= month_num <= 12:
                    customer_data[key]['months_gross'][month_num] = Decimal(str(row[6]))  # gross_revenue
                    customer_data[key]['months_net'][month_num] = Decimal(str(row[7]))    # net_revenue
            except (ValueError, TypeError):
                continue  # Skip invalid month values
        
        # Convert to CustomerMonthlyRow objects
        result = []
        for data in customer_data.values():
            row = CustomerMonthlyRow(
                customer_id=data['customer_id'],
                customer=data['customer'],
                ae=data['ae'],
                revenue_type=data['revenue_type'],
                sector=data['sector']
            )
            
            # Set all monthly values
            for month in range(1, 13):
                gross_val = data['months_gross'][month]
                net_val = data['months_net'][month]
                row.set_month_value(month, gross_val, net_val)
            
            result.append(row)
        
        # Sort by total revenue descending (using gross for consistency)
        result.sort(key=lambda x: x.total, reverse=True)
        
        logger.debug(f"Retrieved {len(result)} customer monthly records for year {year}")
        return result
    
    @lru_cache(maxsize=128)
    def _get_available_years(self, db_connection) -> List[int]:
        """Get list of available years from database using broadcast_month."""
        query = """
        SELECT DISTINCT strftime('%Y', broadcast_month) as year
        FROM spots 
        WHERE broadcast_month IS NOT NULL
        AND broadcast_month != ''
        ORDER BY year DESC
        """
        
        conn = db_connection.connect()
        cursor = conn.execute(query)
        years = []
        for row in cursor.fetchall():
            if row[0] is not None and row[0] != '':
                try:
                    years.append(int(row[0]))
                except (ValueError, TypeError):
                    continue  # Skip invalid years
        conn.close()
        
        # Remove duplicates and sort
        return sorted(list(set(years)), reverse=True)
    
    @lru_cache(maxsize=128)
    def _get_ae_list(self, db_connection) -> List[str]:
        """Get list of Account Executives."""
        query = """
        SELECT DISTINCT sales_person
        FROM spots 
        WHERE sales_person IS NOT NULL 
        AND sales_person != ''
        ORDER BY sales_person
        """
        
        conn = db_connection.connect()
        cursor = conn.execute(query)
        ae_list = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return ae_list
    
    @lru_cache(maxsize=128)
    def _get_revenue_types(self, db_connection) -> List[str]:
        """Get list of revenue types."""
        query = """
        SELECT DISTINCT COALESCE(revenue_type, 'Regular') as revenue_type
        FROM spots 
        WHERE revenue_type != 'Trade' OR revenue_type IS NULL
        ORDER BY revenue_type
        """
        
        conn = db_connection.connect()
        cursor = conn.execute(query)
        revenue_types = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return revenue_types
    
    def _get_month_status(self, db_connection, year: int) -> List[MonthStatus]:
        """Get month closure status for the year."""
        query = """
        SELECT broadcast_month, closed_date, closed_by
        FROM month_closures
        WHERE broadcast_month LIKE ?
        """
        
        # Format: 'Jan-25', 'Feb-25', etc.
        year_suffix = str(year)[-2:]  # Get last 2 digits of year
        pattern = f"%-{year_suffix}"
        
        conn = db_connection.connect()
        cursor = conn.execute(query, (pattern,))
        closures = [{'broadcast_month': row[0], 'closed_date': row[1], 'closed_by': row[2]} 
                   for row in cursor.fetchall()]
        conn.close()
        
        return create_month_status_from_closure_data(closures, year)
    
    def _get_ae_performance_data(self, db_connection, filters: ReportFilters) -> List[AEPerformanceData]:
        """Get AE performance data from database using broadcast_month."""
        query = """
        SELECT
            c.customer_id,
            COALESCE(a.agency_name || ' : ', '') || c.normalized_name as customer,
            COALESCE(s.sales_person, 'Unknown') as ae,
            COALESCE(s.revenue_type, 'Regular') as revenue_type,
            sect.sector_name as sector,
            strftime('%m', s.broadcast_month) as month,
            ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) as gross_revenue,
            ROUND(SUM(COALESCE(s.station_net, 0)), 2) as net_revenue
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
        WHERE strftime('%Y', s.broadcast_month) = ?
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL)
        AND s.gross_rate > 0

        """
        
        params = []
        
        # Add date filters using broadcast_month
        if filters.start_date:
            query += " AND broadcast_month >= ?"
            params.append(filters.start_date)
        if filters.end_date:
            query += " AND broadcast_month <= ?"
            params.append(filters.end_date)
        if filters.year:
            query += " AND strftime('%Y', broadcast_month) = ?"
            params.append(str(filters.year))
        
        query += " GROUP BY sales_person ORDER BY total_revenue DESC"
        
        conn = db_connection.connect()
        cursor = conn.execute(query, params)
        
        ae_data = []
        for row in cursor.fetchall():
            ae_data.append(AEPerformanceData(
                ae_name=row[0],
                spot_count=row[1],
                total_revenue=Decimal(str(row[2])),
                avg_rate=Decimal(str(row[3])),
                first_spot_date=datetime.strptime(row[4], '%Y-%m-%d').date() if row[4] else None,
                last_spot_date=datetime.strptime(row[5], '%Y-%m-%d').date() if row[5] else None
            ))
        
        conn.close()
        return ae_data
    
    def _get_quarterly_data(self, db_connection, filters: ReportFilters) -> List[QuarterlyData]:
        """Get quarterly performance data using broadcast_month."""
        query = """
        SELECT 
            CASE 
                WHEN strftime('%m', broadcast_month) IN ('01', '02', '03') THEN 'Q1'
                WHEN strftime('%m', broadcast_month) IN ('04', '05', '06') THEN 'Q2'
                WHEN strftime('%m', broadcast_month) IN ('07', '08', '09') THEN 'Q3'
                WHEN strftime('%m', broadcast_month) IN ('10', '11', '12') THEN 'Q4'
            END as quarter,
            strftime('%Y', broadcast_month) as year,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """
        
        params = []
        if filters.year:
            query += " AND strftime('%Y', broadcast_month) = ?"
            params.append(str(filters.year))
        
        query += " GROUP BY quarter, year ORDER BY year DESC, quarter"
        
        conn = db_connection.connect()
        cursor = conn.execute(query, params)
        
        quarterly_data = []
        for row in cursor.fetchall():
            quarterly_data.append(QuarterlyData(
                quarter=row[0],
                year=int(row[1]),
                spot_count=row[2],
                total_revenue=Decimal(str(row[3])),
                avg_rate=Decimal(str(row[4]))
            ))
        
        conn.close()
        return quarterly_data
    
    def _get_sector_data(self, db_connection, filters: ReportFilters) -> List[SectorData]:
        """Get sector performance data using broadcast_month."""
        query = """
        SELECT 
            s.sector_name,
            s.sector_code,
            COUNT(sp.*) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        """
        
        params = []
        if filters.year:
            query += " AND strftime('%Y', sp.broadcast_month) = ?"
            params.append(str(filters.year))
        
        query += " GROUP BY s.sector_id, s.sector_name, s.sector_code ORDER BY total_revenue DESC"
        
        conn = db_connection.connect()
        cursor = conn.execute(query, params)
        
        sector_data = []
        for row in cursor.fetchall():
            sector_data.append(SectorData(
                sector_name=row[0],
                sector_code=row[1],
                spot_count=row[2],
                total_revenue=Decimal(str(row[3])),
                avg_rate=Decimal(str(row[4]))
            ))
        
        conn.close()
        return sector_data
    
    def _get_top_customers_by_sector(self, db_connection, filters: ReportFilters) -> List[CustomerSectorData]:
        """Get top customers by sector using broadcast_month."""
        query = """
        SELECT 
            s.sector_name,
            c.normalized_name as customer_name,
            COUNT(sp.*) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        """
        
        params = []
        if filters.year:
            query += " AND strftime('%Y', sp.broadcast_month) = ?"
            params.append(str(filters.year))
        
        query += " GROUP BY s.sector_id, c.customer_id ORDER BY s.sector_name, total_revenue DESC"
        
        conn = db_connection.connect()
        cursor = conn.execute(query, params)
        
        customer_data = []
        for row in cursor.fetchall():
            customer_data.append(CustomerSectorData(
                sector_name=row[0],
                customer_name=row[1],
                spot_count=row[2],
                total_revenue=Decimal(str(row[3]))
            ))
        
        conn.close()
        return customer_data
    
    def _get_data_last_updated(self, db_connection) -> datetime:
        """Get when the data was actually last updated."""
        query = """
        SELECT MAX(load_date) as last_update
        FROM spots 
        WHERE load_date IS NOT NULL
        """
        
        conn = db_connection.connect()
        cursor = conn.execute(query)
        result = cursor.fetchone()
        conn.close()
        
        if result[0]:
            return datetime.fromisoformat(result[0])
        else:
            return datetime.now()  # Fallback


# Factory function for service container
def create_report_data_service():
    """Create ReportDataService instance."""
    return ReportDataService()