"""
Pricing trends and time-series analysis service.
Provides rate trending, margin analysis, and pricing consistency metrics.
Follows same architecture pattern as pricing_analysis_service.
"""
from typing import List, Dict, Optional
import logging
from src.models.pricing_intelligence import (
    TrendPoint,
    MarginTrendPoint,
    RateVolatility,
    ConcentrationMetrics,
    TopCustomerContribution
)

logger = logging.getLogger(__name__)


class PricingTrendsService:
    """
    Analyzes pricing trends over time across multiple dimensions.
    
    Answers questions like:
    - Are our rates improving over time?
    - Is margin being protected or eroded?
    - Is pricing consistent or volatile?
    """
    
    def __init__(self, db_connection):
        """Initialize with database connection (injected by container)."""
        self.db = db_connection
        logger.info("PricingTrendsService initialized")
    
    def get_rate_trends(
        self,
        dimension: str,
        months_back: int = 12,
        min_spot_threshold: int = 5
    ) -> Dict[str, List[TrendPoint]]:
        """
        Get rate trends grouped by dimension value.
        
        Args:
            dimension: 'sector', 'language', 'sales_person', or 'market'
            months_back: How many months of history to include
            min_spot_threshold: Minimum spots required to include dimension value
            
        Returns:
            Dict mapping dimension_value -> List[TrendPoint] sorted by period
        """
        # Map dimension to SQL components
        dimension_map = {
            'sector': ('sec.sector_name', """
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            """),
            'language': ('s.language_code', ''),
            'sales_person': ('s.sales_person', ''),
            'market': ('m.market_code', 'LEFT JOIN markets m ON s.market_id = m.market_id')
        }
        
        if dimension not in dimension_map:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        dim_column, joins = dimension_map[dimension]
        
        # Get cutoff month
        cutoff_month = self._calculate_cutoff_month(months_back)
        
        query = f"""
        SELECT 
            s.broadcast_month AS period,
            COALESCE({dim_column}, 'Unknown') AS dimension_value,
            AVG(s.gross_rate) AS average_rate,
            COUNT(*) AS spot_count,
            SUM(s.gross_rate) AS total_revenue
        FROM spots s
        {joins}
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.broadcast_month >= ?
        GROUP BY s.broadcast_month, {dim_column}
        ORDER BY s.broadcast_month, {dim_column}
        """
        
        with self.db.connection() as conn:
            cursor = conn.execute(query, [cutoff_month])
            rows = cursor.fetchall()
        
        # Group by dimension value
        grouped: Dict[str, List[Dict]] = {}
        for row in rows:
            period, dim_val, avg_rate, spot_count, total_revenue = row
            if dim_val not in grouped:
                grouped[dim_val] = []
            grouped[dim_val].append({
                'period': period,
                'dimension_value': dim_val,
                'average_rate': avg_rate,
                'spot_count': spot_count,
                'total_revenue': total_revenue
            })
        
        # Convert to domain models and filter by threshold
        result: Dict[str, List[TrendPoint]] = {}
        for dim_val, data_points in grouped.items():
            total_spots = sum(dp['spot_count'] for dp in data_points)
            
            if total_spots < min_spot_threshold:
                continue
            
            trend_points = [
                TrendPoint(
                    period=dp['period'],
                    dimension_value=dim_val,
                    average_rate=float(dp['average_rate']),
                    spot_count=dp['spot_count'],
                    total_revenue=float(dp['total_revenue'])
                )
                for dp in data_points
            ]
            
            result[dim_val] = sorted(trend_points, key=lambda tp: tp.period)
        
        return result
    
    def get_margin_trends(
        self,
        groupby: str,
        months_back: int = 12,
        min_spot_threshold: int = 5
    ) -> Dict[str, List[MarginTrendPoint]]:
        """
        Get gross margin percentage trends over time.
        
        Shows whether AEs/sectors/markets are protecting margin or
        discounting aggressively (margin erosion).
        """
        dimension_map = {
            'sector': ('sec.sector_name', """
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            """),
            'language': ('s.language_code', ''),
            'sales_person': ('s.sales_person', ''),
            'market': ('m.market_code', 'LEFT JOIN markets m ON s.market_id = m.market_id')
        }
        
        if groupby not in dimension_map:
            raise ValueError(f"Invalid groupby: {groupby}")
        
        dim_column, joins = dimension_map[groupby]
        cutoff_month = self._calculate_cutoff_month(months_back)
        
        query = f"""
        SELECT 
            s.broadcast_month AS period,
            COALESCE({dim_column}, 'Unknown') AS dimension_value,
            AVG(s.gross_rate) AS gross_rate_avg,
            AVG(s.station_net) AS station_net_avg,
            AVG(s.station_net) * 100.0 / NULLIF(AVG(s.gross_rate), 0) AS margin_percentage,
            COUNT(*) AS spot_count
        FROM spots s
        {joins}
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
          AND s.gross_rate > 0
          AND s.station_net IS NOT NULL
          AND s.broadcast_month >= ?
        GROUP BY s.broadcast_month, {dim_column}
        ORDER BY s.broadcast_month, {dim_column}
        """
        
        with self.db.connection() as conn:
            cursor = conn.execute(query, [cutoff_month])
            rows = cursor.fetchall()
        
        # Group by dimension value
        grouped: Dict[str, List[Dict]] = {}
        for row in rows:
            period, dim_val, gross_avg, net_avg, margin_pct, spot_count = row
            if dim_val not in grouped:
                grouped[dim_val] = []
            grouped[dim_val].append({
                'period': period,
                'dimension_value': dim_val,
                'gross_rate_avg': gross_avg,
                'station_net_avg': net_avg,
                'margin_percentage': margin_pct,
                'spot_count': spot_count
            })
        
        # Convert to domain models
        result: Dict[str, List[MarginTrendPoint]] = {}
        for dim_val, data_points in grouped.items():
            total_spots = sum(dp['spot_count'] for dp in data_points)
            
            if total_spots < min_spot_threshold:
                continue
            
            margin_points = [
                MarginTrendPoint(
                    period=dp['period'],
                    dimension_value=dim_val,
                    gross_rate_avg=float(dp['gross_rate_avg']),
                    station_net_avg=float(dp['station_net_avg']),
                    margin_percentage=float(dp['margin_percentage']),
                    spot_count=dp['spot_count']
                )
                for dp in data_points
            ]
            
            result[dim_val] = sorted(margin_points, key=lambda mp: mp.period)
        
        return result
    
    def get_pricing_consistency(
        self,
        dimension: str,
        timeframe: str
    ) -> List[RateVolatility]:
        """
        Measure pricing consistency using coefficient of variation.
        
        Lower CV = more consistent pricing strategy.
        Higher CV = rates vary widely (potential pricing discipline issue).
        """
        dimension_map = {
            'sector': ('sec.sector_name', """
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            """),
            'language': ('s.language_code', ''),
            'sales_person': ('s.sales_person', ''),
            'market': ('m.market_code', 'LEFT JOIN markets m ON s.market_id = m.market_id')
        }
        
        if dimension not in dimension_map:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        dim_column, joins = dimension_map[dimension]
        
        # SQLite manual stddev calculation
        query = f"""
        WITH stats AS (
            SELECT 
                {dim_column} AS dimension_value,
                AVG(s.gross_rate) AS avg_rate,
                COUNT(*) AS cnt,
                SUM(s.gross_rate * s.gross_rate) AS sum_sq,
                SUM(s.gross_rate) AS sum_x,
                MIN(s.gross_rate) AS min_rate,
                MAX(s.gross_rate) AS max_rate
            FROM spots s
            {joins}
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
              AND s.gross_rate > 0
              AND s.broadcast_month LIKE ?
            GROUP BY {dim_column}
            HAVING COUNT(*) >= 10
        )
        SELECT 
            COALESCE(dimension_value, 'Unknown') AS dimension_value,
            avg_rate AS average_rate,
            SQRT((sum_sq - (sum_x * sum_x / cnt)) / cnt) AS std_deviation,
            SQRT((sum_sq - (sum_x * sum_x / cnt)) / cnt) / NULLIF(avg_rate, 0) AS coefficient_variation,
            min_rate,
            max_rate,
            cnt AS spot_count
        FROM stats
        WHERE avg_rate > 0
        ORDER BY coefficient_variation
        """
        
        with self.db.connection() as conn:
            cursor = conn.execute(query, [f"%-{timeframe[-2:]}"])
            rows = cursor.fetchall()
        
        return [
            RateVolatility(
                dimension_value=row[0],
                average_rate=float(row[1]),
                std_deviation=float(row[2]),
                coefficient_variation=float(row[3]),
                min_rate=float(row[4]),
                max_rate=float(row[5]),
                spot_count=row[6]
            )
            for row in rows
        ]
    
    def get_dimension_comparison_summary(
        self,
        dimension: str,
        period_current: str,
        period_previous: str
    ) -> List[Dict]:
        """Compare current period vs previous period for each dimension value."""
        # Convert full years to suffixes (2025 -> 25, 2024 -> 24)
        current_suffix = period_current[-2:]
        previous_suffix = period_previous[-2:]
        
        # Get trends for last 24 months
        current_trends = self.get_rate_trends(dimension, months_back=24)
        
        comparisons = []
        
        for dim_val, trend_points in current_trends.items():
            # Filter to relevant periods by checking if period ends with year suffix
            current_points = [tp for tp in trend_points if tp.period.endswith(current_suffix)]
            previous_points = [tp for tp in trend_points if tp.period.endswith(previous_suffix)]
            
            if not current_points or not previous_points:
                continue
            
            # Calculate averages
            current_avg = sum(tp.average_rate for tp in current_points) / len(current_points)
            previous_avg = sum(tp.average_rate for tp in previous_points) / len(previous_points)
            
            current_revenue = sum(tp.total_revenue for tp in current_points)
            previous_revenue = sum(tp.total_revenue for tp in previous_points)
            
            # Calculate change
            rate_change_pct = ((current_avg - previous_avg) / previous_avg * 100) if previous_avg > 0 else 0
            revenue_change_pct = ((current_revenue - previous_revenue) / previous_revenue * 100) if previous_revenue > 0 else 0
            
            comparisons.append({
                'dimension_value': dim_val,
                'current_avg_rate': current_avg,
                'previous_avg_rate': previous_avg,
                'rate_change_pct': rate_change_pct,
                'current_revenue': current_revenue,
                'previous_revenue': previous_revenue,
                'revenue_change_pct': revenue_change_pct,
                'is_improving': rate_change_pct > 0
            })
        
        return sorted(comparisons, key=lambda c: c['revenue_change_pct'], reverse=True)

    def get_concentration_metrics(
            self,
            period: str
        ) -> Optional[ConcentrationMetrics]:
            """
            Calculate revenue concentration metrics (HHI, top N percentages).
            
            Args:
                period: Year to analyze (e.g., '2024', '2025')
                
            Returns:
                ConcentrationMetrics with HHI and top customer analysis
            """
            # First get total revenue and customer count
            base_query = """
            SELECT 
                COUNT(DISTINCT s.customer_id) AS total_customers,
                SUM(s.gross_rate) AS total_revenue
            FROM spots s
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.customer_id IS NOT NULL
            AND s.broadcast_month LIKE ?
            """
            
            with self.db.connection() as conn:
                cursor = conn.execute(base_query, [f"%-{period[-2:]}"])
                base_metrics = cursor.fetchone()
                
                if not base_metrics or not base_metrics[1] or base_metrics[1] == 0:
                    logger.warning(f"No revenue data for period {period}")
                    return None
                
                total_customers, total_revenue = base_metrics
                
                # Get per-customer revenue
                customer_query = """
                SELECT 
                    s.customer_id,
                    c.normalized_name,
                    SUM(s.gross_rate) AS customer_revenue,
                    SUM(s.gross_rate) * 100.0 / ? AS percentage
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND s.customer_id IS NOT NULL
                AND s.broadcast_month LIKE ?
                GROUP BY s.customer_id, c.normalized_name
                ORDER BY customer_revenue DESC
                """
                
                cursor = conn.execute(customer_query, [total_revenue, f"%-{period[-2:]}"])
                customers = cursor.fetchall()
            
            # Calculate HHI (sum of squared market shares)
            # Handle None values in percentage field
            hhi = 0.0
            for row in customers:
                if row[3] is not None and row[3] > 0:
                    market_share = float(row[3]) / 100.0
                    hhi += market_share ** 2
                        
            # Calculate top N metrics
            top_10_revenue = sum(row[2] for row in customers[:10]) if len(customers) >= 10 else sum(row[2] for row in customers)
            top_20_revenue = sum(row[2] for row in customers[:20]) if len(customers) >= 20 else sum(row[2] for row in customers)
            top_50_revenue = sum(row[2] for row in customers[:50]) if len(customers) >= 50 else sum(row[2] for row in customers)

            
            return ConcentrationMetrics(
                period=period,
                total_revenue=float(total_revenue),
                total_customers=total_customers,
                herfindahl_index=hhi,
                top_10_revenue=float(top_10_revenue),
                top_10_percentage=(top_10_revenue / total_revenue * 100) if total_revenue > 0 else 0,
                top_20_revenue=float(top_20_revenue),
                top_20_percentage=(top_20_revenue / total_revenue * 100) if total_revenue > 0 else 0,
                top_50_revenue=float(top_50_revenue),
                top_50_percentage=(top_50_revenue / total_revenue * 100) if total_revenue > 0 else 0
            )
    
    def get_top_customers(
        self,
        period: str,
        limit: int = 50
    ) -> List[TopCustomerContribution]:
        """
        Get top customers by revenue contribution.
        
        Args:
            period: Year to analyze
            limit: Number of top customers to return
            
        Returns:
            List of TopCustomerContribution objects
        """
        # Get total revenue first
        total_query = """
        SELECT SUM(s.gross_rate) AS total_revenue
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.customer_id IS NOT NULL
            AND s.broadcast_month LIKE ?
        """
        
        with self.db.connection() as conn:
            cursor = conn.execute(total_query, [f"%-{period[-2:]}"])
            total_revenue = cursor.fetchone()[0]
            
            if not total_revenue:
                return []
            
            # Get customer details
            customer_query = """
            SELECT 
                s.customer_id,
                c.normalized_name,
                SUM(s.gross_rate) AS customer_revenue,
                SUM(s.gross_rate) * 100.0 / ? AS percentage
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                AND s.customer_id IS NOT NULL
                AND s.broadcast_month LIKE ?
            GROUP BY s.customer_id, c.normalized_name
            ORDER BY customer_revenue DESC
            LIMIT ?
            """
            
            cursor = conn.execute(customer_query, [total_revenue, f"%-{period[-2:]}", limit])
            rows = cursor.fetchall()
        
        return [
            TopCustomerContribution(
                customer_id=row[0],
                customer_name=row[1] or f"Customer {row[0]}",
                revenue=float(row[2]),
                percentage_of_total=float(row[3]),
                rank=idx + 1
            )
            for idx, row in enumerate(rows)
        ]
        
    def get_concentration_trend(
        self,
        years: List[str]
    ) -> List[ConcentrationMetrics]:
        """
        Get concentration metrics across multiple years to show trends.
        
        Args:
            years: List of years to analyze (e.g., ['2023', '2024', '2025'])
            
        Returns:
            List of ConcentrationMetrics for each year
        """
        results = []
        for year in years:
            metrics = self.get_concentration_metrics(year)
            if metrics:
                results.append(metrics)
        return results
        
    def _calculate_cutoff_month(self, months_back: int) -> str:
        """Calculate cutoff month for trending queries."""
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        
        now = datetime.now()
        cutoff = now - relativedelta(months=months_back)
        
        month_abbr = cutoff.strftime('%b')
        year_suffix = cutoff.strftime('%y')
        
        return f"{month_abbr}-{year_suffix}"

