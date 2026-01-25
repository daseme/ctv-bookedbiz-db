#!/usr/bin/env python3
"""
Spot Repository - Data access layer for spots table.

All SQL queries related to spots are encapsulated here.
Returns domain objects, not raw tuples.
"""

import sqlite3
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
from decimal import Decimal


# ============================================================================
# Value Objects
# ============================================================================

@dataclass(frozen=True)
class MonthSummary:
    """Immutable summary of a broadcast month's data."""
    month: str
    count: int
    gross_revenue: float
    
    @property
    def has_data(self) -> bool:
        return self.count > 0


@dataclass(frozen=True)
class CustomerAlignmentMismatch:
    """Represents a customer_id mismatch between spots and normalization."""
    bill_code: str
    spots_customer_id: Optional[int]
    audit_customer_id: int
    spot_count: int
    revenue_affected: float


@dataclass(frozen=True)
class CustomerAlignmentValidation:
    """Result of customer alignment validation."""
    mismatches: List[CustomerAlignmentMismatch]
    total_spots_affected: int
    total_revenue_affected: float
    
    @property
    def is_valid(self) -> bool:
        return len(self.mismatches) == 0
    
    @property
    def mismatch_count(self) -> int:
        return len(self.mismatches)


# ============================================================================
# Repository
# ============================================================================

class SpotRepository:
    """
    Data access layer for spots table.
    
    All SQL queries are parameterized and return domain objects.
    Connections are passed in - this class doesn't manage connections.
    """
    
    def get_month_summary(self, month: str, conn: sqlite3.Connection) -> MonthSummary:
        """
        Get count and revenue summary for a broadcast month.
        
        Args:
            month: Broadcast month in display format (e.g., 'Dec-25')
            conn: Active database connection
            
        Returns:
            MonthSummary with count and gross revenue
        """
        cursor = conn.execute("""
            SELECT 
                COUNT(*), 
                COALESCE(SUM(gross_rate), 0)
            FROM spots 
            WHERE broadcast_month = ?
        """, (month,))
        
        row = cursor.fetchone()
        return MonthSummary(
            month=month,
            count=row[0] if row else 0,
            gross_revenue=row[1] if row else 0.0
        )
    
    def get_month_summaries(
        self, 
        months: List[str], 
        conn: sqlite3.Connection
    ) -> Dict[str, MonthSummary]:
        """
        Get summaries for multiple months in a single query.
        
        Args:
            months: List of broadcast months in display format
            conn: Active database connection
            
        Returns:
            Dict mapping month -> MonthSummary
        """
        if not months:
            return {}
        
        placeholders = ','.join(['?'] * len(months))
        cursor = conn.execute(f"""
            SELECT 
                broadcast_month,
                COUNT(*), 
                COALESCE(SUM(gross_rate), 0)
            FROM spots 
            WHERE broadcast_month IN ({placeholders})
            GROUP BY broadcast_month
        """, months)
        
        results = {}
        for row in cursor.fetchall():
            results[row[0]] = MonthSummary(
                month=row[0],
                count=row[1],
                gross_revenue=row[2]
            )
        
        # Include empty summaries for months not in results
        for month in months:
            if month not in results:
                results[month] = MonthSummary(month=month, count=0, gross_revenue=0.0)
        
        return results
    
    def get_record_count_by_month(
        self, 
        month: str, 
        conn: sqlite3.Connection
    ) -> int:
        """Get just the record count for a month."""
        cursor = conn.execute(
            "SELECT COUNT(*) FROM spots WHERE broadcast_month = ?",
            (month,)
        )
        return cursor.fetchone()[0]
    
    def delete_by_months(
        self, 
        months: List[str], 
        conn: sqlite3.Connection
    ) -> int:
        """
        Delete all spots for the specified broadcast months.
        
        Args:
            months: List of broadcast months to delete
            conn: Active database connection (caller manages transaction)
            
        Returns:
            Total number of records deleted
        """
        if not months:
            return 0
        
        total_deleted = 0
        for month in months:
            cursor = conn.execute(
                "DELETE FROM spots WHERE broadcast_month = ?",
                (month,)
            )
            total_deleted += cursor.rowcount
        
        return total_deleted
    
    def insert_spot(
        self, 
        spot_data: Dict[str, Any], 
        conn: sqlite3.Connection
    ) -> Optional[int]:
        """
        Insert a single spot record.
        
        Args:
            spot_data: Dictionary of field names to values
            conn: Active database connection
            
        Returns:
            The inserted row ID
        """
        fields = list(spot_data.keys())
        placeholders = ', '.join(['?'] * len(fields))
        field_names = ', '.join(fields)
        values = [spot_data[field] for field in fields]
        
        cursor = conn.execute(
            f"INSERT INTO spots ({field_names}) VALUES ({placeholders})",
            values
        )
        return cursor.lastrowid
    
    def validate_customer_alignment(
        self, 
        batch_id: str, 
        conn: sqlite3.Connection
    ) -> CustomerAlignmentValidation:
        """
        Validate that imported spots align with customer normalization system.
        
        Args:
            batch_id: The import batch ID to validate
            conn: Active database connection
            
        Returns:
            CustomerAlignmentValidation with any mismatches found
        """
        cursor = conn.execute("""
            SELECT 
                s.bill_code,
                s.customer_id as spots_customer_id,
                audit.customer_id as audit_customer_id,
                COUNT(*) as spot_count,
                SUM(COALESCE(s.gross_rate, 0)) as revenue_affected
            FROM spots s
            LEFT JOIN v_customer_normalization_audit audit 
                ON audit.raw_text = s.bill_code
            WHERE s.import_batch_id = ?
                AND (s.customer_id != audit.customer_id OR s.customer_id IS NULL)
                AND audit.customer_id IS NOT NULL
            GROUP BY s.bill_code, s.customer_id, audit.customer_id
            ORDER BY revenue_affected DESC
        """, (batch_id,))
        
        mismatches = []
        total_spots = 0
        total_revenue = 0.0
        
        for row in cursor.fetchall():
            mismatch = CustomerAlignmentMismatch(
                bill_code=row[0],
                spots_customer_id=row[1],
                audit_customer_id=row[2],
                spot_count=row[3],
                revenue_affected=row[4]
            )
            mismatches.append(mismatch)
            total_spots += row[3]
            total_revenue += row[4]
        
        return CustomerAlignmentValidation(
            mismatches=mismatches,
            total_spots_affected=total_spots,
            total_revenue_affected=total_revenue
        )

    # ============================================================================
    # Pricing
    # ============================================================================

    def get_rate_trend_data(
        self,
        dimension: str,
        months_back: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get time-series rate data grouped by dimension.
        
        Args:
            dimension: 'sector', 'language', 'sales_person', 'market'
            months_back: How many months to look back
            
        Returns:
            List of dicts with period, dimension_value, avg_rate, spot_count, total_revenue
        """
        dimension_map = {
            'sector': 's.customer_id',  # Will join through customer
            'language': 's.language_code',
            'sales_person': 's.sales_person',
            'market': 'm.market_code'
        }
        
        if dimension not in dimension_map:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        # Build the query based on dimension
        if dimension == 'sector':
            query = """
            SELECT 
                s.broadcast_month AS period,
                COALESCE(sec.sector_name, 'Unknown') AS dimension_value,
                AVG(s.gross_rate) AS average_rate,
                COUNT(*) AS spot_count,
                SUM(s.gross_rate) AS total_revenue
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.gross_rate > 0
            AND s.broadcast_month >= ?
            GROUP BY s.broadcast_month, sec.sector_name
            ORDER BY s.broadcast_month, sec.sector_name
            """
        elif dimension == 'market':
            query = """
            SELECT 
                s.broadcast_month AS period,
                COALESCE(m.market_code, 'Unknown') AS dimension_value,
                AVG(s.gross_rate) AS average_rate,
                COUNT(*) AS spot_count,
                SUM(s.gross_rate) AS total_revenue
            FROM spots s
            LEFT JOIN markets m ON s.market_id = m.market_id
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.gross_rate > 0
            AND s.broadcast_month >= ?
            GROUP BY s.broadcast_month, m.market_code
            ORDER BY s.broadcast_month, m.market_code
            """
        else:
            # For language and sales_person, simpler query
            dim_column = dimension_map[dimension]
            query = f"""
            SELECT 
                s.broadcast_month AS period,
                COALESCE({dim_column}, 'Unknown') AS dimension_value,
                AVG(s.gross_rate) AS average_rate,
                COUNT(*) AS spot_count,
                SUM(s.gross_rate) AS total_revenue
            FROM spots s
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.gross_rate > 0
            AND s.broadcast_month >= ?
            GROUP BY s.broadcast_month, {dim_column}
            ORDER BY s.broadcast_month, {dim_column}
            """
        
        # Calculate cutoff month
        cutoff_month = self._calculate_cutoff_month(months_back)
        
        cursor = self.db.cursor()
        cursor.execute(query, [cutoff_month])
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


    def get_margin_trend_data(
        self,
        groupby: str,
        months_back: int = 12
    ) -> List[Dict[str, Any]]:
        """
        Get gross margin percentage trending over time.
        
        Returns period, dimension_value, gross_avg, net_avg, margin_pct, spot_count
        """
        dimension_map = {
            'sector': 'sec.sector_name',
            'language': 's.language_code',
            'sales_person': 's.sales_person',
            'market': 'm.market_code'
        }
        
        if groupby not in dimension_map:
            raise ValueError(f"Invalid groupby: {groupby}")
        
        # Build JOIN clauses based on dimension
        joins = ""
        if groupby == 'sector':
            joins = """
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            """
        elif groupby == 'market':
            joins = "LEFT JOIN markets m ON s.market_id = m.market_id"
        
        dim_column = dimension_map[groupby]
        
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
        
        cutoff_month = self._calculate_cutoff_month(months_back)
        
        cursor = self.db.cursor()
        cursor.execute(query, [cutoff_month])
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


    def get_rate_volatility_data(
        self,
        dimension: str,
        timeframe: str
    ) -> List[Dict[str, Any]]:
        """
        Calculate pricing consistency (coefficient of variation) by dimension.
        Lower CV = more consistent pricing.
        """
        dimension_map = {
            'sector': 'sec.sector_name',
            'language': 's.language_code',
            'sales_person': 's.sales_person',
            'market': 'm.market_code'
        }
        
        if dimension not in dimension_map:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        joins = ""
        if dimension == 'sector':
            joins = """
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
            """
        elif dimension == 'market':
            joins = "LEFT JOIN markets m ON s.market_id = m.market_id"
        
        dim_column = dimension_map[dimension]
        
        # SQLite doesn't have STDDEV, so we calculate it manually
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
        
        cursor = self.db.cursor()
        cursor.execute(query, [f"%-{timeframe[-2:]}"])
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


    def get_concentration_metrics(
        self,
        period: str
    ) -> Dict[str, Any]:
        """
        Calculate revenue concentration metrics (HHI, top N percentages).
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
        
        cursor = self.db.cursor()
        cursor.execute(base_query, [f"%-{period[-2:]}"])
        base_metrics = cursor.fetchone()
        
        if not base_metrics or not base_metrics[1]:
            return None
        
        total_customers, total_revenue = base_metrics
        
        # Get per-customer revenue and calculate HHI
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
        
        cursor.execute(customer_query, [total_revenue, f"%-{period[-2:]}"])
        customers = cursor.fetchall()
        
        # Calculate HHI (sum of squared market shares)
        hhi = sum((row[3] / 100.0) ** 2 for row in customers)
        
        # Calculate top N metrics
        top_10_revenue = sum(row[2] for row in customers[:10]) if len(customers) >= 10 else sum(row[2] for row in customers)
        top_20_revenue = sum(row[2] for row in customers[:20]) if len(customers) >= 20 else sum(row[2] for row in customers)
        top_50_revenue = sum(row[2] for row in customers[:50]) if len(customers) >= 50 else sum(row[2] for row in customers)
        
        return {
            'period': period,
            'total_revenue': total_revenue,
            'total_customers': total_customers,
            'herfindahl_index': hhi,
            'top_10_revenue': top_10_revenue,
            'top_10_percentage': (top_10_revenue / total_revenue * 100) if total_revenue > 0 else 0,
            'top_20_revenue': top_20_revenue,
            'top_20_percentage': (top_20_revenue / total_revenue * 100) if total_revenue > 0 else 0,
            'top_50_revenue': top_50_revenue,
            'top_50_percentage': (top_50_revenue / total_revenue * 100) if total_revenue > 0 else 0,
            'top_customers': [
                {
                    'customer_id': row[0],
                    'customer_name': row[1],
                    'revenue': row[2],
                    'percentage': row[3],
                    'rank': idx + 1
                }
                for idx, row in enumerate(customers[:50])
            ]
        }


    def get_customer_first_seen(self) -> Dict[int, str]:
        """
        Get first broadcast_month for each customer.
        Returns dict of customer_id -> first_month
        """
        query = """
        SELECT 
            customer_id,
            MIN(broadcast_month) AS first_month
        FROM spots
        WHERE customer_id IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY customer_id
        """
        
        cursor = self.db.cursor()
        cursor.execute(query)
        
        return {row[0]: row[1] for row in cursor.fetchall()}


    def get_cohort_performance(
        self,
        cohort_month: str,
        customer_ids: List[int]
    ) -> List[Dict[str, Any]]:
        """
        Get performance metrics for a cohort across all subsequent months.
        """
        if not customer_ids:
            return []
        
        placeholders = ','.join('?' * len(customer_ids))
        
        query = f"""
        SELECT 
            s.broadcast_month AS period,
            COUNT(DISTINCT s.customer_id) AS active_customers,
            AVG(s.gross_rate) AS avg_revenue_per_customer,
            SUM(s.gross_rate) AS total_revenue
        FROM spots s
        WHERE s.customer_id IN ({placeholders})
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.broadcast_month >= ?
        GROUP BY s.broadcast_month
        ORDER BY s.broadcast_month
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, customer_ids + [cohort_month])
        
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


    def get_new_vs_returning_revenue(
        self,
        period: str,
        first_seen_map: Dict[int, str]
    ) -> Dict[str, Any]:
        """
        Calculate new vs returning customer revenue for a period.
        """
        query = """
        SELECT 
            s.customer_id,
            c.normalized_name,
            SUM(s.gross_rate) AS revenue
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.customer_id IS NOT NULL
        AND s.broadcast_month = ?
        GROUP BY s.customer_id, c.normalized_name
        """
        
        cursor = self.db.cursor()
        cursor.execute(query, [period])
        
        new_revenue = Decimal('0')
        new_count = 0
        returning_revenue = Decimal('0')
        returning_count = 0
        
        for row in cursor.fetchall():
            customer_id, customer_name, revenue = row
            first_month = first_seen_map.get(customer_id)
            
            if first_month == period:
                new_revenue += Decimal(str(revenue))
                new_count += 1
            else:
                returning_revenue += Decimal(str(revenue))
                returning_count += 1
        
        total_revenue = new_revenue + returning_revenue
        
        return {
            'period': period,
            'new_customer_count': new_count,
            'new_customer_revenue': new_revenue,
            'returning_customer_count': returning_count,
            'returning_customer_revenue': returning_revenue,
            'total_revenue': total_revenue
        }


    def _calculate_cutoff_month(self, months_back: int) -> str:
        """Helper to calculate cutoff month for trending queries"""
        from datetime import datetime, timedelta
        from dateutil.relativedelta import relativedelta
        
        # Get current month
        now = datetime.now()
        cutoff = now - relativedelta(months=months_back)
        
        # Format as 'Mmm-YY'
        month_abbr = cutoff.strftime('%b')
        year_suffix = cutoff.strftime('%y')
        
        return f"{month_abbr}-{year_suffix}"

    def correct_customer_mismatches(
        self, 
        batch_id: str, 
        conn: sqlite3.Connection
    ) -> int:
        """
        Auto-correct customer_id mismatches for a batch.
        
        Args:
            batch_id: The import batch ID to correct
            conn: Active database connection
            
        Returns:
            Number of records corrected
        """
        cursor = conn.execute("""
            UPDATE spots 
            SET customer_id = (
                SELECT audit.customer_id 
                FROM v_customer_normalization_audit audit
                WHERE audit.raw_text = spots.bill_code
            )
            WHERE spots.import_batch_id = ?
                AND EXISTS (
                    SELECT 1 FROM v_customer_normalization_audit audit
                    WHERE audit.raw_text = spots.bill_code
                        AND (spots.customer_id != audit.customer_id 
                             OR spots.customer_id IS NULL)
                        AND audit.customer_id IS NOT NULL
                )
        """, (batch_id,))
        
        return cursor.rowcount