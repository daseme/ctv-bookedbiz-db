#!/usr/bin/env python3
"""
Spot Repository - Data access layer for spots table.

All SQL queries related to spots are encapsulated here.
Returns domain objects, not raw tuples.
"""

import sqlite3
from typing import List, Optional, Dict, Any
from dataclasses import dataclass


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