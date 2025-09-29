# src/repositories/customer_matching_repository.py
"""Repository for customer matching data access - extends existing normalization patterns."""

from __future__ import annotations
from typing import List, Optional, Dict, Any, Tuple
import sqlite3
from contextlib import contextmanager

from ..models.customer_matching import (
    CustomerMatchCandidate, CustomerMatchFilters, MatchSuggestion,
    CustomerMatchStatus, MatchMethod
)


class CustomerMatchingRepository:
    """Repository for customer matching operations using existing DB patterns."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    @contextmanager
    def _get_db_ro(self):
        """Read-only connection - matches existing pattern."""
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = 1;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def _get_db_rw(self):
        """Read-write connection for updates."""
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 10000;")
        try:
            yield conn
        finally:
            conn.close()
    
    def _retry(self, op, retries=3, delay=0.25):
        """Retry logic - matches existing pattern."""
        import time
        for i in range(retries):
            try:
                return op()
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "database is locked" in msg or "busy" in msg:
                    if i < retries - 1:
                        time.sleep(delay * (2 ** i))
                        continue
                raise
    
    def get_unmatched_customers(self, filters: CustomerMatchFilters) -> List[CustomerMatchCandidate]:
        """
        Get customers that appear in spots but don't exist in customers table.
        This is the core query to identify 'Polaris Campaign' type customers.
        """
        base_query = """
        WITH unmatched_spots AS (
            SELECT 
                s.bill_code,
                COUNT(*) as spot_count,
                SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END) as revenue,
                MIN(s.air_date) as first_seen,
                MAX(s.air_date) as last_seen,
                GROUP_CONCAT(DISTINCT s.broadcast_month) as months_active,
                GROUP_CONCAT(DISTINCT s.revenue_type) as revenue_types_raw
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id
            LEFT JOIN entity_aliases ea ON ea.alias_name = s.bill_code 
                AND ea.entity_type = 'customer' 
                AND ea.is_active = 1
            WHERE s.bill_code IS NOT NULL 
                AND s.bill_code != ''
                AND c.customer_id IS NULL  -- Not in customers table
                AND ea.alias_id IS NULL    -- No existing alias
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.bill_code
        )
        SELECT 
            us.bill_code,
            -- Use existing normalization logic for consistency
            vcna.normalized_name,
            us.spot_count,
            us.revenue,
            us.first_seen,
            us.last_seen,
            us.months_active,
            us.revenue_types_raw
        FROM unmatched_spots us
        LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = us.bill_code
        """
        
        where_clauses = []
        params = []
        
        if filters.min_revenue > 0:
            where_clauses.append("us.revenue >= ?")
            params.append(filters.min_revenue)
        
        if filters.min_spots > 0:
            where_clauses.append("us.spot_count >= ?") 
            params.append(filters.min_spots)
        
        if filters.search_text:
            where_clauses.append("(us.bill_code LIKE ? OR vcna.normalized_name LIKE ?)")
            like_term = f"%{filters.search_text}%"
            params.extend([like_term, like_term])
        
        if filters.revenue_types:
            revenue_conditions = []
            for rev_type in filters.revenue_types:
                revenue_conditions.append("us.revenue_types_raw LIKE ?")
                params.append(f"%{rev_type}%")
            where_clauses.append(f"({' OR '.join(revenue_conditions)})")
        
        if not filters.include_low_value:
            where_clauses.append("(us.revenue >= 500 OR us.spot_count >= 3)")
        
        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
        
        base_query += " ORDER BY us.revenue DESC, us.spot_count DESC LIMIT 200"
        
        with self._get_db_ro() as db:
            rows = self._retry(lambda: db.execute(base_query, params).fetchall())
        
        candidates = []
        for row in rows:
            # Parse the data into domain objects
            months = [m.strip() for m in (row['months_active'] or '').split(',') if m.strip()]
            revenue_types = [r.strip() for r in (row['revenue_types_raw'] or '').split(',') if r.strip()]
            
            candidate = CustomerMatchCandidate(
                bill_code_raw=row['bill_code'],
                normalized_name=row['normalized_name'] or row['bill_code'],
                revenue=float(row['revenue'] or 0),
                spot_count=int(row['spot_count'] or 0),
                first_seen=row['first_seen'],
                last_seen=row['last_seen'], 
                months_active=months,
                revenue_types=revenue_types,
                match_status=CustomerMatchStatus.UNMATCHED
            )
            candidates.append(candidate)
        
        return candidates
    
    def find_customer_matches(self, candidate: CustomerMatchCandidate, limit: int = 5) -> List[MatchSuggestion]:
        """
        Find potential customer matches using fuzzy matching.
        This will eventually integrate with the existing blocking matcher logic.
        """
        # Simple fuzzy matching query - can be enhanced with the blocking matcher
        match_query = """
        WITH normalized_search AS (
            SELECT ? as search_term, ? as search_normalized
        ),
        potential_matches AS (
            SELECT 
                c.customer_id,
                c.normalized_name,
                -- Simple similarity scoring (can be enhanced)
                CASE 
                    WHEN LOWER(c.normalized_name) = LOWER(ns.search_normalized) THEN 1.0
                    WHEN LOWER(c.normalized_name) LIKE '%' || LOWER(ns.search_term) || '%' THEN 0.85
                    WHEN LOWER(ns.search_normalized) LIKE '%' || LOWER(c.normalized_name) || '%' THEN 0.80
                    ELSE 0.0
                END as base_score
            FROM customers c, normalized_search ns
            WHERE c.is_active = 1
                AND (c.normalized_name LIKE '%' || ns.search_term || '%' COLLATE NOCASE
                     OR ns.search_normalized LIKE '%' || c.normalized_name || '%' COLLATE NOCASE)
        ),
        scored_matches AS (
            SELECT 
                customer_id,
                normalized_name,
                base_score,
                -- Boost score based on business rules
                CASE 
                    WHEN base_score >= 0.95 THEN base_score * 1.0
                    WHEN base_score >= 0.85 THEN base_score * 0.95
                    ELSE base_score * 0.85
                END as final_score
            FROM potential_matches
            WHERE base_score > 0.5
        )
        SELECT customer_id, normalized_name, final_score
        FROM scored_matches 
        WHERE final_score > 0.6
        ORDER BY final_score DESC, normalized_name
        LIMIT ?
        """
        
        with self._get_db_ro() as db:
            rows = self._retry(lambda: db.execute(
                match_query, 
                [candidate.bill_code_raw, candidate.normalized_name, limit]
            ).fetchall())
        
        suggestions = []
        for row in rows:
            suggestion = MatchSuggestion(
                customer_id=row['customer_id'],
                customer_name=row['normalized_name'],
                confidence_score=row['final_score'],
                match_reasons=['fuzzy_name_match']  # Can be enhanced
            )
            suggestions.append(suggestion)
        
        return suggestions
    
    def get_matching_stats(self) -> Dict[str, int]:
        """Get statistics for the matching dashboard."""
        stats_query = """
        WITH unmatched_customers AS (
            SELECT 
                s.bill_code,
                COUNT(*) as spot_count,
                SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END) as revenue
            FROM spots s
            LEFT JOIN customers c ON s.customer_id = c.customer_id  
            LEFT JOIN entity_aliases ea ON ea.alias_name = s.bill_code 
                AND ea.entity_type = 'customer' AND ea.is_active = 1
            WHERE s.bill_code IS NOT NULL AND s.bill_code != ''
                AND c.customer_id IS NULL AND ea.alias_id IS NULL
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.bill_code
        )
        SELECT 
            COUNT(*) as total_unmatched,
            COUNT(CASE WHEN revenue >= 2000 THEN 1 END) as high_value,
            COUNT(CASE WHEN revenue >= 500 THEN 1 END) as medium_value,
            COUNT(CASE WHEN spot_count >= 10 THEN 1 END) as high_volume,
            SUM(revenue) as total_unmatched_revenue
        FROM unmatched_customers
        """
        
        with self._get_db_ro() as db:
            row = self._retry(lambda: db.execute(stats_query).fetchone())
        
        return {
            'total_unmatched': row['total_unmatched'] or 0,
            'high_value': row['high_value'] or 0, 
            'medium_value': row['medium_value'] or 0,
            'high_volume': row['high_volume'] or 0,
            'total_unmatched_revenue': float(row['total_unmatched_revenue'] or 0)
        }
    
    def create_customer_alias(self, bill_code: str, target_customer_id: int, 
                            created_by: str, notes: str = "") -> int:
        """Create an entity alias to resolve a customer match."""
        insert_alias_sql = """
        INSERT INTO entity_aliases 
        (alias_name, entity_type, target_entity_id, confidence_score, 
         created_by, notes, is_active)
        VALUES (?, 'customer', ?, 100, ?, ?, 1)
        """
        
        with self._get_db_rw() as db:
            cursor = db.cursor()
            cursor.execute(insert_alias_sql, [bill_code, target_customer_id, created_by, notes])
            alias_id = cursor.lastrowid
            db.commit()
            return alias_id
    
    def get_normalization_context(self, bill_code: str) -> Optional[Dict[str, Any]]:
        """Get normalization context for a bill code - bridges to existing system."""
        context_query = """
        SELECT raw_text, normalized_name, customer, agency1, agency2,
               revenue_types_seen, exists_in_customers, has_alias, 
               alias_conflict, customer_id
        FROM v_customer_normalization_audit 
        WHERE raw_text = ?
        """
        
        with self._get_db_ro() as db:
            row = self._retry(lambda: db.execute(context_query, [bill_code]).fetchone())
        
        return dict(row) if row else None