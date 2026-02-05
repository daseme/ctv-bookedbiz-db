# src/services/customer_resolution_service.py
"""
Simple Customer Resolution Service

Your normalization view already does the hard work.
This just creates customer records for normalized names that don't exist yet.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import sqlite3
from contextlib import contextmanager


@dataclass
class UnresolvedCustomer:
    """A bill_code that doesn't resolve to a customer."""
    bill_code: str           # Raw from spots
    normalized_name: str     # From v_customer_normalization_audit
    agency: Optional[str]    # Parsed agency (if any)
    customer: Optional[str]  # Parsed customer portion
    revenue: float
    spot_count: int
    first_seen: str
    last_seen: str


class CustomerResolutionService:
    """
    Resolves unmatched bill_codes to customers.
    
    Uses your existing v_customer_normalization_audit view —
    no duplicate normalization logic.
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    @contextmanager
    def _db_ro(self):
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def _db_rw(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def get_unresolved(self, min_revenue: float = 0, limit: int = 100) -> List[UnresolvedCustomer]:
        """
        Get bill_codes that don't resolve to a customer.
        Uses v_customer_normalization_audit for normalized names.
        """
        sql = """
        SELECT 
            s.bill_code,
            vcna.normalized_name,
            vcna.agency1,
            vcna.customer,
            COUNT(*) as spot_count,
            SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END) as revenue,
            MIN(s.air_date) as first_seen,
            MAX(s.air_date) as last_seen
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN entity_aliases ea ON ea.alias_name = s.bill_code 
            AND ea.entity_type = 'customer' AND ea.is_active = 1
        LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = s.bill_code
        WHERE s.bill_code IS NOT NULL 
            AND s.bill_code != ''
            AND c.customer_id IS NULL 
            AND ea.alias_id IS NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        GROUP BY s.bill_code
        HAVING revenue >= ?
        ORDER BY revenue DESC
        LIMIT ?
        """
        
        with self._db_ro() as db:
            rows = db.execute(sql, [min_revenue, limit]).fetchall()
        
        return [
            UnresolvedCustomer(
                bill_code=r["bill_code"],
                normalized_name=r["normalized_name"] or r["bill_code"],
                agency=r["agency1"],
                customer=r["customer"],
                revenue=float(r["revenue"] or 0),
                spot_count=int(r["spot_count"] or 0),
                first_seen=r["first_seen"],
                last_seen=r["last_seen"],
            )
            for r in rows
        ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Resolution statistics."""
        with self._db_ro() as db:
            # Total unique bill codes (excluding Trade)
            total = db.execute("""
                SELECT COUNT(DISTINCT bill_code) as cnt
                FROM spots 
                WHERE bill_code IS NOT NULL AND bill_code != ''
                    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """).fetchone()["cnt"]
            
            # Resolved (has customer_id or alias)
            resolved = db.execute("""
                SELECT COUNT(DISTINCT s.bill_code) as cnt
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN entity_aliases ea ON ea.alias_name = s.bill_code 
                    AND ea.entity_type = 'customer' AND ea.is_active = 1
                WHERE s.bill_code IS NOT NULL
                    AND (c.customer_id IS NOT NULL OR ea.alias_id IS NOT NULL)
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            """).fetchone()["cnt"]
            
            # Unresolved revenue
            unresolved = db.execute("""
                SELECT 
                    COUNT(DISTINCT s.bill_code) as cnt,
                    COALESCE(SUM(s.gross_rate), 0) as revenue
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN entity_aliases ea ON ea.alias_name = s.bill_code 
                    AND ea.entity_type = 'customer' AND ea.is_active = 1
                WHERE s.bill_code IS NOT NULL AND s.bill_code != ''
                    AND c.customer_id IS NULL AND ea.alias_id IS NULL
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            """).fetchone()
        
        rate = (resolved / total * 100) if total > 0 else 0
        
        return {
            "total_bill_codes": total,
            "resolved": resolved,
            "unresolved": unresolved["cnt"],
            "unresolved_revenue": float(unresolved["revenue"]),
            "resolution_rate": round(rate, 1),
        }
    
    def check_customer_exists(self, normalized_name: str) -> Optional[int]:
        """Check if a customer with this normalized_name exists."""
        with self._db_ro() as db:
            row = db.execute("""
                SELECT customer_id FROM customers 
                WHERE normalized_name = ? AND is_active = 1
            """, [normalized_name]).fetchone()
        return row["customer_id"] if row else None
    
    def create_customer_and_alias(
        self, 
        bill_code: str, 
        normalized_name: str,
        created_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Create customer (if needed) and alias for a bill_code.
        
        If a customer with normalized_name already exists, just creates the alias.
        """
        with self._db_rw() as db:
            # Check if customer exists
            existing = db.execute("""
                SELECT customer_id FROM customers 
                WHERE normalized_name = ?
            """, [normalized_name]).fetchone()
            
            if existing:
                customer_id = existing["customer_id"]
                customer_created = False
            else:
                # Create customer
                db.execute("""
                    INSERT INTO customers (normalized_name, is_active, notes)
                    VALUES (?, 1, ?)
                """, [normalized_name, f"Created by {created_by}"])
                customer_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                customer_created = True
            
            # Check if alias already exists
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases 
                WHERE alias_name = ? AND entity_type = 'customer'
            """, [bill_code]).fetchone()
            
            if existing_alias:
                alias_created = False

            else:
                # Create alias (only if bill_code != normalized_name)
                if bill_code != normalized_name:
                    db.execute("""
                        INSERT INTO entity_aliases
                        (alias_name, entity_type, target_entity_id, confidence_score,
                            created_by, notes, is_active)
                        VALUES (?, 'customer', ?, 100, ?, 'Created via resolution UI', 1)
                    """, [bill_code, customer_id, created_by])
                    alias_created = True
                else:
                    alias_created = False

                # Backfill spots.customer_id for this bill_code
                spots_updated = db.execute("""
                    UPDATE spots 
                    SET customer_id = ?
                    WHERE bill_code = ? AND customer_id IS NULL
                    """, [customer_id, bill_code]).rowcount

                # Also backfill spots matching the normalized_name directly
                if bill_code != normalized_name:
                    spots_updated += db.execute("""
                        UPDATE spots 
                        SET customer_id = ?
                        WHERE bill_code = ? AND customer_id IS NULL
                    """, [customer_id, normalized_name]).rowcount

                db.commit()

            return {
            "success": True,
            "customer_id": customer_id,
            "normalized_name": normalized_name,
            "bill_code": bill_code,
            "customer_created": customer_created,
            "alias_created": alias_created,
            "spots_updated": spots_updated,
            }
    
    def link_to_existing(
            self,
            bill_code: str,
            customer_id: int,
            created_by: str = "web_user"
        ) -> Dict[str, Any]:
            """
            Link a bill_code to an existing customer via alias.
            Use when the normalized_name doesn't match but they're the same customer.
            """
            with self._db_rw() as db:
                # Verify customer exists
                cust = db.execute("""
                    SELECT customer_id, normalized_name FROM customers
                    WHERE customer_id = ? AND is_active = 1
                """, [customer_id]).fetchone()

                if not cust:
                    return {"success": False, "error": f"Customer {customer_id} not found"}

                # Check if alias exists
                existing = db.execute("""
                    SELECT alias_id FROM entity_aliases
                    WHERE alias_name = ? AND entity_type = 'customer'
                """, [bill_code]).fetchone()

                if existing:
                    return {"success": False, "error": "Alias already exists for this bill_code"}

                # Create alias
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                    created_by, notes, is_active)
                    VALUES (?, 'customer', ?, 100, ?, 'Manually linked via resolution UI', 1)
                """, [bill_code, customer_id, created_by])

                # Backfill spots.customer_id for this bill_code
                spots_updated = db.execute("""
                    UPDATE spots
                    SET customer_id = ?
                    WHERE bill_code = ? AND customer_id IS NULL
                """, [customer_id, bill_code]).rowcount

                db.commit()

            return {
                "success": True,
                "bill_code": bill_code,
                "customer_id": customer_id,
                "normalized_name": cust["normalized_name"],
                "spots_updated": spots_updated,
            }
    
    def search_customers(self, query: str, limit: int = 15) -> List[Dict]:
        """Search existing customers for manual linking."""
        with self._db_ro() as db:
            rows = db.execute("""
                SELECT customer_id, normalized_name
                FROM customers
                WHERE normalized_name LIKE ? AND is_active = 1
                ORDER BY normalized_name
                LIMIT ?
            """, [f"%{query}%", limit]).fetchall()
        return [{"customer_id": r["customer_id"], "normalized_name": r["normalized_name"]} for r in rows]