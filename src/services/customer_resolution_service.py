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

    def delete_alias(self, alias_id: int, deleted_by: str = "web_user") -> Dict[str, Any]:
        """Soft-delete an alias (set is_active = 0)."""
        with self._db_rw() as db:
            # Get alias info first
            alias = db.execute("""
                SELECT alias_id, alias_name, target_entity_id
                FROM entity_aliases
                WHERE alias_id = ? AND entity_type = 'customer' AND is_active = 1
            """, [alias_id]).fetchone()
            
            if not alias:
                return {"success": False, "error": "Alias not found"}
            
            # Soft delete
            db.execute("""
                UPDATE entity_aliases
                SET is_active = 0, updated_date = CURRENT_TIMESTAMP, 
                    notes = COALESCE(notes, '') || ' | Deleted by ' || ? || ' at ' || datetime('now')
                WHERE alias_id = ?
            """, [deleted_by, alias_id])
            
            # Log to history
            db.execute("""
                INSERT INTO entity_alias_history
                (alias_id, action, old_values, new_values, changed_by)
                VALUES (?, 'DEACTIVATE', ?, NULL, ?)
            """, [alias_id, alias["alias_name"], deleted_by])
            
            db.commit()
        
        return {
            "success": True,
            "alias_id": alias_id,
            "alias_name": alias["alias_name"],
        }
    


    def merge_customers(
        self,
        source_id: int,
        target_id: int,
        merged_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Merge source customer INTO target customer.
        
        - Moves all aliases from source → target
        - Updates all spots from source → target
        - Creates alias from source's normalized_name → target
        - Deactivates source customer
        
        Returns stats on what was moved.
        """
        if source_id == target_id:
            return {"success": False, "error": "Cannot merge customer into itself"}
        
        with self._db_rw() as db:
            # Verify both exist
            source = db.execute("""
                SELECT customer_id, normalized_name FROM customers
                WHERE customer_id = ? AND is_active = 1
            """, [source_id]).fetchone()
            
            target = db.execute("""
                SELECT customer_id, normalized_name FROM customers
                WHERE customer_id = ? AND is_active = 1
            """, [target_id]).fetchone()
            
            if not source:
                return {"success": False, "error": f"Source customer {source_id} not found"}
            if not target:
                return {"success": False, "error": f"Target customer {target_id} not found"}
            
            # 1. Move aliases from source to target
            aliases_moved = db.execute("""
                UPDATE entity_aliases
                SET target_entity_id = ?,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Merged from customer ' || ? || ' by ' || ?
                WHERE target_entity_id = ?
                AND entity_type = 'customer'
                AND is_active = 1
            """, [target_id, source_id, merged_by, source_id]).rowcount
            
            # 2. Create alias from source's normalized_name → target
            #    (so future imports with that exact bill_code resolve correctly)
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'customer'
            """, [source["normalized_name"]]).fetchone()
            
            alias_created = False
            if not existing_alias and source["normalized_name"] != target["normalized_name"]:
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                    created_by, notes, is_active)
                    VALUES (?, 'customer', ?, 100, ?,
                            'Created via merge from customer ' || ?, 1)
                """, [source["normalized_name"], target_id, merged_by, source_id])
                alias_created = True
            
            # 3. Update all spots from source → target
            spots_moved = db.execute("""
                UPDATE spots
                SET customer_id = ?
                WHERE customer_id = ?
            """, [target_id, source_id]).rowcount
            
            # 4. Deactivate source customer
            db.execute("""
                UPDATE customers
                SET is_active = 0,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Merged into customer ' || ? || ' by ' || ? || ' at ' || datetime('now')
                WHERE customer_id = ?
            """, [target_id, merged_by, source_id])
            
            # 5. Log to audit via canon_audit (exists and has no constraints)
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'MERGE', ?, ?, ?)
            """, [
                merged_by,
                f"customer:{source_id}",
                f"customer:{target_id}",
                f"source_name={source['normalized_name']}|target_name={target['normalized_name']}|aliases={aliases_moved}|spots={spots_moved}"
            ])
            
            db.commit()
        
        return {
            "success": True,
            "source_id": source_id,
            "source_name": source["normalized_name"],
            "target_id": target_id,
            "target_name": target["normalized_name"],
            "aliases_moved": aliases_moved,
            "alias_created": alias_created,
            "spots_moved": spots_moved,
        }


    def get_customers_with_aliases(
        self,
        search: str = "",
        min_aliases: int = 0,
        limit: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Get customers with their alias counts and revenue summary.
        Ordered by alias count desc (most complex first).
        Excludes agency clients (any spot via agency, or name-prefix match).
        """
        sql = """
        WITH agency_clients AS (
            SELECT customer_id FROM customers
            WHERE is_active = 1 AND normalized_name LIKE '%:%'
            UNION
            SELECT customer_id FROM spots
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
            HAVING COUNT(*) = COUNT(agency_id)
        ),
        alias_counts AS (
            SELECT
                target_entity_id as customer_id,
                COUNT(*) as alias_count
            FROM entity_aliases
            WHERE entity_type = 'customer' AND is_active = 1
            GROUP BY target_entity_id
        ),
        customer_revenue AS (
            SELECT
                customer_id,
                COUNT(*) as spot_count,
                SUM(CASE WHEN gross_rate > 0 THEN gross_rate ELSE 0 END) as total_revenue,
                MIN(air_date) as first_seen,
                MAX(air_date) as last_seen
            FROM spots
            WHERE customer_id IS NOT NULL
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY customer_id
        )
        SELECT
            c.customer_id,
            c.normalized_name,
            c.created_date,
            COALESCE(ac.alias_count, 0) as alias_count,
            COALESCE(cr.spot_count, 0) as spot_count,
            COALESCE(cr.total_revenue, 0) as total_revenue,
            cr.first_seen,
            cr.last_seen
        FROM customers c
        LEFT JOIN alias_counts ac ON c.customer_id = ac.customer_id
        LEFT JOIN customer_revenue cr ON c.customer_id = cr.customer_id
        WHERE c.is_active = 1
        AND c.customer_id NOT IN (SELECT customer_id FROM agency_clients)
        AND (? = '' OR c.normalized_name LIKE '%' || ? || '%')
        AND COALESCE(ac.alias_count, 0) >= ?
        ORDER BY COALESCE(ac.alias_count, 0) DESC, cr.total_revenue DESC
        LIMIT ?
        """

        with self._db_ro() as db:
            rows = db.execute(sql, [search, search, min_aliases, limit]).fetchall()

        return [
            {
                "customer_id": r["customer_id"],
                "normalized_name": r["normalized_name"],
                "created_date": r["created_date"],
                "alias_count": r["alias_count"],
                "spot_count": r["spot_count"],
                "total_revenue": float(r["total_revenue"] or 0),
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
            }
            for r in rows
        ]


    def get_customer_aliases(self, customer_id: int) -> Dict[str, Any]:
        """
        Get a customer with all their aliases and per-alias revenue.
        """
        with self._db_ro() as db:
            # Get customer with address fields
            cust = db.execute("""
                SELECT customer_id, normalized_name, created_date, notes,
                       address, city, state, zip
                FROM customers WHERE customer_id = ? AND is_active = 1
            """, [customer_id]).fetchone()

            if not cust:
                return None

            # Get aliases with revenue attribution
            aliases = db.execute("""
                SELECT
                    ea.alias_id,
                    ea.alias_name,
                    ea.confidence_score,
                    ea.created_by,
                    ea.created_date,
                    ea.notes,
                    COUNT(s.spot_id) as spot_count,
                    COALESCE(SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END), 0) as revenue
                FROM entity_aliases ea
                LEFT JOIN spots s ON s.bill_code = ea.alias_name
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                WHERE ea.target_entity_id = ?
                AND ea.entity_type = 'customer'
                AND ea.is_active = 1
                GROUP BY ea.alias_id
                ORDER BY revenue DESC
            """, [customer_id]).fetchall()

            # Get revenue from normalized_name directly (spots where bill_code = normalized_name)
            direct = db.execute("""
                SELECT
                    COUNT(*) as spot_count,
                    COALESCE(SUM(CASE WHEN gross_rate > 0 THEN gross_rate ELSE 0 END), 0) as revenue
                FROM spots
                WHERE bill_code = ?
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """, [cust["normalized_name"]]).fetchone()

        return {
            "customer_id": cust["customer_id"],
            "normalized_name": cust["normalized_name"],
            "created_date": cust["created_date"],
            "notes": cust["notes"],
            "address": cust["address"],
            "city": cust["city"],
            "state": cust["state"],
            "zip": cust["zip"],
            "direct_revenue": float(direct["revenue"] or 0),
            "direct_spot_count": direct["spot_count"] or 0,
            "aliases": [
                {
                    "alias_id": a["alias_id"],
                    "alias_name": a["alias_name"],
                    "confidence_score": a["confidence_score"],
                    "created_by": a["created_by"],
                    "created_date": a["created_date"],
                    "notes": a["notes"],
                    "spot_count": a["spot_count"] or 0,
                    "revenue": float(a["revenue"] or 0),
                }
                for a in aliases
            ],
        }


    def rename_customer(
        self,
        customer_id: int,
        new_name: str,
        renamed_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Rename a customer's normalized_name.
        
        Creates an alias from old_name → customer_id to preserve
        any spots where bill_code matched the old name directly.
        """
        new_name = new_name.strip()
        if not new_name:
            return {"success": False, "error": "New name cannot be empty"}
        
        with self._db_rw() as db:
            # Get current customer
            cust = db.execute("""
                SELECT customer_id, normalized_name
                FROM customers WHERE customer_id = ? AND is_active = 1
            """, [customer_id]).fetchone()
            
            if not cust:
                return {"success": False, "error": f"Customer {customer_id} not found"}
            
            old_name = cust["normalized_name"]
            
            if old_name == new_name:
                return {"success": False, "error": "New name is same as current name"}
            
            # Check if new_name already exists
            conflict = db.execute("""
                SELECT customer_id FROM customers
                WHERE normalized_name = ? AND is_active = 1 AND customer_id != ?
            """, [new_name, customer_id]).fetchone()
            
            if conflict:
                return {"success": False, "error": f"Customer '{new_name}' already exists (ID: {conflict['customer_id']})"}
            
            # Update the customer name
            db.execute("""
                UPDATE customers
                SET normalized_name = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, [new_name, customer_id])
            
            # Create alias from old_name → customer_id (if not already an alias)
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'customer'
            """, [old_name]).fetchone()
            
            alias_created = False
            if not existing_alias:
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                    created_by, notes, is_active)
                    VALUES (?, 'customer', ?, 100, ?, 'Created during rename from old normalized_name', 1)
                """, [old_name, customer_id, renamed_by])
                alias_created = True
            
            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'RENAME', ?, ?, ?)
            """, [renamed_by, f"customer:{customer_id}", new_name, f"old_name={old_name}"])
            
            db.commit()
        
        return {
            "success": True,
            "customer_id": customer_id,
            "old_name": old_name,
            "new_name": new_name,
            "alias_created": alias_created,
        }

    def update_customer_address(
        self,
        customer_id: int,
        address: str = None,
        city: str = None,
        state: str = None,
        zip_code: str = None,
        updated_by: str = "web_user"
    ) -> Dict[str, Any]:
        """Update customer address fields."""
        with self._db_rw() as db:
            # Verify customer exists
            cust = db.execute("""
                SELECT customer_id FROM customers
                WHERE customer_id = ? AND is_active = 1
            """, [customer_id]).fetchone()

            if not cust:
                return {"success": False, "error": f"Customer {customer_id} not found"}

            # Update address fields
            db.execute("""
                UPDATE customers
                SET address = COALESCE(?, address),
                    city = COALESCE(?, city),
                    state = COALESCE(?, state),
                    zip = COALESCE(?, zip),
                    updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, [address, city, state, zip_code, customer_id])

            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'UPDATE_ADDRESS', ?, 'address', ?)
            """, [
                updated_by,
                f"customer:{customer_id}",
                f"address={address}|city={city}|state={state}|zip={zip_code}"
            ])

            db.commit()

        return {
            "success": True,
            "customer_id": customer_id,
        }

    def get_customer_with_address(self, customer_id: int) -> Optional[Dict[str, Any]]:
        """Get customer including address fields."""
        with self._db_ro() as db:
            cust = db.execute("""
                SELECT customer_id, normalized_name, created_date, notes,
                       address, city, state, zip
                FROM customers WHERE customer_id = ? AND is_active = 1
            """, [customer_id]).fetchone()

            if not cust:
                return None

        return {
            "customer_id": cust["customer_id"],
            "normalized_name": cust["normalized_name"],
            "created_date": cust["created_date"],
            "notes": cust["notes"],
            "address": cust["address"],
            "city": cust["city"],
            "state": cust["state"],
            "zip": cust["zip"],
        }