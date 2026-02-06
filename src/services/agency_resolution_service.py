# src/services/agency_resolution_service.py
"""
Agency Resolution Service

Mirrors CustomerResolutionService patterns for agencies.
Handles agency resolution, aliases, merging, and renaming.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import sqlite3
from contextlib import contextmanager


@dataclass
class UnresolvedAgency:
    """A bill_code agency portion that doesn't resolve to an agency."""
    agency_raw: str          # Raw agency name from spots
    normalized_name: str     # Proposed normalized name
    revenue: float
    spot_count: int
    first_seen: str
    last_seen: str


class AgencyResolutionService:
    """
    Resolves unmatched agency names to agency records.

    Uses v_customer_normalization_audit view for parsing agency names,
    then matches against agencies table and entity_aliases.
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

    def get_unresolved(self, min_revenue: float = 0, limit: int = 100) -> List[UnresolvedAgency]:
        """
        Get agency names from spots that don't resolve to an agency.
        Parses agency from bill_code using normalization view.
        """
        sql = """
        SELECT
            vcna.agency1 as agency_raw,
            COALESCE(vcna.agency1, s.ae) as normalized_name,
            COUNT(*) as spot_count,
            SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END) as revenue,
            MIN(s.air_date) as first_seen,
            MAX(s.air_date) as last_seen
        FROM spots s
        LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = s.bill_code
        LEFT JOIN agencies a ON a.agency_name = vcna.agency1 AND a.is_active = 1
        LEFT JOIN entity_aliases ea ON ea.alias_name = vcna.agency1
            AND ea.entity_type = 'agency' AND ea.is_active = 1
        WHERE vcna.agency1 IS NOT NULL
            AND vcna.agency1 != ''
            AND a.agency_id IS NULL
            AND ea.alias_id IS NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        GROUP BY vcna.agency1
        HAVING revenue >= ?
        ORDER BY revenue DESC
        LIMIT ?
        """

        with self._db_ro() as db:
            rows = db.execute(sql, [min_revenue, limit]).fetchall()

        return [
            UnresolvedAgency(
                agency_raw=r["agency_raw"],
                normalized_name=r["normalized_name"] or r["agency_raw"],
                revenue=float(r["revenue"] or 0),
                spot_count=int(r["spot_count"] or 0),
                first_seen=r["first_seen"],
                last_seen=r["last_seen"],
            )
            for r in rows
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Resolution statistics for agencies."""
        with self._db_ro() as db:
            # Total unique agency names from bill_codes (excluding Trade)
            total = db.execute("""
                SELECT COUNT(DISTINCT vcna.agency1) as cnt
                FROM spots s
                LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = s.bill_code
                WHERE vcna.agency1 IS NOT NULL AND vcna.agency1 != ''
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            """).fetchone()["cnt"]

            # Resolved (has agency record or alias)
            resolved = db.execute("""
                SELECT COUNT(DISTINCT vcna.agency1) as cnt
                FROM spots s
                LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = s.bill_code
                LEFT JOIN agencies a ON a.agency_name = vcna.agency1 AND a.is_active = 1
                LEFT JOIN entity_aliases ea ON ea.alias_name = vcna.agency1
                    AND ea.entity_type = 'agency' AND ea.is_active = 1
                WHERE vcna.agency1 IS NOT NULL
                    AND (a.agency_id IS NOT NULL OR ea.alias_id IS NOT NULL)
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            """).fetchone()["cnt"]

            # Unresolved with revenue
            unresolved = db.execute("""
                SELECT
                    COUNT(DISTINCT vcna.agency1) as cnt,
                    COALESCE(SUM(s.gross_rate), 0) as revenue
                FROM spots s
                LEFT JOIN v_customer_normalization_audit vcna ON vcna.raw_text = s.bill_code
                LEFT JOIN agencies a ON a.agency_name = vcna.agency1 AND a.is_active = 1
                LEFT JOIN entity_aliases ea ON ea.alias_name = vcna.agency1
                    AND ea.entity_type = 'agency' AND ea.is_active = 1
                WHERE vcna.agency1 IS NOT NULL AND vcna.agency1 != ''
                    AND a.agency_id IS NULL AND ea.alias_id IS NULL
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            """).fetchone()

        rate = (resolved / total * 100) if total > 0 else 0

        return {
            "total_agencies": total,
            "resolved": resolved,
            "unresolved": unresolved["cnt"],
            "unresolved_revenue": float(unresolved["revenue"]),
            "resolution_rate": round(rate, 1),
        }

    def check_agency_exists(self, agency_name: str) -> Optional[int]:
        """Check if an agency with this name exists."""
        with self._db_ro() as db:
            row = db.execute("""
                SELECT agency_id FROM agencies
                WHERE agency_name = ? AND is_active = 1
            """, [agency_name]).fetchone()
        return row["agency_id"] if row else None

    def create_agency_and_alias(
        self,
        agency_raw: str,
        agency_name: str,
        created_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Create agency (if needed) and alias for an agency name.

        If an agency with agency_name already exists, just creates the alias.
        """
        with self._db_rw() as db:
            # Check if agency exists
            existing = db.execute("""
                SELECT agency_id FROM agencies
                WHERE agency_name = ?
            """, [agency_name]).fetchone()

            if existing:
                agency_id = existing["agency_id"]
                agency_created = False
            else:
                # Create agency
                db.execute("""
                    INSERT INTO agencies (agency_name, is_active, notes)
                    VALUES (?, 1, ?)
                """, [agency_name, f"Created by {created_by}"])
                agency_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                agency_created = True

            # Check if alias already exists
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'agency'
            """, [agency_raw]).fetchone()

            alias_created = False
            if not existing_alias and agency_raw != agency_name:
                # Create alias
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                        created_by, notes, is_active)
                    VALUES (?, 'agency', ?, 100, ?, 'Created via resolution UI', 1)
                """, [agency_raw, agency_id, created_by])
                alias_created = True

            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'CREATE', ?, ?, ?)
            """, [
                created_by,
                f"agency:{agency_id}",
                agency_name,
                f"raw={agency_raw}|agency_created={agency_created}|alias_created={alias_created}"
            ])

            db.commit()

        return {
            "success": True,
            "agency_id": agency_id,
            "agency_name": agency_name,
            "agency_raw": agency_raw,
            "agency_created": agency_created,
            "alias_created": alias_created,
        }

    def link_to_existing(
        self,
        agency_raw: str,
        agency_id: int,
        created_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Link an agency name to an existing agency via alias.
        Use when the raw name doesn't match but they're the same agency.
        """
        with self._db_rw() as db:
            # Verify agency exists
            agency = db.execute("""
                SELECT agency_id, agency_name FROM agencies
                WHERE agency_id = ? AND is_active = 1
            """, [agency_id]).fetchone()

            if not agency:
                return {"success": False, "error": f"Agency {agency_id} not found"}

            # Check if alias exists
            existing = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'agency'
            """, [agency_raw]).fetchone()

            if existing:
                return {"success": False, "error": "Alias already exists for this agency name"}

            # Create alias
            db.execute("""
                INSERT INTO entity_aliases
                (alias_name, entity_type, target_entity_id, confidence_score,
                created_by, notes, is_active)
                VALUES (?, 'agency', ?, 100, ?, 'Manually linked via resolution UI', 1)
            """, [agency_raw, agency_id, created_by])

            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'LINK', ?, ?, ?)
            """, [
                created_by,
                f"agency:{agency_id}",
                agency["agency_name"],
                f"alias={agency_raw}"
            ])

            db.commit()

        return {
            "success": True,
            "agency_raw": agency_raw,
            "agency_id": agency_id,
            "agency_name": agency["agency_name"],
        }

    def search_agencies(self, query: str, limit: int = 15) -> List[Dict]:
        """Search existing agencies for manual linking."""
        with self._db_ro() as db:
            rows = db.execute("""
                SELECT agency_id, agency_name
                FROM agencies
                WHERE agency_name LIKE ? AND is_active = 1
                ORDER BY agency_name
                LIMIT ?
            """, [f"%{query}%", limit]).fetchall()
        return [{"agency_id": r["agency_id"], "agency_name": r["agency_name"]} for r in rows]

    def get_agencies_with_aliases(
        self,
        search: str = "",
        min_aliases: int = 0,
        limit: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Get agencies with their alias counts and revenue summary.
        Ordered by alias count desc (most complex first).
        """
        sql = """
        WITH alias_counts AS (
            SELECT
                target_entity_id as agency_id,
                COUNT(*) as alias_count
            FROM entity_aliases
            WHERE entity_type = 'agency' AND is_active = 1
            GROUP BY target_entity_id
        ),
        agency_revenue AS (
            SELECT
                c.agency_id,
                COUNT(DISTINCT s.spot_id) as spot_count,
                SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END) as total_revenue,
                MIN(s.air_date) as first_seen,
                MAX(s.air_date) as last_seen
            FROM customers c
            JOIN spots s ON s.customer_id = c.customer_id
            WHERE c.agency_id IS NOT NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY c.agency_id
        )
        SELECT
            a.agency_id,
            a.agency_name,
            a.created_date,
            COALESCE(ac.alias_count, 0) as alias_count,
            COALESCE(ar.spot_count, 0) as spot_count,
            COALESCE(ar.total_revenue, 0) as total_revenue,
            ar.first_seen,
            ar.last_seen
        FROM agencies a
        LEFT JOIN alias_counts ac ON a.agency_id = ac.agency_id
        LEFT JOIN agency_revenue ar ON a.agency_id = ar.agency_id
        WHERE a.is_active = 1
        AND (? = '' OR a.agency_name LIKE '%' || ? || '%')
        AND COALESCE(ac.alias_count, 0) >= ?
        ORDER BY COALESCE(ac.alias_count, 0) DESC, ar.total_revenue DESC
        LIMIT ?
        """

        with self._db_ro() as db:
            rows = db.execute(sql, [search, search, min_aliases, limit]).fetchall()

        return [
            {
                "agency_id": r["agency_id"],
                "agency_name": r["agency_name"],
                "created_date": r["created_date"],
                "alias_count": r["alias_count"],
                "spot_count": r["spot_count"],
                "total_revenue": float(r["total_revenue"] or 0),
                "first_seen": r["first_seen"],
                "last_seen": r["last_seen"],
            }
            for r in rows
        ]

    def get_agency_aliases(self, agency_id: int) -> Dict[str, Any]:
        """
        Get an agency with all their aliases and per-alias info.
        """
        with self._db_ro() as db:
            # Get agency
            agency = db.execute("""
                SELECT agency_id, agency_name, created_date, notes,
                       address, city, state, zip
                FROM agencies WHERE agency_id = ? AND is_active = 1
            """, [agency_id]).fetchone()

            if not agency:
                return None

            # Get aliases
            aliases = db.execute("""
                SELECT
                    ea.alias_id,
                    ea.alias_name,
                    ea.confidence_score,
                    ea.created_by,
                    ea.created_date,
                    ea.notes
                FROM entity_aliases ea
                WHERE ea.target_entity_id = ?
                AND ea.entity_type = 'agency'
                AND ea.is_active = 1
                ORDER BY ea.created_date DESC
            """, [agency_id]).fetchall()

            # Get customer count and revenue via this agency
            stats = db.execute("""
                SELECT
                    COUNT(DISTINCT c.customer_id) as customer_count,
                    COUNT(DISTINCT s.spot_id) as spot_count,
                    COALESCE(SUM(CASE WHEN s.gross_rate > 0 THEN s.gross_rate ELSE 0 END), 0) as revenue
                FROM customers c
                LEFT JOIN spots s ON s.customer_id = c.customer_id
                    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                WHERE c.agency_id = ?
            """, [agency_id]).fetchone()

        return {
            "agency_id": agency["agency_id"],
            "agency_name": agency["agency_name"],
            "created_date": agency["created_date"],
            "notes": agency["notes"],
            "address": agency["address"],
            "city": agency["city"],
            "state": agency["state"],
            "zip": agency["zip"],
            "customer_count": stats["customer_count"] or 0,
            "spot_count": stats["spot_count"] or 0,
            "total_revenue": float(stats["revenue"] or 0),
            "aliases": [
                {
                    "alias_id": a["alias_id"],
                    "alias_name": a["alias_name"],
                    "confidence_score": a["confidence_score"],
                    "created_by": a["created_by"],
                    "created_date": a["created_date"],
                    "notes": a["notes"],
                }
                for a in aliases
            ],
        }

    def delete_alias(self, alias_id: int, deleted_by: str = "web_user") -> Dict[str, Any]:
        """Soft-delete an alias (set is_active = 0)."""
        with self._db_rw() as db:
            # Get alias info first
            alias = db.execute("""
                SELECT alias_id, alias_name, target_entity_id
                FROM entity_aliases
                WHERE alias_id = ? AND entity_type = 'agency' AND is_active = 1
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

    def merge_agencies(
        self,
        source_id: int,
        target_id: int,
        merged_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Merge source agency INTO target agency.

        - Moves all aliases from source -> target
        - Updates all customers from source -> target
        - Creates alias from source's agency_name -> target
        - Deactivates source agency

        Returns stats on what was moved.
        """
        if source_id == target_id:
            return {"success": False, "error": "Cannot merge agency into itself"}

        with self._db_rw() as db:
            # Verify both exist
            source = db.execute("""
                SELECT agency_id, agency_name FROM agencies
                WHERE agency_id = ? AND is_active = 1
            """, [source_id]).fetchone()

            target = db.execute("""
                SELECT agency_id, agency_name FROM agencies
                WHERE agency_id = ? AND is_active = 1
            """, [target_id]).fetchone()

            if not source:
                return {"success": False, "error": f"Source agency {source_id} not found"}
            if not target:
                return {"success": False, "error": f"Target agency {target_id} not found"}

            # 1. Move aliases from source to target
            aliases_moved = db.execute("""
                UPDATE entity_aliases
                SET target_entity_id = ?,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Merged from agency ' || ? || ' by ' || ?
                WHERE target_entity_id = ?
                AND entity_type = 'agency'
                AND is_active = 1
            """, [target_id, source_id, merged_by, source_id]).rowcount

            # 2. Create alias from source's agency_name -> target
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'agency'
            """, [source["agency_name"]]).fetchone()

            alias_created = False
            if not existing_alias and source["agency_name"] != target["agency_name"]:
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                    created_by, notes, is_active)
                    VALUES (?, 'agency', ?, 100, ?,
                            'Created via merge from agency ' || ?, 1)
                """, [source["agency_name"], target_id, merged_by, source_id])
                alias_created = True

            # 3. Update all customers from source -> target
            customers_moved = db.execute("""
                UPDATE customers
                SET agency_id = ?
                WHERE agency_id = ?
            """, [target_id, source_id]).rowcount

            # 4. Deactivate source agency
            db.execute("""
                UPDATE agencies
                SET is_active = 0,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Merged into agency ' || ? || ' by ' || ? || ' at ' || datetime('now')
                WHERE agency_id = ?
            """, [target_id, merged_by, source_id])

            # 5. Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'MERGE', ?, ?, ?)
            """, [
                merged_by,
                f"agency:{source_id}",
                f"agency:{target_id}",
                f"source_name={source['agency_name']}|target_name={target['agency_name']}|aliases={aliases_moved}|customers={customers_moved}"
            ])

            db.commit()

        return {
            "success": True,
            "source_id": source_id,
            "source_name": source["agency_name"],
            "target_id": target_id,
            "target_name": target["agency_name"],
            "aliases_moved": aliases_moved,
            "alias_created": alias_created,
            "customers_moved": customers_moved,
        }

    def rename_agency(
        self,
        agency_id: int,
        new_name: str,
        renamed_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Rename an agency's name.

        Creates an alias from old_name -> agency_id to preserve
        any references to the old name.
        """
        new_name = new_name.strip()
        if not new_name:
            return {"success": False, "error": "New name cannot be empty"}

        with self._db_rw() as db:
            # Get current agency
            agency = db.execute("""
                SELECT agency_id, agency_name
                FROM agencies WHERE agency_id = ? AND is_active = 1
            """, [agency_id]).fetchone()

            if not agency:
                return {"success": False, "error": f"Agency {agency_id} not found"}

            old_name = agency["agency_name"]

            if old_name == new_name:
                return {"success": False, "error": "New name is same as current name"}

            # Check if new_name already exists
            conflict = db.execute("""
                SELECT agency_id FROM agencies
                WHERE agency_name = ? AND is_active = 1 AND agency_id != ?
            """, [new_name, agency_id]).fetchone()

            if conflict:
                return {"success": False, "error": f"Agency '{new_name}' already exists (ID: {conflict['agency_id']})"}

            # Update the agency name
            db.execute("""
                UPDATE agencies
                SET agency_name = ?, updated_date = CURRENT_TIMESTAMP
                WHERE agency_id = ?
            """, [new_name, agency_id])

            # Create alias from old_name -> agency_id
            existing_alias = db.execute("""
                SELECT alias_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'agency'
            """, [old_name]).fetchone()

            alias_created = False
            if not existing_alias:
                db.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                    created_by, notes, is_active)
                    VALUES (?, 'agency', ?, 100, ?, 'Created during rename from old agency_name', 1)
                """, [old_name, agency_id, renamed_by])
                alias_created = True

            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'RENAME', ?, ?, ?)
            """, [renamed_by, f"agency:{agency_id}", new_name, f"old_name={old_name}"])

            db.commit()

        return {
            "success": True,
            "agency_id": agency_id,
            "old_name": old_name,
            "new_name": new_name,
            "alias_created": alias_created,
        }

    def update_agency_address(
        self,
        agency_id: int,
        address: str = None,
        city: str = None,
        state: str = None,
        zip_code: str = None,
        updated_by: str = "web_user"
    ) -> Dict[str, Any]:
        """Update agency address fields."""
        with self._db_rw() as db:
            # Verify agency exists
            agency = db.execute("""
                SELECT agency_id FROM agencies
                WHERE agency_id = ? AND is_active = 1
            """, [agency_id]).fetchone()

            if not agency:
                return {"success": False, "error": f"Agency {agency_id} not found"}

            # Update address fields
            db.execute("""
                UPDATE agencies
                SET address = COALESCE(?, address),
                    city = COALESCE(?, city),
                    state = COALESCE(?, state),
                    zip = COALESCE(?, zip),
                    updated_date = CURRENT_TIMESTAMP
                WHERE agency_id = ?
            """, [address, city, state, zip_code, agency_id])

            # Log to audit
            db.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'UPDATE_ADDRESS', ?, 'address', ?)
            """, [
                updated_by,
                f"agency:{agency_id}",
                f"address={address}|city={city}|state={state}|zip={zip_code}"
            ])

            db.commit()

        return {
            "success": True,
            "agency_id": agency_id,
        }
