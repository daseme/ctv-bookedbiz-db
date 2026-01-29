# -*- coding: utf-8 -*-
"""
Canon Tool API: upsert canonical mappings (agency/customer) and optional entity_aliases.
Adds: conflict checks, Unicode normalization, indexes, audit log, suggestions.
SQLite 3.40 compatible.
"""

from __future__ import annotations
import sqlite3
import unicodedata
from dataclasses import dataclass
from typing import Optional
from flask import Blueprint, current_app, request, jsonify

canon_bp = Blueprint("canon", __name__, url_prefix="/api/canon")

# ---------- helpers -----------------------------------------------------------


@dataclass(frozen=True)
class UpsertResult:
    alias: str
    canonical: str
    affected_preview: int
    created_entity_alias: bool


def _open_rw(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=8000;")
    return conn


def _nfkc(s: Optional[str]) -> str:
    return unicodedata.normalize("NFKC", (s or "")).strip()


def _ensure_tables(conn: sqlite3.Connection) -> None:
    # canon maps
    conn.execute("""
        CREATE TABLE IF NOT EXISTS agency_canonical_map(
          alias_name TEXT PRIMARY KEY,
          canonical_name TEXT NOT NULL,
          updated_date TEXT
        );
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customer_canonical_map(
          alias_name TEXT PRIMARY KEY,
          canonical_name TEXT NOT NULL,
          updated_date TEXT
        );
    """)
    # add updated_date if missing (PRAGMA doesn't support bound params)
    for tbl in ("agency_canonical_map", "customer_canonical_map"):
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({tbl});").fetchall()]
        if "updated_date" not in cols:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN updated_date TEXT;")
        conn.execute(
            f"UPDATE {tbl} SET updated_date = COALESCE(updated_date, datetime('now'));"
        )

    # audit trail
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canon_audit(
          ts       TEXT DEFAULT (datetime('now')),
          actor    TEXT,
          action   TEXT,
          key      TEXT,
          value    TEXT,
          extra    TEXT
        );
    """)

    # helpful indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_entity_aliases_customer
        ON entity_aliases(alias_name, entity_type);
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_customers_normalized_name
        ON customers(normalized_name);
    """)


def _ensure_agency(conn: sqlite3.Connection, name: str) -> int:
    conn.execute(
        "INSERT INTO agencies(agency_name) SELECT ? WHERE NOT EXISTS (SELECT 1 FROM agencies WHERE agency_name=?);",
        (name, name),
    )
    return conn.execute(
        "SELECT agency_id FROM agencies WHERE agency_name=?;", (name,)
    ).fetchone()[0]


def _ensure_customer_id_by_normalized(
    conn: sqlite3.Connection, normalized_name: str
) -> Optional[int]:
    row = conn.execute(
        "SELECT customer_id FROM customers WHERE normalized_name=?;", (normalized_name,)
    ).fetchone()
    return row[0] if row else None


def _upsert_alias_map(
    conn: sqlite3.Connection, table: str, alias_: str, canonical: str
) -> None:
    conn.execute(
        f"""INSERT INTO {table}(alias_name, canonical_name, updated_date)
            VALUES(?,?,datetime('now'))
            ON CONFLICT(alias_name) DO UPDATE
            SET canonical_name=excluded.canonical_name,
                updated_date=datetime('now');""",
        (alias_, canonical),
    )


def _upsert_entity_alias(
    conn: sqlite3.Connection, alias_: str, etype: str, target_id: int
) -> None:
    conn.execute(
        """INSERT INTO entity_aliases(alias_name, entity_type, target_entity_id, confidence_score, created_by, notes, is_active, updated_date)
           VALUES(?, ?, ?, 100, 'canon_tool', 'canonicalized', 1, datetime('now'))
           ON CONFLICT(alias_name, entity_type) DO UPDATE
           SET target_entity_id=excluded.target_entity_id,
               updated_date=datetime('now'),
               is_active=1;""",
        (alias_, etype, target_id),
    )


def _preview_agency_hits(conn: sqlite3.Connection, alias_: str) -> int:
    q = """
    WITH v AS (SELECT cleaned FROM v_raw_clean)
    SELECT COUNT(*) FROM v
    WHERE cleaned LIKE ? || ':%' COLLATE NOCASE
       OR cleaned LIKE '%:' || ? || ':%' COLLATE NOCASE;
    """
    return conn.execute(q, (alias_, alias_)).fetchone()[0]


def _preview_customer_hits(conn: sqlite3.Connection, alias_: str) -> int:
    q = """
    WITH base AS (SELECT cleaned FROM v_raw_clean),
    lastseg AS (
      SELECT
        RTRIM(
          REPLACE(REPLACE(REPLACE(REPLACE(
            CASE
              WHEN INSTR(cleaned, ':')=0 THEN cleaned
              ELSE SUBSTR(cleaned, INSTR(cleaned, ':')+1 + INSTR(SUBSTR(cleaned, INSTR(cleaned, ':')+1), ':'))
            END || '|',
            ' PRODUCTION|','|'),' PROD|','|'),'- PRODUCTION|','|'),'- PROD|','|'
          ),'|'
        ) AS tail
      FROM base
    )
    SELECT COUNT(*) FROM lastseg WHERE tail = ? COLLATE NOCASE;
    """
    return conn.execute(q, (alias_,)).fetchone()[0]


def _audit(
    conn: sqlite3.Connection,
    actor: str,
    action: str,
    key: str,
    value: str,
    extra: str = "",
) -> None:
    conn.execute(
        "INSERT INTO canon_audit(actor, action, key, value, extra) VALUES(?,?,?,?,?);",
        (actor, action, key, value, extra),
    )


# ---------- endpoints ---------------------------------------------------------

@canon_bp.post("/create-customer")
def create_customer():
    """
    Create a new customer record in the customers table.
    Body: { "normalized_name": "...", "sector_id": null }
    """
    d = request.get_json(force=True)
    normalized_name = _nfkc(d.get("normalized_name"))
    sector_id = d.get("sector_id")  # Optional
    
    if not normalized_name:
        return jsonify({"success": False, "error": "normalized_name required"}), 400

    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        conn.execute("BEGIN IMMEDIATE;")
        
        # Check if customer already exists
        existing = conn.execute(
            "SELECT customer_id FROM customers WHERE normalized_name = ?",
            (normalized_name,)
        ).fetchone()
        
        if existing:
            conn.execute("ROLLBACK;")
            return jsonify({
                "success": False, 
                "error": f"Customer already exists with id {existing[0]}"
            }), 409
        
        # Create the customer
        cursor = conn.execute(
            """INSERT INTO customers (normalized_name, sector_id, is_active, created_date)
               VALUES (?, ?, 1, datetime('now'))""",
            (normalized_name, sector_id)
        )
        customer_id = cursor.lastrowid
        
        conn.execute("COMMIT;")
        
        _audit(
            conn,
            request.remote_addr or "",
            "create_customer",
            normalized_name,
            str(customer_id),
            f"sector_id:{sector_id}"
        )
        
        return jsonify({
            "success": True,
            "customer_id": customer_id,
            "normalized_name": normalized_name,
            "sector_id": sector_id
        })
        
    except Exception as e:
        conn.execute("ROLLBACK;")
        current_app.logger.exception("create_customer failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()

@canon_bp.get("/sectors")
def api_sectors():
    """Return list of active sectors for dropdowns."""
    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        rows = conn.execute("""
            SELECT sector_id, sector_code, sector_name 
            FROM sectors 
            WHERE is_active = 1 
            ORDER BY sector_code
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()

@canon_bp.post("/agency")
def canon_agency():
    d = request.get_json(force=True)
    alias = _nfkc(d.get("alias"))
    canonical = _nfkc(d.get("canonical"))
    add_entity = bool(d.get("create_entity_alias", True))
    if not alias or not canonical:
        return jsonify({"success": False, "error": "alias and canonical required"}), 400

    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        conn.execute("BEGIN IMMEDIATE;")
        _ensure_tables(conn)
        _upsert_alias_map(conn, "agency_canonical_map", alias, canonical)
        created_alias = False
        if add_entity:
            agid = _ensure_agency(conn, canonical)
            _upsert_entity_alias(conn, alias, "agency", agid)
            created_alias = True
        conn.execute("COMMIT;")

        hits = _preview_agency_hits(conn, alias)
        _audit(
            conn,
            request.remote_addr or "",
            "agency_canon",
            alias,
            canonical,
            f"hits~{hits}",
        )
        return jsonify(
            {
                "success": True,
                "alias": alias,
                "canonical": canonical,
                "affected_preview": hits,
                "created_entity_alias": created_alias,
            }
        )
    except Exception as e:
        conn.execute("ROLLBACK;")
        current_app.logger.exception("canon_agency failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@canon_bp.post("/customer")
def canon_customer():
    d = request.get_json(force=True)
    alias = _nfkc(d.get("alias"))
    canonical = _nfkc(d.get("canonical"))
    if not alias or not canonical:
        return jsonify({"success": False, "error": "alias and canonical required"}), 400

    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        conn.execute("BEGIN IMMEDIATE;")
        _ensure_tables(conn)
        _upsert_alias_map(conn, "customer_canonical_map", alias, canonical)
        conn.execute("COMMIT;")

        hits = _preview_customer_hits(conn, alias)
        _audit(
            conn,
            request.remote_addr or "",
            "customer_canon",
            alias,
            canonical,
            f"hits~{hits}",
        )
        return jsonify(
            {
                "success": True,
                "alias": alias,
                "canonical": canonical,
                "affected_preview": hits,
                "created_entity_alias": False,
            }
        )
    except Exception as e:
        conn.execute("ROLLBACK;")
        current_app.logger.exception("canon_customer failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@canon_bp.post("/raw-to-customer")
def raw_to_customer():
    """
    Map a specific raw string to an existing customers.normalized_name.
    Body: { "raw": "...", "normalized_name": "..." }
    """
    d = request.get_json(force=True)
    raw = _nfkc(d.get("raw"))
    normalized = _nfkc(d.get("normalized_name"))
    if not raw or not normalized:
        return jsonify(
            {"success": False, "error": "raw and normalized_name required"}
        ), 400

    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        conn.execute("BEGIN IMMEDIATE;")
        _ensure_tables(conn)

        cid = _ensure_customer_id_by_normalized(conn, normalized)
        if cid is None:
            conn.execute("ROLLBACK;")
            return jsonify(
                {"success": False, "error": f"normalized_name not found: {normalized}"}
            ), 400

        # Conflict check: already mapped to a different customer?
        existing = conn.execute(
            "SELECT target_entity_id FROM entity_aliases WHERE alias_name=? AND entity_type='customer';",
            (raw,),
        ).fetchone()
        if existing and existing[0] != cid:
            conn.execute("ROLLBACK;")
            return jsonify(
                {
                    "success": False,
                    "error": "raw already mapped to a different customer",
                },
            ), 409

        _upsert_entity_alias(conn, raw, "customer", cid)
        conn.execute("COMMIT;")

        _audit(
            conn, request.remote_addr or "", "raw_map", raw, normalized, f"cid~{cid}"
        )
        return jsonify(
            {
                "success": True,
                "raw": raw,
                "normalized_name": normalized,
                "customer_id": cid,
            }
        )
    except Exception as e:
        conn.execute("ROLLBACK;")
        current_app.logger.exception("raw_to_customer failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@canon_bp.get("/suggest/normalized")
def suggest_normalized():
    """Return up to 20 normalized_name suggestions containing q."""
    q = _nfkc(request.args.get("q"))
    if not q:
        return jsonify([])
    with _open_rw(current_app.config["DB_PATH"]) as conn:
        rows = conn.execute(
            "SELECT normalized_name FROM customers WHERE normalized_name LIKE ? ORDER BY normalized_name LIMIT 20;",
            (f"%{q}%",),
        ).fetchall()
    return jsonify([r[0] for r in rows])


@canon_bp.post("/consolidate-customer")
def consolidate_customer():
    """
    Consolidate duplicate customer records that normalize to the same target.
    Body: { "source_customer_id": 366, "target_customer_id": 443, "dry_run": true }
    """
    d = request.get_json(force=True)
    source_id = d.get("source_customer_id")
    target_id = d.get("target_customer_id")
    dry_run = bool(d.get("dry_run", True))

    if not source_id or not target_id or source_id == target_id:
        return jsonify(
            {"success": False, "error": "Valid source and target customer IDs required"}
        ), 400

    conn = _open_rw(current_app.config["DB_PATH"])
    try:
        conn.execute("BEGIN IMMEDIATE;")

        # Verify both customers exist and get their details
        source_customer = conn.execute(
            "SELECT customer_id, normalized_name FROM customers WHERE customer_id = ?",
            (source_id,),
        ).fetchone()
        target_customer = conn.execute(
            "SELECT customer_id, normalized_name FROM customers WHERE customer_id = ?",
            (target_id,),
        ).fetchone()

        if not source_customer:
            conn.execute("ROLLBACK;")
            return jsonify(
                {"success": False, "error": f"Source customer {source_id} not found"}
            ), 404
        if not target_customer:
            conn.execute("ROLLBACK;")
            return jsonify(
                {"success": False, "error": f"Target customer {target_id} not found"}
            ), 404

        # Get consolidation stats
        spots_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM spots WHERE customer_id = ?", (source_id,)
        ).fetchone()["cnt"]

        total_revenue = conn.execute(
            "SELECT COALESCE(SUM(gross_rate), 0) as total FROM spots WHERE customer_id = ?",
            (source_id,),
        ).fetchone()["total"]

        # Check for any entity_aliases that point to source customer
        alias_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM entity_aliases WHERE target_entity_id = ? AND entity_type = 'customer'",
            (source_id,),
        ).fetchone()["cnt"]

        # Get month breakdown for audit
        monthly_spots = conn.execute(
            """SELECT broadcast_month, COUNT(*) as spots, SUM(gross_rate) as revenue 
               FROM spots WHERE customer_id = ? 
               GROUP BY broadcast_month ORDER BY broadcast_month""",
            (source_id,),
        ).fetchall()

        consolidation_summary = {
            "source_customer": {
                "id": source_id,
                "normalized_name": source_customer["normalized_name"],
            },
            "target_customer": {
                "id": target_id,
                "normalized_name": target_customer["normalized_name"],
            },
            "impact": {
                "spots_affected": spots_count,
                "total_revenue": float(total_revenue),
                "aliases_affected": alias_count,
                "monthly_breakdown": [
                    {
                        "month": row["broadcast_month"],
                        "spots": row["spots"],
                        "revenue": float(row["revenue"]),
                    }
                    for row in monthly_spots
                ],
            },
        }

        if dry_run:
            conn.execute("ROLLBACK;")
            return jsonify(
                {
                    "success": True,
                    "dry_run": True,
                    "consolidation_summary": consolidation_summary,
                }
            )

        # Perform actual consolidation
        # 1. Update all spots to point to target customer
        conn.execute(
            "UPDATE spots SET customer_id = ? WHERE customer_id = ?",
            (target_id, source_id),
        )

        # 2. Update any entity_aliases to point to target customer
        conn.execute(
            "UPDATE entity_aliases SET target_entity_id = ? WHERE target_entity_id = ? AND entity_type = 'customer'",
            (target_id, source_id),
        )

        # 3. Delete the source customer record
        conn.execute("DELETE FROM customers WHERE customer_id = ?", (source_id,))

        conn.execute("COMMIT;")

        # Audit log
        _audit(
            conn,
            request.remote_addr or "",
            "customer_consolidation",
            f"source_{source_id}_to_{target_id}",
            f"{source_customer['normalized_name']} -> {target_customer['normalized_name']}",
            f"spots:{spots_count},revenue:{total_revenue:.2f},aliases:{alias_count}",
        )

        return jsonify(
            {
                "success": True,
                "dry_run": False,
                "consolidation_summary": consolidation_summary,
                "message": f"Successfully consolidated customer {source_id} into {target_id}",
            }
        )

    except Exception as e:
        conn.execute("ROLLBACK;")
        current_app.logger.exception("consolidate_customer failed")
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@canon_bp.get("/duplicate-customers")
def find_duplicate_customers():
    """
    Find customer records that should be consolidated based on normalization rules.
    Finds customers where multiple actual normalized_names should map to the same target.
    """
    try:
        with _open_rw(current_app.config["DB_PATH"]) as conn:
            # Strategy: Find customers whose tails (after colon) would normalize to same target
            # via customer_canonical_map, but exist as separate customer records
            duplicates_sql = """
            WITH customer_tails AS (
                SELECT 
                    customer_id,
                    normalized_name,
                    created_date,
                    -- Extract tail (part after last colon)
                    CASE 
                        WHEN INSTR(normalized_name, ':') > 0 THEN
                            TRIM(SUBSTR(normalized_name, INSTR(normalized_name, ':') + 1))
                        ELSE normalized_name
                    END as customer_tail,
                    -- Extract prefix (part before colon)  
                    CASE
                        WHEN INSTR(normalized_name, ':') > 0 THEN
                            TRIM(SUBSTR(normalized_name, 1, INSTR(normalized_name, ':') - 1))
                        ELSE ''
                    END as customer_prefix
                FROM customers
            ),
            normalized_tails AS (
                SELECT 
                    ct.*,
                    -- Apply canonical mapping to tail
                    COALESCE(ccm.canonical_name, ct.customer_tail) as canonical_tail,
                    -- Reconstruct full canonical name
                    CASE
                        WHEN ct.customer_prefix != '' THEN ct.customer_prefix || ':' || COALESCE(ccm.canonical_name, ct.customer_tail)
                        ELSE COALESCE(ccm.canonical_name, ct.customer_tail)  
                    END as canonical_full_name
                FROM customer_tails ct
                LEFT JOIN customer_canonical_map ccm ON ct.customer_tail = ccm.alias_name
            ),
            duplicate_groups AS (
                SELECT 
                    canonical_full_name,
                    COUNT(*) as customer_count,
                    GROUP_CONCAT(customer_id) as customer_ids,
                    GROUP_CONCAT(normalized_name) as actual_names,
                    -- Get stats for each group
                    SUM((SELECT COUNT(*) FROM spots WHERE customer_id = nt.customer_id)) as total_spots,
                    SUM((SELECT COALESCE(SUM(gross_rate), 0) FROM spots WHERE customer_id = nt.customer_id)) as total_revenue
                FROM normalized_tails nt
                GROUP BY canonical_full_name
                HAVING customer_count > 1
            )
            SELECT 
                dg.*,
                -- Get the customer with most revenue as suggested target
                (SELECT customer_id FROM normalized_tails nt2 
                 WHERE nt2.canonical_full_name = dg.canonical_full_name 
                 ORDER BY (SELECT COALESCE(SUM(gross_rate), 0) FROM spots WHERE customer_id = nt2.customer_id) DESC 
                 LIMIT 1) as suggested_target_id
            FROM duplicate_groups dg
            ORDER BY total_revenue DESC;
            """

            rows = conn.execute(duplicates_sql).fetchall()

            duplicates = []
            for row in rows:
                # Parse customer IDs and get detailed info for each
                customer_ids = [int(x.strip()) for x in row["customer_ids"].split(",")]
                actual_names = row["actual_names"].split(",")

                # Get detailed info for each customer
                customer_details = []
                for cid in customer_ids:
                    cust_info = conn.execute(
                        """
                        SELECT 
                            customer_id, 
                            normalized_name, 
                            created_date,
                            (SELECT COUNT(*) FROM spots WHERE customer_id = ?) as spot_count,
                            (SELECT COALESCE(SUM(gross_rate), 0) FROM spots WHERE customer_id = ?) as total_revenue
                        FROM customers WHERE customer_id = ?
                    """,
                        (cid, cid, cid),
                    ).fetchone()

                    if cust_info:
                        customer_details.append(
                            {
                                "customer_id": cust_info["customer_id"],
                                "normalized_name": cust_info["normalized_name"],
                                "created_date": cust_info["created_date"],
                                "spot_count": cust_info["spot_count"],
                                "total_revenue": float(cust_info["total_revenue"]),
                            }
                        )

                # Sort customers by revenue (largest first) to suggest merge direction
                customer_details.sort(key=lambda x: x["total_revenue"], reverse=True)

                duplicates.append(
                    {
                        "canonical_target": row["canonical_full_name"],
                        "customer_count": row["customer_count"],
                        "actual_names": actual_names,
                        "customers": customer_details,
                        "suggested_target": customer_details[0]["customer_id"]
                        if customer_details
                        else None,
                        "total_revenue": float(row["total_revenue"]),
                        "total_spots": row["total_spots"],
                    }
                )

            return jsonify(
                {
                    "success": True,
                    "duplicates_found": len(duplicates),
                    "duplicates": duplicates,
                }
            )

    except Exception as e:
        current_app.logger.exception("find_duplicate_customers failed")
        return jsonify({"success": False, "error": str(e)}), 500
