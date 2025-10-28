# -*- coding: utf-8 -*-
"""
Canon Tool API: upsert canonical mappings (agency/customer) and optional entity_aliases.
Adds: conflict checks, Unicode normalization, indexes, audit log, suggestions.
SQLite 3.40 compatible.
"""

from __future__ import annotations
import sqlite3, unicodedata
from dataclasses import dataclass
from typing import Optional, Tuple, List
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
