# src/web/routes/customer_normalization.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
import sqlite3
from flask import Blueprint, current_app, render_template, request, jsonify


customer_norm_bp = Blueprint("customer_norm", __name__)

# ---- DB helpers (pure-ish wrappers) ---------------------------------


def _db_path():
    return (
        current_app.config.get("DB_PATH")
        or current_app.config.get("DATABASE_PATH")
        or "./data/database/production.db"
    )


def _get_db_ro() -> sqlite3.Connection:
    # Read-only connection with busy timeout; safe for concurrent writers (WAL recommended)
    uri = f"file:{_db_path()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Query-only prevents accidental writes in GET endpoints
    conn.execute("PRAGMA query_only = 1;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def _retry(op, retries=3, delay=0.25):
    for i in range(retries):
        try:
            return op()
        except sqlite3.OperationalError as e:
            # Retry only for lock-related issues
            msg = str(e).lower()
            if "database is locked" in msg or "busy" in msg:
                if i < retries - 1:
                    time.sleep(delay * (2**i))
                    continue
            raise


def _sqlite_safe_order(order_sql: str) -> str:
    # SQLite doesn't support NULLS LAST; strip it if present
    return order_sql.replace(" NULLS LAST", "")


def _error_response(e, path):
    payload = {"error": "Internal server error", "path": path, "success": False}
    if current_app.config.get("DEBUG"):
        payload["details"] = str(e)
    return jsonify(payload), 500


def _get_db() -> sqlite3.Connection:
    # Adapt to your app; ensure row_factory for dict-ish rows
    db_path = current_app.config.get("DATABASE_PATH", "./data/database/production.db")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _build_filters(args: Dict[str, str]) -> Tuple[str, List[Any]]:
    q = args.get("q", "").strip()
    status = args.get("status", "all")  # all | exists | missing | conflict
    rev = args.get("rev", "all")  # all | IAS | BC
    where = []
    params: List[Any] = []

    if q:
        where.append("(raw_text LIKE ? OR normalized_name LIKE ? OR customer LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like]

    if status == "exists":
        where.append("exists_in_customers = 1")
    elif status == "missing":
        where.append("exists_in_customers = 0")
    elif status == "conflict":
        where.append("alias_conflict = 1")

    if rev == "IAS":
        where.append("(revenue_types_seen LIKE '%Internal Ad Sales%')")
    elif rev == "BC":
        where.append("(revenue_types_seen LIKE '%Branded Content%')")

    clause = "WHERE " + " AND ".join(where) if where else ""
    return clause, params


def _build_sort(args: Dict[str, str]) -> str:
    sort = args.get("sort", "raw_az")
    mapping = {
        "raw_az": "raw_text COLLATE NOCASE ASC",
        "raw_za": "raw_text COLLATE NOCASE DESC",
        "norm_az": "normalized_name COLLATE NOCASE ASC",
        "norm_za": "normalized_name COLLATE NOCASE DESC",
        "status": "exists_in_customers DESC, alias_conflict DESC, has_alias DESC, normalized_name",
        "rev": "revenue_types_seen COLLATE NOCASE ASC, normalized_name",
        "created_desc": "customer_created_date DESC NULLS LAST, normalized_name",
    }
    return "ORDER BY " + mapping.get(sort, mapping["raw_az"])


def _paginate(args: Dict[str, str]) -> Tuple[int, int, int]:
    page = max(int(args.get("page", 1)), 1)
    size = min(max(int(args.get("size", 25)), 5), 200)
    offset = (page - 1) * size
    return page, size, offset


# ---- Pages -----------------------------------------------------------


@customer_norm_bp.route("/customer-normalization")
def page_customer_normalization():
    return render_template("customer_normalization_manager.html")


# ---- API -------------------------------------------------------------


@customer_norm_bp.route("/api/customer-normalization")
def api_customer_normalization():
    try:
        # These helpers should already exist in your module
        clause, params = _build_filters(request.args)
        order = _sqlite_safe_order(_build_sort(request.args))
        page, size, offset = _paginate(request.args)

        base_sql = f"""
          SELECT raw_text, normalized_name, customer, agency1, agency2,
                 revenue_types_seen, exists_in_customers, has_alias, alias_conflict,
                 customer_id, COALESCE(customer_created_date,'') AS customer_created_date
          FROM v_customer_normalization_audit
          {clause}
          {order}
          LIMIT ? OFFSET ?;
        """
        count_sql = f"""
          SELECT COUNT(*) AS cnt
          FROM v_customer_normalization_audit
          {clause};
        """

        with _get_db_ro() as db:
            total = _retry(lambda: db.execute(count_sql, params).fetchone()["cnt"])
            rows = _retry(
                lambda: db.execute(base_sql, [*params, size, offset]).fetchall()
            )

        return jsonify(
            {
                "total": total,
                "page": page,
                "size": size,
                "items": [dict(r) for r in rows],
            }
        )
    except Exception as e:
        return _error_response(e, request.path)


@customer_norm_bp.route("/api/customer-normalization/stats")
def api_customer_normalization_stats():
    try:
        stats_sql = """
        SELECT
          (SELECT COUNT(*) FROM v_customer_normalization_audit) AS total,
          (SELECT COUNT(*) FROM v_customer_normalization_audit WHERE exists_in_customers=1) AS in_customers,
          (SELECT COUNT(*) FROM v_customer_normalization_audit WHERE alias_conflict=1) AS conflicts,
          (SELECT COUNT(*) FROM v_customer_normalization_audit WHERE revenue_types_seen LIKE '%Internal Ad Sales%') AS seen_internal_ad_sales,
          (SELECT COUNT(*) FROM v_customer_normalization_audit WHERE revenue_types_seen LIKE '%Branded Content%') AS seen_branded_content
        ;
        """
        with _get_db_ro() as db:
            row = _retry(lambda: db.execute(stats_sql).fetchone())
        return jsonify(
            {
                "total": row["total"],
                "in_customers": row["in_customers"],
                "conflicts": row["conflicts"],
                "seen_internal_ad_sales": row["seen_internal_ad_sales"],
                "seen_branded_content": row["seen_branded_content"],
            }
        )
    except Exception as e:
        return _error_response(e, request.path)
