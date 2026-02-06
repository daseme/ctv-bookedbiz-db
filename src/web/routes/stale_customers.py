# src/web/routes/stale_customers.py
"""
Stale Customer Report - surfaces customers and agencies with zero spots
or no recent activity for human review/triage.
"""

from flask import Blueprint, render_template, jsonify, request, current_app
import sqlite3
from contextlib import contextmanager
from datetime import datetime, date

stale_customers_bp = Blueprint("stale_customers", __name__)


def _get_db_path():
    return current_app.config.get("DB_PATH") or "./.data/dev.db"


@contextmanager
def _db_ro():
    uri = f"file:{_get_db_path()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _db_rw():
    conn = sqlite3.connect(_get_db_path(), timeout=10.0)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


@stale_customers_bp.route("/stale-customers")
def stale_customers_page():
    """Render the stale customer report page."""
    return render_template("stale_customers.html")


@stale_customers_bp.route("/api/stale-customers/sectors")
def api_stale_sectors():
    """Get list of all sectors for filter dropdown."""
    with _db_ro() as conn:
        sectors = conn.execute("""
            SELECT sector_id, sector_code, sector_name
            FROM sectors
            WHERE is_active = 1
            ORDER BY sector_name
        """).fetchall()
        return jsonify([dict(s) for s in sectors])


@stale_customers_bp.route("/api/stale-customers/stats")
def api_stale_stats():
    """Summary stat cards: zero-spot counts, stale counts, revenue at risk."""
    threshold = request.args.get("threshold", "2", type=str)
    try:
        threshold_years = int(threshold)
    except ValueError:
        threshold_years = 2

    cutoff = f"{date.today().year - threshold_years}-01-01"

    with _db_ro() as conn:
        # Zero-spot customers (active, no spots at all)
        zero_spot_customers = conn.execute("""
            SELECT COUNT(*) as cnt FROM customers c
            WHERE c.is_active = 1
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.customer_id = c.customer_id)
        """).fetchone()["cnt"]

        # Zero-spot agencies
        zero_spot_agencies = conn.execute("""
            SELECT COUNT(*) as cnt FROM agencies a
            WHERE a.is_active = 1
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.agency_id = a.agency_id)
        """).fetchone()["cnt"]

        # Stale customers (have spots, but none after cutoff)
        stale_customers = conn.execute("""
            SELECT COUNT(*) as cnt FROM customers c
            WHERE c.is_active = 1
            AND EXISTS (SELECT 1 FROM spots s WHERE s.customer_id = c.customer_id)
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.customer_id = c.customer_id AND s.air_date >= ?)
        """, [cutoff]).fetchone()["cnt"]

        # Stale agencies
        stale_agencies = conn.execute("""
            SELECT COUNT(*) as cnt FROM agencies a
            WHERE a.is_active = 1
            AND EXISTS (SELECT 1 FROM spots s WHERE s.agency_id = a.agency_id)
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.agency_id = a.agency_id AND s.air_date >= ?)
        """, [cutoff]).fetchone()["cnt"]

        # Revenue at risk (total non-trade revenue from stale customers)
        revenue_at_risk = conn.execute("""
            SELECT COALESCE(SUM(
                CASE WHEN s.revenue_type != 'Trade' OR s.revenue_type IS NULL
                     THEN s.gross_rate ELSE 0 END
            ), 0) as total
            FROM spots s
            JOIN customers c ON s.customer_id = c.customer_id
            WHERE c.is_active = 1
            AND NOT EXISTS (
                SELECT 1 FROM spots s2
                WHERE s2.customer_id = c.customer_id AND s2.air_date >= ?
            )
        """, [cutoff]).fetchone()["total"]

        return jsonify({
            "zero_spot_customers": zero_spot_customers,
            "zero_spot_agencies": zero_spot_agencies,
            "zero_spot_total": zero_spot_customers + zero_spot_agencies,
            "stale_customers": stale_customers,
            "stale_agencies": stale_agencies,
            "stale_total": stale_customers + stale_agencies,
            "revenue_at_risk": float(revenue_at_risk or 0),
            "total_for_review": (zero_spot_customers + zero_spot_agencies +
                                 stale_customers + stale_agencies),
            "threshold_years": threshold_years,
            "cutoff_date": cutoff,
        })


@stale_customers_bp.route("/api/stale-customers/entities")
def api_stale_entities():
    """
    Main data: all stale/zero-spot entities with metadata.

    Query params:
        type: all | customer | agency (default: all)
        category: all | zero_spot | stale (default: all)
        threshold: years integer (default: 2)
        sector_id: filter by sector
        include_inactive: 0 | 1 (default: 0)
        sort: name | last_active | revenue | spots | type (default: name)
    """
    entity_type = request.args.get("type", "all")
    category = request.args.get("category", "all")
    threshold = request.args.get("threshold", "2", type=str)
    sector_filter = request.args.get("sector_id", "")
    include_inactive = request.args.get("include_inactive", "0") == "1"
    sort_by = request.args.get("sort", "name")

    try:
        threshold_years = int(threshold)
    except ValueError:
        threshold_years = 2

    cutoff = f"{date.today().year - threshold_years}-01-01"
    results = []

    with _db_ro() as conn:
        # Build customer metrics lookup
        customer_metrics = {}
        rows = conn.execute("""
            SELECT
                customer_id,
                COUNT(*) as spot_count,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                MAX(air_date) as last_active
            FROM spots
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        """).fetchall()
        for row in rows:
            customer_metrics[row["customer_id"]] = {
                "spot_count": row["spot_count"],
                "total_revenue": float(row["total_revenue"] or 0),
                "last_active": row["last_active"],
            }

        # Build agency metrics lookup
        agency_metrics = {}
        rows = conn.execute("""
            SELECT
                agency_id,
                COUNT(*) as spot_count,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                MAX(air_date) as last_active
            FROM spots
            WHERE agency_id IS NOT NULL
            GROUP BY agency_id
        """).fetchall()
        for row in rows:
            agency_metrics[row["agency_id"]] = {
                "spot_count": row["spot_count"],
                "total_revenue": float(row["total_revenue"] or 0),
                "last_active": row["last_active"],
            }

        # Alias counts
        customer_alias_counts = {}
        rows = conn.execute("""
            SELECT target_entity_id, COUNT(*) as cnt
            FROM entity_aliases
            WHERE entity_type = 'customer' AND is_active = 1
            GROUP BY target_entity_id
        """).fetchall()
        for row in rows:
            customer_alias_counts[row["target_entity_id"]] = row["cnt"]

        agency_alias_counts = {}
        rows = conn.execute("""
            SELECT target_entity_id, COUNT(*) as cnt
            FROM entity_aliases
            WHERE entity_type = 'agency' AND is_active = 1
            GROUP BY target_entity_id
        """).fetchall()
        for row in rows:
            agency_alias_counts[row["target_entity_id"]] = row["cnt"]

        # Get customers
        if entity_type in ("all", "customer"):
            active_clause = "" if include_inactive else "AND c.is_active = 1"
            customers = conn.execute(f"""
                SELECT
                    c.customer_id as entity_id,
                    'customer' as entity_type,
                    c.normalized_name as entity_name,
                    c.is_active,
                    c.sector_id,
                    s.sector_name,
                    c.assigned_ae,
                    c.notes
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE 1=1 {active_clause}
                ORDER BY c.normalized_name
            """).fetchall()

            for c in customers:
                row = dict(c)
                cid = row["entity_id"]
                metrics = customer_metrics.get(cid, {})
                row["spot_count"] = metrics.get("spot_count", 0)
                row["total_revenue"] = metrics.get("total_revenue", 0)
                row["last_active"] = metrics.get("last_active")
                row["alias_count"] = customer_alias_counts.get(cid, 0)
                row["is_agency_client"] = ":" in (row["entity_name"] or "")

                # Classify
                if row["spot_count"] == 0:
                    row["category"] = "zero_spot"
                elif row["last_active"] and row["last_active"] < cutoff:
                    row["category"] = "stale"
                else:
                    continue  # active customer, skip

                results.append(row)

        # Get agencies
        if entity_type in ("all", "agency"):
            active_clause = "" if include_inactive else "AND a.is_active = 1"
            agencies = conn.execute(f"""
                SELECT
                    a.agency_id as entity_id,
                    'agency' as entity_type,
                    a.agency_name as entity_name,
                    a.is_active,
                    NULL as sector_id,
                    NULL as sector_name,
                    a.assigned_ae,
                    a.notes
                FROM agencies a
                WHERE 1=1 {active_clause}
                ORDER BY a.agency_name
            """).fetchall()

            for a in agencies:
                row = dict(a)
                aid = row["entity_id"]
                metrics = agency_metrics.get(aid, {})
                row["spot_count"] = metrics.get("spot_count", 0)
                row["total_revenue"] = metrics.get("total_revenue", 0)
                row["last_active"] = metrics.get("last_active")
                row["alias_count"] = agency_alias_counts.get(aid, 0)
                row["is_agency_client"] = False

                # Classify
                if row["spot_count"] == 0:
                    row["category"] = "zero_spot"
                elif row["last_active"] and row["last_active"] < cutoff:
                    row["category"] = "stale"
                else:
                    continue  # active agency, skip

                results.append(row)

    # Apply filters
    if category != "all":
        results = [r for r in results if r["category"] == category]

    if sector_filter:
        try:
            sid = int(sector_filter)
            results = [r for r in results if r.get("sector_id") == sid]
        except ValueError:
            pass

    # Sort
    if sort_by == "last_active":
        results.sort(key=lambda r: (r.get("last_active") is None, r.get("last_active") or ""))
        results.sort(key=lambda r: r.get("last_active") is None)
    elif sort_by == "revenue":
        results.sort(key=lambda r: -(r.get("total_revenue") or 0))
    elif sort_by == "spots":
        results.sort(key=lambda r: -(r.get("spot_count") or 0))
    elif sort_by == "type":
        results.sort(key=lambda r: (
            0 if r["entity_type"] == "agency" else 1,
            r["entity_name"].lower()
        ))
    else:
        results.sort(key=lambda r: r["entity_name"].lower())

    return jsonify(results)


@stale_customers_bp.route("/api/stale-customers/deactivate", methods=["POST"])
def api_deactivate_entity():
    """
    Deactivate an entity with required reason.
    Audits to canon_audit table.
    """
    data = request.get_json() or {}
    entity_type = data.get("entity_type")
    entity_id = data.get("entity_id")
    reason = (data.get("reason") or "").strip()

    if entity_type not in ("customer", "agency"):
        return jsonify({"error": "Invalid entity_type"}), 400
    if not entity_id:
        return jsonify({"error": "entity_id required"}), 400
    if not reason:
        return jsonify({"error": "Reason is required for deactivation"}), 400

    with _db_rw() as conn:
        try:
            table = "agencies" if entity_type == "agency" else "customers"
            id_col = "agency_id" if entity_type == "agency" else "customer_id"
            name_col = "agency_name" if entity_type == "agency" else "normalized_name"

            # Get current entity
            entity = conn.execute(
                f"SELECT {name_col} as name, notes, is_active FROM {table} WHERE {id_col} = ?",
                [entity_id]
            ).fetchone()
            if not entity:
                return jsonify({"error": "Entity not found"}), 404

            # Append reason to notes
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            deactivation_note = f"\n[Deactivated {timestamp}] {reason}"
            new_notes = (entity["notes"] or "") + deactivation_note

            # Set is_active = 0 and update notes
            conn.execute(
                f"UPDATE {table} SET is_active = 0, notes = ? WHERE {id_col} = ?",
                [new_notes, entity_id]
            )

            # Audit
            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'DEACTIVATE', ?, ?, ?)
            """, [
                "web_user",
                f"{entity_type}:{entity_id}",
                entity["name"],
                f"reason={reason}"
            ])

            conn.commit()
            return jsonify({"success": True, "entity_name": entity["name"]})

        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500
