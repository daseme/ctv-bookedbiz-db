# src/web/routes/stale_customers.py
"""
Stale Customer Report - surfaces customers and agencies with zero spots
or no recent activity for human review/triage.
"""

from flask import Blueprint, render_template, jsonify, request, current_app
import sqlite3
from contextlib import contextmanager
from datetime import datetime, date, timedelta

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
        # Zero-spot customers (active, no spots, not created in last 90 days)
        grace_cutoff = (date.today() - timedelta(days=90)).isoformat()
        zero_spot_customers = conn.execute("""
            SELECT COUNT(*) as cnt FROM customers c
            WHERE c.is_active = 1
            AND (c.created_date IS NULL OR c.created_date < ?)
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.customer_id = c.customer_id)
        """, [grace_cutoff]).fetchone()["cnt"]

        # Zero-spot agencies
        zero_spot_agencies = conn.execute("""
            SELECT COUNT(*) as cnt FROM agencies a
            WHERE a.is_active = 1
            AND (a.created_date IS NULL OR a.created_date < ?)
            AND NOT EXISTS (SELECT 1 FROM spots s WHERE s.agency_id = a.agency_id)
        """, [grace_cutoff]).fetchone()["cnt"]

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
    grace_cutoff = (date.today() - timedelta(days=90)).isoformat()
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
                    c.notes,
                    c.created_date
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

                # Classify — skip zero-spot entities created in last 90 days
                created = row.get("created_date") or ""
                if row["spot_count"] == 0:
                    if created >= grace_cutoff:
                        continue  # recently created, not stale yet
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
                    a.notes,
                    a.created_date
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

                # Classify — skip zero-spot entities created in last 90 days
                created = row.get("created_date") or ""
                if row["spot_count"] == 0:
                    if created >= grace_cutoff:
                        continue  # recently created, not stale yet
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


@stale_customers_bp.route("/stale-customers/<entity_type>/<int:entity_id>")
def stale_customer_detail_page(entity_type, entity_id):
    """Render the stale customer detail page."""
    if entity_type not in ("customer", "agency"):
        return "Invalid entity type", 404
    return render_template(
        "stale_customer_detail.html",
        entity_type=entity_type,
        entity_id=entity_id,
    )


@stale_customers_bp.route("/api/stale-customers/<entity_type>/<int:entity_id>")
def api_stale_customer_detail(entity_type, entity_id):
    """Return all detail data for a single entity."""
    if entity_type not in ("customer", "agency"):
        return jsonify({"error": "Invalid entity_type"}), 400

    with _db_ro() as conn:
        # 1. Entity basics
        if entity_type == "customer":
            entity = conn.execute("""
                SELECT c.customer_id as entity_id, 'customer' as entity_type,
                       c.normalized_name as entity_name, c.is_active,
                       s.sector_name, c.assigned_ae, c.notes,
                       c.address, c.city, c.state, c.zip
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.customer_id = ?
            """, [entity_id]).fetchone()
        else:
            entity = conn.execute("""
                SELECT a.agency_id as entity_id, 'agency' as entity_type,
                       a.agency_name as entity_name, a.is_active,
                       NULL as sector_name, a.assigned_ae, a.notes,
                       a.address, a.city, a.state, a.zip
                FROM agencies a
                WHERE a.agency_id = ?
            """, [entity_id]).fetchone()

        if not entity:
            return jsonify({"error": "Entity not found"}), 404

        result = dict(entity)

        # 2. Aggregate metrics
        id_col = "customer_id" if entity_type == "customer" else "agency_id"
        agg = conn.execute(f"""
            SELECT COUNT(*) as spot_count,
                   COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                                     THEN gross_rate ELSE 0 END), 0) as total_revenue,
                   MIN(air_date) as first_active,
                   MAX(air_date) as last_active
            FROM spots
            WHERE {id_col} = ?
        """, [entity_id]).fetchone()
        result["spot_count"] = agg["spot_count"]
        result["total_revenue"] = float(agg["total_revenue"] or 0)
        result["first_active"] = agg["first_active"]
        result["last_active"] = agg["last_active"]

        # 3. Recent spots (last 50)
        spots = conn.execute(f"""
            SELECT air_date, market_name, sales_person, gross_rate,
                   revenue_type, length_seconds, bill_code
            FROM spots
            WHERE {id_col} = ?
            ORDER BY air_date DESC
            LIMIT 50
        """, [entity_id]).fetchall()
        result["recent_spots"] = [dict(s) for s in spots]

        # 4. Revenue by year
        rev_year = conn.execute(f"""
            SELECT strftime('%Y', air_date) as year,
                   COUNT(*) as spot_count,
                   COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                                     THEN gross_rate ELSE 0 END), 0) as revenue
            FROM spots
            WHERE {id_col} = ?
            GROUP BY strftime('%Y', air_date)
            ORDER BY year DESC
        """, [entity_id]).fetchall()
        result["revenue_by_year"] = [
            {"year": r["year"], "spot_count": r["spot_count"], "revenue": float(r["revenue"] or 0)}
            for r in rev_year
        ]

        # 5. Revenue by market
        rev_market = conn.execute(f"""
            SELECT market_name,
                   COUNT(*) as spot_count,
                   COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                                     THEN gross_rate ELSE 0 END), 0) as revenue
            FROM spots
            WHERE {id_col} = ?
            GROUP BY market_name
            ORDER BY revenue DESC
        """, [entity_id]).fetchall()
        result["revenue_by_market"] = [
            {"market_name": r["market_name"], "spot_count": r["spot_count"], "revenue": float(r["revenue"] or 0)}
            for r in rev_market
        ]

        # 6. Aliases
        aliases = conn.execute("""
            SELECT alias_name, created_by
            FROM entity_aliases
            WHERE entity_type = ? AND target_entity_id = ? AND is_active = 1
            ORDER BY alias_name
        """, [entity_type, entity_id]).fetchall()
        result["aliases"] = [{"alias_name": a["alias_name"], "created_by": a["created_by"]} for a in aliases]

        # 7. Audit trail
        audit_key = f"{entity_type}:{entity_id}"
        audit = conn.execute("""
            SELECT ts, actor, action, key, value, extra
            FROM canon_audit
            WHERE key = ? OR value = ? OR extra LIKE ?
            ORDER BY ts DESC
            LIMIT 50
        """, [audit_key, audit_key, f"%{entity_id}%"]).fetchall()
        result["audit_trail"] = [
            {"ts": a["ts"], "actor": a["actor"], "action": a["action"],
             "key": a["key"], "value": a["value"], "extra": a["extra"]}
            for a in audit
        ]

        # 8. Contacts
        contacts = conn.execute("""
            SELECT contact_name, email, phone, contact_role, is_primary
            FROM entity_contacts
            WHERE entity_type = ? AND entity_id = ? AND is_active = 1
            ORDER BY is_primary DESC, contact_name
        """, [entity_type, entity_id]).fetchall()
        result["contacts"] = [dict(c) for c in contacts]

    return jsonify(result)


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


@stale_customers_bp.route("/api/stale-customers/bulk-deactivate", methods=["POST"])
def api_bulk_deactivate():
    """
    Bulk deactivate entities that are 3+ years old with zero revenue.

    Uses same filter logic as api_stale_entities() to match visible rows,
    then further filters to: total_revenue == 0 AND created_date > 3 years ago.

    JSON body:
        dry_run: bool (default true) - preview only, no changes
    Query params: same as /entities (type, category, threshold, sector_id)
    """
    data = request.get_json() or {}
    dry_run = data.get("dry_run", True)

    entity_type = request.args.get("type", "all")
    category = request.args.get("category", "all")
    threshold = request.args.get("threshold", "2", type=str)
    sector_filter = request.args.get("sector_id", "")

    try:
        threshold_years = int(threshold)
    except ValueError:
        threshold_years = 2

    cutoff = f"{date.today().year - threshold_years}-01-01"
    age_cutoff = f"{date.today().year - 3}-01-01"
    candidates = []

    with _db_ro() as conn:
        # --- Build metrics lookups (same as api_stale_entities) ---
        customer_metrics = {}
        for row in conn.execute("""
            SELECT customer_id,
                   COUNT(*) as spot_count,
                   SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                       THEN gross_rate ELSE 0 END) as total_revenue,
                   MAX(air_date) as last_active
            FROM spots WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        """).fetchall():
            customer_metrics[row["customer_id"]] = {
                "spot_count": row["spot_count"],
                "total_revenue": float(row["total_revenue"] or 0),
                "last_active": row["last_active"],
            }

        agency_metrics = {}
        for row in conn.execute("""
            SELECT agency_id,
                   COUNT(*) as spot_count,
                   SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                       THEN gross_rate ELSE 0 END) as total_revenue,
                   MAX(air_date) as last_active
            FROM spots WHERE agency_id IS NOT NULL
            GROUP BY agency_id
        """).fetchall():
            agency_metrics[row["agency_id"]] = {
                "spot_count": row["spot_count"],
                "total_revenue": float(row["total_revenue"] or 0),
                "last_active": row["last_active"],
            }

        # --- Collect qualifying entities ---
        if entity_type in ("all", "customer"):
            for c in conn.execute("""
                SELECT customer_id as entity_id, 'customer' as entity_type,
                       normalized_name as entity_name, sector_id, created_date
                FROM customers
                WHERE is_active = 1
            """).fetchall():
                row = dict(c)
                cid = row["entity_id"]
                metrics = customer_metrics.get(cid, {})
                spot_count = metrics.get("spot_count", 0)
                total_revenue = metrics.get("total_revenue", 0)
                last_active = metrics.get("last_active")

                # Must be stale or zero-spot
                if spot_count == 0:
                    cat = "zero_spot"
                elif last_active and last_active < cutoff:
                    cat = "stale"
                else:
                    continue

                if category != "all" and cat != category:
                    continue

                # Bulk criteria: zero revenue AND created 3+ years ago
                if total_revenue != 0:
                    continue
                if not row["created_date"] or row["created_date"] >= age_cutoff:
                    continue

                # Sector filter
                if sector_filter:
                    try:
                        if row.get("sector_id") != int(sector_filter):
                            continue
                    except ValueError:
                        pass

                candidates.append({
                    "entity_type": "customer",
                    "entity_id": cid,
                    "entity_name": row["entity_name"],
                    "category": cat,
                    "created_date": row["created_date"],
                })

        if entity_type in ("all", "agency"):
            for a in conn.execute("""
                SELECT agency_id as entity_id, 'agency' as entity_type,
                       agency_name as entity_name, created_date
                FROM agencies
                WHERE is_active = 1
            """).fetchall():
                row = dict(a)
                aid = row["entity_id"]
                metrics = agency_metrics.get(aid, {})
                spot_count = metrics.get("spot_count", 0)
                total_revenue = metrics.get("total_revenue", 0)
                last_active = metrics.get("last_active")

                if spot_count == 0:
                    cat = "zero_spot"
                elif last_active and last_active < cutoff:
                    cat = "stale"
                else:
                    continue

                if category != "all" and cat != category:
                    continue

                if total_revenue != 0:
                    continue
                if not row["created_date"] or row["created_date"] >= age_cutoff:
                    continue

                if sector_filter:
                    continue  # agencies have no sector; if sector filter is set, skip

                candidates.append({
                    "entity_type": "agency",
                    "entity_id": aid,
                    "entity_name": row["entity_name"],
                    "category": cat,
                    "created_date": row["created_date"],
                })

    if dry_run:
        return jsonify({
            "dry_run": True,
            "count": len(candidates),
            "entities": [c["entity_name"] for c in candidates],
        })

    # --- Execute deactivation ---
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    reason = "Bulk deactivate: 3+ years old, zero revenue"
    deactivated = 0

    with _db_rw() as conn:
        try:
            for c in candidates:
                table = "agencies" if c["entity_type"] == "agency" else "customers"
                id_col = "agency_id" if c["entity_type"] == "agency" else "customer_id"
                name_col = "agency_name" if c["entity_type"] == "agency" else "normalized_name"

                entity = conn.execute(
                    f"SELECT {name_col} as name, notes FROM {table} WHERE {id_col} = ? AND is_active = 1",
                    [c["entity_id"]]
                ).fetchone()
                if not entity:
                    continue  # already deactivated or missing

                new_notes = (entity["notes"] or "") + f"\n[Deactivated {timestamp}] {reason}"
                conn.execute(
                    f"UPDATE {table} SET is_active = 0, notes = ? WHERE {id_col} = ?",
                    [new_notes, c["entity_id"]]
                )
                conn.execute("""
                    INSERT INTO canon_audit (actor, action, key, value, extra)
                    VALUES (?, 'DEACTIVATE', ?, ?, ?)
                """, [
                    "web_user",
                    f"{c['entity_type']}:{c['entity_id']}",
                    entity["name"],
                    f"reason={reason}"
                ])
                deactivated += 1

            conn.commit()
            return jsonify({"success": True, "deactivated_count": deactivated})

        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500
