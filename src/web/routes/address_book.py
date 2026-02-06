# src/web/routes/address_book.py
"""
Unified Address Book - view and manage contacts/addresses for agencies and customers.
"""

from flask import Blueprint, render_template, jsonify, request, current_app, Response
import sqlite3
import json
import csv
import io
from contextlib import contextmanager

address_book_bp = Blueprint("address_book", __name__)


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


def _get_agency_client_ids(conn):
    """
    Return set of customer_ids that are agency clients (hidden from front page).
    A customer is an agency client if:
      1. Their name contains ':' (Agency:Customer naming convention), OR
      2. ALL of their spots are booked through an agency (agency_id is never null)
    """
    rows = conn.execute("""
        SELECT customer_id FROM customers
        WHERE is_active = 1 AND normalized_name LIKE '%:%'
        UNION
        SELECT customer_id FROM spots
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        HAVING COUNT(*) = COUNT(agency_id)
    """).fetchall()
    return {row["customer_id"] for row in rows}


@address_book_bp.route("/address-book")
def address_book_page():
    """Render the unified address book page."""
    return render_template("address_book.html")


@address_book_bp.route("/api/address-book/sectors")
def api_sectors():
    """Get list of all sectors for dropdown."""
    with _db_ro() as conn:
        sectors = conn.execute("""
            SELECT sector_id, sector_code, sector_name, sector_group
            FROM sectors
            WHERE is_active = 1
            ORDER BY sector_name
        """).fetchall()
        return jsonify([dict(s) for s in sectors])


@address_book_bp.route("/api/address-book/markets")
def api_markets():
    """Get list of all markets that have spots, for dropdown filter."""
    with _db_ro() as conn:
        # Get distinct markets from spots data
        markets = conn.execute("""
            SELECT DISTINCT market_name
            FROM spots
            WHERE market_name IS NOT NULL AND market_name != ''
            ORDER BY market_name
        """).fetchall()
        return jsonify([row["market_name"] for row in markets])


@address_book_bp.route("/api/address-book")
def api_address_book():
    """
    Get all entities (agencies + customers) with their addresses, contacts, sectors, markets, and notes.

    Query params:
        search: filter by name
        type: 'all', 'agency', 'customer'
        has_contacts: 'all', 'yes', 'no'
        has_address: 'all', 'yes', 'no'
        sector_id: filter by sector (customers only)
        market: filter by market name
        ae: filter by assigned AE name
        sort: 'name', 'sector', 'type', 'market', 'ae' (default: 'name')
    """
    search = request.args.get("search", "").strip()
    entity_type = request.args.get("type", "all")
    has_contacts = request.args.get("has_contacts", "all")
    has_address = request.args.get("has_address", "all")
    sector_filter = request.args.get("sector_id", "")
    market_filter = request.args.get("market", "")
    ae_filter = request.args.get("ae", "")
    sort_by = request.args.get("sort", "name")

    results = []

    with _db_ro() as conn:
        # Get agency-booked customer IDs to exclude
        agency_client_ids = _get_agency_client_ids(conn)

        # Build lookup of markets by entity
        # For agencies: markets where agency_id matches
        agency_markets = {}
        rows = conn.execute("""
            SELECT agency_id, GROUP_CONCAT(DISTINCT market_name) as markets
            FROM spots
            WHERE agency_id IS NOT NULL AND market_name IS NOT NULL AND market_name != ''
            GROUP BY agency_id
        """).fetchall()
        for row in rows:
            agency_markets[row["agency_id"]] = row["markets"]

        # For customers: markets where customer_id matches
        customer_markets = {}
        rows = conn.execute("""
            SELECT customer_id, GROUP_CONCAT(DISTINCT market_name) as markets
            FROM spots
            WHERE customer_id IS NOT NULL AND market_name IS NOT NULL AND market_name != ''
            GROUP BY customer_id
        """).fetchall()
        for row in rows:
            customer_markets[row["customer_id"]] = row["markets"]

        # Build lookup of metrics (last_active, revenue, spot_count) by entity
        # For agencies
        agency_metrics = {}
        rows = conn.execute("""
            SELECT
                agency_id,
                MAX(air_date) as last_active,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                COUNT(*) as spot_count
            FROM spots
            WHERE agency_id IS NOT NULL
            GROUP BY agency_id
        """).fetchall()
        for row in rows:
            agency_metrics[row["agency_id"]] = {
                "last_active": row["last_active"],
                "total_revenue": float(row["total_revenue"] or 0),
                "spot_count": row["spot_count"]
            }

        # For customers
        customer_metrics = {}
        rows = conn.execute("""
            SELECT
                customer_id,
                MAX(air_date) as last_active,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                COUNT(*) as spot_count
            FROM spots
            WHERE customer_id IS NOT NULL
            GROUP BY customer_id
        """).fetchall()
        for row in rows:
            customer_metrics[row["customer_id"]] = {
                "last_active": row["last_active"],
                "total_revenue": float(row["total_revenue"] or 0),
                "spot_count": row["spot_count"]
            }

        # Get agencies (no sector for agencies)
        if entity_type in ("all", "agency"):
            agencies = conn.execute("""
                SELECT
                    a.agency_id as entity_id,
                    'agency' as entity_type,
                    a.agency_name as entity_name,
                    a.address,
                    a.city,
                    a.state,
                    a.zip,
                    a.notes,
                    a.assigned_ae,
                    NULL as sector_id,
                    NULL as sector_name,
                    NULL as sector_code,
                    (SELECT COUNT(*) FROM entity_contacts ec
                     WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id AND ec.is_active = 1) as contact_count,
                    (SELECT contact_name FROM entity_contacts ec
                     WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                     AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact
                FROM agencies a
                WHERE a.is_active = 1
                ORDER BY a.agency_name
            """).fetchall()

            for a in agencies:
                row = dict(a)
                row["markets"] = agency_markets.get(row["entity_id"], "")
                metrics = agency_metrics.get(row["entity_id"], {})
                row["last_active"] = metrics.get("last_active")
                row["total_revenue"] = metrics.get("total_revenue", 0)
                row["spot_count"] = metrics.get("spot_count", 0)
                results.append(row)

        # Get customers with sector info (exclude agency-booked)
        if entity_type in ("all", "customer"):
            customers = conn.execute("""
                SELECT
                    c.customer_id as entity_id,
                    'customer' as entity_type,
                    c.normalized_name as entity_name,
                    c.address,
                    c.city,
                    c.state,
                    c.zip,
                    c.notes,
                    c.assigned_ae,
                    c.sector_id,
                    s.sector_name,
                    s.sector_code,
                    (SELECT COUNT(*) FROM entity_contacts ec
                     WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id AND ec.is_active = 1) as contact_count,
                    (SELECT contact_name FROM entity_contacts ec
                     WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                     AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
                ORDER BY c.normalized_name
            """).fetchall()

            for c in customers:
                row = dict(c)
                # Skip agency-booked customers
                if row["entity_id"] in agency_client_ids:
                    continue
                row["markets"] = customer_markets.get(row["entity_id"], "")
                metrics = customer_metrics.get(row["entity_id"], {})
                row["last_active"] = metrics.get("last_active")
                row["total_revenue"] = metrics.get("total_revenue", 0)
                row["spot_count"] = metrics.get("spot_count", 0)
                results.append(row)

    # Apply filters
    if search:
        q = search.lower()
        # Build set of entities matching via contacts (name, email, notes)
        contact_matches = set()
        with _db_ro() as conn:
            contact_rows = conn.execute("""
                SELECT DISTINCT entity_type, entity_id
                FROM entity_contacts
                WHERE is_active = 1 AND (
                    LOWER(contact_name) LIKE ? OR
                    LOWER(email) LIKE ? OR
                    LOWER(notes) LIKE ?
                )
            """, [f"%{q}%", f"%{q}%", f"%{q}%"]).fetchall()
            for cr in contact_rows:
                contact_matches.add((cr["entity_type"], cr["entity_id"]))

        # Filter: match name, notes, or contact lookup
        results = [r for r in results if (
            q in r["entity_name"].lower() or
            q in (r.get("notes") or "").lower() or
            (r["entity_type"], r["entity_id"]) in contact_matches
        )]

    if has_contacts == "yes":
        results = [r for r in results if r["contact_count"] > 0]
    elif has_contacts == "no":
        results = [r for r in results if r["contact_count"] == 0]

    if has_address == "yes":
        results = [r for r in results if r["address"] or r["city"]]
    elif has_address == "no":
        results = [r for r in results if not r["address"] and not r["city"]]

    if sector_filter:
        try:
            sid = int(sector_filter)
            results = [r for r in results if r.get("sector_id") == sid]
        except ValueError:
            pass

    if market_filter:
        # Filter to entities that have run spots in this market
        results = [r for r in results if market_filter in (r.get("markets") or "").split(",")]

    if ae_filter:
        if ae_filter == "__none__":
            results = [r for r in results if not r.get("assigned_ae")]
        else:
            results = [r for r in results if r.get("assigned_ae") == ae_filter]

    # Sort
    if sort_by == "sector":
        # Sort by sector name (nulls last), then by name
        results.sort(key=lambda r: (
            r["sector_name"] is None,
            (r["sector_name"] or "").lower(),
            r["entity_name"].lower()
        ))
    elif sort_by == "type":
        # Sort by type (agency first), then by name
        results.sort(key=lambda r: (
            0 if r["entity_type"] == "agency" else 1,
            r["entity_name"].lower()
        ))
    elif sort_by == "market":
        # Sort by first market (nulls last), then by name
        results.sort(key=lambda r: (
            not r.get("markets"),
            (r.get("markets") or "").lower(),
            r["entity_name"].lower()
        ))
    elif sort_by == "last_active":
        # Sort by most recently active first (nulls last)
        # Use a tuple: (has_no_date, negative_date_for_desc_sort)
        results.sort(key=lambda r: (
            0 if r.get("last_active") else 1,  # nulls last
            "" if not r.get("last_active") else r.get("last_active")
        ), reverse=True)
        # Re-sort to put nulls at end (reverse messes this up)
        results.sort(key=lambda r: r.get("last_active") is None)
    elif sort_by == "revenue":
        # Sort by highest revenue first
        results.sort(key=lambda r: (
            -(r.get("total_revenue") or 0),
            r["entity_name"].lower()
        ))
    elif sort_by == "ae":
        # Sort by AE name (nulls last), then by name
        results.sort(key=lambda r: (
            r.get("assigned_ae") is None,
            (r.get("assigned_ae") or "").lower(),
            r["entity_name"].lower()
        ))
    else:
        # Default: sort by name
        results.sort(key=lambda r: r["entity_name"].lower())

    return jsonify(results)


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>")
def api_entity_detail(entity_type, entity_id):
    """Get full details for a single entity including all contacts and markets."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    with _db_ro() as conn:
        # Get entity
        if entity_type == "agency":
            entity = conn.execute("""
                SELECT agency_id as entity_id, 'agency' as entity_type, agency_name as entity_name,
                       address, city, state, zip, notes, assigned_ae,
                       NULL as sector_id, NULL as sector_name
                FROM agencies WHERE agency_id = ? AND is_active = 1
            """, [entity_id]).fetchone()
        else:
            entity = conn.execute("""
                SELECT c.customer_id as entity_id, 'customer' as entity_type, c.normalized_name as entity_name,
                       c.address, c.city, c.state, c.zip, c.notes, c.assigned_ae,
                       c.sector_id, s.sector_name
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.customer_id = ? AND c.is_active = 1
            """, [entity_id]).fetchone()

        if not entity:
            return jsonify({"error": "Entity not found"}), 404

        result = dict(entity)

        # Get contacts
        contacts = conn.execute("""
            SELECT contact_id, contact_name, contact_title, email, phone,
                   contact_role, is_primary, last_contacted
            FROM entity_contacts
            WHERE entity_type = ? AND entity_id = ? AND is_active = 1
            ORDER BY is_primary DESC, contact_name
        """, [entity_type, entity_id]).fetchall()

        result["contacts"] = [dict(c) for c in contacts]

        # Get markets where this entity has run spots
        if entity_type == "agency":
            markets = conn.execute("""
                SELECT DISTINCT market_name
                FROM spots
                WHERE agency_id = ? AND market_name IS NOT NULL AND market_name != ''
                ORDER BY market_name
            """, [entity_id]).fetchall()
        else:
            markets = conn.execute("""
                SELECT DISTINCT market_name
                FROM spots
                WHERE customer_id = ? AND market_name IS NOT NULL AND market_name != ''
                ORDER BY market_name
            """, [entity_id]).fetchall()

        result["markets"] = [m["market_name"] for m in markets]

        return jsonify(result)


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/address", methods=["PUT"])
def api_update_address(entity_type, entity_id):
    """Update address for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}

    with _db_rw() as conn:
        try:
            table = "agencies" if entity_type == "agency" else "customers"
            id_col = "agency_id" if entity_type == "agency" else "customer_id"

            conn.execute(f"""
                UPDATE {table}
                SET address = ?, city = ?, state = ?, zip = ?
                WHERE {id_col} = ?
            """, [
                data.get("address"),
                data.get("city"),
                data.get("state"),
                data.get("zip"),
                entity_id
            ])

            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/notes", methods=["PUT"])
def api_update_notes(entity_type, entity_id):
    """Update notes for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    notes = data.get("notes")

    with _db_rw() as conn:
        try:
            table = "agencies" if entity_type == "agency" else "customers"
            id_col = "agency_id" if entity_type == "agency" else "customer_id"

            conn.execute(f"""
                UPDATE {table}
                SET notes = ?
                WHERE {id_col} = ?
            """, [notes, entity_id])

            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/sector", methods=["PUT"])
def api_update_sector(entity_type, entity_id):
    """Update sector for a customer (agencies don't have sectors)."""
    if entity_type != "customer":
        return jsonify({"error": "Only customers have sectors"}), 400

    data = request.get_json() or {}
    sector_id = data.get("sector_id")

    # Allow null/None to clear sector
    if sector_id is not None:
        try:
            sector_id = int(sector_id) if sector_id else None
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid sector_id"}), 400

    with _db_rw() as conn:
        try:
            conn.execute("""
                UPDATE customers
                SET sector_id = ?
                WHERE customer_id = ?
            """, [sector_id, entity_id])

            conn.commit()

            # Return the new sector name
            if sector_id:
                sector = conn.execute(
                    "SELECT sector_name FROM sectors WHERE sector_id = ?",
                    [sector_id]
                ).fetchone()
                sector_name = sector["sector_name"] if sector else None
            else:
                sector_name = None

            return jsonify({"success": True, "sector_name": sector_name})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/agency/<int:agency_id>/customers")
def api_agency_customers(agency_id):
    """Get customers associated with an agency via spots data."""
    with _db_ro() as conn:
        # First verify agency exists
        agency = conn.execute(
            "SELECT agency_name FROM agencies WHERE agency_id = ? AND is_active = 1",
            [agency_id]
        ).fetchone()
        if not agency:
            return jsonify({"error": "Agency not found"}), 404

        # Collect all known names for this agency (canonical + aliases)
        agency_names = [agency["agency_name"]]
        alias_rows = conn.execute("""
            SELECT alias_name FROM entity_aliases
            WHERE entity_type = 'agency' AND target_entity_id = ? AND is_active = 1
        """, [agency_id]).fetchall()
        for ar in alias_rows:
            agency_names.append(ar["alias_name"])

        # Get customers booked through this agency (via spots)
        spot_customers = conn.execute("""
            SELECT
                c.customer_id,
                c.normalized_name as customer_name,
                c.sector_id,
                s.sector_name,
                SUM(CASE WHEN sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL
                    THEN sp.gross_rate ELSE 0 END) as revenue_via_agency,
                COUNT(sp.spot_id) as spot_count,
                MAX(sp.air_date) as last_active
            FROM spots sp
            JOIN customers c ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.agency_id = ?
                AND c.is_active = 1
            GROUP BY c.customer_id
        """, [agency_id]).fetchall()

        seen_ids = set()
        result_customers = []
        for c in spot_customers:
            result_customers.append(dict(c))
            seen_ids.add(c["customer_id"])

        # Also find customers whose name matches any agency name variant + ':'
        for name in agency_names:
            name_customers = conn.execute("""
                SELECT
                    c.customer_id,
                    c.normalized_name as customer_name,
                    c.sector_id,
                    s.sector_name,
                    COALESCE((SELECT SUM(CASE WHEN sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL
                        THEN sp.gross_rate ELSE 0 END) FROM spots sp WHERE sp.customer_id = c.customer_id), 0) as revenue_via_agency,
                    COALESCE((SELECT COUNT(*) FROM spots sp WHERE sp.customer_id = c.customer_id), 0) as spot_count,
                    (SELECT MAX(sp.air_date) FROM spots sp WHERE sp.customer_id = c.customer_id) as last_active
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
                  AND c.normalized_name LIKE ? || ':%'
            """, [name]).fetchall()

            for c in name_customers:
                if c["customer_id"] not in seen_ids:
                    result_customers.append(dict(c))
                    seen_ids.add(c["customer_id"])

        # Sort by revenue descending
        result_customers.sort(key=lambda x: -(x.get("revenue_via_agency") or 0))

        return jsonify({
            "agency_id": agency_id,
            "agency_name": agency["agency_name"],
            "customers": result_customers
        })


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/spots-link")
def api_spots_link(entity_type, entity_id):
    """Return URL to filtered spots view for this entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    # Build URL to datasette or report page with filter
    if entity_type == "customer":
        url = f"/datasette/dev/spots?customer_id={entity_id}"
    else:
        url = f"/datasette/dev/spots?agency_id={entity_id}"

    return jsonify({"url": url})


# ============================================================
# Saved Filters
# ============================================================

@address_book_bp.route("/api/address-book/filters")
def api_get_filters():
    """Get saved filter presets."""
    with _db_ro() as conn:
        filters = conn.execute("""
            SELECT filter_id, filter_name, filter_config, created_by, created_date, is_shared
            FROM saved_filters
            WHERE filter_type = 'address_book'
            ORDER BY filter_name
        """).fetchall()
        result = []
        for f in filters:
            row = dict(f)
            row["filter_config"] = json.loads(row["filter_config"]) if row["filter_config"] else {}
            result.append(row)
        return jsonify(result)


@address_book_bp.route("/api/address-book/filters", methods=["POST"])
def api_save_filter():
    """Save a filter preset."""
    data = request.get_json() or {}
    filter_name = data.get("filter_name", "").strip()
    filter_config = data.get("filter_config", {})

    if not filter_name:
        return jsonify({"error": "Filter name required"}), 400

    with _db_rw() as conn:
        try:
            conn.execute("""
                INSERT INTO saved_filters (filter_name, filter_type, filter_config, created_by, is_shared)
                VALUES (?, 'address_book', ?, ?, ?)
            """, [filter_name, json.dumps(filter_config), "web_user", data.get("is_shared", False)])
            conn.commit()
            filter_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            return jsonify({"success": True, "filter_id": filter_id})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/filters/<int:filter_id>", methods=["DELETE"])
def api_delete_filter(filter_id):
    """Delete a saved filter."""
    with _db_rw() as conn:
        try:
            conn.execute("DELETE FROM saved_filters WHERE filter_id = ?", [filter_id])
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# CSV Export
# ============================================================

@address_book_bp.route("/api/address-book/export")
def api_export_csv():
    """Export filtered address book as CSV for mailing lists."""
    # Get filter params (same as main endpoint)
    search = request.args.get("search", "").strip()
    entity_type = request.args.get("type", "all")
    has_contacts = request.args.get("has_contacts", "all")
    has_address = request.args.get("has_address", "all")
    sector_filter = request.args.get("sector_id", "")
    market_filter = request.args.get("market", "")
    ae_filter = request.args.get("ae", "")

    results = []

    with _db_ro() as conn:
        # Get agency-booked customer IDs to exclude
        agency_client_ids = _get_agency_client_ids(conn)

        # Get metrics lookups
        agency_metrics = {}
        rows = conn.execute("""
            SELECT agency_id,
                MAX(air_date) as last_active,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                COUNT(*) as spot_count
            FROM spots WHERE agency_id IS NOT NULL GROUP BY agency_id
        """).fetchall()
        for row in rows:
            agency_metrics[row["agency_id"]] = dict(row)

        customer_metrics = {}
        rows = conn.execute("""
            SELECT customer_id,
                MAX(air_date) as last_active,
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                    THEN gross_rate ELSE 0 END) as total_revenue,
                COUNT(*) as spot_count
            FROM spots WHERE customer_id IS NOT NULL GROUP BY customer_id
        """).fetchall()
        for row in rows:
            customer_metrics[row["customer_id"]] = dict(row)

        # Get agencies
        if entity_type in ("all", "agency"):
            agencies = conn.execute("""
                SELECT a.agency_id as entity_id, 'agency' as entity_type, a.agency_name as entity_name,
                       a.address, a.city, a.state, a.zip, a.notes, a.assigned_ae, NULL as sector_name,
                       (SELECT contact_name FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact,
                       (SELECT email FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_email,
                       (SELECT phone FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_phone,
                       (SELECT GROUP_CONCAT(DISTINCT market_name) FROM spots
                        WHERE agency_id = a.agency_id AND market_name IS NOT NULL) as markets
                FROM agencies a WHERE a.is_active = 1
            """).fetchall()
            for a in agencies:
                row = dict(a)
                m = agency_metrics.get(row["entity_id"], {})
                row["last_active"] = m.get("last_active", "")
                row["total_revenue"] = m.get("total_revenue", 0)
                results.append(row)

        # Get customers (exclude agency-booked)
        if entity_type in ("all", "customer"):
            customers = conn.execute("""
                SELECT c.customer_id as entity_id, 'customer' as entity_type, c.normalized_name as entity_name,
                       c.address, c.city, c.state, c.zip, c.notes, c.assigned_ae, s.sector_name,
                       (SELECT contact_name FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact,
                       (SELECT email FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_email,
                       (SELECT phone FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_phone,
                       (SELECT GROUP_CONCAT(DISTINCT market_name) FROM spots
                        WHERE customer_id = c.customer_id AND market_name IS NOT NULL) as markets
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
            """).fetchall()
            for c in customers:
                row = dict(c)
                # Skip agency-booked customers
                if row["entity_id"] in agency_client_ids:
                    continue
                m = customer_metrics.get(row["entity_id"], {})
                row["last_active"] = m.get("last_active", "")
                row["total_revenue"] = m.get("total_revenue", 0)
                results.append(row)

    # Apply filters
    if search:
        q = search.lower()
        results = [r for r in results if q in r["entity_name"].lower() or q in (r.get("notes") or "").lower()]

    if has_contacts == "yes":
        results = [r for r in results if r.get("primary_contact")]
    elif has_contacts == "no":
        results = [r for r in results if not r.get("primary_contact")]

    if has_address == "yes":
        results = [r for r in results if r.get("address") or r.get("city")]
    elif has_address == "no":
        results = [r for r in results if not r.get("address") and not r.get("city")]

    if sector_filter:
        results = [r for r in results if str(r.get("sector_id")) == sector_filter]

    if market_filter:
        results = [r for r in results if market_filter in (r.get("markets") or "")]

    if ae_filter:
        if ae_filter == "__none__":
            results = [r for r in results if not r.get("assigned_ae")]
        else:
            results = [r for r in results if r.get("assigned_ae") == ae_filter]

    # Sort by name
    results.sort(key=lambda r: r["entity_name"].lower())

    # Build CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Entity Name", "Type", "Sector", "Assigned AE", "Primary Contact", "Email", "Phone",
        "Address", "City", "State", "ZIP", "Markets", "Last Active", "Total Revenue", "Notes"
    ])

    for r in results:
        writer.writerow([
            r.get("entity_name", ""),
            r.get("entity_type", ""),
            r.get("sector_name", ""),
            r.get("assigned_ae", ""),
            r.get("primary_contact", ""),
            r.get("primary_email", ""),
            r.get("primary_phone", ""),
            r.get("address", ""),
            r.get("city", ""),
            r.get("state", ""),
            r.get("zip", ""),
            r.get("markets", ""),
            r.get("last_active", ""),
            f"${r.get('total_revenue', 0):,.2f}" if r.get("total_revenue") else "",
            r.get("notes", "")
        ])

    csv_content = output.getvalue()
    return Response(
        csv_content,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=address_book_export.csv"}
    )


# ============================================================
# Activity Log
# ============================================================

VALID_ACTIVITY_TYPES = ['note', 'call', 'email', 'meeting', 'status_change']


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/activities")
def api_get_activities(entity_type, entity_id):
    """Get activity log for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    limit = request.args.get("limit", 50, type=int)

    with _db_ro() as conn:
        activities = conn.execute("""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.activity_type,
                ea.activity_date,
                ea.description,
                ea.created_by,
                ea.created_date,
                ea.contact_id,
                ec.contact_name
            FROM entity_activity ea
            LEFT JOIN entity_contacts ec ON ea.contact_id = ec.contact_id
            WHERE ea.entity_type = ? AND ea.entity_id = ?
            ORDER BY ea.activity_date DESC
            LIMIT ?
        """, [entity_type, entity_id, limit]).fetchall()

        return jsonify([dict(a) for a in activities])


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/activities", methods=["POST"])
def api_create_activity(entity_type, entity_id):
    """Create a new activity log entry."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    activity_type = data.get("activity_type", "").strip()
    description = data.get("description", "").strip()
    contact_id = data.get("contact_id")

    if not activity_type:
        return jsonify({"error": "activity_type is required"}), 400

    if activity_type not in VALID_ACTIVITY_TYPES:
        return jsonify({"error": f"Invalid activity_type. Must be one of: {VALID_ACTIVITY_TYPES}"}), 400

    with _db_rw() as conn:
        try:
            # Verify entity exists
            if entity_type == "agency":
                exists = conn.execute(
                    "SELECT 1 FROM agencies WHERE agency_id = ? AND is_active = 1",
                    [entity_id]
                ).fetchone()
            else:
                exists = conn.execute(
                    "SELECT 1 FROM customers WHERE customer_id = ? AND is_active = 1",
                    [entity_id]
                ).fetchone()

            if not exists:
                return jsonify({"error": f"{entity_type} not found"}), 404

            # Insert activity
            conn.execute("""
                INSERT INTO entity_activity
                    (entity_type, entity_id, activity_type, description, created_by, contact_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [entity_type, entity_id, activity_type, description or None, "web_user", contact_id])

            activity_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()

            return jsonify({
                "success": True,
                "activity_id": activity_id,
                "activity_type": activity_type
            }), 201

        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


# ============================================================
# AE Assignment
# ============================================================

@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/ae", methods=["PUT"])
def api_update_ae(entity_type, entity_id):
    """Update assigned AE for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    assigned_ae = (data.get("assigned_ae") or "").strip() or None

    with _db_rw() as conn:
        try:
            table = "agencies" if entity_type == "agency" else "customers"
            id_col = "agency_id" if entity_type == "agency" else "customer_id"
            name_col = "agency_name" if entity_type == "agency" else "normalized_name"

            # Get current value for audit
            row = conn.execute(
                f"SELECT {name_col} as name, assigned_ae FROM {table} WHERE {id_col} = ?",
                [entity_id]
            ).fetchone()
            if not row:
                return jsonify({"error": "Entity not found"}), 404

            old_ae = row["assigned_ae"]

            conn.execute(
                f"UPDATE {table} SET assigned_ae = ? WHERE {id_col} = ?",
                [assigned_ae, entity_id]
            )

            # Audit the change
            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'AE_ASSIGN', ?, ?, ?)
            """, [
                "web_user",
                f"{entity_type}:{entity_id}",
                assigned_ae or "(cleared)",
                f"name={row['name']}|old_ae={old_ae or '(none)'}|new_ae={assigned_ae or '(none)'}"
            ])

            conn.commit()
            return jsonify({"success": True, "assigned_ae": assigned_ae})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/ae-list")
def api_ae_list():
    """Get sorted list of AE names from spots data and existing assignments."""
    with _db_ro() as conn:
        # Get distinct sales_person values from spots
        spot_aes = conn.execute("""
            SELECT DISTINCT sales_person
            FROM spots
            WHERE sales_person IS NOT NULL AND sales_person != ''
            ORDER BY sales_person
        """).fetchall()

        # Get distinct assigned_ae values from agencies and customers
        assigned_aes = conn.execute("""
            SELECT DISTINCT assigned_ae FROM agencies
            WHERE assigned_ae IS NOT NULL AND assigned_ae != ''
            UNION
            SELECT DISTINCT assigned_ae FROM customers
            WHERE assigned_ae IS NOT NULL AND assigned_ae != ''
        """).fetchall()

        # Combine and deduplicate
        ae_set = set()
        for row in spot_aes:
            ae_set.add(row["sales_person"])
        for row in assigned_aes:
            ae_set.add(row["assigned_ae"])

        return jsonify(sorted(ae_set, key=str.lower))
