# src/web/routes/address_book.py
"""
Unified Address Book - view and manage contacts/addresses for agencies and customers.
"""

from flask import Blueprint, render_template, jsonify, request, current_app, Response
import sqlite3
import json
import csv
import io
import math
from datetime import date, datetime
from src.services.customer_resolution_service import _score_name
from contextlib import contextmanager

address_book_bp = Blueprint("address_book", __name__)

import logging
logger = logging.getLogger(__name__)


def _get_db_path():
    return current_app.config.get("DB_PATH") or "./.data/dev.db"


def refresh_entity_metrics(conn):
    """Rebuild entity_metrics from spots data. Callable from import service."""
    conn.execute("DELETE FROM entity_metrics")
    conn.execute("""
        INSERT INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count)
        SELECT
            'agency', agency_id,
            GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
            MAX(air_date),
            SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
            COUNT(*)
        FROM spots WHERE agency_id IS NOT NULL
        GROUP BY agency_id
    """)
    conn.execute("""
        INSERT INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count, agency_spot_count)
        SELECT
            'customer', customer_id,
            GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
            MAX(air_date),
            SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
            COUNT(*),
            COUNT(agency_id)
        FROM spots WHERE customer_id IS NOT NULL
        GROUP BY customer_id
    """)


def _fmt_revenue(val):
    """Format revenue for signal labels: $1.2M / $145K / $800."""
    if val is None:
        return "$0"
    v = abs(val)
    if v >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    elif v >= 1_000:
        return f"${v/1_000:.0f}K"
    else:
        return f"${v:,.0f}"


def refresh_entity_signals(conn):
    """Rebuild entity_signals from spots data. Callable from import service."""
    conn.execute("DELETE FROM entity_signals")
    today = date.today().isoformat()

    _SIGNAL_QUERY = """
        SELECT {id_col} as entity_id,
          SUM(CASE WHEN air_date >= date('now','-12 months') AND air_date <= date('now')
                   AND (revenue_type != 'Trade' OR revenue_type IS NULL) THEN gross_rate ELSE 0 END) as trailing_12m,
          SUM(CASE WHEN air_date >= date('now','-24 months') AND air_date < date('now','-12 months')
                   AND (revenue_type != 'Trade' OR revenue_type IS NULL) THEN gross_rate ELSE 0 END) as prior_12m,
          SUM(CASE WHEN air_date > date('now')
                   AND (revenue_type != 'Trade' OR revenue_type IS NULL) THEN gross_rate ELSE 0 END) as future_rev,
          SUM(CASE WHEN air_date > date('now') THEN 1 ELSE 0 END) as future_spots,
          SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END) as lifetime_rev,
          MIN(air_date) as first_spot,
          MAX(CASE WHEN air_date <= date('now') THEN air_date END) as last_past_spot,
          COUNT(DISTINCT CASE WHEN air_date >= date('now','-24 months') AND air_date <= date('now')
                THEN strftime('%Y-%m', air_date) END) as active_months_24m
        FROM spots WHERE {id_col} IS NOT NULL GROUP BY {id_col}
    """

    rows_to_insert = []

    for entity_type, id_col in [("agency", "agency_id"), ("customer", "customer_id")]:
        query = _SIGNAL_QUERY.format(id_col=id_col)
        for row in conn.execute(query).fetchall():
            eid = row["entity_id"]
            trailing = row["trailing_12m"] or 0
            prior = row["prior_12m"] or 0
            future_rev = row["future_rev"] or 0
            future_spots = row["future_spots"] or 0
            lifetime = row["lifetime_rev"] or 0
            first_spot = row["first_spot"]
            last_past = row["last_past_spot"]
            active_months = row["active_months_24m"] or 0

            # --- Signal 1: Churned ---
            # prior_12m >= $10K AND trailing + future == 0 AND no future spots
            if prior >= 10_000 and (trailing + future_rev) == 0 and future_spots == 0:
                priority = 1
                label = f"{_fmt_revenue(prior)} prior year \u2192 $0"
                rows_to_insert.append((entity_type, eid, "churned", label, priority, trailing, prior))

            # --- Signal 2: Declining ---
            # prior_12m >= $10K AND trailing < prior * 0.70
            # Suppress if future_rev >= gap * 0.50
            elif prior >= 10_000 and trailing > 0 and trailing < prior * 0.70:
                gap = prior - trailing
                if future_rev < gap * 0.50:
                    pct = round((1 - trailing / prior) * 100)
                    priority = 2
                    label = f"{_fmt_revenue(prior)} \u2192 {_fmt_revenue(trailing)} (-{pct}%)"
                    rows_to_insert.append((entity_type, eid, "declining", label, priority, trailing, prior))

            # --- Signal 3: Gone Quiet ---
            # lifetime >= $10K, days since last_past_spot exceeds tier threshold, no future spots
            if lifetime >= 10_000 and last_past and future_spots == 0:
                try:
                    last_date = date.fromisoformat(last_past)
                    days_quiet = (date.today() - last_date).days
                except (ValueError, TypeError):
                    days_quiet = 0

                # Determine tier threshold
                if active_months >= 20:
                    threshold = 90
                    cadence = "books monthly"
                elif active_months >= 12:
                    threshold = 120
                    cadence = "books regularly"
                elif active_months >= 6:
                    threshold = 240
                    cadence = "books seasonally"
                else:
                    threshold = None  # Skip occasional/new accounts
                    cadence = ""

                if threshold and days_quiet > threshold:
                    # Don't duplicate if already flagged churned
                    if not any(r[0] == entity_type and r[1] == eid and r[2] == "churned" for r in rows_to_insert):
                        priority = 3
                        label = f"Quiet {days_quiet}d \u00b7 {cadence}"
                        rows_to_insert.append((entity_type, eid, "gone_quiet", label, priority, trailing, prior))

            # --- Signal 4: New Account ---
            # first_spot within 12 months AND lifetime >= $5K
            if first_spot and lifetime >= 5_000:
                try:
                    first_date = date.fromisoformat(first_spot)
                    months_since_first = (date.today().year - first_date.year) * 12 + (date.today().month - first_date.month)
                except (ValueError, TypeError):
                    months_since_first = 999

                if months_since_first <= 12:
                    priority = 4
                    month_str = first_date.strftime("%b %Y")
                    label = f"New \u00b7 first booked {month_str} \u00b7 {_fmt_revenue(lifetime)}"
                    rows_to_insert.append((entity_type, eid, "new_account", label, priority, trailing, prior))

            # --- Signal 5: Growing ---
            # trailing >= $10K AND prior > 0 AND trailing > prior * 1.30
            if trailing >= 10_000 and prior > 0 and trailing > prior * 1.30:
                pct = round((trailing / prior - 1) * 100)
                priority = 5
                label = f"{_fmt_revenue(prior)} \u2192 {_fmt_revenue(trailing)} (+{pct}%)"
                rows_to_insert.append((entity_type, eid, "growing", label, priority, trailing, prior))

    if rows_to_insert:
        conn.executemany("""
            INSERT INTO entity_signals (entity_type, entity_id, signal_type, signal_label, signal_priority, trailing_revenue, prior_revenue)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)


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


@address_book_bp.route("/address-book")
def address_book_page():
    """Render the unified address book page."""
    return render_template("address_book.html")


@address_book_bp.route("/address-book/guide")
def address_book_guide():
    """Render the address book feature guide."""
    return render_template("address_book_guide.html")


@address_book_bp.route("/api/address-book/sectors")
def api_sectors():
    """Get list of all sectors for dropdown."""
    with _db_ro() as conn:
        sectors = conn.execute("""
            SELECT sector_id, sector_code, sector_name, sector_group
            FROM sectors
            WHERE is_active = 1
            ORDER BY CASE sector_group
                WHEN 'Commercial' THEN 1 WHEN 'Financial' THEN 2
                WHEN 'Healthcare' THEN 3 WHEN 'Outreach' THEN 4
                WHEN 'Political' THEN 5 WHEN 'Other' THEN 6 ELSE 7
            END, sector_name
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
    Get all entities (agencies + customers) with contacts, sectors, markets, and metrics.
    Filtering/sorting is done client-side. Only include_inactive controls the SQL WHERE.
    """
    include_inactive = request.args.get("include_inactive", "0") == "1"

    results = []

    with _db_ro() as conn:
        # Batch: contact stats for all entities (fixes N+1)
        contact_stats = {}
        for row in conn.execute("""
            SELECT entity_type, entity_id, COUNT(*) as contact_count,
                   MAX(CASE WHEN is_primary = 1 THEN contact_name END) as primary_contact
            FROM entity_contacts WHERE is_active = 1
            GROUP BY entity_type, entity_id
        """).fetchall():
            contact_stats[(row["entity_type"], row["entity_id"])] = {
                "contact_count": row["contact_count"],
                "primary_contact": row["primary_contact"]
            }

        # Batch: sector counts + sector_ids per customer (fixes N+1)
        sector_counts = {}
        customer_sector_ids = {}
        for row in conn.execute("""
            SELECT customer_id, COUNT(*) as cnt, GROUP_CONCAT(sector_id) as sids
            FROM customer_sectors GROUP BY customer_id
        """).fetchall():
            sector_counts[row["customer_id"]] = row["cnt"]
            customer_sector_ids[row["customer_id"]] = row["sids"] or ""

        # Read pre-computed metrics from entity_metrics cache
        metrics_count = conn.execute("SELECT COUNT(*) FROM entity_metrics").fetchone()[0]
        if metrics_count == 0:
            # Safety net: auto-refresh if table exists but is empty (first load after migration)
            # Need a RW connection since the main conn is readonly
            logger.info("entity_metrics empty — refreshing inline")
            with _db_rw() as rw_conn:
                refresh_entity_metrics(rw_conn)
                refresh_entity_signals(rw_conn)
                rw_conn.commit()

        # Safety net for signals table
        signals_count = conn.execute("SELECT COUNT(*) FROM entity_signals").fetchone()[0]
        if signals_count == 0 and metrics_count > 0:
            logger.info("entity_signals empty — refreshing inline")
            with _db_rw() as rw_conn:
                refresh_entity_signals(rw_conn)
                rw_conn.commit()

        agency_markets = {}
        agency_metrics = {}
        customer_markets = {}
        customer_metrics = {}
        agency_client_ids_from_spots = set()

        for row in conn.execute("SELECT * FROM entity_metrics").fetchall():
            eid = row["entity_id"]
            if row["entity_type"] == "agency":
                agency_markets[eid] = row["markets"] or ""
                agency_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "spot_count": row["spot_count"]
                }
            else:
                customer_markets[eid] = row["markets"] or ""
                customer_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "spot_count": row["spot_count"]
                }
                if row["agency_spot_count"] == row["spot_count"]:
                    agency_client_ids_from_spots.add(eid)

        # Load entity signals into lookup dict
        entity_signals = {}  # {(type, id): [{"signal_type":..., "signal_label":..., "signal_priority":...}]}
        for srow in conn.execute(
            "SELECT entity_type, entity_id, signal_type, signal_label, signal_priority FROM entity_signals ORDER BY signal_priority"
        ).fetchall():
            key = (srow["entity_type"], srow["entity_id"])
            if key not in entity_signals:
                entity_signals[key] = []
            entity_signals[key].append({
                "signal_type": srow["signal_type"],
                "signal_label": srow["signal_label"],
                "signal_priority": srow["signal_priority"]
            })

        # Get agencies (no sector for agencies)
        active_clause = "" if include_inactive else "WHERE a.is_active = 1"
        agencies = conn.execute(f"""
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
                a.is_active,
                NULL as sector_id,
                NULL as sector_name,
                NULL as sector_code
            FROM agencies a
            {active_clause}
            ORDER BY a.agency_name
        """).fetchall()

        for a in agencies:
            row = dict(a)
            cs = contact_stats.get(("agency", row["entity_id"]), {})
            row["contact_count"] = cs.get("contact_count", 0)
            row["primary_contact"] = cs.get("primary_contact")
            row["sector_count"] = 0
            row["sector_ids"] = ""
            row["markets"] = agency_markets.get(row["entity_id"], "")
            metrics = agency_metrics.get(row["entity_id"], {})
            row["last_active"] = metrics.get("last_active")
            row["total_revenue"] = metrics.get("total_revenue", 0)
            row["spot_count"] = metrics.get("spot_count", 0)
            row["signals"] = entity_signals.get(("agency", row["entity_id"]), [])
            results.append(row)

        # Get customers with sector info (exclude agency-booked)
        active_clause = "" if include_inactive else "WHERE c.is_active = 1"
        customers = conn.execute(f"""
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
                c.is_active,
                c.sector_id,
                s.sector_name,
                s.sector_code
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            {active_clause}
            ORDER BY c.normalized_name
        """).fetchall()

        for c in customers:
            row = dict(c)
            if ':' in row["entity_name"] or row["entity_id"] in agency_client_ids_from_spots:
                continue
            cs = contact_stats.get(("customer", row["entity_id"]), {})
            row["contact_count"] = cs.get("contact_count", 0)
            row["primary_contact"] = cs.get("primary_contact")
            row["sector_count"] = sector_counts.get(row["entity_id"], 0)
            row["sector_ids"] = customer_sector_ids.get(row["entity_id"], "")
            row["markets"] = customer_markets.get(row["entity_id"], "")
            metrics = customer_metrics.get(row["entity_id"], {})
            row["last_active"] = metrics.get("last_active")
            row["total_revenue"] = metrics.get("total_revenue", 0)
            row["spot_count"] = metrics.get("spot_count", 0)
            row["signals"] = entity_signals.get(("customer", row["entity_id"]), [])
            results.append(row)

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
                       po_number, edi_billing,
                       commission_rate, order_rate_basis,
                       NULL as sector_id, NULL as sector_name
                FROM agencies WHERE agency_id = ? AND is_active = 1
            """, [entity_id]).fetchone()
        else:
            entity = conn.execute("""
                SELECT c.customer_id as entity_id, 'customer' as entity_type, c.normalized_name as entity_name,
                       c.address, c.city, c.state, c.zip, c.notes, c.assigned_ae,
                       c.po_number, c.edi_billing, c.affidavit_required,
                       c.sector_id, s.sector_name,
                       c.agency_id, a.agency_name
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                LEFT JOIN agencies a ON c.agency_id = a.agency_id
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

        # Get additional addresses
        addresses = conn.execute("""
            SELECT address_id, address_label, address, city, state, zip,
                   is_primary, notes
            FROM entity_addresses
            WHERE entity_type = ? AND entity_id = ? AND is_active = 1
            ORDER BY is_primary DESC, address_label
        """, [entity_type, entity_id]).fetchall()

        result["addresses"] = [dict(a) for a in addresses]

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

        # Get all sectors from junction table (customers only)
        if entity_type == "customer":
            sectors = conn.execute("""
                SELECT cs.sector_id, s.sector_name, s.sector_code, cs.is_primary
                FROM customer_sectors cs
                JOIN sectors s ON cs.sector_id = s.sector_id
                WHERE cs.customer_id = ?
                ORDER BY cs.is_primary DESC, s.sector_name
            """, [entity_id]).fetchall()
            result["sectors"] = [dict(s) for s in sectors]

        # Get signals for this entity
        signals = conn.execute("""
            SELECT signal_type, signal_label, signal_priority
            FROM entity_signals
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY signal_priority
        """, [entity_type, entity_id]).fetchall()
        result["signals"] = [dict(s) for s in signals]

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


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/billing-info", methods=["PUT"])
def api_update_billing_info(entity_type, entity_id):
    """Update PO number and EDI billing flag for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    po_number = (data.get("po_number") or "").strip() or None
    edi_billing = 1 if data.get("edi_billing") else 0
    affidavit_required = 1 if data.get("affidavit_required") else 0

    with _db_rw() as conn:
        try:
            table = "agencies" if entity_type == "agency" else "customers"
            id_col = "agency_id" if entity_type == "agency" else "customer_id"

            if entity_type == "customer":
                conn.execute(f"""
                    UPDATE {table}
                    SET po_number = ?, edi_billing = ?, affidavit_required = ?
                    WHERE {id_col} = ?
                """, [po_number, edi_billing, affidavit_required, entity_id])
            else:
                # Parse commission fields (agency only)
                commission_rate = data.get("commission_rate")
                if commission_rate is not None and commission_rate != "":
                    try:
                        commission_rate = float(commission_rate)
                    except (ValueError, TypeError):
                        return jsonify({"error": "Commission rate must be a number"}), 400
                    if not (0 <= commission_rate <= 100):
                        return jsonify({"error": "Commission rate must be 0-100"}), 400
                else:
                    commission_rate = None

                order_rate_basis = data.get("order_rate_basis") or None
                if order_rate_basis is not None and order_rate_basis not in ("gross", "net"):
                    return jsonify({"error": "Order rate basis must be 'gross' or 'net'"}), 400

                conn.execute(f"""
                    UPDATE {table}
                    SET po_number = ?, edi_billing = ?, commission_rate = ?, order_rate_basis = ?
                    WHERE {id_col} = ?
                """, [po_number, edi_billing, commission_rate, order_rate_basis, entity_id])

            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/sector", methods=["PUT"])
def api_update_sector(entity_type, entity_id):
    """Update primary sector for a customer via junction table (backward compat)."""
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
            if sector_id:
                # Upsert into junction table as primary; triggers sync customers.sector_id
                conn.execute("""
                    INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
                    VALUES (?, ?, 1, 'web_user')
                    ON CONFLICT(customer_id, sector_id) DO UPDATE SET is_primary = 1
                """, [entity_id, sector_id])
            else:
                # Clear all sectors
                conn.execute(
                    "DELETE FROM customer_sectors WHERE customer_id = ?",
                    [entity_id]
                )
                # Trigger handles NULL cache, but be explicit for no-row case
                conn.execute(
                    "UPDATE customers SET sector_id = NULL WHERE customer_id = ?",
                    [entity_id]
                )

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


@address_book_bp.route("/api/address-book/customer/<int:entity_id>/sectors", methods=["PUT"])
def api_update_sectors(entity_id):
    """Replace all sector assignments for a customer.
    Body: { "sectors": [{"sector_id": 1, "is_primary": true}, {"sector_id": 3}] }
    """
    data = request.get_json() or {}
    sectors = data.get("sectors", [])

    # Validate input
    if not isinstance(sectors, list):
        return jsonify({"error": "sectors must be an array"}), 400

    primary_count = sum(1 for s in sectors if s.get("is_primary"))
    if len(sectors) > 0 and primary_count != 1:
        return jsonify({"error": "Exactly one sector must be marked as primary"}), 400

    with _db_rw() as conn:
        try:
            # Get current state for audit
            old_sectors = conn.execute(
                "SELECT sector_id, is_primary FROM customer_sectors WHERE customer_id = ?",
                [entity_id]
            ).fetchall()

            # Delete existing assignments
            conn.execute(
                "DELETE FROM customer_sectors WHERE customer_id = ?",
                [entity_id]
            )

            # If clearing all sectors, set cache to NULL explicitly
            if not sectors:
                conn.execute(
                    "UPDATE customers SET sector_id = NULL WHERE customer_id = ?",
                    [entity_id]
                )
            else:
                # Insert new assignments (primary first so trigger fires correctly)
                for s in sorted(sectors, key=lambda x: not x.get("is_primary", False)):
                    sid = int(s["sector_id"])
                    is_primary = 1 if s.get("is_primary") else 0
                    conn.execute("""
                        INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
                        VALUES (?, ?, ?, 'web_user')
                    """, [entity_id, sid, is_primary])

            # Audit
            old_ids = [r["sector_id"] for r in old_sectors]
            new_ids = [s["sector_id"] for s in sectors]
            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'SECTOR_ASSIGN', ?, ?, ?)
            """, [
                "web_user",
                f"customer:{entity_id}",
                f"sectors={new_ids}",
                f"old_sectors={old_ids}"
            ])

            conn.commit()

            # Return updated sectors
            rows = conn.execute("""
                SELECT cs.sector_id, s.sector_name, s.sector_code, cs.is_primary
                FROM customer_sectors cs
                JOIN sectors s ON cs.sector_id = s.sector_id
                WHERE cs.customer_id = ?
                ORDER BY cs.is_primary DESC, s.sector_name
            """, [entity_id]).fetchall()

            return jsonify({
                "success": True,
                "sectors": [dict(r) for r in rows]
            })
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/customer/<int:entity_id>/agency", methods=["PUT"])
def api_update_agency(entity_id):
    """Update agency assignment for a customer."""
    data = request.get_json() or {}
    agency_id = data.get("agency_id")

    if agency_id is not None:
        try:
            agency_id = int(agency_id) if agency_id else None
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid agency_id"}), 400

    if agency_id:
        with _db_ro() as conn:
            agency = conn.execute(
                "SELECT agency_name FROM agencies WHERE agency_id = ? AND is_active = 1",
                [agency_id]
            ).fetchone()
            if not agency:
                return jsonify({"error": "Agency not found or inactive"}), 400
            agency_name = agency["agency_name"]
    else:
        agency_name = None

    with _db_rw() as conn:
        try:
            conn.execute(
                "UPDATE customers SET agency_id = ? WHERE customer_id = ?",
                [agency_id, entity_id]
            )
            conn.commit()
            return jsonify({"success": True, "agency_name": agency_name})
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
                c.po_number,
                c.edi_billing,
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
                    c.po_number,
                    c.edi_billing,
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

        # Also include customers with agency_id directly assigned
        assigned_customers = conn.execute("""
            SELECT
                c.customer_id,
                c.normalized_name as customer_name,
                c.sector_id,
                s.sector_name,
                c.po_number,
                c.edi_billing,
                COALESCE((SELECT SUM(CASE WHEN sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL
                    THEN sp.gross_rate ELSE 0 END) FROM spots sp WHERE sp.customer_id = c.customer_id), 0) as revenue_via_agency,
                COALESCE((SELECT COUNT(*) FROM spots sp WHERE sp.customer_id = c.customer_id), 0) as spot_count,
                (SELECT MAX(sp.air_date) FROM spots sp WHERE sp.customer_id = c.customer_id) as last_active
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE c.agency_id = ? AND c.is_active = 1
        """, [agency_id]).fetchall()

        for c in assigned_customers:
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
# Create New Entity
# ============================================================

@address_book_bp.route("/api/address-book/entities", methods=["POST"])
def api_create_entity():
    """Create a new agency or advertiser (customer) with optional address, contact, and AE."""
    data = request.get_json() or {}
    entity_type = data.get("entity_type", "").strip()
    name = data.get("name", "").strip()
    sector_id = data.get("sector_id")
    agency_id = data.get("agency_id")
    notes = data.get("notes", "").strip() or None
    po_number = (data.get("po_number") or "").strip() or None
    affidavit_required = 1 if data.get("affidavit_required") else 0
    assigned_ae = (data.get("assigned_ae") or "").strip() or None
    # Agency commission fields
    commission_rate = data.get("commission_rate")
    if commission_rate is not None and commission_rate != "":
        try:
            commission_rate = float(commission_rate)
        except (ValueError, TypeError):
            commission_rate = None
        if commission_rate is not None and not (0 <= commission_rate <= 100):
            return jsonify({"error": "Commission rate must be 0-100"}), 400
    else:
        commission_rate = None
    order_rate_basis = data.get("order_rate_basis") or None
    if order_rate_basis is not None and order_rate_basis not in ("gross", "net"):
        return jsonify({"error": "Order rate basis must be 'gross' or 'net'"}), 400

    # Address fields
    address = (data.get("address") or "").strip() or None
    city = (data.get("city") or "").strip() or None
    state = (data.get("state") or "").strip() or None
    zip_code = (data.get("zip") or "").strip() or None

    # Contact fields
    contact_name = (data.get("contact_name") or "").strip() or None
    contact_title = (data.get("contact_title") or "").strip() or None
    contact_email = (data.get("contact_email") or "").strip() or None
    contact_phone = (data.get("contact_phone") or "").strip() or None
    contact_role = (data.get("contact_role") or "").strip() or None

    force = data.get("force", False)

    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "entity_type must be 'agency' or 'customer'"}), 400
    if not name:
        return jsonify({"error": "Name is required"}), 400

    with _db_rw() as conn:
        try:
            # Exact duplicate check (case-insensitive)
            if entity_type == "agency":
                existing = conn.execute(
                    "SELECT agency_id FROM agencies WHERE agency_name = ? COLLATE NOCASE",
                    [name]
                ).fetchone()
                if existing:
                    return jsonify({
                        "error": f"Agency '{name}' already exists",
                        "existing_id": existing["agency_id"]
                    }), 409

                # Fuzzy duplicate check (skip if user confirmed)
                if not force:
                    rows = conn.execute(
                        "SELECT agency_id, agency_name FROM agencies WHERE is_active = 1"
                    ).fetchall()
                    similar = []
                    for row in rows:
                        score = _score_name(name, row["agency_name"])
                        if score >= 0.60:
                            similar.append({"id": row["agency_id"], "name": row["agency_name"],
                                            "score": round(score * 100)})
                    if similar:
                        similar.sort(key=lambda x: x["score"], reverse=True)
                        return jsonify({
                            "needs_confirmation": True,
                            "similar_entities": similar[:5]
                        }), 200

                conn.execute("""
                    INSERT INTO agencies (agency_name, po_number, assigned_ae,
                                          address, city, state, zip, notes, is_active,
                                          commission_rate, order_rate_basis)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """, [name, po_number, assigned_ae, address, city, state, zip_code, notes,
                      commission_rate, order_rate_basis])
                entity_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            else:
                existing = conn.execute(
                    "SELECT customer_id FROM customers WHERE normalized_name = ? COLLATE NOCASE",
                    [name]
                ).fetchone()
                if existing:
                    return jsonify({
                        "error": f"Advertiser '{name}' already exists",
                        "existing_id": existing["customer_id"]
                    }), 409

                # Fuzzy duplicate check (skip if user confirmed)
                if not force:
                    rows = conn.execute(
                        "SELECT customer_id, normalized_name FROM customers WHERE is_active = 1"
                    ).fetchall()
                    similar = []
                    for row in rows:
                        score = _score_name(name, row["normalized_name"])
                        if score >= 0.60:
                            similar.append({"id": row["customer_id"], "name": row["normalized_name"],
                                            "score": round(score * 100)})
                    if similar:
                        similar.sort(key=lambda x: x["score"], reverse=True)
                        return jsonify({
                            "needs_confirmation": True,
                            "similar_entities": similar[:5]
                        }), 200

                # Validate sector_id if provided
                if sector_id is not None:
                    try:
                        sector_id = int(sector_id) if sector_id else None
                    except (ValueError, TypeError):
                        return jsonify({"error": "Invalid sector_id"}), 400

                # Validate agency_id if provided
                if agency_id is not None:
                    try:
                        agency_id = int(agency_id) if agency_id else None
                    except (ValueError, TypeError):
                        return jsonify({"error": "Invalid agency_id"}), 400
                    if agency_id:
                        agency_check = conn.execute(
                            "SELECT agency_id FROM agencies WHERE agency_id = ? AND is_active = 1",
                            [agency_id]
                        ).fetchone()
                        if not agency_check:
                            return jsonify({"error": "Selected agency does not exist or is inactive"}), 400

                conn.execute("""
                    INSERT INTO customers (normalized_name, sector_id, agency_id, po_number,
                                           affidavit_required, assigned_ae, address, city,
                                           state, zip, notes, is_active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, [name, sector_id, agency_id, po_number, affidavit_required,
                      assigned_ae, address, city, state, zip_code, notes])
                entity_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # Sector junction table (customers only) — trigger syncs customers.sector_id
            if entity_type == "customer" and sector_id:
                conn.execute("""
                    INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
                    VALUES (?, ?, 1, 'web_user')
                """, [entity_id, sector_id])

            # AE assignment history (same pattern as api_update_ae)
            if assigned_ae:
                conn.execute("""
                    INSERT INTO ae_assignments (entity_type, entity_id, ae_name, created_by)
                    VALUES (?, ?, ?, 'web_user')
                """, [entity_type, entity_id, assigned_ae])

            # Primary contact
            if contact_name:
                conn.execute("""
                    INSERT INTO entity_contacts
                        (entity_type, entity_id, contact_name, contact_title, email, phone,
                         is_primary, contact_role, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'web_user')
                """, [entity_type, entity_id, contact_name, contact_title,
                      contact_email, contact_phone, contact_role])

            # Audit trail
            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'CREATE_ENTITY', ?, ?, ?)
            """, [
                "web_user",
                f"{entity_type}:{entity_id}",
                name,
                f"type={entity_type}|sector_id={sector_id or 'none'}|agency_id={agency_id or 'none'}"
            ])

            conn.commit()
            return jsonify({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "name": name
            }), 201

        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/deactivate", methods=["POST"])
def api_deactivate_entity(entity_type, entity_id):
    """Soft-deactivate an entity (set is_active=0)."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    table = "agencies" if entity_type == "agency" else "customers"
    id_col = "agency_id" if entity_type == "agency" else "customer_id"
    name_col = "agency_name" if entity_type == "agency" else "normalized_name"

    with _db_rw() as conn:
        try:
            row = conn.execute(
                f"SELECT {name_col} AS name, is_active FROM {table} WHERE {id_col} = ?",
                [entity_id]
            ).fetchone()

            if not row:
                return jsonify({"error": "Entity not found"}), 404
            if not row["is_active"]:
                return jsonify({"error": "Entity is already inactive"}), 400

            conn.execute(
                f"UPDATE {table} SET is_active = 0 WHERE {id_col} = ?",
                [entity_id]
            )

            conn.execute("""
                INSERT INTO canon_audit (actor, action, key, value, extra)
                VALUES (?, 'DEACTIVATE_ENTITY', ?, ?, ?)
            """, [
                "web_user",
                f"{entity_type}:{entity_id}",
                row["name"],
                f"type={entity_type}"
            ])

            conn.commit()
            return jsonify({"success": True, "message": f"{row['name']} has been deactivated"})

        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


# ============================================================
# Additional Addresses CRUD
# ============================================================

VALID_ADDRESS_LABELS = ['Billing', 'Shipping', 'PO Box', 'Office', 'Other']


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/addresses")
def api_get_addresses(entity_type, entity_id):
    """Get active additional addresses for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    with _db_ro() as conn:
        addresses = conn.execute("""
            SELECT address_id, address_label, address, city, state, zip,
                   is_primary, notes
            FROM entity_addresses
            WHERE entity_type = ? AND entity_id = ? AND is_active = 1
            ORDER BY is_primary DESC, address_label
        """, [entity_type, entity_id]).fetchall()

        return jsonify([dict(a) for a in addresses])


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/addresses", methods=["POST"])
def api_create_address(entity_type, entity_id):
    """Create an additional address for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    data = request.get_json() or {}
    label = (data.get("address_label") or "").strip()
    if label not in VALID_ADDRESS_LABELS:
        return jsonify({"error": f"Invalid label. Must be one of: {VALID_ADDRESS_LABELS}"}), 400

    with _db_rw() as conn:
        try:
            conn.execute("""
                INSERT INTO entity_addresses
                    (entity_type, entity_id, address_label, address, city, state, zip,
                     is_primary, created_by, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                entity_type, entity_id, label,
                (data.get("address") or "").strip() or None,
                (data.get("city") or "").strip() or None,
                (data.get("state") or "").strip() or None,
                (data.get("zip") or "").strip() or None,
                1 if data.get("is_primary") else 0,
                "web_user",
                (data.get("notes") or "").strip() or None
            ])
            address_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.commit()
            return jsonify({"success": True, "address_id": address_id}), 201
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/addresses/<int:address_id>", methods=["PUT"])
def api_update_address_entry(address_id):
    """Update an additional address."""
    data = request.get_json() or {}

    with _db_rw() as conn:
        try:
            existing = conn.execute(
                "SELECT 1 FROM entity_addresses WHERE address_id = ? AND is_active = 1",
                [address_id]
            ).fetchone()
            if not existing:
                return jsonify({"error": "Address not found"}), 404

            label = (data.get("address_label") or "").strip()
            if label and label not in VALID_ADDRESS_LABELS:
                return jsonify({"error": f"Invalid label. Must be one of: {VALID_ADDRESS_LABELS}"}), 400

            conn.execute("""
                UPDATE entity_addresses
                SET address_label = COALESCE(?, address_label),
                    address = ?,
                    city = ?,
                    state = ?,
                    zip = ?,
                    is_primary = ?,
                    notes = ?,
                    updated_date = CURRENT_TIMESTAMP
                WHERE address_id = ?
            """, [
                label or None,
                (data.get("address") or "").strip() or None,
                (data.get("city") or "").strip() or None,
                (data.get("state") or "").strip() or None,
                (data.get("zip") or "").strip() or None,
                1 if data.get("is_primary") else 0,
                (data.get("notes") or "").strip() or None,
                address_id
            ])
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


@address_book_bp.route("/api/address-book/addresses/<int:address_id>", methods=["DELETE"])
def api_delete_address(address_id):
    """Soft-delete an additional address."""
    with _db_rw() as conn:
        try:
            conn.execute("""
                UPDATE entity_addresses
                SET is_active = 0, updated_date = CURRENT_TIMESTAMP
                WHERE address_id = ?
            """, [address_id])
            conn.commit()
            return jsonify({"success": True})
        except Exception as e:
            conn.rollback()
            return jsonify({"success": False, "error": str(e)}), 500


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
        # Read pre-computed metrics from entity_metrics cache
        metrics_count = conn.execute("SELECT COUNT(*) FROM entity_metrics").fetchone()[0]
        if metrics_count == 0:
            with _db_rw() as rw_conn:
                refresh_entity_metrics(rw_conn)
                rw_conn.commit()

        agency_metrics = {}
        customer_metrics = {}
        agency_client_ids_from_spots = set()

        for row in conn.execute("SELECT * FROM entity_metrics").fetchall():
            eid = row["entity_id"]
            if row["entity_type"] == "agency":
                agency_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "markets": row["markets"] or ""
                }
            else:
                customer_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "markets": row["markets"] or ""
                }
                if row["agency_spot_count"] == row["spot_count"]:
                    agency_client_ids_from_spots.add(eid)

        # Get agencies
        if entity_type in ("all", "agency"):
            agencies = conn.execute("""
                SELECT a.agency_id as entity_id, 'agency' as entity_type, a.agency_name as entity_name,
                       a.address, a.city, a.state, a.zip, a.notes, a.assigned_ae,
                       a.po_number, a.edi_billing, NULL as sector_name,
                       (SELECT contact_name FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact,
                       (SELECT email FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_email,
                       (SELECT phone FROM entity_contacts ec
                        WHERE ec.entity_type = 'agency' AND ec.entity_id = a.agency_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_phone
                FROM agencies a WHERE a.is_active = 1
            """).fetchall()
            for a in agencies:
                row = dict(a)
                m = agency_metrics.get(row["entity_id"], {})
                row["last_active"] = m.get("last_active", "")
                row["total_revenue"] = m.get("total_revenue", 0)
                row["markets"] = m.get("markets", "")
                results.append(row)

        # Get customers (exclude agency-booked)
        if entity_type in ("all", "customer"):
            customers = conn.execute("""
                SELECT c.customer_id as entity_id, 'customer' as entity_type, c.normalized_name as entity_name,
                       c.address, c.city, c.state, c.zip, c.notes, c.assigned_ae,
                       c.po_number, c.edi_billing, s.sector_name,
                       (SELECT contact_name FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_contact,
                       (SELECT email FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_email,
                       (SELECT phone FROM entity_contacts ec
                        WHERE ec.entity_type = 'customer' AND ec.entity_id = c.customer_id
                        AND ec.is_active = 1 AND ec.is_primary = 1 LIMIT 1) as primary_phone
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
            """).fetchall()
            for c in customers:
                row = dict(c)
                # Skip agency clients: name contains ':' or all spots booked via agency
                if ':' in row["entity_name"] or row["entity_id"] in agency_client_ids_from_spots:
                    continue
                m = customer_metrics.get(row["entity_id"], {})
                row["last_active"] = m.get("last_active", "")
                row["total_revenue"] = m.get("total_revenue", 0)
                row["markets"] = m.get("markets", "")
                results.append(row)

    # Apply filters
    if search:
        q = search.lower()
        results = [r for r in results if (
            q in r["entity_name"].lower() or
            q in (r.get("notes") or "").lower() or
            q in (r.get("sector_name") or "").lower() or
            q in (r.get("sector_code") or "").lower()
        )]

    if has_contacts == "yes":
        results = [r for r in results if r.get("primary_contact")]
    elif has_contacts == "no":
        results = [r for r in results if not r.get("primary_contact")]

    if has_address == "yes":
        results = [r for r in results if r.get("address") or r.get("city")]
    elif has_address == "no":
        results = [r for r in results if not r.get("address") and not r.get("city")]

    if sector_filter:
        try:
            sid = int(sector_filter)
            with _db_ro() as sconn:
                export_sector_ids = set(
                    r["customer_id"] for r in sconn.execute(
                        "SELECT customer_id FROM customer_sectors WHERE sector_id = ?", [sid]
                    ).fetchall()
                )
            results = [r for r in results if (
                r.get("entity_type") == "customer" and r.get("entity_id") in export_sector_ids
            )]
        except ValueError:
            pass

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
        "Address", "City", "State", "ZIP", "PO Number", "EDI Billing",
        "Markets", "Last Active", "Total Revenue", "Notes"
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
            r.get("po_number", ""),
            "Yes" if r.get("edi_billing") else "No",
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
# CSV Import for Contacts
# ============================================================

VALID_IMPORT_ROLES = ['decision_maker', 'account_manager', 'billing', 'technical', 'other']


@address_book_bp.route("/api/address-book/import-contacts", methods=["POST"])
def api_import_contacts():
    """Import contacts from a CSV file. Expected columns: Entity Name, Type, Contact Name, Title, Email, Phone, Role."""
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    if not file.filename or not file.filename.endswith('.csv'):
        return jsonify({"error": "File must be a .csv"}), 400

    try:
        content = file.read().decode('utf-8-sig')
    except UnicodeDecodeError:
        return jsonify({"error": "File must be UTF-8 encoded"}), 400

    reader = csv.DictReader(io.StringIO(content))
    required_cols = {'Entity Name', 'Type', 'Contact Name'}
    if not required_cols.issubset(set(reader.fieldnames or [])):
        return jsonify({"error": f"CSV must have columns: {', '.join(sorted(required_cols))}"}), 400

    imported = 0
    skipped = 0
    errors = []

    with _db_rw() as conn:
        for i, row in enumerate(reader, start=2):
            entity_name = (row.get('Entity Name') or '').strip()
            entity_type = (row.get('Type') or '').strip().lower()
            contact_name = (row.get('Contact Name') or '').strip()

            if not entity_name or not entity_type or not contact_name:
                errors.append(f"Row {i}: Missing required field (Entity Name, Type, or Contact Name)")
                skipped += 1
                continue

            if entity_type not in ('agency', 'customer'):
                errors.append(f"Row {i}: Type must be 'agency' or 'customer', got '{entity_type}'")
                skipped += 1
                continue

            # Look up entity
            if entity_type == 'agency':
                entity = conn.execute(
                    "SELECT agency_id FROM agencies WHERE agency_name = ? COLLATE NOCASE AND is_active = 1",
                    [entity_name]
                ).fetchone()
                entity_id = entity["agency_id"] if entity else None
            else:
                entity = conn.execute(
                    "SELECT customer_id FROM customers WHERE normalized_name = ? COLLATE NOCASE AND is_active = 1",
                    [entity_name]
                ).fetchone()
                entity_id = entity["customer_id"] if entity else None

            if not entity_id:
                errors.append(f"Row {i}: Entity '{entity_name}' ({entity_type}) not found")
                skipped += 1
                continue

            role = (row.get('Role') or '').strip().lower() or None
            if role and role not in VALID_IMPORT_ROLES:
                role = None

            try:
                conn.execute("""
                    INSERT INTO entity_contacts
                        (entity_type, entity_id, contact_name, contact_title, email, phone,
                         contact_role, is_primary, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'csv_import')
                """, [
                    entity_type, entity_id, contact_name,
                    (row.get('Title') or '').strip() or None,
                    (row.get('Email') or '').strip() or None,
                    (row.get('Phone') or '').strip() or None,
                    role
                ])
                imported += 1
            except Exception as e:
                errors.append(f"Row {i}: {str(e)}")
                skipped += 1

        conn.commit()

    return jsonify({
        "imported": imported,
        "skipped": skipped,
        "errors": errors[:20]
    })


# ============================================================
# Activity Log
# ============================================================

VALID_ACTIVITY_TYPES = ['note', 'call', 'email', 'meeting', 'status_change', 'follow_up']


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
                ea.due_date,
                ea.is_completed,
                ea.completed_date,
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
    due_date = data.get("due_date", "").strip() or None

    if not activity_type:
        return jsonify({"error": "activity_type is required"}), 400

    if activity_type not in VALID_ACTIVITY_TYPES:
        return jsonify({"error": f"Invalid activity_type. Must be one of: {VALID_ACTIVITY_TYPES}"}), 400

    if activity_type == 'follow_up' and not due_date:
        return jsonify({"error": "due_date is required for follow_up activities"}), 400

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
                    (entity_type, entity_id, activity_type, description, created_by, contact_id, due_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [entity_type, entity_id, activity_type, description or None, "web_user", contact_id, due_date])

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


@address_book_bp.route("/api/address-book/activities/<int:activity_id>/complete", methods=["POST"])
def api_complete_activity(activity_id):
    """Mark a follow-up activity as completed."""
    with _db_rw() as conn:
        try:
            activity = conn.execute(
                "SELECT activity_type, is_completed FROM entity_activity WHERE activity_id = ?",
                [activity_id]
            ).fetchone()

            if not activity:
                return jsonify({"error": "Activity not found"}), 404

            if activity["activity_type"] != "follow_up":
                return jsonify({"error": "Only follow_up activities can be completed"}), 400

            new_status = 0 if activity["is_completed"] else 1
            conn.execute("""
                UPDATE entity_activity
                SET is_completed = ?, completed_date = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END
                WHERE activity_id = ?
            """, [new_status, new_status, activity_id])

            conn.commit()
            return jsonify({"success": True, "is_completed": new_status})

        except Exception as e:
            conn.rollback()
            return jsonify({"error": str(e)}), 500


@address_book_bp.route("/api/address-book/follow-ups")
def api_get_follow_ups():
    """Get all incomplete follow-up activities for the dashboard, plus recently completed."""
    with _db_ro() as conn:
        follow_ups = conn.execute("""
            SELECT
                ea.activity_id,
                ea.entity_type,
                ea.entity_id,
                ea.description,
                ea.due_date,
                ea.is_completed,
                ea.completed_date,
                ea.activity_date,
                CASE ea.entity_type
                    WHEN 'agency' THEN (SELECT agency_name FROM agencies WHERE agency_id = ea.entity_id)
                    WHEN 'customer' THEN (SELECT normalized_name FROM customers WHERE customer_id = ea.entity_id)
                END AS entity_name
            FROM entity_activity ea
            WHERE ea.activity_type = 'follow_up'
              AND (ea.is_completed = 0 OR ea.completed_date >= datetime('now', '-7 days'))
            ORDER BY ea.is_completed ASC, ea.due_date ASC
        """).fetchall()

        today = __import__('datetime').date.today().isoformat()
        results = []
        for f in follow_ups:
            d = dict(f)
            if d['is_completed']:
                d['urgency'] = 'completed'
            elif d['due_date'] and d['due_date'] < today:
                d['urgency'] = 'overdue'
            elif d['due_date'] and d['due_date'] == today:
                d['urgency'] = 'due-today'
            elif d['due_date']:
                from datetime import date, timedelta
                due = date.fromisoformat(d['due_date'])
                if due <= date.today() + timedelta(days=3):
                    d['urgency'] = 'due-soon'
                else:
                    d['urgency'] = 'upcoming'
            else:
                d['urgency'] = 'upcoming'
            results.append(d)

        return jsonify(results)


# ============================================================
# AE Assignment
# ============================================================

@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/ae", methods=["PUT"])
def api_update_ae(entity_type, entity_id):
    """Update assigned AE for an entity. Manages ae_assignments history."""
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

            # End any current active assignment
            conn.execute("""
                UPDATE ae_assignments
                SET ended_date = datetime('now')
                WHERE entity_type = ? AND entity_id = ? AND ended_date IS NULL
            """, [entity_type, entity_id])

            # Insert new assignment if AE is set
            if assigned_ae:
                conn.execute("""
                    INSERT INTO ae_assignments (entity_type, entity_id, ae_name, created_by)
                    VALUES (?, ?, ?, 'web_user')
                """, [entity_type, entity_id, assigned_ae])

            # Update denormalized value on main table
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


@address_book_bp.route("/api/address-book/<entity_type>/<int:entity_id>/ae-history")
def api_ae_history(entity_type, entity_id):
    """Get AE assignment history for an entity."""
    if entity_type not in ("agency", "customer"):
        return jsonify({"error": "Invalid entity type"}), 400

    with _db_ro() as conn:
        rows = conn.execute("""
            SELECT ae_name, assigned_date, ended_date, created_by, notes
            FROM ae_assignments
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY assigned_date DESC
        """, [entity_type, entity_id]).fetchall()

        return jsonify([dict(r) for r in rows])


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
