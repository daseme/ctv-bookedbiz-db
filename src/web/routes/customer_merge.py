# src/web/routes/customer_merge.py
"""Customer merge tool — link unresolved bill_codes and merge duplicates."""

import sqlite3

from flask import Blueprint, current_app, jsonify, request, render_template

customer_merge_bp = Blueprint("customer_merge", __name__)


@customer_merge_bp.before_request
def _require_admin_for_writes():
    if request.method in ('POST', 'PUT', 'DELETE'):
        from flask_login import current_user
        if not hasattr(current_user, 'role') or current_user.role.value != 'admin':
            return jsonify({"error": "Admin access required"}), 403


@customer_merge_bp.route("/customer-merge")
def customer_merge_page():
    return render_template("customer_merge.html")


@customer_merge_bp.route("/api/customer-merge/unresolved")
def unresolved_bill_codes():
    """Bill codes on spots with NULL customer_id, split by resolution state.

    Returns two lists:
    - unlinked: no alias exists yet, needs Link action
    - needs_backfill: alias exists but spots.customer_id still NULL
    """
    db_path = (
        current_app.config.get("DB_PATH")
        or "./data/database/production.db"
    )
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT
                s.bill_code,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS revenue,
                ea.alias_id,
                ea.target_entity_id AS customer_id,
                c.normalized_name AS customer_name
            FROM spots s
            LEFT JOIN entity_aliases ea
                ON ea.alias_name = s.bill_code
                AND ea.entity_type = 'customer'
                AND ea.is_active = 1
            LEFT JOIN customers c
                ON c.customer_id = ea.target_entity_id
            WHERE s.customer_id IS NULL
              AND s.bill_code IS NOT NULL AND s.bill_code != ''
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.bill_code
            ORDER BY revenue DESC
        """).fetchall()
    finally:
        conn.close()

    unlinked = []
    needs_backfill = []
    for r in rows:
        item = {
            "bill_code": r["bill_code"],
            "spot_count": r["spot_count"],
            "revenue": float(r["revenue"] or 0),
        }
        if r["alias_id"]:
            item["customer_id"] = r["customer_id"]
            item["customer_name"] = r["customer_name"]
            needs_backfill.append(item)
        else:
            unlinked.append(item)

    return jsonify({
        "unlinked": unlinked,
        "needs_backfill": needs_backfill,
    })


@customer_merge_bp.route("/api/customer-merge/backfill", methods=["POST"])
def backfill_spots():
    """Backfill spots.customer_id for a bill_code that already has an alias."""
    data = request.json or {}
    bill_code = data.get("bill_code")
    customer_id = data.get("customer_id")
    if not bill_code or not customer_id:
        return jsonify({
            "success": False,
            "error": "bill_code and customer_id required",
        }), 400

    db_path = (
        current_app.config.get("DB_PATH")
        or "./data/database/production.db"
    )
    conn = sqlite3.connect(db_path, timeout=10.0)
    try:
        spots_updated = conn.execute("""
            UPDATE spots
            SET customer_id = ?
            WHERE bill_code = ? AND customer_id IS NULL
        """, [customer_id, bill_code]).rowcount
        conn.commit()
    finally:
        conn.close()

    return jsonify({
        "success": True,
        "bill_code": bill_code,
        "customer_id": customer_id,
        "spots_updated": spots_updated,
    })
