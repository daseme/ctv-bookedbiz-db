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
    """Bill codes on spots with NULL customer_id, grouped with revenue."""
    db_path = current_app.config.get("DB_PATH") or "./data/database/production.db"
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("""
            SELECT
                s.bill_code,
                COUNT(*) AS spot_count,
                ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS revenue
            FROM spots s
            WHERE s.customer_id IS NULL
              AND s.bill_code IS NOT NULL AND s.bill_code != ''
              AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            GROUP BY s.bill_code
            ORDER BY revenue DESC
        """).fetchall()
    finally:
        conn.close()

    return jsonify([
        {
            "bill_code": r["bill_code"],
            "spot_count": r["spot_count"],
            "revenue": float(r["revenue"] or 0),
        }
        for r in rows
    ])
