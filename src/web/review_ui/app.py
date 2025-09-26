#!/usr/bin/env python3
"""
Customer match review UI (Flask)

Env:
  DB_PATH=/path/to/production.db
  APP_PIN=1234      # simple protection

Run:
  pip install flask
  python review_ui/app.py --host 0.0.0.0 --port 5088
"""

from __future__ import annotations
import os, json, sqlite3, argparse
from functools import wraps
from datetime import datetime
from flask import Flask, request, redirect, url_for, render_template_string, abort

TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Customer Match Review</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px 10px; border-bottom: 1px solid #ddd; vertical-align: top; }
    .pill { padding: 2px 8px; border-radius: 999px; font-size: 12px; background:#eee; display:inline-block; }
    .actions form { display:inline-block; margin-right: 6px; }
    .suggestions { font-size: 12px; color:#333; }
    .mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
  </style>
</head>
<body>
  <h1>Customer Match Review</h1>
  <p><b>Pending:</b> {{pending_count}} | <b>Approved:</b> {{approved_count}} | <b>Rejected:</b> {{rejected_count}}</p>

  <form method="get" action="{{ url_for('index') }}">
    <input type="hidden" name="pin" value="{{ pin }}">
    <label>Min revenue: <input type="number" step="100" name="min_rev" value="{{min_rev or ''}}"></label>
    <label>Limit: <input type="number" name="limit" value="{{limit}}"></label>
    <button>Filter</button>
  </form>

  <table>
    <tr>
      <th>Customer (bill_code)</th>
      <th>Suggestion</th>
      <th>Score</th>
      <th>Revenue</th>
      <th>Spots</th>
      <th>Date range</th>
      <th>Actions</th>
    </tr>
    {% for r in rows %}
    <tr>
      <td>
        <div class="mono">{{ r['bill_code_name_raw'] }}</div>
        <div class="suggestions">norm: {{ r['norm_name'] }}</div>
      </td>
      <td>
        {% if r['suggested_customer_name'] %}
          {{ r['suggested_customer_name'] }}
        {% else %}
          <span class="pill">no candidate</span>
        {% endif %}
        <div class="suggestions">
          {% for s in r['suggestions'] %}
            {{ s['name'] }} ({{ '%.3f'|format(s['score']) }}){% if not loop.last %}, {% endif %}
          {% endfor %}
        </div>
      </td>
      <td>{{ '%.3f'|format(r['best_score']) }}</td>
      <td>${{ '%.0f'|format(r['revenue']) }}</td>
      <td>{{ r['spot_count'] }}</td>
      <td>{{ r['first_seen'] }} → {{ r['last_seen'] }}</td>
      <td class="actions">
        {% if r['suggested_customer_id'] %}
        <form method="post" action="{{ url_for('approve', id=r['id']) }}">
          <input type="hidden" name="pin" value="{{ pin }}">
          <input type="hidden" name="alias_name" value="{{ r['bill_code_name_raw'] }}">
          <input type="hidden" name="target_customer_id" value="{{ r['suggested_customer_id'] }}">
          <input type="hidden" name="confidence" value="{{ (r['best_score']*100)|int }}">
          <input type="text" name="notes" placeholder="notes" />
          <button>Approve → Create alias</button>
        </form>
        {% endif %}
        <form method="post" action="{{ url_for('reject', id=r['id']) }}">
          <input type="hidden" name="pin" value="{{ pin }}">
          <input type="text" name="notes" placeholder="reason" />
          <button>Reject</button>
        </form>
      </td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
"""

def require_pin(f):
    @wraps(f)
    def wrapped(*a, **kw):
        pin = request.values.get("pin", "")
        if not pin or pin != os.environ.get("APP_PIN", ""):
            return abort(401)
        return f(*a, **kw)
    return wrapped

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def get_counts(conn):
    cur = conn.execute("SELECT status, COUNT(*) c FROM customer_match_review GROUP BY status")
    d = {"pending":0,"approved":0,"rejected":0,"aliased":0}
    for r in cur.fetchall():
        d[r["status"]] = r["c"]
    return d

def list_pending(conn, min_rev: float | None, limit: int):
    if min_rev:
        cur = conn.execute("""
          SELECT * FROM customer_match_review
          WHERE status='pending' AND revenue>=?
          ORDER BY revenue DESC, best_score DESC
          LIMIT ?
        """, (min_rev, limit))
    else:
        cur = conn.execute("""
          SELECT * FROM customer_match_review
          WHERE status='pending'
          ORDER BY revenue DESC, best_score DESC
          LIMIT ?
        """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        try:
            r["suggestions"] = json.loads(r.get("suggestions_json") or "[]")
        except Exception:
            r["suggestions"] = []
    return rows

def create_alias(conn, alias_name: str, target_customer_id: int, confidence_score: int, created_by: str, notes: str):
    conn.execute("""
      INSERT INTO entity_aliases (alias_name, entity_type, target_entity_id, confidence_score, created_by, notes, is_active)
      VALUES (?, 'customer', ?, ?, ?, ?, 1)
    """, (alias_name, target_customer_id, confidence_score, created_by, notes))
    conn.commit()

def mark_review(conn, row_id: int, status: str, who: str, notes: str):
    conn.execute("""
      UPDATE customer_match_review
         SET status=?, decided_at=?, decided_by=?, notes=COALESCE(NULLIF(?,'') , notes)
       WHERE id=?
    """, (status, datetime.utcnow().isoformat(timespec="seconds")+"Z", who, notes, row_id))
    conn.commit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5088)
    args = ap.parse_args()

    db_path = os.environ.get("DB_PATH")
    app_pin = os.environ.get("APP_PIN")
    if not db_path or not app_pin:
        raise SystemExit("Set env: DB_PATH and APP_PIN")

    app = Flask(__name__)

    @app.get("/")
    @require_pin
    def index():
        pin = request.args.get("pin")
        min_rev = request.args.get("min_rev", type=float)
        limit = request.args.get("limit", type=int, default=100)
        conn = connect(db_path)
        try:
            counts = get_counts(conn)
            rows = list_pending(conn, min_rev, limit)
        finally:
            conn.close()
        return render_template_string(
            TEMPLATE,
            rows=rows, pending_count=counts["pending"], approved_count=counts["approved"],
            rejected_count=counts["rejected"], pin=pin, min_rev=min_rev, limit=limit
        )

    @app.post("/approve/<int:id>")
    @require_pin
    def approve(id: int):
        pin = request.form.get("pin")
        alias_name = request.form.get("alias_name","").strip()
        target_cust = int(request.form.get("target_customer_id","0"))
        confidence = int(request.form.get("confidence","95"))
        notes = request.form.get("notes","").strip()
        who = "review_ui"

        if not alias_name or target_cust <= 0:
            return abort(400, "Missing alias or target id")

        conn = connect(db_path)
        try:
            # create alias then mark review row
            create_alias(conn, alias_name, target_cust, confidence, who, notes)
            mark_review(conn, id, "aliased", who, notes or "approved & alias created")
        finally:
            conn.close()
        return redirect(url_for("index", pin=pin))

    @app.post("/reject/<int:id>")
    @require_pin
    def reject(id: int):
        pin = request.form.get("pin")
        notes = request.form.get("notes","").strip()
        conn = connect(db_path)
        try:
            mark_review(conn, id, "rejected", "review_ui", notes or "rejected")
        finally:
            conn.close()
        return redirect(url_for("index", pin=pin))

    app.run(host=args.host, port=args.port)

if __name__ == "__main__":
    main()
