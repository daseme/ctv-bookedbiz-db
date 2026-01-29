#!/usr/bin/env python3
"""
Load customer name matches into review queue.
...
"""

from __future__ import annotations
import argparse
import json
import sqlite3
import sys
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

# ---- import the analyzer from the previous drop-in file ----
from src.services.customer_matching.blocking_matcher import (
    analyze_customer_names,
    summarize,
    NORMALIZATION_CONFIG,
)


def insert_review_rows(conn: sqlite3.Connection, rows: List[dict]) -> int:
    q = """
    INSERT INTO customer_match_review (
      bill_code_name_raw, norm_name, suggested_customer_id, suggested_customer_name,
      best_score, revenue, spot_count, first_seen, last_seen, months,
      suggestions_json, status, notes
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur = conn.cursor()
    n = 0
    for r in rows:
        cur.execute(
            q,
            (
                r["bill_code_name_raw"],
                r["norm_name"],
                r.get("suggested_customer_id"),
                r.get("suggested_customer_name"),
                r["best_score"],
                r["revenue"],
                r["spot_count"],
                r.get("first_seen"),
                r.get("last_seen"),
                "|".join(sorted(r.get("months", []))),
                json.dumps(r.get("suggestions", []), ensure_ascii=False),
                r["status"],
                r.get("notes", ""),
            ),
        )
        n += 1
    conn.commit()
    return n


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument(
        "--limit", type=int, default=10000, help="Max rows to insert into queue"
    )
    p.add_argument(
        "--auto-approve",
        action="store_true",
        help="Auto-approve very high-confidence matches (score>=0.97 & revenue>=2000)",
    )
    args = p.parse_args()

    if not Path(args.db).exists():
        raise SystemExit(f"DB not found: {args.db}")

    # Analyze (read-only inside the analyzer)
    matches = analyze_customer_names(args.db, NORMALIZATION_CONFIG)
    s = summarize(matches)
    print("SUMMARY:", s)

    # Build rows for queue
    queue_rows: List[Dict] = []
    for m in matches:
        if m.status in ("review", "unknown"):
            row = {
                "bill_code_name_raw": m.bill_code_name_raw,
                "norm_name": m.norm_name,
                "suggested_customer_id": m.matched_customer_id,
                "suggested_customer_name": m.matched_customer_name,
                "best_score": round(m.best_score, 6),
                "revenue": float(m.revenue),
                "spot_count": int(m.spot_count),
                "first_seen": m.first_seen,
                "last_seen": m.last_seen,
                "months": list(m.months),
                "suggestions": [
                    {"name": n, "score": round(s, 3)} for n, s in m.suggestions
                ],
                "status": "pending",
                "notes": "",
            }
            queue_rows.append(row)

    # Auto-approve ultra high confidence if requested
    approved_rows: List[Dict] = []
    if args.auto_approve:
        for r in queue_rows:
            if (
                r.get("suggested_customer_id")
                and r["best_score"] >= 0.97
                and r["revenue"] >= 2000
            ):
                r["status"] = "approved"
                r["notes"] = "auto-approved (very high confidence)"
                approved_rows.append(r)

    # Insert into queue (single write connection)
    conn = sqlite3.connect(args.db)
    try:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA journal_mode=WAL;")
        # Insert approved first so the UI shows them as such
        inserted = 0
        if approved_rows:
            inserted += insert_review_rows(conn, approved_rows[: args.limit])
        # Insert remaining pending
        pending_rows = [r for r in queue_rows if r["status"] == "pending"]
        if pending_rows:
            inserted += insert_review_rows(
                conn, pending_rows[: max(0, args.limit - inserted)]
            )
        print(f"Inserted into review queue: {inserted}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
