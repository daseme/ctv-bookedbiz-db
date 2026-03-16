"""027_add_political_classification

Expand revenue_class CHECK constraint to include 'political'.

Uses writable_schema to update the constraint in-place, avoiding a full
table rebuild that would break dependent views and triggers.

Usage:
    python sql/migrations/027_add_political_classification.py [db_path]

Defaults to /var/lib/ctv-bookedbiz-db/production.db
"""

import sqlite3
import sys

OLD_CHECK = "CHECK (revenue_class IN ('regular', 'irregular'))"
NEW_CHECK = "CHECK (revenue_class IN ('regular', 'irregular', 'political'))"

def migrate(db_path):
    conn = sqlite3.connect(db_path)

    schema = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name='customers'"
    ).fetchone()[0]

    if NEW_CHECK in schema:
        print("Already migrated — 'political' constraint present.")
        conn.close()
        return

    if OLD_CHECK not in schema:
        print(f"ERROR: Expected CHECK constraint not found in schema:\n{schema}")
        conn.close()
        sys.exit(1)

    conn.execute("PRAGMA writable_schema = ON")
    conn.execute(
        "UPDATE sqlite_master SET sql = REPLACE(sql, ?, ?) "
        "WHERE type = 'table' AND name = 'customers'",
        (OLD_CHECK, NEW_CHECK),
    )
    conn.execute("PRAGMA writable_schema = OFF")

    ver = conn.execute("PRAGMA schema_version").fetchone()[0]
    conn.execute(f"PRAGMA schema_version = {ver + 1}")

    result = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if result != "ok":
        print(f"ERROR: Integrity check failed: {result}")
        conn.close()
        sys.exit(1)

    conn.commit()
    conn.close()
    print(f"Migration complete: revenue_class now accepts 'political' in {db_path}")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/var/lib/ctv-bookedbiz-db/production.db"
    migrate(path)
