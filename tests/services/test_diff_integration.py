"""Integration tests for diff-based import against a real temp database."""
import pytest
import sqlite3
import tempfile
import os
from datetime import datetime


SCHEMA = """
CREATE TABLE spots (
    spot_id INTEGER PRIMARY KEY,
    bill_code TEXT, air_date DATE, end_date DATE, day_of_week TEXT,
    time_in TEXT, time_out TEXT, length_seconds TEXT, media TEXT,
    comments TEXT, language_code TEXT, format TEXT, sequence_number INTEGER,
    line_number INTEGER, spot_type TEXT, estimate TEXT,
    gross_rate DECIMAL(12,2), make_good TEXT, spot_value DECIMAL(12,2),
    broadcast_month TEXT, broker_fees DECIMAL(12,2), priority INTEGER,
    station_net DECIMAL(12,2), sales_person TEXT, revenue_type TEXT,
    billing_type TEXT, agency_flag TEXT, affidavit_flag TEXT,
    contract TEXT, market_name TEXT, customer_id INTEGER,
    agency_id INTEGER, market_id INTEGER, language_id INTEGER,
    load_date TIMESTAMP, source_file TEXT, is_historical BOOLEAN,
    effective_date DATE, import_batch_id TEXT, spot_category TEXT
);
CREATE TABLE month_closures (broadcast_month TEXT PRIMARY KEY);
"""


def _make_row(bill_code, contract, month_date, spot_value):
    """Build a 30-element tuple matching EXCEL_COLUMN_POSITIONS layout."""
    row = [None] * 30
    row[0] = bill_code
    row[17] = spot_value
    row[27] = contract
    if month_date:
        row[18] = datetime.strptime(month_date, "%Y-%m-%d")
    row[1] = row[18]  # air_date
    return tuple(row)


@pytest.fixture()
def db_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.close()
    yield path
    os.unlink(path)


class TestNoChangeImport:
    def test_unchanged_groups_preserve_spot_ids(self, db_path):
        """When fingerprints match, spot_ids must be preserved (zero writes)."""
        from src.services.import_diff import (
            build_db_fingerprints,
            build_excel_fingerprints,
            compare_fingerprints,
        )

        conn = sqlite3.connect(db_path)
        conn.executemany(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value, air_date) VALUES (?,?,?,?,?)",
            [
                ("Acme:Widget", "100", "Mar-26", 150.00, "2026-03-01"),
                ("Acme:Widget", "100", "Mar-26", 250.00, "2026-03-02"),
                ("Beta:Gizmo", "200", "Mar-26", 500.00, "2026-03-01"),
            ],
        )
        conn.commit()

        original_ids = {r[0] for r in conn.execute("SELECT spot_id FROM spots").fetchall()}

        excel_rows = [
            _make_row("Acme:Widget", "100", "2026-03-01", 150.00),
            _make_row("Acme:Widget", "100", "2026-03-01", 250.00),
            _make_row("Beta:Gizmo", "200", "2026-03-01", 500.00),
        ]

        excel_fps, grouped, months = build_excel_fingerprints(excel_rows)
        db_fps = build_db_fingerprints(["Mar-26"], conn)
        diff = compare_fingerprints(excel_fps, db_fps)

        assert len(diff.unchanged) == 2
        assert len(diff.changed) == 0
        assert len(diff.added) == 0
        assert len(diff.removed) == 0

        current_ids = {r[0] for r in conn.execute("SELECT spot_id FROM spots").fetchall()}
        assert current_ids == original_ids
        conn.close()

    def test_changed_group_gets_replaced(self, db_path):
        """When a group's fingerprint changes, its rows are deleted and reinserted."""
        from src.services.import_diff import (
            build_db_fingerprints,
            build_excel_fingerprints,
            compare_fingerprints,
        )

        conn = sqlite3.connect(db_path)
        conn.executemany(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value, air_date) VALUES (?,?,?,?,?)",
            [
                ("Acme:Widget", "100", "Mar-26", 150.00, "2026-03-01"),
                ("Beta:Gizmo", "200", "Mar-26", 500.00, "2026-03-01"),
            ],
        )
        conn.commit()

        # Change Acme's value from 150 to 200
        excel_rows = [
            _make_row("Acme:Widget", "100", "2026-03-01", 200.00),
            _make_row("Beta:Gizmo", "200", "2026-03-01", 500.00),
        ]

        excel_fps, grouped, months = build_excel_fingerprints(excel_rows)
        db_fps = build_db_fingerprints(["Mar-26"], conn)
        diff = compare_fingerprints(excel_fps, db_fps)

        assert ("Acme:Widget", "100", "Mar-26") in diff.changed
        assert ("Beta:Gizmo", "200", "Mar-26") in diff.unchanged
        conn.close()

    def test_new_group_detected(self, db_path):
        """A group in Excel but not DB is classified as added."""
        from src.services.import_diff import (
            build_db_fingerprints,
            build_excel_fingerprints,
            compare_fingerprints,
        )

        conn = sqlite3.connect(db_path)

        excel_rows = [
            _make_row("NewClient:Product", "300", "2026-03-01", 1000.00),
        ]

        excel_fps, grouped, months = build_excel_fingerprints(excel_rows)
        db_fps = build_db_fingerprints(["Mar-26"], conn)
        diff = compare_fingerprints(excel_fps, db_fps)

        assert ("NewClient:Product", "300", "Mar-26") in diff.added
        conn.close()

    def test_removed_group_detected(self, db_path):
        """A group in DB but not Excel is classified as removed."""
        from src.services.import_diff import (
            build_db_fingerprints,
            build_excel_fingerprints,
            compare_fingerprints,
        )

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value, air_date) VALUES (?,?,?,?,?)",
            ("OldClient:Gone", "999", "Mar-26", 100.00, "2026-03-01"),
        )
        conn.commit()

        excel_fps, grouped, months = build_excel_fingerprints([])
        db_fps = build_db_fingerprints(["Mar-26"], conn)
        diff = compare_fingerprints(excel_fps, db_fps)

        assert ("OldClient:Gone", "999", "Mar-26") in diff.removed
        conn.close()
