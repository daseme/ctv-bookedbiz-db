"""Tests for import_diff module — contract-group fingerprint diffing."""

import sqlite3
import tempfile
import os
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_test_db():
    """Create a temp SQLite DB with the spots + month_closures schema."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            bill_code TEXT,
            contract TEXT,
            broadcast_month TEXT,
            spot_value DECIMAL(12,2),
            air_date DATE,
            import_batch_id TEXT
        );
        CREATE TABLE month_closures (
            broadcast_month TEXT PRIMARY KEY
        );
        """
    )
    conn.commit()
    return conn, path


def _make_row(bill_code="BC1", air_date="2026-03-01",
              spot_value=100.00, broadcast_month="Mar-26",
              contract="C100"):
    """Build a 30-element tuple matching EXCEL_COLUMN_POSITIONS.

    Index 0=bill_code, 1=air_date, 17=spot_value,
    18=broadcast_month, 27=contract.
    """
    row = [None] * 30
    row[0] = bill_code
    row[1] = air_date
    row[17] = spot_value
    row[18] = broadcast_month
    row[27] = contract
    return tuple(row)


# ===========================================================================
# Task 1 — build_db_fingerprints
# ===========================================================================

class TestBuildDbFingerprints:

    def test_groups_by_contract(self):
        from src.services.import_diff import build_db_fingerprints

        conn, path = _create_test_db()
        try:
            conn.executemany(
                "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
                [
                    ("Acme:Widget", "C100", "Mar-26", 150.00),
                    ("Acme:Widget", "C100", "Mar-26", 50.00),
                    ("Acme:Widget", "C200", "Mar-26", 75.00),
                ],
            )
            conn.commit()

            fps = build_db_fingerprints(["Mar-26"], conn)

            assert fps[("Acme:Widget", "C100", "Mar-26")] == (20000, 2)
            assert fps[("Acme:Widget", "C200", "Mar-26")] == (7500, 1)
        finally:
            conn.close()
            os.unlink(path)

    def test_coalesces_null_contract(self):
        from src.services.import_diff import build_db_fingerprints

        conn, path = _create_test_db()
        try:
            conn.execute(
                "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
                ("BC1", None, "Mar-26", 10.00),
            )
            conn.commit()

            fps = build_db_fingerprints(["Mar-26"], conn)
            assert ("BC1", "", "Mar-26") in fps
            assert fps[("BC1", "", "Mar-26")] == (1000, 1)
        finally:
            conn.close()
            os.unlink(path)

    def test_coalesces_null_spot_value(self):
        from src.services.import_diff import build_db_fingerprints

        conn, path = _create_test_db()
        try:
            conn.execute(
                "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
                ("BC1", "C1", "Mar-26", None),
            )
            conn.commit()

            fps = build_db_fingerprints(["Mar-26"], conn)
            assert fps[("BC1", "C1", "Mar-26")] == (0, 1)
        finally:
            conn.close()
            os.unlink(path)

    def test_empty_months_returns_empty(self):
        from src.services.import_diff import build_db_fingerprints

        conn, path = _create_test_db()
        try:
            conn.execute(
                "INSERT INTO spots (bill_code, contract, broadcast_month, spot_value) VALUES (?,?,?,?)",
                ("BC1", "C1", "Mar-26", 100.00),
            )
            conn.commit()

            fps = build_db_fingerprints([], conn)
            assert fps == {}
        finally:
            conn.close()
            os.unlink(path)


# ===========================================================================
# Task 2 — build_excel_fingerprints
# ===========================================================================

class TestBuildExcelFingerprints:

    def test_groups_rows_and_computes_cents(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [
            _make_row("Acme:Widget", "2026-03-01", 150.00, "Mar-26", "C100"),
            _make_row("Acme:Widget", "2026-03-02", 50.00, "Mar-26", "C100"),
            _make_row("Acme:Widget", "2026-03-03", 75.00, "Mar-26", "C200"),
        ]

        fps, grouped, months = build_excel_fingerprints(rows)

        assert fps[("Acme:Widget", "C100", "Mar-26")] == (20000, 2)
        assert fps[("Acme:Widget", "C200", "Mar-26")] == (7500, 1)
        assert len(grouped[("Acme:Widget", "C100", "Mar-26")]) == 2
        assert len(grouped[("Acme:Widget", "C200", "Mar-26")]) == 1
        assert "Mar-26" in months

    def test_null_spot_value_treated_as_zero(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [_make_row("BC1", "2026-03-01", None, "Mar-26", "C1")]
        fps, _, _ = build_excel_fingerprints(rows)
        assert fps[("BC1", "C1", "Mar-26")] == (0, 1)

    def test_null_contract_coalesced(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [_make_row("BC1", "2026-03-01", 10.00, "Mar-26", None)]
        fps, _, _ = build_excel_fingerprints(rows)
        assert ("BC1", "", "Mar-26") in fps

    def test_skips_rows_without_broadcast_month(self):
        from src.services.import_diff import build_excel_fingerprints

        rows = [
            _make_row("BC1", "2026-03-01", 100.00, None, "C1"),
            _make_row("BC1", "2026-03-01", 100.00, "", "C1"),
            _make_row("BC1", "2026-03-01", 50.00, "Mar-26", "C1"),
        ]
        fps, grouped, months = build_excel_fingerprints(rows)
        # Only the third row should be included
        assert len(fps) == 1
        assert fps[("BC1", "C1", "Mar-26")] == (5000, 1)

    def test_handles_rows_with_sheet_tag(self):
        """Rows with >30 columns (sheet-name tag) should be handled."""
        from src.services.import_diff import build_excel_fingerprints

        base = _make_row("BC1", "2026-03-01", 100.00, "Mar-26", "C1")
        tagged_row = base + ("Commercials",)  # 31 elements
        assert len(tagged_row) == 31

        fps, grouped, _ = build_excel_fingerprints([tagged_row])
        assert fps[("BC1", "C1", "Mar-26")] == (10000, 1)
        # grouped_rows should preserve the full raw row (with tag)
        assert len(grouped[("BC1", "C1", "Mar-26")][0]) == 31


# ===========================================================================
# Task 3 — compare_fingerprints
# ===========================================================================

class TestCompareFingerprints:

    def test_unchanged_groups(self):
        from src.services.import_diff import compare_fingerprints

        excel = {("BC1", "C1", "Mar-26"): (10000, 2)}
        db = {("BC1", "C1", "Mar-26"): (10000, 2)}
        result = compare_fingerprints(excel, db)

        assert ("BC1", "C1", "Mar-26") in result.unchanged
        assert len(result.changed) == 0
        assert len(result.added) == 0
        assert len(result.removed) == 0

    def test_changed_value(self):
        from src.services.import_diff import compare_fingerprints

        key = ("BC1", "C1", "Mar-26")
        excel = {key: (10000, 2)}
        db = {key: (9999, 2)}  # different sum_cents
        result = compare_fingerprints(excel, db)

        assert key in result.changed
        assert key not in result.unchanged

    def test_changed_count(self):
        from src.services.import_diff import compare_fingerprints

        key = ("BC1", "C1", "Mar-26")
        excel = {key: (10000, 3)}
        db = {key: (10000, 2)}  # different row_count
        result = compare_fingerprints(excel, db)

        assert key in result.changed

    def test_new_group(self):
        from src.services.import_diff import compare_fingerprints

        key = ("BC1", "C1", "Mar-26")
        excel = {key: (10000, 2)}
        db = {}
        result = compare_fingerprints(excel, db)

        assert key in result.added
        assert len(result.unchanged) == 0

    def test_removed_group(self):
        from src.services.import_diff import compare_fingerprints

        key = ("BC1", "C1", "Mar-26")
        excel = {}
        db = {key: (10000, 2)}
        result = compare_fingerprints(excel, db)

        assert key in result.removed
        assert len(result.unchanged) == 0

    def test_fallback_threshold(self):
        """When >80% of overlapping groups changed, should_fallback=True."""
        from src.services.import_diff import compare_fingerprints

        # 10 groups total in both sides; 9 changed, 1 unchanged → 90%
        excel = {}
        db = {}
        for i in range(10):
            key = (f"BC{i}", "C1", "Mar-26")
            if i < 9:
                excel[key] = (100, 1)
                db[key] = (999, 1)  # different
            else:
                excel[key] = (100, 1)
                db[key] = (100, 1)  # same

        result = compare_fingerprints(excel, db)
        assert result.should_fallback is True
        assert len(result.changed) == 9
        assert len(result.unchanged) == 1

    def test_no_fallback_below_threshold(self):
        """When <=80% of overlapping groups changed, should_fallback=False."""
        from src.services.import_diff import compare_fingerprints

        # 10 groups; 1 changed, 9 unchanged → 10%
        excel = {}
        db = {}
        for i in range(10):
            key = (f"BC{i}", "C1", "Mar-26")
            if i < 1:
                excel[key] = (100, 1)
                db[key] = (999, 1)  # different
            else:
                excel[key] = (100, 1)
                db[key] = (100, 1)  # same

        result = compare_fingerprints(excel, db)
        assert result.should_fallback is False
        assert len(result.changed) == 1
        assert len(result.unchanged) == 9
