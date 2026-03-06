"""Tests for DatabaseConnection."""

import sqlite3
import tempfile
import os

import pytest

from src.database.connection import DatabaseConnection


@pytest.fixture()
def tmp_db():
    """Create a temporary SQLite database with a test table."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT)")
    conn.execute("INSERT INTO test (val) VALUES ('hello')")
    conn.commit()
    conn.close()
    yield path
    os.unlink(path)


class TestConnectionRo:
    """Tests for DatabaseConnection.connection_ro()."""

    def test_reads_data(self, tmp_db):
        dc = DatabaseConnection(tmp_db)
        with dc.connection_ro() as conn:
            row = conn.execute("SELECT val FROM test").fetchone()
        assert row["val"] == "hello"

    def test_rejects_writes(self, tmp_db):
        dc = DatabaseConnection(tmp_db)
        with dc.connection_ro() as conn:
            with pytest.raises(sqlite3.OperationalError):
                conn.execute("INSERT INTO test (val) VALUES ('nope')")

    def test_applies_sqlite_settings(self, tmp_db):
        dc = DatabaseConnection(tmp_db)
        with dc.connection_ro() as conn:
            timeout = conn.execute(
                "PRAGMA busy_timeout"
            ).fetchone()[0]
            assert timeout == 30000

            fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
            assert fk == 1


class TestConnectionRw:
    """Tests for DatabaseConnection.connection() (read-write)."""

    def test_allows_writes(self, tmp_db):
        dc = DatabaseConnection(tmp_db)
        with dc.connection() as conn:
            conn.execute("INSERT INTO test (val) VALUES ('world')")
            conn.commit()

        with dc.connection_ro() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM test"
            ).fetchone()[0]
        assert count == 2
