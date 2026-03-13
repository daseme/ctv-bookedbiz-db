"""Tests for ManagerDashboardService."""

import sqlite3
from datetime import date, timedelta

import pytest


@pytest.fixture
def mgr_db():
    """In-memory DB with tables and seed data for manager dashboard tests."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            normalized_name TEXT,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE agencies (
            agency_id INTEGER PRIMARY KEY,
            agency_name TEXT,
            assigned_ae TEXT,
            is_active INTEGER DEFAULT 1
        );
        CREATE TABLE entity_signals (
            entity_type TEXT,
            entity_id INTEGER,
            signal_type TEXT,
            signal_label TEXT,
            signal_priority INTEGER,
            trailing_revenue REAL DEFAULT 0,
            PRIMARY KEY (entity_type, entity_id, signal_type)
        );
        CREATE TABLE signal_actions (
            action_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            signal_type TEXT NOT NULL,
            assigned_ae TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            reason TEXT,
            snooze_until DATE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT
        );
        CREATE TABLE entity_activity (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            activity_type TEXT NOT NULL,
            activity_date TEXT DEFAULT (date('now')),
            description TEXT,
            created_by TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            contact_id INTEGER,
            due_date TEXT,
            is_completed INTEGER DEFAULT 0,
            completed_date TIMESTAMP
        );

        -- AE Alice: 2 customers + 1 agency
        INSERT INTO customers VALUES (10, 'Acme Corp', 'Alice', 1);
        INSERT INTO customers VALUES (11, 'Beta LLC', 'Alice', 1);
        INSERT INTO agencies VALUES (1, 'MediaMax', 'Alice', 1);

        -- AE Bob: 1 customer
        INSERT INTO customers VALUES (20, 'Zeta Inc', 'Bob', 1);

        -- Signals
        INSERT INTO entity_signals VALUES
            ('customer', 10, 'renewal_gap', 'Renewal gap: $5K', 1, 5000);
        INSERT INTO entity_signals VALUES
            ('customer', 20, 'churned', 'Churned', 5, 3000);

        -- Follow-ups: customer 10 overdue, customer 11 future, customer 20 overdue
        INSERT INTO entity_activity
            (entity_type, entity_id, activity_type, due_date, is_completed)
        VALUES
            ('customer', 10, 'follow_up', date('now', '-3 days'), 0),
            ('customer', 11, 'follow_up', date('now', '+5 days'), 0),
            ('customer', 20, 'follow_up', date('now', '-1 day'), 0);
    """)

    # Signal actions with explicit created_date for aging tests
    ten_days_ago = (date.today() - timedelta(days=10)).isoformat()
    three_days_ago = (date.today() - timedelta(days=3)).isoformat()
    conn.execute("""
        INSERT INTO signal_actions
            (entity_type, entity_id, signal_type, assigned_ae, status,
             created_date)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ['customer', 10, 'renewal_gap', 'Alice', 'new', ten_days_ago])
    conn.execute("""
        INSERT INTO signal_actions
            (entity_type, entity_id, signal_type, assigned_ae, status,
             created_date)
    VALUES (?, ?, ?, ?, ?, ?)
    """, ['customer', 20, 'churned', 'Bob', 'new', three_days_ago])

    from src.services.manager_dashboard_service import ManagerDashboardService
    svc = ManagerDashboardService.__new__(ManagerDashboardService)
    svc.db_connection = None

    yield svc, conn
    conn.close()


class TestScoreboardStats:
    """Tests for get_scoreboard()."""

    @pytest.fixture(autouse=True)
    def _setup(self, mgr_db):
        self.svc, self.conn = mgr_db

    def test_account_counts_per_ae(self):
        result = self.svc.get_scoreboard(self.conn, ["Alice", "Bob"])
        assert result["Alice"]["account_count"] == 3  # 2 customers + 1 agency
        assert result["Bob"]["account_count"] == 1

    def test_revenue_at_risk_per_ae(self):
        result = self.svc.get_scoreboard(self.conn, ["Alice", "Bob"])
        assert result["Alice"]["revenue_at_risk"] == 5000
        assert result["Bob"]["revenue_at_risk"] == 0  # churned, not renewal_gap

    def test_unworked_signals_7d(self):
        result = self.svc.get_scoreboard(self.conn, ["Alice", "Bob"])
        assert result["Alice"]["unworked_signals_7d"] == 1  # 10 days old
        assert result["Bob"]["unworked_signals_7d"] == 0  # only 3 days old

    def test_follow_up_counts(self):
        result = self.svc.get_scoreboard(self.conn, ["Alice", "Bob"])
        assert result["Alice"]["open_followups"] == 2
        assert result["Alice"]["overdue_followups"] == 1
        assert result["Bob"]["open_followups"] == 1
        assert result["Bob"]["overdue_followups"] == 1

    def test_empty_ae_list_returns_empty_dict(self):
        result = self.svc.get_scoreboard(self.conn, [])
        assert result == {}

    def test_unknown_ae_returns_zeros(self):
        result = self.svc.get_scoreboard(self.conn, ["Nobody"])
        assert result["Nobody"]["account_count"] == 0
        assert result["Nobody"]["revenue_at_risk"] == 0


class TestAttentionItems:
    """Tests for get_attention_items()."""

    @pytest.fixture(autouse=True)
    def _setup(self, mgr_db):
        self.svc, self.conn = mgr_db

    def test_includes_unworked_signal_over_7d(self):
        items = self.svc.get_attention_items(self.conn)
        unworked = [i for i in items if i["item_type"] == "unworked_signal"]
        assert len(unworked) == 1
        assert unworked[0]["entity_name"] == "Acme Corp"
        assert unworked[0]["assigned_ae"] == "Alice"
        assert unworked[0]["days_aging"] >= 10

    def test_excludes_signal_under_7d(self):
        items = self.svc.get_attention_items(self.conn)
        unworked = [i for i in items if i["item_type"] == "unworked_signal"]
        bobs = [i for i in unworked if i["assigned_ae"] == "Bob"]
        assert len(bobs) == 0

    def test_customer_10_in_renewal_gap_stale_by_default(self):
        items = self.svc.get_attention_items(self.conn)
        stale = [i for i in items
                 if i["item_type"] == "renewal_gap_stale"
                 and i["entity_name"] == "Acme Corp"]
        assert len(stale) == 1

    def test_includes_renewal_gap_stale_14d(self):
        self.conn.execute("""
            INSERT INTO entity_signals VALUES
                ('agency', 1, 'renewal_gap', 'Renewal gap', 1, 8000)
        """)
        items = self.svc.get_attention_items(self.conn)
        stale = [i for i in items if i["item_type"] == "renewal_gap_stale"]
        assert any(i["entity_name"] == "MediaMax" for i in stale)

    def test_excludes_renewal_gap_with_recent_activity(self):
        self.conn.execute("""
            INSERT INTO entity_activity
                (entity_type, entity_id, activity_type, activity_date,
                 created_by)
            VALUES ('customer', 10, 'call', date('now', '-2 days'), 'Alice')
        """)
        items = self.svc.get_attention_items(self.conn)
        stale = [i for i in items
                 if i["item_type"] == "renewal_gap_stale"
                 and i["entity_name"] == "Acme Corp"]
        assert len(stale) == 0

    def test_sorted_by_revenue_descending(self):
        self.conn.execute("""
            INSERT INTO entity_signals VALUES
                ('agency', 1, 'renewal_gap', 'Renewal gap', 1, 8000)
        """)
        items = self.svc.get_attention_items(self.conn)
        revenues = [i.get("trailing_revenue", 0) for i in items]
        assert revenues == sorted(revenues, reverse=True)

    def test_empty_when_no_issues(self):
        self.conn.execute(
            "UPDATE signal_actions SET status = 'acknowledged'"
        )
        self.conn.execute("DELETE FROM entity_signals")
        items = self.svc.get_attention_items(self.conn)
        assert items == []
