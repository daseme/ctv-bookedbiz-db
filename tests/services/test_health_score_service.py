"""Tests for HealthScoreService health score computation."""

import sqlite3
from datetime import date, timedelta

import pytest

from src.database.connection import DatabaseConnection
from src.services.health_score_service import TIER_CADENCE, HealthScoreService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bm(months_offset: int) -> str:
    """Return a broadcast_month string (e.g. 'Jan-26') offset from today."""
    d = date.today()
    # Shift month by offset (negative = in the past)
    year = d.year
    month = d.month + months_offset
    while month <= 0:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1
    month_abbr = date(year, month, 1).strftime("%b")
    return f"{month_abbr}-{str(year)[2:]}"


def _insert_spots_for_trend(conn, customer_id, prior, trailing):
    """Insert spots to produce desired prior_3m and trailing_3m revenue totals.

    Prior window: 4–6 months ago (3 months, split evenly).
    Trailing window: 1–3 months ago (3 months, split evenly).
    """
    prior_per_month = prior / 3
    trailing_per_month = trailing / 3

    for offset in [-6, -5, -4]:
        if prior_per_month:
            conn.execute(
                "INSERT INTO spots "
                "(customer_id, broadcast_month, gross_rate, is_historical) "
                "VALUES (?, ?, ?, 0)",
                (customer_id, _bm(offset), prior_per_month),
            )

    for offset in [-3, -2, -1]:
        if trailing_per_month:
            conn.execute(
                "INSERT INTO spots "
                "(customer_id, broadcast_month, gross_rate, is_historical) "
                "VALUES (?, ?, ?, 0)",
                (customer_id, _bm(offset), trailing_per_month),
            )

    conn.commit()


def _find(scores, entity_type, entity_id):
    """Return the health-score dict for a given entity, or None."""
    for s in scores:
        if s["entity_type"] == entity_type and s["entity_id"] == entity_id:
            return s
    return None


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def health_db():
    """In-memory DB with all required tables and seed data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    conn.executescript("""
        CREATE TABLE agencies (
            agency_id   INTEGER PRIMARY KEY,
            agency_name TEXT,
            assigned_ae TEXT,
            is_active   INTEGER DEFAULT 1
        );

        CREATE TABLE customers (
            customer_id     INTEGER PRIMARY KEY,
            normalized_name TEXT,
            assigned_ae     TEXT,
            agency_id       INTEGER,
            is_active       INTEGER DEFAULT 1
        );

        CREATE TABLE entity_signals (
            signal_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type     TEXT,
            entity_id       INTEGER,
            signal_type     TEXT,
            signal_priority INTEGER DEFAULT 0
        );

        CREATE TABLE entity_activity (
            activity_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type  TEXT,
            entity_id    INTEGER,
            activity_type TEXT,
            description  TEXT,
            activity_date TEXT DEFAULT (datetime('now')),
            created_by   TEXT,
            due_date     TEXT,
            is_completed INTEGER DEFAULT 0
        );

        CREATE TABLE spots (
            spot_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id     INTEGER,
            broadcast_month TEXT,
            gross_rate      REAL DEFAULT 0,
            revenue_type    TEXT,
            is_historical   INTEGER DEFAULT 0
        );

        -- Alice has customers 10, 11, 12; Bob has customer 20
        INSERT INTO customers (customer_id, normalized_name, assigned_ae)
        VALUES
            (10, 'Alice Customer A', 'Alice'),
            (11, 'Alice Customer B', 'Alice'),
            (12, 'Alice Customer C', 'Alice'),
            (20, 'Bob Customer',     'Bob');

        -- One agency for Alice
        INSERT INTO agencies (agency_id, agency_name, assigned_ae)
        VALUES (1, 'Alice Agency', 'Alice');
    """)
    conn.commit()

    db_conn = DatabaseConnection.__new__(DatabaseConnection)
    db_conn.db_path = ":memory:"
    svc = HealthScoreService(db_conn)

    yield svc, conn

    conn.close()


# ---------------------------------------------------------------------------
# Revenue trend scoring (unit tests on the scoring method)
# ---------------------------------------------------------------------------


class TestRevenueTrendScore:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

    def _score(self, trailing, prior):
        return self.svc._score_revenue_trend({"trailing": trailing, "prior": prior})

    def test_growing_plus_ten_percent(self):
        assert self._score(110, 100) == 100

    def test_flat_within_plus_minus_ten(self):
        assert self._score(105, 100) == 60

    def test_moderate_decline_fifteen_percent(self):
        assert self._score(85, 100) == 40

    def test_steep_decline_forty_percent(self):
        assert self._score(60, 100) == 20

    def test_severe_decline_sixty_percent(self):
        assert self._score(30, 100) == 0

    def test_no_prior_revenue_returns_neutral(self):
        assert self._score(500, 0) == 60


# ---------------------------------------------------------------------------
# Signal state scoring
# ---------------------------------------------------------------------------


class TestSignalStateScore:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

    def test_no_signal_returns_100(self):
        assert self.svc._score_signal(None) == 100

    def test_growing_returns_100(self):
        assert self.svc._score_signal("growing") == 100

    def test_new_account_returns_80(self):
        assert self.svc._score_signal("new_account") == 80

    def test_gone_quiet_returns_40(self):
        assert self.svc._score_signal("gone_quiet") == 40

    def test_declining_returns_25(self):
        assert self.svc._score_signal("declining") == 25

    def test_churned_returns_0(self):
        assert self.svc._score_signal("churned") == 0


# ---------------------------------------------------------------------------
# Last touch scoring
# ---------------------------------------------------------------------------


class TestLastTouchScore:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

    def test_recent_2_days_returns_100(self):
        assert self.svc._score_last_touch(2) == 100

    def test_exactly_7_days_returns_75(self):
        assert self.svc._score_last_touch(7) == 75

    def test_old_45_days_returns_25(self):
        assert self.svc._score_last_touch(45) == 25

    def test_no_touch_returns_0(self):
        assert self.svc._score_last_touch(None) == 0

    def test_follow_up_not_counted_as_touch(self, health_db):
        """follow_up activity type must not count toward last touch."""
        svc, conn = health_db
        # Insert a follow_up dated today for customer 10
        conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, activity_date) "
            "VALUES ('customer', 10, 'follow_up', date('now'))"
        )
        conn.commit()

        touches = svc._load_last_touches(conn)
        # The follow_up should not produce a touch entry
        assert ("customer", 10) not in touches


# ---------------------------------------------------------------------------
# Follow-up compliance scoring
# ---------------------------------------------------------------------------


class TestFollowUpComplianceScore:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

    def test_no_followups_returns_100(self):
        overdue = self.svc._load_overdue_set(self.conn)
        assert ("customer", 10) not in overdue

    def test_overdue_followup_marks_entity(self):
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, due_date, is_completed) "
            "VALUES ('customer', 10, 'follow_up', ?, 0)",
            (yesterday,),
        )
        self.conn.commit()

        overdue = self.svc._load_overdue_set(self.conn)
        assert ("customer", 10) in overdue

    def test_future_followup_not_overdue(self):
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, due_date, is_completed) "
            "VALUES ('customer', 11, 'follow_up', ?, 0)",
            (tomorrow,),
        )
        self.conn.commit()

        overdue = self.svc._load_overdue_set(self.conn)
        assert ("customer", 11) not in overdue


# ---------------------------------------------------------------------------
# Composite score integration tests
# ---------------------------------------------------------------------------


class TestCompositeScore:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

    def test_perfect_score(self):
        """Customer with growing revenue, no signal issues, recent touch, no overdue."""
        _insert_spots_for_trend(self.conn, 10, prior=100, trailing=200)

        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, activity_date) "
            "VALUES ('customer', 10, 'call', date('now'))"
        )
        self.conn.commit()

        scores = self.svc.get_health_scores(self.conn)
        s = _find(scores, "customer", 10)
        assert s is not None
        # revenue=100, signal=100, touch=100, followup=100
        # composite = 100*0.30 + 100*0.25 + 100*0.25 + 100*0.20 = 100
        assert s["health_score"] == 100
        assert s["health_color"] == "green"

    def test_unhealthy_score(self):
        """Customer with severe revenue decline, churned signal, no touch, overdue followup."""
        _insert_spots_for_trend(self.conn, 11, prior=1000, trailing=100)

        self.conn.execute(
            "INSERT INTO entity_signals "
            "(entity_type, entity_id, signal_type, signal_priority) "
            "VALUES ('customer', 11, 'churned', 1)"
        )
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, due_date, is_completed) "
            "VALUES ('customer', 11, 'follow_up', ?, 0)",
            (yesterday,),
        )
        self.conn.commit()

        scores = self.svc.get_health_scores(self.conn)
        s = _find(scores, "customer", 11)
        assert s is not None
        # revenue=0, signal=0, touch=0, followup=0 → 0
        assert s["health_score"] == 0
        assert s["health_color"] == "red"

    def test_yellow_range(self):
        """Customer with flat revenue, gone_quiet signal, 20-day-old touch, no overdue."""
        # flat revenue: prior=100, trailing=105 → score 60
        _insert_spots_for_trend(self.conn, 12, prior=100, trailing=105)

        self.conn.execute(
            "INSERT INTO entity_signals "
            "(entity_type, entity_id, signal_type, signal_priority) "
            "VALUES ('customer', 12, 'gone_quiet', 1)"
        )
        twenty_days_ago = (date.today() - timedelta(days=20)).isoformat()
        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, activity_date) "
            "VALUES ('customer', 12, 'note', ?)",
            (twenty_days_ago,),
        )
        self.conn.commit()

        scores = self.svc.get_health_scores(self.conn)
        s = _find(scores, "customer", 12)
        assert s is not None
        # revenue=60 (flat), signal=40 (gone_quiet), touch=50 (15-30d), followup=100
        # composite = 60*0.30 + 40*0.25 + 50*0.25 + 100*0.20
        # = 18 + 10 + 12.5 + 20 = 60.5 → 61
        assert 40 <= s["health_score"] < 70
        assert s["health_color"] == "yellow"

    def test_ae_filter_returns_only_ae_entities(self):
        scores = self.svc.get_health_scores(self.conn, ae_name="Bob")
        entity_ids = [(s["entity_type"], s["entity_id"]) for s in scores]
        assert ("customer", 20) in entity_ids
        # Alice's customers must not appear
        assert ("customer", 10) not in entity_ids
        assert ("customer", 11) not in entity_ids
        assert ("customer", 12) not in entity_ids

    def test_no_ae_filter_returns_all(self):
        scores = self.svc.get_health_scores(self.conn)
        ids = {(s["entity_type"], s["entity_id"]) for s in scores}
        # All 4 customers + 1 agency
        assert ("customer", 10) in ids
        assert ("customer", 20) in ids
        assert ("agency", 1) in ids
        assert len(ids) >= 5


# ---------------------------------------------------------------------------
# Helpers for tiering/cadence tests
# ---------------------------------------------------------------------------


def _insert_trailing_12m(conn, customer_id, total_revenue):
    """Insert spots spread across trailing 12 months for tier ranking."""
    monthly = total_revenue / 12
    d = date.today()
    for offset in range(1, 13):
        year = d.year
        month = d.month - offset
        while month <= 0:
            month += 12
            year -= 1
        month_abbr = date(year, month, 1).strftime("%b")
        bm = f"{month_abbr}-{str(year)[2:]}"
        conn.execute(
            "INSERT INTO spots "
            "(customer_id, broadcast_month, gross_rate, is_historical) "
            "VALUES (?, ?, ?, 0)",
            (customer_id, bm, monthly),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Account tiering
# ---------------------------------------------------------------------------


class TestAccountTiering:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

        # Remove all fixture entities so we have exactly 5 controlled customers
        self.conn.execute("DELETE FROM customers")
        self.conn.execute("DELETE FROM agencies")
        self.conn.executemany(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae, is_active) "
            "VALUES (?, ?, 'Alice', 1)",
            [
                (100, "Alice Top"),
                (101, "Alice High"),
                (102, "Alice Mid"),
                (103, "Alice Low"),
                (104, "Alice Bottom"),
            ],
        )
        self.conn.commit()

        # Revenue: 100 > 80 > 60 > 40 > 20
        # Percentile rank (0-based / 5): 0/5=0.0, 1/5=0.2, 2/5=0.4, 3/5=0.6, 4/5=0.8
        # A: <0.20  → customer 100 only
        # B: 0.20–0.60 → customers 101, 102
        # C: >=0.60 → customers 103, 104
        _insert_trailing_12m(self.conn, 100, 100_000)
        _insert_trailing_12m(self.conn, 101, 80_000)
        _insert_trailing_12m(self.conn, 102, 60_000)
        _insert_trailing_12m(self.conn, 103, 40_000)
        _insert_trailing_12m(self.conn, 104, 20_000)

    def test_tiers_assigned_by_revenue_rank(self):
        scores = self.svc.get_health_with_tiers(self.conn, ae_name="Alice")
        by_id = {s["entity_id"]: s for s in scores if s["entity_type"] == "customer"}

        assert by_id[100]["tier"] == "A"
        assert by_id[101]["tier"] == "B"
        assert by_id[102]["tier"] == "B"
        assert by_id[103]["tier"] == "C"
        assert by_id[104]["tier"] == "C"

    def test_tier_cadence_days(self):
        assert TIER_CADENCE["A"] == 7
        assert TIER_CADENCE["B"] == 14
        assert TIER_CADENCE["C"] == 30

    def test_single_account_is_tier_a(self):
        """An AE with a single account gets tier A (top 20% of 1)."""
        self.conn.execute(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae, is_active) "
            "VALUES (200, 'Bob Only', 'Bob', 1)"
        )
        self.conn.commit()
        _insert_trailing_12m(self.conn, 200, 5_000)

        scores = self.svc.get_health_with_tiers(self.conn, ae_name="Bob")
        by_id = {s["entity_id"]: s for s in scores if s["entity_type"] == "customer"}
        assert by_id[200]["tier"] == "A"


# ---------------------------------------------------------------------------
# Touch cadence
# ---------------------------------------------------------------------------


class TestTouchCadence:
    @pytest.fixture(autouse=True)
    def _setup(self, health_db):
        self.svc, self.conn = health_db

        # Single customer for Alice — becomes tier A (1 account = rank 0 / 1 = 0.0 < 0.20)
        self.conn.execute("DELETE FROM customers")
        self.conn.execute(
            "INSERT INTO customers (customer_id, normalized_name, assigned_ae, is_active) "
            "VALUES (300, 'Alice Solo', 'Alice', 1)"
        )
        self.conn.commit()
        _insert_trailing_12m(self.conn, 300, 10_000)

    def _add_touch(self, days_ago):
        touch_date = (date.today() - timedelta(days=days_ago)).isoformat()
        self.conn.execute(
            "INSERT INTO entity_activity "
            "(entity_type, entity_id, activity_type, activity_date) "
            "VALUES ('customer', 300, 'call', ?)",
            (touch_date,),
        )
        self.conn.commit()

    def test_touch_within_cadence_is_green(self):
        """Touch 3 days ago on tier A (7d cadence, 3/7=43% < 75%) = green."""
        self._add_touch(3)
        scores = self.svc.get_health_with_tiers(self.conn, ae_name="Alice")
        s = _find(scores, "customer", 300)
        assert s["tier"] == "A"
        assert s["tier_cadence_days"] == 7
        assert s["touch_status"] == "green"

    def test_no_touch_is_red(self):
        """No touch at all = red."""
        scores = self.svc.get_health_with_tiers(self.conn, ae_name="Alice")
        s = _find(scores, "customer", 300)
        assert s["touch_status"] == "red"
        assert s["days_since_touch"] is None

    def test_touch_nearing_cadence_is_yellow(self):
        """Touch 6 days ago on tier A (7d cadence, 6/7=86% > 75%) = yellow."""
        self._add_touch(6)
        scores = self.svc.get_health_with_tiers(self.conn, ae_name="Alice")
        s = _find(scores, "customer", 300)
        assert s["tier"] == "A"
        assert s["touch_status"] == "yellow"
