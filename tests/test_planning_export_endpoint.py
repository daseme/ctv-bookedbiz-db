"""Route-level tests for GET /api/revenue/planning-export.

These tests exercise the real schema (budget, forecast, revenue_entities,
spots), so the client fixture points at production.db per CLAUDE.md.
"""

import os

import pytest


@pytest.fixture(scope="module")
def client():
    """Planning-export tests need real planning data; point at production.db."""
    prev = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = "/srv/spotops/db/production.db"
    try:
        import src.services.container as _container_mod
        _container_mod._container = None  # type: ignore[attr-defined]

        from src.web.app import create_app
        app = create_app()
        app.config["TESTING"] = True
        app.config["LOGIN_DISABLED"] = True
        app.config["DB_PATH"] = "/srv/spotops/db/production.db"
        yield app.test_client()
    finally:
        if prev is None:
            os.environ.pop("DB_PATH", None)
        else:
            os.environ["DB_PATH"] = prev


@pytest.fixture
def sheet_token(monkeypatch):
    """Planning-export uses the same X-SpotOps-Token as sheet-export."""
    monkeypatch.setenv("SHEET_EXPORT_TOKEN", "test-token-planning-123")
    yield "test-token-planning-123"


def test_missing_token_returns_401(client, sheet_token):
    resp = client.get("/api/revenue/planning-export")
    assert resp.status_code == 401


def test_wrong_token_returns_401(client, sheet_token):
    resp = client.get(
        "/api/revenue/planning-export",
        headers={"X-SpotOps-Token": "wrong-token"},
    )
    assert resp.status_code == 401


def test_missing_env_var_returns_503(client, monkeypatch):
    monkeypatch.delenv("SHEET_EXPORT_TOKEN", raising=False)
    resp = client.get(
        "/api/revenue/planning-export",
        headers={"X-SpotOps-Token": "anything"},
    )
    assert resp.status_code == 503


def test_happy_path_returns_expected_envelope(client, sheet_token):
    resp = client.get(
        "/api/revenue/planning-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    # Raw payload (not wrapped by create_success_response).
    payload = body.get("data", body)
    assert payload["metadata"]["schema_version"] == "1.0"
    assert isinstance(payload["rows"], list)
    assert "year" in payload["metadata"]
    assert payload["metadata"]["row_count"] == len(payload["rows"])


def test_rows_have_expected_fields(client, sheet_token):
    resp = client.get(
        "/api/revenue/planning-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    payload = resp.get_json()
    payload = payload.get("data", payload)
    if payload["rows"]:
        row = payload["rows"][0]
        required = {
            "ae1", "broadcast_month",
            "budget", "forecast", "booked",
            "new_accts", "new_dollars",
            "expected", "pipeline", "vs_budget",
        }
        assert required.issubset(set(row.keys()))


def test_explicit_year_param(client, sheet_token):
    resp = client.get(
        "/api/revenue/planning-export?year=2025",
        headers={"X-SpotOps-Token": sheet_token},
    )
    assert resp.status_code == 200
    payload = resp.get_json()
    payload = payload.get("data", payload)
    assert payload["metadata"]["year"] == 2025


def test_invalid_year_returns_400(client, sheet_token):
    resp = client.get(
        "/api/revenue/planning-export?year=banana",
        headers={"X-SpotOps-Token": sheet_token},
    )
    assert resp.status_code == 400
