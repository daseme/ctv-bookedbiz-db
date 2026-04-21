"""Route-level tests for GET /api/revenue/sheet-export.

These tests need the real spots schema (including `contract`), so the
`client` fixture below overrides the session-scoped one from conftest
to point at production.db per CLAUDE.md guidance.
"""

import os

import pytest


@pytest.fixture(scope="module")
def client():
    """Sheet-export tests exercise real SQL against the production schema.

    The repo-wide conftest points at `.data/dev.db`, a 12-column skeleton
    that lacks `spots.contract` (and most other columns). Per CLAUDE.md,
    tests that call `app.test_client()` must override DB_PATH to a real DB.
    """
    prev = os.environ.get("DB_PATH")
    os.environ["DB_PATH"] = "/srv/spotops/db/production.db"
    try:
        # Drop any cached container built during the session `app` fixture
        # so the new DB_PATH actually takes effect.
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
    """Set the server-side token env var for the test."""
    monkeypatch.setenv("SHEET_EXPORT_TOKEN", "test-token-123")
    yield "test-token-123"


def test_missing_token_returns_401(client, sheet_token):
    """No X-SpotOps-Token header → 401.

    sheet_token fixture sets the env var so we exercise the
    "missing header" branch, not the "missing env var" branch.
    """
    resp = client.get("/api/revenue/sheet-export")
    assert resp.status_code == 401
    assert resp.get_json()["error"] == "Authentication required"


def test_wrong_token_returns_401(client, sheet_token):
    """X-SpotOps-Token header present but doesn't match env → 401."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": "wrong-token"},
    )
    assert resp.status_code == 401


def test_missing_env_var_returns_503(client, monkeypatch):
    """SHEET_EXPORT_TOKEN env var unset on the server → 503."""
    monkeypatch.delenv("SHEET_EXPORT_TOKEN", raising=False)
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": "anything"},
    )
    assert resp.status_code == 503
    assert "misconfigured" in resp.get_json()["error"].lower()


def test_happy_path_returns_expected_shape(client, sheet_token):
    """200 response with metadata + rows shape matching spec §5."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    assert resp.status_code == 200
    body = resp.get_json()
    # The envelope from create_success_response wraps data, so the
    # endpoint should return the raw {metadata, rows} object directly
    # without double-wrapping. Verify the structure.
    assert "metadata" in body or ("data" in body and "metadata" in body["data"])
    # Prefer direct shape (unwrapped) per spec §5.
    payload = body.get("data", body)
    assert payload["metadata"]["hash_version"] == "v1"
    assert isinstance(payload["rows"], list)


def test_rows_have_expected_fields(client, sheet_token):
    """Each row has all seven metadata fields plus three amounts."""
    resp = client.get(
        "/api/revenue/sheet-export",
        headers={"X-SpotOps-Token": sheet_token},
    )
    body = resp.get_json()
    payload = body.get("data", body)
    if payload["rows"]:
        row = payload["rows"][0]
        required = {
            "customer", "market", "revenue_class", "ae1",
            "agency_flag", "sector", "broadcast_month",
            "gross_rate", "station_net", "broker_fees",
        }
        assert required.issubset(set(row.keys()))
