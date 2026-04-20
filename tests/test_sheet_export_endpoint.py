"""Route-level tests for GET /api/revenue/sheet-export."""

import pytest


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
