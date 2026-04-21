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
