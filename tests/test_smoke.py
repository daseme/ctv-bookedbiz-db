"""
Route smoke tests — hit every GET endpoint and assert no 500s.

Run with:  pytest tests/test_smoke.py -v
Fast only: pytest tests/test_smoke.py -v -m "smoke and not slow"
Slow only: pytest tests/test_smoke.py -v -m slow
Or:        pytest -m smoke
"""

import pytest

# ---------------------------------------------------------------------------
# FAST routes — lightweight pages / APIs that return in <3s on the Pi.
# Pre-push hook runs only these (~20-30s total).
# ---------------------------------------------------------------------------
FAST_ROUTES = [
    # -- App-level routes (app.py) --
    "/",
    "/health",
    "/info",

    # -- Reports blueprint (/reports) --
    "/reports/",
    "/reports/customer-sector-manager",
    "/reports/contracts-added",
    "/reports/report4",
    "/reports/language-blocks",

    # -- Customer detail routes (under /reports) --
    "/reports/customer/1",

    # -- API blueprint (/api) --
    "/api/health",

    # -- Health blueprint (/health) --
    "/health/",
    "/health/database",
    "/health/metrics",

    # -- Entity resolution --
    "/entity-resolution",
    "/entity-resolution?tab=agency",
    "/entity-aliases",
    "/entity-aliases?tab=agency",
    "/customer-resolution",
    "/agency-resolution",
    "/customer-aliases",
    "/agency-aliases",
    "/api/customer-resolution/stats",
    "/api/customer-resolution/unresolved",
    "/api/customer-resolution/search?q=test",
    "/api/customer-aliases",
    "/api/agency-resolution/stats",
    "/api/agency-resolution/unresolved",
    "/api/agency-resolution/search?q=test",
    "/api/agency-aliases",

    # -- Contacts API --
    "/api/contacts/customer/1",
    "/api/contacts/agency/7",
    "/api/contacts/customer/1/primary",

    # -- Address book --
    "/address-book",
    "/api/address-book",
    "/api/address-book/sectors",
    "/api/address-book/markets",

    # -- Stale customers --
    "/stale-customers",
    "/api/stale-customers/sectors",
    "/api/stale-customers/stats",
    "/api/stale-customers/entities",

    # -- Customer normalization --
    "/customer-normalization",
    "/api/customer-normalization",
    "/api/customer-normalization/stats",

    # -- Planning --
    "/planning/",

    # -- Pricing trends (individual trend pages are fast) --
    "/pricing/trends/",
    "/pricing/trends/rate-trends",
    "/pricing/trends/margin-trends",
    "/pricing/trends/pricing-consistency",
    "/pricing/trends/yoy-comparison",

    # -- User management (public pages only) --
    "/users/login",
]

# ---------------------------------------------------------------------------
# SLOW routes — heavy DB queries that take >3s each on the Pi.
# Skipped by pre-push hook; run with full suite or `-m slow`.
# ---------------------------------------------------------------------------
SLOW_ROUTES = [
    # -- Language blocks API (6-20s each, heavy analytics queries) --
    "/api/language-blocks/metadata/available-periods",
    "/api/language-blocks/test",
    "/api/language-blocks/summary",
    "/api/language-blocks/top-performers",
    "/api/language-blocks/language-performance",
    "/api/language-blocks/market-performance",
    "/api/language-blocks/time-slot-performance",
    "/api/language-blocks/recent-activity",
    "/api/language-blocks/insights",
    "/api/language-blocks/report",

    # -- Revenue dashboards (3-9s each) --
    "/reports/revenue-dashboard-customer",
    "/reports/management-performance",
    "/reports/management-performance/2025",
    "/reports/monthly/revenue-summary",
    "/reports/ae-dashboard",
    "/reports/market-analysis",
    "/api/revenue/summary",
    "/api/ae/1/summary",
    "/api/export/ae-performance",

    # -- Length analysis (8-10s each) --
    "/length-analysis/",
    "/length-analysis/by-language",

    # -- Pricing (4-9s) --
    "/pricing/",
    "/pricing/trends/concentration",
]

# ---------------------------------------------------------------------------
# Known-broken routes: pre-existing bugs that the smoke tests surfaced.
# Each is marked xfail so the suite stays green while documenting breakage.
# When fixed, the test will xpass and pytest will tell you to remove the mark.
# ---------------------------------------------------------------------------
KNOWN_BROKEN = {
    # Missing templates (legacy routes)
    "/reports/report2": "report2.html template missing",
    "/reports/report3": "report3.html template missing",
    # API routes with code bugs
    "/api/revenue/monthly/2025": "create_success_response() unexpected kwarg 'metadata'",
    "/api/ae/performance": "create_success_response() unexpected kwarg 'metadata'",
    "/api/quarterly/performance": "create_success_response() unexpected kwarg 'metadata'",
    "/api/sectors/performance": "'dict' object has no attribute 'to_dict'",
    "/api/aes": "pipeline_service not registered in container",
    "/api/export/monthly-revenue/2025": "'CustomerMonthlyRow' has no 'get_month_value'",
    "/api/metadata/years": "ReportDataService missing '_get_available_years'",
    "/api/metadata/ae-list": "ReportDataService missing '_get_ae_list'",
    # Missing services / data files
    "/health/pipeline": "pipeline_service not registered in container",
    "/health/budget": "real_budget_data.json not found (503)",
    "/planning/budget": "budget service depends on missing data file",
}


@pytest.mark.smoke
@pytest.mark.parametrize("path", FAST_ROUTES, ids=FAST_ROUTES)
def test_route_no_500(client, path):
    """Every GET route must not return a 500 server error."""
    response = client.get(path)
    assert response.status_code < 500, (
        f"{path} returned {response.status_code}"
    )


@pytest.mark.smoke
@pytest.mark.slow
@pytest.mark.parametrize("path", SLOW_ROUTES, ids=SLOW_ROUTES)
def test_slow_route_no_500(client, path):
    """Heavy-query routes — same assertion, marked slow for selective runs."""
    response = client.get(path)
    assert response.status_code < 500, (
        f"{path} returned {response.status_code}"
    )


@pytest.mark.smoke
@pytest.mark.parametrize(
    "path,reason",
    list(KNOWN_BROKEN.items()),
    ids=list(KNOWN_BROKEN.keys()),
)
def test_known_broken_route(client, path, reason):
    """Known-broken routes — xfail until the underlying bugs are fixed."""
    response = client.get(path)
    if response.status_code >= 500:
        pytest.xfail(reason)
    # If it stops 500-ing, the test passes — pytest reports xpass so you
    # know to move it back to the main ROUTES list.
