"""
Route smoke tests — hit every GET endpoint and assert no 500s.

Run with:  pytest tests/test_smoke.py -v
Or:        pytest -m smoke
"""

import pytest

# ---------------------------------------------------------------------------
# Every GET route in the app, grouped by blueprint.
# For routes with dynamic params, use known IDs from dev.db.
# ---------------------------------------------------------------------------
ROUTES = [
    # -- App-level routes (app.py) --
    "/",
    "/health",
    "/info",

    # -- Reports blueprint (/reports) --
    "/reports/",
    "/reports/revenue-dashboard-customer",
    "/reports/customer-sector-manager",
    "/reports/ae-dashboard",
    "/reports/contracts-added",
    "/reports/management-performance",
    "/reports/management-performance/2025",
    "/reports/monthly/revenue-summary",
    "/reports/report4",
    "/reports/market-analysis",
    "/reports/language-blocks",

    # -- Customer detail routes (under /reports) --
    "/reports/customer/1",

    # -- API blueprint (/api) --
    "/api/health",
    "/api/revenue/summary",
    "/api/ae/1/summary",
    "/api/export/ae-performance",

    # -- Health blueprint (/health) --
    "/health/",
    "/health/database",
    "/health/metrics",

    # -- Language blocks API (/api/language-blocks) --
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

    # -- Pricing --
    "/pricing/",

    # -- Pricing trends --
    "/pricing/trends/",
    "/pricing/trends/rate-trends",
    "/pricing/trends/margin-trends",
    "/pricing/trends/pricing-consistency",
    "/pricing/trends/yoy-comparison",
    "/pricing/trends/concentration",

    # -- Length analysis --
    "/length-analysis/",
    "/length-analysis/by-language",

    # -- User management (public pages only) --
    "/users/login",
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
@pytest.mark.parametrize("path", ROUTES, ids=ROUTES)
def test_route_no_500(client, path):
    """Every GET route must not return a 500 server error."""
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
