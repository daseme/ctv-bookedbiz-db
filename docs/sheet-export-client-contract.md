# Sheet Export — Client Contract (Workbook Side)

**Audience:** the LLM / engineer building the Power Query side of `Revenue Master.xlsx`.
**Server:** SpotOps Flask app, `GET /api/revenue/sheet-export`.
**Full design:** `docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md`.
**Server runbook:** `docs/sheet-export-runbook.md`.

This doc is the self-contained handoff: what the endpoint returns, what PQ
must guarantee, and the known server-side gaps you should plan around.

---

## 1. Endpoint

```
GET http://<host>:8000/api/revenue/sheet-export
Header: X-SpotOps-Token: <shared-secret>
```

- Shared secret lives in the workbook's hidden `Config!ApiToken` named range.
- Server reads expected value from env var `SHEET_EXPORT_TOKEN`.
- Port is **8000** in the current Docker compose stack (the runbook example
  shows 5000 — stale, ignore).
- Tailscale provides the network layer; the header is the app-level auth.

## 2. Response shape

```json
{
  "metadata": {
    "generated_at": "2026-04-21T00:03:06Z",
    "start_month":  null,
    "end_month":    null,
    "hash_version": "v1",
    "row_count":    5346
  },
  "rows": [
    {
      "customer":        "Admerasia:McDonalds",
      "market":          "SFO",
      "revenue_class":   "Internal Ad Sales",
      "ae1":             "Charmaine",
      "agency_flag":     "Y",
      "sector":          "Outreach",
      "broadcast_month": "2025-01-01",
      "gross_rate":      4690.00,
      "station_net":     3986.50,
      "broker_fees":     0.00
    }
  ]
}
```

### Field reference

| Field | Type | Source | Notes |
|---|---|---|---|
| `customer` | string | `spots.bill_code` | e.g. `"Admerasia:McDonalds"` |
| `market` | string | `markets.market_code` | e.g. `"SFO"`, `"LAX"` |
| `revenue_class` | string | `spots.revenue_type` | Trade is excluded server-side |
| `ae1` | string | `spots.sales_person` | |
| `agency_flag` | `"Y"` \| `"N"` | derived from `spots.agency_flag` | Server converts `'Agency'`→`Y` |
| `sector` | string \| null | `sectors.sector_name` | nullable |
| `broadcast_month` | ISO date string | `spots.broadcast_month` (`Mmm-YY`) | Always first-of-month `YYYY-MM-01` |
| `gross_rate` | number | `SUM(spots.gross_rate)` | Per-spot sum for the grain |
| `station_net` | number | `SUM(spots.station_net)` | |
| `broker_fees` | number | `SUM(spots.broker_fees)` | |

### Grain

One row per `(customer, market, revenue_class, ae1, agency_flag, sector) ×
broadcast_month`. Rows where all three amounts sum to zero are suppressed
server-side. `is_historical = 1` spots are **included** (forward bookings
are the main point).

## 3. PQ derivations (not returned by API)

Compute client-side:

- `AgencyPercent` = `1 - SUM(station_net) / SUM(gross_rate)` across the row's months, zero-safe.
- `Broker` = `"Y" if SUM(broker_fees) > 0 else "N"`.
- `BrokerPercent` = `SUM(broker_fees) / SUM(gross_rate)`, zero-safe.
- `BrokerName` = blank in v1.
- `GrossCommission` = lookup in `tblCommissionByAE` keyed by `ae1`. Unknown AE → emit to `New Rows` with `Reason = "Unknown AE for commission"`.
- `Active` = `"Y"` (no churn tracking in this workbook).

## 4. Hash contract (critical)

Ambiguity here silently breaks `tblKnownRows` acknowledgements. Spec §6.6 is
authoritative; summary:

- **Algorithm:** `SHA1(lower(trim(f1)) ⊕ U+001F ⊕ lower(trim(f2)) ⊕ … ⊕ U+001F ⊕ lower(trim(fn)))`
- **Field order (FIXED — never change without bumping `hash_version`):**
  1. `customer`
  2. `market`
  3. `revenue_class`
  4. `ae1`
  5. `agency_flag`
  6. `sector`
- **Nulls** → empty string `""`. Never the literal `"null"`.
- **Whitespace** — trim both sides, no interior collapse. PQ: `Text.Trim`.
- **Case** — lowercase for hashing only; display keeps original case.
  **Invariant culture required** (Turkish dotted-I). PQ: `Text.Lower(x, "en-US")`.
- **Encoding** — UTF-8 before SHA1.
- **Hash version** — `Config!HashVersion` must match `metadata.hash_version`.
  PQ **must assert equality and error loudly** on mismatch.

Test vectors are in spec §6.6. Concrete SHA1 hex values are not yet pinned —
pin them in both server-side and PQ-side unit tests before go-live.

## 5. Error handling

| Situation | PQ behavior |
|---|---|
| `401 {"error": "Authentication required"}` | Refresh fails; user checks `Config!ApiToken`. |
| `503 {"error": "...misconfigured..."}` | Server env missing `SHEET_EXPORT_TOKEN`. Ops fix, not user fix. |
| `504` | DB timeout. Retry once; surface as refresh failure. |
| API unreachable (Tailscale down, server off) | Refresh fails; sheet unchanged from prior refresh. |
| `hash_version` ≠ `Config!HashVersion` | **Error loudly.** Do not proceed; acknowledgements would silently break. |
| Empty `rows` array | **Error**, don't blank the grid. Technically valid but almost always means something is wrong. Override via `Config!AllowEmptyResponse = TRUE` + loud red banner when flipped on. |
| Row has null `broadcast_month` or null required metadata | Error at normalization. Treat as malformed, not dropped silently. |
| `tblForecasts` row has invalid Month (not first-of-month, or blank) | Row excluded from merge; surface on `New Rows` with `Data Quality = "Invalid forecast month"`. |
| `tblForecasts` row has Customer the DB has never seen | Include; metadata blank on `Data`; `New Rows` flags as `"Forecast without DB match"`. |

## 6. First-refresh credentials dialog (setup footgun)

On Excel desktop's first `Web.Contents` refresh, PQ prompts for credentials.
Auth is already in the `X-SpotOps-Token` header, so:

- **Choose `Anonymous`.** Never `Windows` or `Basic` — both cause `401`s
  (wrong auth mode, header ignored).
- If wrong option picked: Data → Get Data → Data Source Settings → pick the URL → Edit Permissions → Anonymous.

This is the single most common setup footgun.

## 7. Data sheet is read-only

PQ overwrites on each refresh, so any hand-typed value disappears silently
next refresh. Two mitigations, both applied:

1. Sheet protection (Review → Protect Sheet) with password in runbook.
2. Row-1 yellow banner:
   `⚠ READ-ONLY. Type forecasts into the Forecasts tab, not here. Any edits here are erased on next refresh.`
   PQ output starts at row 3 so the banner survives re-pivots.

## 8. Known server-side gaps (plan around)

These are real today. Don't design the workbook assuming they'll be fixed
before your v0.

1. **`start_month` / `end_month` query params are accepted but not applied.**
   The service signature takes them, echoes them into `metadata`, but
   `_query()` does not bind them into the SQL WHERE clause. Passing
   `?start_month=Jan-25&end_month=Feb-25` today returns the full dataset
   (5346 rows including months outside that window). **Workbook v1 does not
   send query params** per spec §6, so this doesn't block you — but don't
   add them.
2. **`include_closed` query param in spec §5 is not implemented at all.**
   Same call: don't depend on it.
3. **Hash SHA1 test-vector values not yet pinned.** Spec §6.6 defines the
   algorithm and two test vectors but leaves the hex digests as
   `[compute during implementation]`. When you implement the PQ hash,
   compute both vectors and post the hex to the server-side test author so
   they pin the same values.
4. **`/api/health` returns 401** because the app-wide `_require_login`
   before_request hook allow-lists `/health` but not `/api/health`.
   Unrelated to export, but affects any liveness probe you might want to
   hit from the workbook or monitoring.

## 9. Related tabs in the workbook (spec §7)

| Tab | Hidden | Writer | Purpose |
|---|---|---|---|
| `Data` | No | PQ | Main grid (wide pivot). Read-only (§7). |
| `Forecasts` | No | User | `tblForecasts`: Customer, Market, Revenue Class, Month, Forecast |
| `New Rows` | No | PQ | Flagged new/drifted tuples + data-quality warnings |
| `Known Rows` | Yes | User | `tblKnownRows`: single `hash` column |
| `Config` | Yes | User | Named ranges: `ApiBaseUrl`, `ApiToken`, `HashVersion`. Plus `tblCommissionByAE`. |

See spec §6 for the five Power Query definitions (`qRevenueActuals`,
`qForecasts`, `qMerged`, `qDataPivot`, `qNewRows`) and the two-phase
forecast-reattachment rule under metadata drift (§3.9).

## 10. Smoke test (server side)

```bash
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/sheet-export \
  | jq '.metadata'
```

Expected: `{"generated_at": "...", "hash_version": "v1", "row_count": N, ...}`
with `N > 0` on a populated DB.
