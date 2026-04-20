# Revenue Sheet Export — Design

**Date:** 2026-04-20
**Status:** Approved for planning
**Target:** Automate the manual customer × market × revenue_class × month revenue workbook Kurt currently maintains by hand from commercial log output.

---

## 1. Problem

Kurt maintains a master Excel workbook (lives in Dropbox, opened on desktop Excel) that pivots revenue by:

- **Row grain:** `Customer` (= bill_code, e.g. `"Admerasia:McDonalds"`), `Market`, `Revenue Class` (= `revenue_type`), plus per-customer metadata columns: `AE1`, `GrossCommission`, `Broker`, `BrokerName`, `BrokerPercent`, `Agency`, `AgencyPercent`, `Sector`.
- **Column grain:** one column per broadcast month (e.g. `1/1/2025`, `2/1/2025`, …) spanning several past years and as far forward as bookings exist.
- **Values:** gross revenue (pre-commission). Past months are actuals; future months are a mix of booked revenue (`is_historical = 1` spots) and Kurt's forward forecasts.

Currently Kurt pulls numbers from the commercial log and types them into the sheet by hand. This workbook also feeds downstream Admin, CTV, and Consolidated sheets.

Goal: replace the manual transcription step with a Power Query refresh, while preserving Kurt's ability to enter forward-looking forecasts for cells the DB doesn't yet cover.

## 2. Non-goals (v1)

- Scheduled / automatic refresh (Power Automate, gateway, Power BI). Manual "Refresh All" only.
- Writing data back from the sheet to the SpotOps DB.
- Automating the downstream Admin / CTV / Consolidated sheets.
- An "acknowledge new rows" button UX. Ship the hash list, wire a button in v2 if the manual step is annoying.
- A manual-override table for metadata on forecast-only rows (no DB presence). Add in v2 if needed.

## 3. Key behavioral decisions (recorded)

These were settled during brainstorming. Implementers should not re-litigate them without talking to Kurt.

1. **Forecast survival (sparse overlay).** A cell in a non-closed month that has a forecast but no DB data keeps the forecast. DB data wins over forecasts for any cell where both exist.
2. **New row handling.** New `(customer, market, revenue_class, ae1, broker_*, agency_*, sector)` tuples appearing in the DB auto-add to the sheet with metadata populated from the DB.
3. **Metadata drift.** When DB metadata changes for an existing customer/market/revenue_class (e.g. AE reassignment), a **new row** is created; the old row is preserved intact. The DB naturally supports this because `spots.sales_person` is captured per spot at import time, so historical revenue stays attached to the old AE.
4. **Flagging.** New and drifted tuples appear on a `New Rows` tab for review. Acknowledged tuples are stored as hashes in a hidden `Known Rows` table.
5. **Closed months.** Rebuilt each refresh from the DB. Closed-month DB values are immutable by design, so this is idempotent. One-time consequence: any closed-month hand-typed value in the current sheet that differs from the DB will be "corrected" to the DB value on first refresh. Accepted.
6. **Active column.** Always `Y` in PQ output. (Sheet doesn't track churn in this workbook context.)
7. **Obsolete columns.** `2022 & 2023` and `2023 & 2024` aggregation columns are dropped in v1.
8. **Forward bookings.** Include `is_historical = 1` spots so booked-but-not-aired future revenue shows in the sheet, matching current commercial-log-based workflow.

## 4. Architecture

```
┌────────────────────────────┐        HTTPS over Tailscale
│   SpotOps Flask app        │ ◄────────────────────────────────┐
│   new endpoint:            │                                  │
│   /api/revenue/sheet-export│                                  │
│   returns JSON (long)      │                                  │
└────────────────────────────┘                                  │
                                                                │
┌──────────────────────────────────────────────────────────────┐│
│  Revenue Master.xlsx  (Dropbox, desktop Excel)               ││
│                                                              ││
│  Sheet "Data"        ← PQ-owned, rebuilt each refresh ◄──────┘│
│  Sheet "Forecasts"   ← native Excel Table, user edits here    │
│  Sheet "New Rows"    ← PQ-owned, flagged tuples               │
│  Sheet "Known Rows"  ← hidden, hash list of acknowledged rows │
│  Sheet "Config"      ← hidden, API base URL + shared token    │
└──────────────────────────────────────────────────────────────┘
```

**One Flask endpoint. No DB schema changes. No new services beyond the endpoint's handler + supporting repository/service layer to keep clean-architecture boundaries.**

## 5. API

### Endpoint

`GET /api/revenue/sheet-export`

### Headers

- `X-SpotOps-Token: <shared-secret>` — required. Returns `401` if missing or wrong.
- `Accept: application/json` — default.

### Query parameters (all optional for v1)

- `start_month` — `Mmm-YY`. Default: earliest non-trade spot broadcast_month in DB.
- `end_month` — `Mmm-YY`. Default: latest broadcast_month with any DB data (including future `is_historical = 1` bookings).
- `include_closed` — `true` / `false`. Default `true`. Reserved for lighter future refreshes; v1 always uses default.

### Response schema

```json
{
  "metadata": {
    "generated_at": "2026-04-20T15:04:00Z",
    "start_month":  "Jan-22",
    "end_month":    "Dec-26",
    "row_count":    18452
  },
  "rows": [
    {
      "customer":       "Admerasia:McDonalds",
      "market":         "SFO",
      "revenue_class":  "Internal Ad Sales",
      "ae1":            "Charmaine",
      "broker_flag":    "N",
      "broker_name":    null,
      "broker_percent": null,
      "agency_flag":    "Y",
      "agency_percent": 15,
      "sector":         "Outreach",
      "broadcast_month":"Jan-25",
      "gross_rate":     4690.00
    }
  ]
}
```

One object per unique metadata tuple × broadcast_month. Values with `SUM(gross_rate) = 0` are suppressed (no-revenue months don't need rows).

### Query logic

Conceptual SQL, to be implemented via the container + `spots_reporting` view (or direct from `spots` joined to dimension tables if `spots_reporting` lacks needed columns — verify in implementation):

```sql
SELECT
  bill_code               AS customer,
  market_code             AS market,
  revenue_type            AS revenue_class,
  sales_person            AS ae1,
  broker_flag,            broker_name,   broker_percent,
  agency_flag,            agency_percent,
  sector_name             AS sector,
  broadcast_month,
  SUM(gross_rate)         AS gross_rate
FROM spots_reporting
-- spots_reporting already excludes revenue_type = 'Trade'.
-- is_historical = 1 is INCLUDED (forward bookings are the main point).
GROUP BY 1,2,3,4,5,6,7,8,9,10,broadcast_month
HAVING SUM(gross_rate) <> 0
ORDER BY 1,2,3,4,5,6,7,8,9,10,
  CASE SUBSTR(broadcast_month,1,3)
    WHEN 'Jan' THEN 1  WHEN 'Feb' THEN 2  WHEN 'Mar' THEN 3
    WHEN 'Apr' THEN 4  WHEN 'May' THEN 5  WHEN 'Jun' THEN 6
    WHEN 'Jul' THEN 7  WHEN 'Aug' THEN 8  WHEN 'Sep' THEN 9
    WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
  END,
  SUBSTR(broadcast_month,5,2);
```

### Fields to verify during implementation

The following columns exist on the sheet but need confirmation in the DB before finalizing the query. For any that don't exist, the spec falls back to: emit `null` from the API, require manual entry in a v2 overrides table.

- `broker_flag`, `broker_name`, `broker_percent` — may live on `customers`, a `brokers` table, or the `revenue_entity` resolution. Implementer should grep schema and confirm.
- `agency_flag`, `agency_percent` — `agency_flag` is `Y`/`N` based on whether `bill_code` has an agency prefix; that's derivable. `agency_percent` is almost certainly per-customer configuration — verify.
- `GrossCommission` — appears to be a function of `AE1` (Charmaine = 10%, House = 0%). If so, compute from AE; otherwise treat as manual.

If any of these prove DB-absent, update this spec and add them to the v2 manual-override list rather than fabricating the design around them.

### Performance budget

~18k rows × manual refresh. Expected response size ~2–5 MB uncompressed JSON. Response time target: < 10s p95 on the dev box. If exceeded, add `materialized = true` option that reads from a nightly-built materialized table (not v1).

## 6. Power Query structure

All queries live in the `Revenue Master.xlsx` workbook.

### `qRevenueActuals` (staging, loads nowhere)

- Source: `Web.Contents(base_url & "/api/revenue/sheet-export", [Headers = [#"X-SpotOps-Token" = token]])`
- `base_url` and `token` come from the `Config` sheet (named ranges).
- Parse `rows` → table.
- Add column `month_date` = first-of-month Date derived from `broadcast_month` (`Jan-25` → `2025-01-01`).

### `qForecasts` (staging, loads nowhere)

- Source: `Excel.CurrentWorkbook(){[Name="tblForecasts"]}[Content]`.
- Columns: `Customer`, `Market`, `Revenue Class`, `Month` (Date, must be first-of-month), `Forecast` (Number).
- Normalize: trim whitespace on Customer/Market/Revenue Class. Reject rows where `Month` is not first-of-month (emitted to a `Data Quality` column on `New Rows`).

### `qMerged` (staging, loads nowhere)

- Full outer join of `qRevenueActuals` and `qForecasts` on `(customer, market, revenue_class, month_date)`.
- Compute `value = if gross_rate <> null then gross_rate else forecast`.
- Metadata columns come from `qRevenueActuals` if present, else null (forecast-only rows).

### `qDataPivot` → loads to `Data` sheet

- Group by the full metadata tuple: `customer, market, revenue_class, ae1, broker_flag, broker_name, broker_percent, agency_flag, agency_percent, sector`.
- Pivot `month_date` to columns, chronologically sorted, MM/DD/YYYY formatted headers to match existing sheet.
- Insert `Active` column = `"Y"`.
- Final column order matches Kurt's existing sheet: `Customer, Active, Market, Revenue Class, AE1, GrossCommission, Broker, BrokerName, BrokerPercent, Agency, AgencyPercent, Sector, <month columns>`. (The `2022 & 2023` / `2023 & 2024` aggregates are dropped.)
- Sort rows alphabetically by `Customer`, `Market`, `Revenue Class`.

### `qNewRows` → loads to `New Rows` sheet

- Compute a stable hash of the metadata tuple (e.g. SHA1 of concatenated fields with a separator).
- Anti-join against `tblKnownRows` (hash list) to find unacknowledged tuples.
- For each unacknowledged tuple, compute `Reason`:
  - If another tuple with the same `(customer, market, revenue_class)` but different metadata is already in `tblKnownRows`, reason is `"Metadata drift"`. (E.g. the AE changed from Charmaine to Kurt — the new-AE tuple is unacknowledged, the old-AE tuple is acknowledged.)
  - Otherwise reason is `"New combo"`.
- Output columns: full metadata, the computed tuple `hash`, and `Reason`.
- Plus a `Data Quality` column for forecast rows rejected during `qForecasts` normalization (e.g. invalid month).

### `Known Rows` mechanism

- Hidden sheet, single column `hash` stored as a native Excel Table `tblKnownRows`.
- User acknowledges a flagged row by copying its hash from `New Rows` (another column on that sheet) into `tblKnownRows`.
- V2: a small Office Script or "Acknowledge Selected" button. Not v1.

## 7. Sheet layout

### Tabs

| Tab | Hidden? | Writer | Purpose |
|---|---|---|---|
| `Data` | No | PQ | Main grid (wide pivot). Read-only to user. |
| `Forecasts` | No | User | `tblForecasts`: Customer, Market, Revenue Class, Month, Forecast |
| `New Rows` | No | PQ | Flagged new/drifted tuples + data-quality warnings |
| `Known Rows` | Yes | User | `tblKnownRows`: single `hash` column |
| `Config` | Yes | User | Named ranges: `ApiBaseUrl`, `ApiToken` |

### `tblForecasts` example seed rows

```
Customer                           Market   Revenue Class       Month       Forecast
Admerasia:McDonalds                SFO      Internal Ad Sales   2026-07-01  5500
Borough of Manhattan CC            NYC      Internal Ad Sales   2026-09-01  4000
```

One-time migration task: extract Kurt's current sheet's forward-looking cells into an initial `tblForecasts` population. Implementer should write a small helper script (Python or Excel formula) rather than hand-retyping.

### Downstream impact

Admin / CTV / Consolidated sheets reference the current workbook by cell/range. The new `Data` sheet preserves column order and customer sort order, so downstream references continue to resolve. **Verification during implementation:** open each downstream workbook after first successful PQ refresh and spot-check that references resolve and totals match.

## 8. Auth

- Shared secret in an `X-SpotOps-Token` header.
- Token lives in the workbook's hidden `Config` tab (named range `ApiToken`).
- Endpoint reads expected value from env var `SHEET_EXPORT_TOKEN`. Missing env var → endpoint returns `503` (misconfigured), not `200`.
- Tailscale provides the network auth layer. The token prevents casual browser hits from anyone on the tailnet.
- Token rotation: change env var on server, update `Config` cell, refresh. Not automated in v1.

## 9. Error handling

| Failure | Behavior |
|---|---|
| API unreachable (Tailscale down, server off) | PQ refresh fails with a clear message; sheet unchanged from prior refresh. |
| API returns `401` | PQ refresh fails; user checks Config token. |
| API returns unexpected schema | PQ errors at the parse step; user sees error in refresh dialog. |
| `tblForecasts` row has invalid Month (not first-of-month, or blank) | Row is excluded from merge; surfaces on `New Rows` with `Data Quality = "Invalid forecast month"`. |
| `tblForecasts` row has Customer the DB has never seen | Row is included; metadata columns blank on `Data`; `New Rows` flags the tuple. |
| DB query times out | Endpoint returns `504`; PQ surfaces as refresh failure. |

## 10. Testing

### Flask endpoint

- Unit tests: grouping correctness, Trade exclusion, `is_historical = 1` inclusion, month sorting, auth header required, `401` on missing/wrong token, `503` on missing env var.
- Integration test: hit endpoint on dev DB, diff result against a hand-computed pivot of `spots_reporting` for a bounded slice (e.g. `Admerasia:McDonalds`, all markets, 2025).

### Power Query

- Manual: build the workbook once, refresh, compare `Data` sheet against Kurt's existing hand-maintained sheet row-by-row for a representative slice.
  - Expected diffs: open-month values may differ where DB has more recent log data than Kurt's last manual transcription.
  - Unexpected diffs: closed-month values should match exactly. Any mismatch in a closed month is either a bug in the endpoint or a hand-entry error in the old sheet. Investigate each.
- Refresh-idempotence: two consecutive refreshes on unchanged DB produce identical `Data` sheets.
- Forecast survival: put a forecast in `tblForecasts` for a future month the DB has no data for; confirm it appears on `Data` after refresh; confirm it's replaced by DB value once DB data for that cell arrives.
- New Rows: force a new tuple (e.g. via a test spot import), refresh, confirm it appears on `New Rows`; add hash to `tblKnownRows`; refresh again; confirm it's gone from `New Rows`.

### End-to-end

- Run `daily_update.sh` on the dev stack, then refresh the workbook, then open Admin/CTV/Consolidated downstream sheets and verify their pulled totals match expectations.

## 11. Risks and open items

- **Broker/percent/commission field sources** (§5) — must be resolved during Step 1 of implementation. Block further work on §6–§7 until confirmed.
- **Downstream sheet compatibility** — the current sheet's exact column order and customer sort order must be preserved. Any deviation breaks Admin/CTV/Consolidated references. Implementer must snapshot the current column order as the first implementation step.
- **Initial seed of `tblForecasts`** — the migration from "forecasts live as cells in the main grid" to "forecasts live in a long table" is a one-time manual-ish step. Needs a small helper script or a carefully scripted Power Query pass on the existing sheet.
- **Dropbox + open workbook + refresh** — not a known footgun in this configuration, but worth noting: if Dropbox is mid-sync when the file is open, PQ refresh may race. Mitigation: close → sync → open → refresh when in doubt.
- **`agency_percent` as per-customer vs. per-agency** — if it's per-customer, the DB likely has it; if it's a manual policy the business applies, it's not DB-derivable. Resolve with Kurt during Step 1.

## 12. Implementation sequencing (high-level — detailed plan to follow)

1. **DB schema audit** — confirm broker / agency_percent / gross_commission field sources. Resolve any gaps with Kurt before writing code.
2. **Flask endpoint + tests** — add the endpoint, repository, service. Wire auth. Confirm output shape against `spots_reporting` hand-pivot.
3. **Power Query workbook v0** — one-off scratch file to validate the PQ structure end-to-end against the real API.
4. **Forecasts seed** — extract forward-looking cells from the current master sheet into a `tblForecasts` seed.
5. **Power Query workbook v1** — final file shape with all five queries, config/known/new-rows tabs, formatted to match Kurt's column order.
6. **Downstream verification** — open Admin/CTV/Consolidated, verify references resolve and totals match.
7. **Runbook** — short doc on how to refresh, how to ack new rows, how to rotate the token. In `docs/`.
