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
9. **Forecast join key on drift.** `tblForecasts` keys on `(customer, market, revenue_class, month)` — *no AE*. After a metadata drift event the DB has two tuples with the same `(customer, market, revenue_class)` and different metadata; a single forecast row would otherwise fan out to both. Rule: the forecast attaches **only to the newest tuple** (the one whose hash isn't yet in `tblKnownRows`, or — if all are acknowledged — the one whose most recent DB-side `broadcast_month` is latest). The attachment decision is recorded on `New Rows` as `Reason = "Forecast reattached to new tuple"` so Kurt sees it happened. If none of that feels right after drift, Kurt can delete or re-key the forecast row manually.
10. **Forecast vs. DB collision — "DB wins" mental model.** For any cell where both a DB value and a forecast exist, the DB value wins. Concretely: a Jun-26 forecast of $10k that later sees $3k of bookings will render as $3k on the Data sheet; the $10k forecast disappears from view (but stays in `tblForecasts`). This is **forecast-as-floor-for-empty-cells**, not forecast-as-target-with-variance. If Kurt wants variance tracking later, that's a separate column, not a mode change. This rule is surfaced in the `Forecasts` tab header row as a visible reminder so it doesn't feel like magic six months from now.

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
    "hash_version": "v1",
    "row_count":    18452
  },
  "rows": [
    {
      "customer":        "Admerasia:McDonalds",
      "market":          "SFO",
      "revenue_class":   "Internal Ad Sales",
      "ae1":             "Charmaine",
      "broker_flag":     "N",
      "broker_name":     null,
      "broker_percent":  null,
      "agency_flag":     "Y",
      "agency_percent":  15,
      "sector":          "Outreach",
      "broadcast_month": "2025-01-01",
      "gross_rate":      4690.00
    }
  ]
}
```

**Date format rule:** query params use `Mmm-YY` (matches how `broadcast_month` is stored in the DB); response `broadcast_month` values are ISO `YYYY-MM-DD` (first-of-month). PQ reformats display headers anyway, and ISO is parse-safe across locales. The endpoint performs the `Mmm-YY` → ISO conversion server-side via the canonical CASE/WHEN pattern from `v_planning_data`.

One object per unique metadata tuple × broadcast_month. Values with `SUM(gross_rate) = 0` are suppressed (no-revenue months don't need rows). An **empty `rows` array is itself a valid response** (legitimate if `start_month`/`end_month` frame no data) but almost certainly a red flag in practice — see §10 error handling.

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
- `GrossCommission` — appears to be a function of `AE1` (Charmaine = 10%, House = 0%). If confirmed, **do not hard-code the lookup in API code or PQ**. Stash it as a two-column Excel Table `tblCommissionByAE` on the `Config` tab (`ae`, `commission_pct`). `qDataPivot` joins against it. Kurt updates the table when a commission rate changes — no deploy required. The endpoint does not return `GrossCommission` in the response; PQ computes it from `ae1`.

If any of these prove DB-absent, update this spec and add them to the v2 manual-override list rather than fabricating the design around them.

### Performance budget

~18k rows × manual refresh. Expected response size ~2–5 MB uncompressed JSON. Response time target: < 10s p95 on the dev box. If exceeded, add `materialized = true` option that reads from a nightly-built materialized table (not v1).

## 6. Power Query structure

All queries live in the `Revenue Master.xlsx` workbook.

### `qRevenueActuals` (staging, loads nowhere)

- Source: `Web.Contents(base_url & "/api/revenue/sheet-export", [Headers = [#"X-SpotOps-Token" = token]])`
- `base_url` and `token` come from the `Config` sheet (named ranges).
- Parse `rows` → table. Type `broadcast_month` as Date (server emits ISO `YYYY-MM-DD`), rename to `month_date` for internal use.
- Normalize metadata fields to match the hash spec (§6.6): trim whitespace on `customer`, `market`, `revenue_class`, `ae1`, `broker_name`, `sector`; lowercase **only** for hashing — the display value stays as returned by the API.

### `qForecasts` (staging, loads nowhere)

- Source: `Excel.CurrentWorkbook(){[Name="tblForecasts"]}[Content]`.
- Columns: `Customer`, `Market`, `Revenue Class`, `Month` (Date, must be first-of-month), `Forecast` (Number).
- Normalize: trim whitespace on Customer/Market/Revenue Class. Reject rows where `Month` is not first-of-month (emitted to a `Data Quality` column on `New Rows`).

### `qMerged` (staging, loads nowhere)

Merging proceeds in two phases to handle metadata drift (§3.9) deterministically:

**Phase 1 — resolve each forecast row to exactly one tuple.**
For each `(customer, market, revenue_class, month_date)` in `qForecasts`, find DB tuples from `qRevenueActuals` that match on `(customer, market, revenue_class)`. Then:

- **Zero matches** (forecast for a combo DB has never seen): forecast is kept as a forecast-only row with blank metadata. Flagged on `New Rows` as `Data Quality = "Forecast without DB match"`.
- **One match** (the common case): forecast attaches to that tuple's metadata.
- **Multiple matches** (drift case): forecast attaches to the **unacknowledged tuple** (hash not in `tblKnownRows`). If all candidates are acknowledged or all are unacknowledged, fall back to the tuple with the latest DB-side `broadcast_month`. Record `Reason = "Forecast reattached to new tuple"` on `New Rows` for the chosen tuple.

**Phase 2 — combine.**
Full outer join `qRevenueActuals` with the Phase-1-resolved forecasts on the full tuple + `month_date`. Compute `value = if gross_rate <> null then gross_rate else forecast`. Metadata columns come from the DB side when present, else from the resolved-forecast side for forecast-only rows.

### `qDataPivot` → loads to `Data` sheet

- Group by the full metadata tuple: `customer, market, revenue_class, ae1, broker_flag, broker_name, broker_percent, agency_flag, agency_percent, sector`.
- **Pivot on the `month_date` Date column** (not on a text label — pivoting on text sorts `10/1/2025` before `2/1/2025` because lexicographic). After pivoting, rename the resulting column headers to `MM/DD/YYYY` format to match Kurt's existing sheet. Column order is chronological by the underlying date.
- Insert `Active` column = `"Y"`.
- Join `tblCommissionByAE` on `ae1` to populate `GrossCommission`.
- Final column order matches Kurt's existing sheet: `Customer, Active, Market, Revenue Class, AE1, GrossCommission, Broker, BrokerName, BrokerPercent, Agency, AgencyPercent, Sector, <month columns>`. (The `2022 & 2023` / `2023 & 2024` aggregates are dropped.)
- Sort rows alphabetically by `Customer`, `Market`, `Revenue Class`.

### `qNewRows` → loads to `New Rows` sheet

- Compute a stable hash of the metadata tuple (e.g. SHA1 of concatenated fields with a separator).
- Anti-join against `tblKnownRows` (hash list) to find unacknowledged tuples.
- For each unacknowledged tuple, compute `Reason`:
  - `"Metadata drift"` — another tuple with the same `(customer, market, revenue_class)` but different metadata is already in `tblKnownRows` (e.g. AE changed from Charmaine to Kurt).
  - `"New combo"` — no prior tuple with this `(customer, market, revenue_class)` is acknowledged.
  - `"Forecast reattached to new tuple"` — phase 1 of `qMerged` reattached a forecast to this tuple because of drift (§3.9). Emitted whether or not the tuple itself is new.
- Also emit rows for `Data Quality` warnings from `qForecasts`: `"Invalid forecast month"`, `"Forecast without DB match"`.
- Output columns: full metadata, the computed tuple `hash`, `Reason`, `Data Quality` (nullable).

### `Known Rows` mechanism

- Hidden sheet, single column `hash` stored as a native Excel Table `tblKnownRows`.
- User acknowledges a flagged row by copying its hash from `New Rows` (another column on that sheet) into `tblKnownRows`.
- V2: a small Office Script or "Acknowledge Selected" button. Not v1.

### 6.6 Hash specification

Ambiguity here causes two unrelated workbooks to produce different hashes for the same tuple, which silently breaks acknowledgements. Fully specifying it:

**Algorithm.** `SHA1(lower(trim(f1)) ⊕ US ⊕ lower(trim(f2)) ⊕ US ⊕ … ⊕ US ⊕ lower(trim(fn)))` where:

- `US` is U+001F (ASCII Unit Separator). Unlikely to appear in any customer or AE name; safer than pipe or tab.
- Field order (fixed, must never change without bumping hash version):
  1. `customer`
  2. `market`
  3. `revenue_class`
  4. `ae1`
  5. `broker_flag`
  6. `broker_name`
  7. `broker_percent` (stringified; `null` → empty)
  8. `agency_flag`
  9. `agency_percent` (stringified; `null` → empty)
  10. `sector`
- **Nulls** → empty string `""`. Never the string `"null"`.
- **Whitespace** — trimmed on both sides (`TRIM`), no interior collapse.
- **Case** — lowercased for hashing only. Display values retain their original case.
- **Encoding** — UTF-8 before SHA1.

**Hash version.** Stored in `Config` as `hash_version = "v1"`. The API also emits `"hash_version": "v1"` in its response metadata. PQ asserts the two match; mismatch → refresh errors loudly. Bumping to `v2` forces Kurt to empty `tblKnownRows` and re-acknowledge — which is the intended migration path if the formula ever needs to change.

**Test vector** (for implementer's unit test — both server side, if the API ever computes hashes, and PQ side):
```
customer:         "Admerasia:McDonalds"
market:           "SFO"
revenue_class:    "Internal Ad Sales"
ae1:              "Charmaine"
broker_flag:      "N"
broker_name:      null
broker_percent:   null
agency_flag:      "Y"
agency_percent:   15
sector:           "Outreach"

joined:           "admerasia:mcdonalds␟sfo␟internal ad sales␟charmaine␟n␟␟␟y␟15␟outreach"
SHA1 (hex):       [compute and pin in tests]
```

## 7. Sheet layout

### Tabs

| Tab | Hidden? | Writer | Purpose |
|---|---|---|---|
| `Data` | No | PQ | Main grid (wide pivot). Read-only — see below. |
| `Forecasts` | No | User | `tblForecasts`: Customer, Market, Revenue Class, Month, Forecast |
| `New Rows` | No | PQ | Flagged new/drifted tuples + data-quality warnings |
| `Known Rows` | Yes | User | `tblKnownRows`: single `hash` column |
| `Config` | Yes | User | Named ranges: `ApiBaseUrl`, `ApiToken`, `HashVersion`. Plus `tblCommissionByAE` (columns: `ae`, `commission_pct`). |

### Data sheet read-only enforcement

PQ will not prevent typing into the Data sheet — it will just silently discard the typed values on next refresh. That's a guaranteed "someone typed a number, it disappeared, now they're filing a bug" incident. Two mitigations, both applied:

1. **Sheet protection** (Review → Protect Sheet) with a known password stored in the runbook. Prevents edits unless the user deliberately unlocks.
2. **Row-1 banner** in loud yellow: `"⚠ READ-ONLY. Type forecasts into the Forecasts tab, not here. Any edits here are erased on next refresh."` Survives re-pivots because it's written above the PQ table output (PQ's table starts at row 3+).

### Forecasts tab header note

The first row of the `Forecasts` tab (above the `tblForecasts` table) carries a human-readable reminder of the DB-wins rule (§3.10):

> `Forecasts here fill cells the DB has no data for. Once the DB has any revenue for a cell, the DB value replaces the forecast on the Data sheet (your forecast stays listed here but won't display).`

### `tblForecasts` example seed rows

```
Customer                           Market   Revenue Class       Month       Forecast
Admerasia:McDonalds                SFO      Internal Ad Sales   2026-07-01  5500
Borough of Manhattan CC            NYC      Internal Ad Sales   2026-09-01  4000
```

### Downstream impact

Admin / CTV / Consolidated sheets reference the current workbook by cell/range. The new `Data` sheet preserves column order and customer sort order, so downstream references continue to resolve. **Verification during implementation:** open each downstream workbook after first successful PQ refresh and spot-check that references resolve and totals match.

## 8. One-time forecast migration

This is the bridge from "forecasts live as cells in the main grid" to "forecasts live in `tblForecasts`." If it's botched, Kurt's multi-year forward-looking work evaporates. It deserves its own section, its own deliverable (a reconciliation report), and Kurt's sign-off before the new workbook replaces the old one.

### The problem

The current master sheet does not mark which cells are forecasts. It's a single pane of glass where past-month cells happen to be actuals (typed from commercial log) and future-month cells happen to be forecasts (Kurt's planning). There's no column or flag distinguishing the two — only the relationship of the month to the DB's data frontier.

### The reconciliation approach

Treat this as a cell-by-cell diff between the **old sheet** and a **full-history API pull**, bucketed by the month's closure status. The diff produces three buckets; Kurt resolves each:

| Bucket | Condition | Action |
|---|---|---|
| **A. Closed-month actuals drift** | Month is closed. Old-sheet value ≠ API value. | Accept the API as authoritative. Kurt reviews the diff report but no forecast migration. |
| **B. Open-month forecast candidate** | Month is not closed. Old-sheet value > 0. API value is 0 or absent. | Seed into `tblForecasts` as `Customer, Market, Revenue Class, Month, Forecast = old-sheet value`. |
| **C. Open-month collision** | Month is not closed. Both old-sheet > 0 and API > 0. | Human judgment. Likely: old-sheet value was a forecast that has since been partially realized. Default action: seed `tblForecasts` with the **old-sheet value** (preserves Kurt's intent); DB-wins rule will then render the API value on Data. But flag each one for Kurt to confirm. |

### Deliverable: reconciliation report

A script (Python, runs against the exported-to-CSV old sheet + a live API pull) produces a single CSV:

```
Bucket, Customer, Market, Revenue Class, Month, Old Sheet Value, API Value, Proposed Action
A,      Admerasia:McDonalds, SFO, Internal Ad Sales, 2024-06-01, 4700.00, 4690.00, "Accept API"
B,      Admerasia:McDonalds, SFO, Internal Ad Sales, 2026-07-01, 5500.00,     0.00, "Seed as forecast"
C,      Borough of Manhattan CC, NYC, Internal Ad Sales, 2026-07-01, 4050.00, 4050.00, "Confirm: seed as forecast?"
```

Kurt reviews. Any row in bucket C that Kurt rejects is simply omitted from the seed. Any bucket A he disputes triggers a deeper DB investigation before proceeding. Once he signs off, a second script consumes the (possibly edited) CSV and emits the final `tblForecasts` seed.

### Safety rails

- **Keep the old sheet.** Copy `Revenue Master - archived YYYY-MM-DD.xlsx` into a separate Dropbox folder before the new workbook replaces it. Don't overwrite.
- **Dry-run the migration on a copy first.** Run the full refresh → downstream verification loop on a copy of the workbook before it becomes the live one.
- **Version the seed itself.** Commit the reconciled CSV to `docs/migration-artifacts/` so the seed is reproducible.

### This is not optional scope

Until this migration runs cleanly, the new workbook isn't usable by Kurt — he'd lose the forward-looking plans he's been building. It's on the critical path, not a "finish later" item.

## 9. Auth

- Shared secret in an `X-SpotOps-Token` header.
- Token lives in the workbook's hidden `Config` tab (named range `ApiToken`).
- Endpoint reads expected value from env var `SHEET_EXPORT_TOKEN`. Missing env var → endpoint returns `503` (misconfigured), not `200`.
- Tailscale provides the network auth layer. The token prevents casual browser hits from anyone on the tailnet.
- Token rotation: change env var on server, update `Config` cell, refresh. Not automated in v1.

## 10. Error handling

| Failure | Behavior |
|---|---|
| API unreachable (Tailscale down, server off) | PQ refresh fails with a clear message; sheet unchanged from prior refresh. |
| API returns `401` | PQ refresh fails; user checks Config token. |
| API returns unexpected schema | PQ errors at the parse step; user sees error in refresh dialog. |
| API returns `"hash_version"` that doesn't match `Config!HashVersion` | PQ refresh errors loudly. Do not proceed; acknowledgements would silently break. |
| API returns empty `rows` array | PQ refresh **errors** rather than blanking the Data sheet. An empty response is technically valid but in practice means something is wrong (wrong env, token scoped to nothing, migration in progress). Preferable to scare the user than to silently nuke the grid. Override available via a `Config!AllowEmptyResponse = TRUE` flag for legitimate empty-window cases. |
| API returns a row with `null` `broadcast_month` or `null` required metadata field | PQ errors at normalization. Treated as malformed, not dropped silently. |
| `tblForecasts` row has invalid Month (not first-of-month, or blank) | Row is excluded from merge; surfaces on `New Rows` with `Data Quality = "Invalid forecast month"`. |
| `tblForecasts` row has Customer the DB has never seen | Row is included; metadata columns blank on `Data`; `New Rows` flags the tuple as `"Forecast without DB match"`. |
| DB query times out | Endpoint returns `504`; PQ surfaces as refresh failure. |

## 11. Testing

### Flask endpoint

- Unit tests: grouping correctness, Trade exclusion, `is_historical = 1` inclusion, month ordering (ISO output), auth header required, `401` on missing/wrong token, `503` on missing env var, `hash_version` present in metadata.
- Hash round-trip test: server-side hash (if the endpoint computes it for any reason) matches PQ-side hash for the test vector in §6.6.
- Integration test: hit endpoint on dev DB, diff result against a hand-computed pivot of `spots_reporting` for a bounded slice (e.g. `Admerasia:McDonalds`, all markets, 2025).

### Power Query

- Manual: build the workbook once, refresh, compare `Data` sheet against Kurt's existing hand-maintained sheet row-by-row for a representative slice.
  - Expected diffs: open-month values may differ where DB has more recent log data than Kurt's last manual transcription.
  - Unexpected diffs: closed-month values should match exactly. Any mismatch in a closed month is either a bug in the endpoint or a hand-entry error in the old sheet. Investigate each.
- Refresh-idempotence: two consecutive refreshes on unchanged DB produce identical `Data` sheets.
- Forecast survival: put a forecast in `tblForecasts` for a future month the DB has no data for; confirm it appears on `Data` after refresh; confirm it's replaced by DB value once DB data for that cell arrives.
- Drift reattachment: seed a drift scenario (same customer/market/revenue_class, two different AEs in DB — one acknowledged in `tblKnownRows`, one not). Put a forecast for that combo in `tblForecasts`. Confirm the forecast attaches to the unacknowledged (newer) tuple and that `New Rows` shows `Reason = "Forecast reattached to new tuple"`.
- New Rows: force a new tuple (e.g. via a test spot import), refresh, confirm it appears on `New Rows`; add hash to `tblKnownRows`; refresh again; confirm it's gone from `New Rows`.
- Empty-response safety: point the workbook at a dev endpoint that returns an empty `rows` array; confirm refresh **errors** rather than blanking the Data sheet.

### End-to-end

- Run `daily_update.sh` on the dev stack, then refresh the workbook, then open Admin/CTV/Consolidated downstream sheets and verify their pulled totals match expectations.

## 12. Operational notes

### Power Query credentials dialog (one-time-per-machine)

On Excel desktop's first `Web.Contents` refresh, Power Query prompts for credentials. The auth is already in the `X-SpotOps-Token` header, so:

- **Choose `Anonymous`** on the prompt. Don't pick `Windows` or `Basic` — both will cause `401`s from the endpoint (wrong auth mode, ignores the header).
- If Kurt accidentally picks the wrong option: Data → Get Data → Data Source Settings → pick the URL → Edit Permissions → change to Anonymous.

This is the single most common setup footgun. Put it at the top of the runbook.

### Dropbox + open workbook

If Dropbox is mid-sync when the workbook is open, PQ refresh may race with the sync process. Safe order: close workbook → let Dropbox settle → open workbook → refresh. Not usually needed, but the escape hatch when refresh behaves weirdly.

### Token rotation

1. Change `SHEET_EXPORT_TOKEN` on the server; restart Flask.
2. Open the workbook, unhide `Config`, update `ApiToken` cell, save.
3. Refresh.

## 13. Risks and open items

- **Broker/percent/commission field sources** (§5) — must be resolved during Step 1 of implementation. Block further work on §6–§7 until confirmed.
- **Downstream sheet compatibility** — the current sheet's exact column order and customer sort order must be preserved. Any deviation breaks Admin/CTV/Consolidated references. Implementer must snapshot the current column order as the first implementation step.
- **One-time forecast migration** (§8) — on the critical path. Without a clean reconciliation + Kurt sign-off, the new workbook can't replace the old one.
- **`agency_percent` as per-customer vs. per-agency** — if it's per-customer, the DB likely has it; if it's a manual policy the business applies, it's not DB-derivable. Resolve with Kurt during Step 1.

## 14. Implementation sequencing (high-level — detailed plan to follow)

1. **DB schema audit** — confirm broker / agency_percent / gross_commission field sources. Resolve any gaps with Kurt before writing code.
2. **Flask endpoint + tests** — add the endpoint, repository, service. Wire auth. Confirm output shape against `spots_reporting` hand-pivot.
3. **Power Query workbook v0** — one-off scratch file to validate the PQ structure end-to-end against the real API.
4. **Reconciliation + forecast seed** (§8) — run the diff script, produce the reconciliation CSV, Kurt reviews and signs off, emit `tblForecasts` seed.
5. **Power Query workbook v1** — final file shape with all five queries, config/known/new-rows tabs, sheet protection, banners, `tblCommissionByAE`, formatted to match Kurt's column order.
6. **Downstream verification** — open Admin/CTV/Consolidated, verify references resolve and totals match.
7. **Runbook** — short doc on how to refresh, how to ack new rows, how to rotate the token, credentials-dialog first-refresh guidance. In `docs/`.
