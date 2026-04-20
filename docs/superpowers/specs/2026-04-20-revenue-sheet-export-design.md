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
2. **New row handling.** New `(customer, market, revenue_class, ae1, agency_flag, sector)` tuples appearing in the DB auto-add to the sheet with metadata populated from the DB. **(Tuple finalized post-audit — see §5 resolution. Broker status and agency percent are display attributes, not identity.)**
3. **Metadata drift.** When DB metadata changes for an existing customer/market/revenue_class (AE reassignment, agency prefix change, or sector re-classification), a **new row** is created; the old row is preserved intact. The DB naturally supports this because `spots.sales_person` and `spots.agency_flag` are captured per spot at import time, so historical revenue stays attached to its original metadata. Broker status (`spots.broker_fees > 0`) and effective agency percent (`1 - station_net/gross_rate`) change with each spot and do NOT create new rows — they're aggregated for display only.
4. **Flagging.** New and drifted tuples appear on a `New Rows` tab for review. Acknowledged tuples are stored as hashes in a hidden `Known Rows` table.
5. **Closed months.** Rebuilt each refresh from the DB. Closed-month DB values are immutable by design, so this is idempotent. One-time consequence: any closed-month hand-typed value in the current sheet that differs from the DB will be "corrected" to the DB value on first refresh. Accepted.
6. **Active column.** Always `Y` in PQ output. (Sheet doesn't track churn in this workbook context.)
7. **Obsolete columns.** `2022 & 2023` and `2023 & 2024` aggregation columns are dropped in v1.
8. **Forward bookings.** Include `is_historical = 1` spots so booked-but-not-aired future revenue shows in the sheet, matching current commercial-log-based workflow.
9. **Forecast join key on drift.** `tblForecasts` keys on `(customer, market, revenue_class, month)` — *no AE*. After a metadata drift event the DB has multiple tuples with the same `(customer, market, revenue_class)` and different metadata; a single forecast row would otherwise fan out to all of them. **Attachment rule (single, deterministic, covers all cardinalities):** attach the forecast to the **unacknowledged tuple with the latest DB-side `broadcast_month`**. If no unacknowledged tuples exist for that `(customer, market, revenue_class)`, attach to the **acknowledged tuple with the latest DB-side `broadcast_month`**. Ties broken by a stable ordering of the hash. The attachment is recorded on `New Rows` as `Reason = "Forecast reattached to new tuple"` so Kurt sees it happened. If none of that feels right after drift, Kurt can delete or re-key the forecast row manually.
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

**Design choice (finalized post-audit):** the API emits raw per-spot-sum amounts (`gross_rate`, `station_net`, `broker_fees`). PQ derives every display percentage from those three numbers plus `agency_flag`:

- `AgencyPercent` = `1 - SUM(station_net) / SUM(gross_rate)` across the row's months (zero-safe).
- `Broker` (Y/N) = `"Y" if SUM(broker_fees) > 0 else "N"`.
- `BrokerPercent` = `SUM(broker_fees) / SUM(gross_rate)` (zero-safe).
- `BrokerName` = blank in v1 (not in DB; Kurt OK with this — v2 Excel override if ever wanted).
- `GrossCommission` = lookup in `tblCommissionByAE` keyed by `ae1` (unchanged).

This is strictly more accurate than emitting a pre-computed percent — it reflects actual booked amounts rather than a stored audit-field rate (`customers.commission_rate` / `agencies.commission_rate` are entity-level audit fields, not per-spot transactional rates; see the schema audit doc for details).

**Date format rule:** query params use `Mmm-YY` (matches how `broadcast_month` is stored in the DB); response `broadcast_month` values are ISO `YYYY-MM-DD` (first-of-month). PQ reformats display headers anyway, and ISO is parse-safe across locales. The endpoint performs the `Mmm-YY` → ISO conversion server-side via the canonical CASE/WHEN pattern from `v_planning_data`.

One object per unique metadata tuple × broadcast_month. Values with `SUM(gross_rate) = 0` are suppressed (no-revenue months don't need rows). An **empty `rows` array is itself a valid response** (legitimate if `start_month`/`end_month` frame no data) but almost certainly a red flag in practice — see §10 error handling.

### Query logic

Implemented via the container. `spots_reporting` doesn't expose `agency_flag`, so the query builds directly from `spots` with the necessary joins (see schema audit doc, Schema Surprise #1). Trade exclusion is applied explicitly.

```sql
SELECT
  s.bill_code                                                           AS customer,
  m.market_code                                                         AS market,
  s.revenue_type                                                        AS revenue_class,
  s.sales_person                                                        AS ae1,
  CASE WHEN s.agency_flag = 'Agency' THEN 'Y' ELSE 'N' END              AS agency_flag,
  sect.sector_name                                                      AS sector,
  s.broadcast_month                                                     AS broadcast_month_raw,
  SUM(s.gross_rate)                                                     AS gross_rate,
  SUM(s.station_net)                                                    AS station_net,
  SUM(s.broker_fees)                                                    AS broker_fees
FROM spots s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN sectors   sect ON c.sector_id = sect.sector_id
LEFT JOIN markets   m ON s.market_id = m.market_id
WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
-- is_historical = 1 is INCLUDED (forward bookings are the main point).
GROUP BY 1,2,3,4,5,6,7
HAVING SUM(s.gross_rate) <> 0 OR SUM(s.station_net) <> 0 OR SUM(s.broker_fees) <> 0
ORDER BY 1,2,3,4,5,6,
  CASE SUBSTR(s.broadcast_month,1,3)
    WHEN 'Jan' THEN 1  WHEN 'Feb' THEN 2  WHEN 'Mar' THEN 3
    WHEN 'Apr' THEN 4  WHEN 'May' THEN 5  WHEN 'Jun' THEN 6
    WHEN 'Jul' THEN 7  WHEN 'Aug' THEN 8  WHEN 'Sep' THEN 9
    WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
  END,
  SUBSTR(s.broadcast_month,5,2);
```

Server-side: convert `broadcast_month_raw` (`Mmm-YY`) → ISO `YYYY-MM-DD` on the way out. `HAVING` suppresses groupings where all three amounts are zero (full zero rows are noise).

`agency_flag` note: read from `spots.agency_flag` (TEXT; values `'Agency'` or `'Non-agency'`) and convert to `Y`/`N` in the SQL. **Not derived from `bill_code`** — the stored value is written by the Etere importer and is authoritative.

`GrossCommission` is **not returned by the API**. PQ computes it via a join against `tblCommissionByAE` on the `Config` tab (`ae`, `commission_pct`). Kurt edits the table when a commission rate changes — no deploy required.

### Performance budget

~18k rows × manual refresh. Expected response size ~2–5 MB uncompressed JSON. Response time target: < 10s p95 on the dev box. If exceeded, add `materialized = true` option that reads from a nightly-built materialized table (not v1).

## 6. Power Query structure

All queries live in the `Revenue Master.xlsx` workbook.

### `qRevenueActuals` (staging, loads nowhere)

- Source: `Web.Contents(base_url & "/api/revenue/sheet-export", [Headers = [#"X-SpotOps-Token" = token]])`
- `base_url` and `token` come from the `Config` sheet (named ranges).
- Parse `rows` → table. Type `broadcast_month` as Date (server emits ISO `YYYY-MM-DD`), rename to `month_date` for internal use.
- Normalize metadata fields to match the hash spec (§6.6): trim whitespace on `customer`, `market`, `revenue_class`, `ae1`, `sector`; lowercase **only** for hashing — the display value stays as returned by the API.

### `qForecasts` (staging, loads nowhere)

- Source: `Excel.CurrentWorkbook(){[Name="tblForecasts"]}[Content]`.
- Columns: `Customer`, `Market`, `Revenue Class`, `Month` (Date, must be first-of-month), `Forecast` (Number).
- Normalize: trim whitespace on Customer/Market/Revenue Class. Reject rows where `Month` is not first-of-month (emitted to a `Data Quality` column on `New Rows`).

### `qMerged` (staging, loads nowhere)

Merging proceeds in two phases to handle metadata drift (§3.9) deterministically:

**Phase 1 — resolve each forecast row to exactly one tuple.**
For each `(customer, market, revenue_class, month_date)` in `qForecasts`, find DB tuples from `qRevenueActuals` that match on `(customer, market, revenue_class)`. Apply §3.9's single attachment rule:

1. Among those matches, take the subset whose hash is **not** in `tblKnownRows` (unacknowledged tuples).
2. If that subset is non-empty, attach to the one with the latest DB-side `broadcast_month`.
3. Otherwise, attach to the acknowledged tuple with the latest DB-side `broadcast_month`.
4. Ties on `broadcast_month` broken by ascending `hash` (deterministic across refreshes).
5. **Zero matches at all** (forecast for a combo DB has never seen): forecast is kept as a forecast-only row with blank metadata. Flagged on `New Rows` as `Data Quality = "Forecast without DB match"`.

When the chosen tuple is unacknowledged, emit `Reason = "Forecast reattached to new tuple"` on `New Rows`.

**Phase 2 — combine.**
Full outer join `qRevenueActuals` with the Phase-1-resolved forecasts on the full tuple + `month_date`. Compute `value = if gross_rate <> null then gross_rate else forecast`. Metadata columns come from the DB side when present, else from the resolved-forecast side for forecast-only rows.

### `qDataPivot` → loads to `Data` sheet

- **Load target is `Data!A3`** — rows 1–2 are reserved for the read-only banner (§7). The query is loaded to an existing-worksheet destination, not "new worksheet."
- Group by the full metadata tuple: `customer, market, revenue_class, ae1, agency_flag, sector`. Sum `gross_rate`, `station_net`, and `broker_fees` across the tuple's months (for per-row display derivations like `AgencyPercent`, `Broker`, `BrokerPercent`).
- **Pivot on the `month_date` Date column** (not on a text label — pivoting on text sorts `10/1/2025` before `2/1/2025` because lexicographic). After pivoting, rename the resulting column headers to `MM/DD/YYYY` format to match Kurt's existing sheet. Column order is chronological by the underlying date.
- Insert `Active` column = `"Y"`.
- Left-join `tblCommissionByAE` on `ae1` to populate `GrossCommission`. Any row where the join yields null (AE is in DB output but not in `tblCommissionByAE`) is emitted to `New Rows` with `Reason = "Unknown AE for commission"` so Kurt sees the gap and can add the rate.
- Final column order matches Kurt's existing sheet: `Customer, Active, Market, Revenue Class, AE1, GrossCommission, Broker, BrokerName, BrokerPercent, Agency, AgencyPercent, Sector, <month columns>`. (The `2022 & 2023` / `2023 & 2024` aggregates are dropped.)
- **Row sort — deterministic, hard constraint.** Sort key is the full hash-input tuple in order: `Customer, Market, Revenue Class, AE1, AgencyFlag, Sector`. The first three match Kurt's visual expectation; the rest are tiebreakers that matter under drift (§3.3 lets multiple rows share Customer/Market/Revenue Class when AE / agency / sector differs). A shorter sort key would let PQ reorder drift-siblings arbitrarily across refreshes and silently corrupt any downstream cell-address reference.

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
  5. `agency_flag`
  6. `sector`
- **Nulls** → empty string `""`. Never the string `"null"`.
- **Whitespace** — trimmed on both sides, no interior collapse. On the PQ side: `Text.Trim(x)`.
- **Case** — lowercased for hashing only. Display values retain their original case. **Invariant culture required** (don't let the machine's locale change the result — the Turkish dotted-I problem is real). On the PQ side: `Text.Lower(x, "en-US")`. On the server side: `str.lower()` in Python is already invariant.
- **Encoding** — UTF-8 before SHA1.

All six hash inputs are TEXT. No numeric stringification needed in v1 — the numeric fields the original spec had (`broker_percent`, `agency_percent`) are no longer part of the tuple. If a future hash version re-introduces numeric inputs, the prior stringification rule (integer when whole, else fixed-point with trailing zeros trimmed, invariant culture) is the intended recipe.

**Hash version.** Stored in `Config` as `hash_version = "v1"`. The API also emits `"hash_version": "v1"` in its response metadata. PQ asserts the two match; mismatch → refresh errors loudly. Bumping to `v2` forces Kurt to empty `tblKnownRows` and re-acknowledge — which is the intended migration path if the formula ever needs to change.

**Test vectors** (both must be pinned in server-side and PQ-side unit tests — a divergence between the two is the bug class this spec is trying to prevent). Vector 1 covers a typical tuple; Vector 2 covers whitespace and case edge cases.

**Vector 1 — typical:**
```
customer:      "Admerasia:McDonalds"
market:        "SFO"
revenue_class: "Internal Ad Sales"
ae1:           "Charmaine"
agency_flag:   "Y"
sector:        "Outreach"

joined:        "admerasia:mcdonalds␟sfo␟internal ad sales␟charmaine␟y␟outreach"
SHA1 (hex):    [compute during implementation; pin in tests]
```

**Vector 2 — whitespace & case edge cases:**
```
customer:      "  Blue 449:Denny's Co-op  "   # leading/trailing whitespace
market:        "sfo"                           # lowercase input
revenue_class: "Branded Content"
ae1:           "CHARMAINE"                     # all-caps input
agency_flag:   "Y"
sector:        "Restaurant"

joined:        "blue 449:denny's co-op␟sfo␟branded content␟charmaine␟y␟restaurant"
                                               # whitespace trimmed, case folded
SHA1 (hex):    [compute during implementation; pin in tests]
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
| **C. Open-month collision** | Month is not closed. Both old-sheet > 0 and API > 0. | Human judgment. Likely: old-sheet value was a forecast that has since been partially realized. Default action: seed `tblForecasts` with the **old-sheet value** (preserves Kurt's intent); DB-wins rule will then render the API value on Data. But flag each one for Kurt to confirm. **Why seed a forecast that will currently be hidden under DB-wins?** Because forecasts act as a *floor* for empty cells (§3.10) — if the booked revenue is later cancelled or never airs, the DB value drops and the seeded forecast re-emerges as the displayed value. Without seeding, a cancelled booking silently zeroes that cell and Kurt loses the forward plan. |

### Deliverable: reconciliation report

A script (Python, runs against the exported-to-CSV old sheet + a live API pull) produces a single CSV:

```
Bucket, Customer, Market, Revenue Class, Month, Old Sheet Value, API Value, Proposed Action
A,      Admerasia:McDonalds, SFO, Internal Ad Sales, 2024-06-01, 4700.00, 4690.00, "Accept API"
B,      Admerasia:McDonalds, SFO, Internal Ad Sales, 2026-07-01, 5500.00,     0.00, "Seed as forecast"
C,      Borough of Manhattan CC, NYC, Internal Ad Sales, 2026-07-01, 4050.00, 4050.00, "Confirm: seed as forecast?"
```

Kurt reviews. Any row in bucket C that Kurt rejects is simply omitted from the seed. **Bucket A disputes don't block the migration** — Kurt's going to accept the API value on the Data sheet regardless (DB is authoritative for closed months), so the seed can proceed. Investigation of any disputed closed-month value runs in parallel and may retroactively adjust DB state; the seed itself is unaffected. Once Kurt signs off on bucket B and C choices, a second script consumes the (possibly edited) CSV and emits the final `tblForecasts` seed.

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
| API returns empty `rows` array | PQ refresh **errors** rather than blanking the Data sheet. An empty response is technically valid but in practice means something is wrong (wrong env, token scoped to nothing, migration in progress). Preferable to scare the user than to silently nuke the grid. Override available via a `Config!AllowEmptyResponse = TRUE` flag for legitimate empty-window cases. **While the flag is TRUE, a red banner on the `Config` tab** (conditional formatting on the cell, plus an adjacent `"⚠ Empty-response safety disabled — a silent outage will wipe the grid"` label) makes it visually obvious that the safety is off. Kurt flips it back when the legitimate empty case is resolved. |
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

- ~~**Broker/percent/commission field sources**~~ — **Resolved by schema audit.** Broker fields not in DB (emit null / derive in PQ from `broker_fees`); agency percent derived in PQ from gross/net; commission from `tblCommissionByAE`.
- **Downstream sheet compatibility** — the current sheet's exact column order and customer sort order must be preserved. Any deviation breaks Admin/CTV/Consolidated references. Implementer must snapshot the current column order as the first implementation step.
- **One-time forecast migration** (§8) — on the critical path. Without a clean reconciliation + Kurt sign-off, the new workbook can't replace the old one.
- ~~**`agency_percent` as per-customer vs. per-agency**~~ — **Resolved by schema audit.** Per-spot percent is implied by `gross_rate` - `station_net` - `broker_fees`. API emits the three raw amounts; PQ derives percent. No per-customer lookup needed.

## 14. Implementation sequencing (high-level — detailed plan to follow)

1. ~~**DB schema audit**~~ — **Complete** (see `2026-04-20-db-schema-audit.md`). Broker fields absent from DB; per-spot percent derivable from gross/net/broker_fees; `agency_flag` stored on `spots`, not derived.
2. **Flask endpoint + tests** — add the endpoint, repository, service. Wire auth. Confirm output shape against `spots_reporting` hand-pivot.
3. **Power Query workbook v0** — one-off scratch file to validate the PQ structure end-to-end against the real API.
4. **Reconciliation + forecast seed** (§8) — run the diff script, produce the reconciliation CSV, Kurt reviews and signs off, emit `tblForecasts` seed.
5. **Power Query workbook v1** — final file shape with all five queries, config/known/new-rows tabs, sheet protection, banners, `tblCommissionByAE`, formatted to match Kurt's column order.
6. **Downstream verification** — open Admin/CTV/Consolidated, verify references resolve and totals match.
7. **Runbook** — short doc on how to refresh, how to ack new rows, how to rotate the token, credentials-dialog first-refresh guidance. In `docs/`.
