# API & Export Contracts

**Audience:** LLM / engineer building Power Query, monitoring, or any external client of the SpotOps Flask app.
**Purpose:** Canonical contract reference for SpotOps API endpoints — request shape, response shape, field semantics, error behavior, and known gaps. The two `/api/revenue/*` endpoints feed `Revenue Master.xlsx`; the `/api/canon/*` endpoints back the in-app Customer Normalization Manager.
**Last reviewed:** 2026-04-30

---

## Endpoints at a glance

| Endpoint | Auth | Purpose | Section |
|---|---|---|---|
| `GET /api/revenue/sheet-export` | `X-SpotOps-Token` | All-history per-month tuple grain for the workbook Data tab | [Sheet Export](#sheet-export) |
| `GET /api/revenue/planning-export` | `X-SpotOps-Token` | AE × month rollup for the workbook Planning tab | [Planning Export](#planning-export) |
| `POST /api/canon/agency` | session (Tailscale) | Add/update agency canonical alias | [Canon endpoints](#canon-endpoints) |
| `POST /api/canon/customer` | session (Tailscale) | Add/update customer-tail canonical alias | [Canon endpoints](#canon-endpoints) |
| `POST /api/canon/raw-to-customer` | session (Tailscale) | Map raw bill_code text to a canonical customer | [Canon endpoints](#canon-endpoints) |
| `GET /api/canon/suggest/normalized?q=…` | session (Tailscale) | Autocomplete for normalized customer names | [Canon endpoints](#canon-endpoints) |
| `GET /health` | none | Liveness probe | The `/api/health` variant returns 401 — see [Known gaps](#known-gaps) |

Workbook-side companion design (Excel/PQ) for the two revenue exports lives in [Workbook AE Drift Tracker](#workbook-ae-drift-tracker-proposed).

---

## Common conventions

### Authentication

- The two workbook-facing exports use `X-SpotOps-Token: <secret>`. **One** shared secret covers both: server reads it from env var `SHEET_EXPORT_TOKEN`. Workbook stores it in the hidden `Config!ApiToken` named range.
- The `/api/canon/*` endpoints use the app's session-based Tailscale auth (no token header).
- Tailscale provides the network layer in all cases; the header / session is the app-level auth.

### Network / port

- Port `8000` inside the Docker compose stack (`127.0.0.1:8000:8000` — see [ARCHITECTURE.md](ARCHITECTURE.md)).
- Tailscale fronts the host externally; clients reach the box by tailnet name or IP.

### Error envelope

| HTTP | Body | Meaning |
|---|---|---|
| `401` | `{"error": "Authentication required"}` | Missing or wrong token / no Tailscale session |
| `503` | `{"error": "...misconfigured..."}` | Server-side env var unset (operator fix, not user fix) |
| `504` | (timeout) | DB timeout — workbook should retry once, then surface as refresh failure |
| `400` | `{"error": "Invalid …"}` | Bad query param (e.g. non-integer year) |

### Date format

- All `broadcast_month` **API outputs** are ISO `YYYY-MM-01` (first-of-month, UTC date string).
- Internal storage is title-cased `Mmm-YY` (`Sep-26`, `Oct-25`); see [ARCHITECTURE.md](ARCHITECTURE.md) for the data dictionary.

### Trade exclusion (system-wide invariant)

Every revenue query in the app excludes Trade revenue:

```sql
(revenue_type != 'Trade' OR revenue_type IS NULL)
```

Both export endpoints apply this. You cannot get Trade rows out of either.

---

## Sheet Export

`GET /api/revenue/sheet-export` — workbook-facing, all-history, per-month tuple grain.

> Full design spec: `docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` (preserved as historical record).

### 1. Endpoint

```
GET http://<host>:8000/api/revenue/sheet-export
Header: X-SpotOps-Token: <shared-secret>
```

### 2. Response shape

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

#### Field reference

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

#### Grain

One row per `(customer, market, revenue_class, ae1, agency_flag, sector) × broadcast_month`. Rows where all three amounts sum to zero are suppressed server-side. `is_historical = 1` spots are **included** (forward bookings are the main point).

### 3. PQ derivations (not returned by API)

Compute client-side:

- `GrossCommission` = lookup in `tblCommissionByAE` keyed by `ae1`. Unknown AE → emit to `New Rows` with `Reason = "Unknown AE for commission"`.
- `BrokerName` = blank in v1 (not in DB).
- `Active` = `"Y"` (no churn tracking in this workbook).

#### Deferred until endpoint v1.1 — `AgencyPercent`, `BrokerPercent`, `Broker (Y/N)`

These three are **per-contract** display attributes. The rule is absolute:

> Agency rate is per contract number. Never an average. Always the rate NOW for the current contract, not historical.

The endpoint currently emits per-month sums of `gross_rate`, `station_net`, and `broker_fees` at the six-field tuple grain, but **does not emit a contract identifier**. PQ cannot isolate a single contract to represent the tuple, so computing the three derivations client-side would require summing those per-month amounts across the row's months — a weighted average that the business rule forbids, especially when multiple contracts coexist within the tuple.

**PQ v0 therefore ships these three columns blank / null.** Every other column in Kurt's historical format (Customer, Active, Market, Revenue Class, AE1, GrossCommission, BrokerName, Sector, and all month columns) is fully populated in v0.

Endpoint v1.1 must emit `contract` (exact field name TBD in that spec) alongside the three amounts. PQ v1.1 then selects the current-contract row per tuple (selection rule — e.g. latest-broadcast_month — settles in the v1.1 design) and derives the percents and flag from that single contract's amounts, zero-safe:

- `AgencyPercent` = `1 - station_net / gross_rate` on the selected contract.
- `BrokerPercent` = `broker_fees / gross_rate` on the selected contract.
- `Broker` = `"Y" if broker_fees > 0 else "N"` on the selected contract.

**Do not implement the SUM-across-months formula** that appears in earlier drafts of this doc and in spec §5. That formula is rejected.

### 4. Hash contract (critical)

Ambiguity here silently breaks `tblKnownRows` acknowledgements. The design spec §6.6 is authoritative; summary:

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
- **Case** — lowercase for hashing only; display keeps original case. **Invariant culture required** (Turkish dotted-I). PQ: `Text.Lower(x, "en-US")`.
- **Encoding** — UTF-8 before SHA1.

**Server-side grouping matches the hash normalization.** The endpoint's SQL `GROUP BY` and representative-spot `PARTITION BY` use `LOWER(TRIM(...))` on the six identity columns, so casing / whitespace drift in the source data (e.g. `"iGRAPHIX"` vs `"iGraphix"`; `"Riley Van Patten"` vs `"Riley van Patten"`) collapses to a single emitted row with one representative broker attribution. Display casing is resolved deterministically at the tuple level (`MIN()` over the source strings — capitals sort before lowercase in ASCII) so it stays consistent across months of the same row_hash.

- **Hash version** — `Config!HashVersion` must match `metadata.hash_version`. PQ **must assert equality and error loudly** on mismatch.

> Test vectors are in spec §6.6. Concrete SHA1 hex values are not yet pinned — pin them in both server-side and PQ-side unit tests before go-live.

### 5. Error handling (sheet export)

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

### 6. First-refresh credentials dialog (setup footgun)

On Excel desktop's first `Web.Contents` refresh, PQ prompts for credentials. Auth is already in the `X-SpotOps-Token` header, so:

- **Choose `Anonymous`.** Never `Windows` or `Basic` — both cause `401`s (wrong auth mode, header ignored).
- If wrong option picked: Data → Get Data → Data Source Settings → pick the URL → Edit Permissions → Anonymous.

This is the single most common setup footgun.

### 7. Data sheet is read-only

PQ overwrites on each refresh, so any hand-typed value disappears silently next refresh. Two mitigations, both applied:

1. Sheet protection (Review → Protect Sheet) with password in [RUNBOOKS.md](RUNBOOKS.md).
2. Row-1 yellow banner: `⚠ READ-ONLY. Type forecasts into the Forecasts tab, not here. Any edits here are erased on next refresh.` PQ output starts at row 3 so the banner survives re-pivots.

### 8. Related tabs in the workbook

| Tab | Hidden | Writer | Purpose |
|---|---|---|---|
| `Data` | No | PQ | Main grid (wide pivot). Read-only. |
| `Forecasts` | No | User | `tblForecasts`: Customer, Market, Revenue Class, Month, Forecast |
| `New Rows` | No | PQ | Flagged new/drifted tuples + data-quality warnings |
| `Known Rows` | Yes | User | `tblKnownRows`: single `hash` column |
| `Config` | Yes | User | Named ranges: `ApiBaseUrl`, `ApiToken`, `HashVersion`. Plus `tblCommissionByAE`. |

### 9. Smoke test

```bash
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/sheet-export \
  | jq '.metadata'
```

Expected: `{"generated_at": "...", "hash_version": "v1", "row_count": N, ...}` with `N > 0` on a populated DB.

---

## Planning Export

`GET /api/revenue/planning-export` — workbook-facing AE × month rollup carrying budget / forecast / booked plus three derived fields.

### 1. Endpoint

```
GET http://<host>:8000/api/revenue/planning-export[?year=YYYY]
Header: X-SpotOps-Token: <shared-secret>
```

- Auth: **same `X-SpotOps-Token` header as sheet-export.** No separate token.
- Query params:
  - `year` (optional, integer). Defaults to `date.today().year`.
  - Invalid `year=foo` → `400 INVALID_YEAR`.

### 2. Response shape

```json
{
  "metadata": {
    "generated_at":   "2026-04-21T21:57:16Z",
    "schema_version": "1.0",
    "year":           2026,
    "row_count":      62
  },
  "rows": [
    {
      "ae1":             "Charmaine Lane",
      "broadcast_month": "2026-04-01",
      "budget":          132688.00,
      "forecast":        153178.00,
      "booked":          233156.40,
      "new_accts":       0,
      "new_dollars":     0.00,
      "expected":        233156.40,
      "pipeline":        0.00,
      "vs_budget":       100468.40
    }
  ]
}
```

#### Identity

Each row is keyed by `(ae1, broadcast_month)`. No customer / market / revenue_class sub-grain — planning is AE × month only.

#### Field reference

| Field | Type | Source / formula |
|---|---|---|
| `ae1` | string | `revenue_entities.entity_name` where `is_active = 1` |
| `broadcast_month` | ISO date | First-of-month, same format as sheet-export |
| `budget` | number \| null | `budget.budget_amount` for `(ae, year, month)` |
| `forecast` | number \| null | `forecast.forecast_amount` for `(ae, year, month)` |
| `booked` | number | `SUM(gross_rate)` from `spots`, Trade-excluded, with WorldLink/House special cases (see [§4 booked semantics](#4-booked-semantics-copied-from-dashboard)). **Never null** — `0.0` when no spots. |
| `new_accts` | integer \| null | `forecast.new_accounts_forecast`. `null` when no forecast row. |
| `new_dollars` | number \| null | `forecast.new_dollars_forecast`. `null` when no forecast row. |
| `expected` | number | `max(booked, COALESCE(forecast, budget))`. Falls back to `booked` when both plan columns are null. Never null in emitted rows. |
| `pipeline` | number \| null | `max(0, COALESCE(forecast, budget) − booked)`. **Floored** — never negative. `null` when both forecast and budget are null. |
| `vs_budget` | number \| null | `booked − budget`, signed. `null` when `budget` is null. |

#### Null rules (explicit)

| Input state | `expected` | `pipeline` | `vs_budget` |
|---|---|---|---|
| All of `budget`, `forecast`, `booked>0` present | `max(booked, forecast)` | `max(0, forecast − booked)` | `booked − budget` |
| Budget + booked, no forecast | `max(booked, budget)` | `max(0, budget − booked)` | `booked − budget` |
| Forecast + booked, no budget | `max(booked, forecast)` | `max(0, forecast − booked)` | **null** |
| Booked only (no budget, no forecast) | `booked` | **null** | **null** |
| Budget only (booked = 0) | `budget` | `budget` | `−budget` |
| Forecast only (booked = 0) | `forecast` | `forecast` | null |

#### Emission rule

One row per `(ae1, year, month)` where **at least one of**:
- a `budget` row exists for `(ae, year, month)`, OR
- a `forecast` row exists for `(ae, year, month)`, OR
- `booked > 0` for `(ae, year, month)`.

Months with no activity for an AE are **suppressed** (no row).

### 3. Kurt's workflow invariant

Kurt designed the columns so that, per `(ae, month)`:

```
booked + pipeline = expected
```

- **Underbooked month** (`booked < forecast`): `booked + (forecast − booked) = forecast`. The sum "lands on forecast" — exactly what Kurt means when he says "sum to our forecast number."
- **Overbooked month** (`booked ≥ forecast`): `booked + 0 = booked`. The sum lands on actual booked, which is the honest "operating expectation" once you've overshot the plan.

This is why `pipeline` is **floored at 0** — keeping the invariant `booked + pipeline = expected` true in both regimes.

### 4. `booked` semantics (copied from dashboard)

The endpoint's `booked` is computed exactly like the dashboard's `planning_repository.get_booked_revenue` (at `src/repositories/planning_repository.py:435-548`):

| AE | SQL filter |
|---|---|
| `"WorldLink"` | `bill_code LIKE 'WorldLink%'` — **ignores `sales_person`** |
| `"House"` | `sales_person = 'House' AND bill_code NOT LIKE 'WorldLink%'` |
| All others | `sales_person = :ae1` — no `bill_code` filter |

All three branches: Trade excluded via `(revenue_type != 'Trade' OR revenue_type IS NULL)`, `SUM(gross_rate)`, grouped by `broadcast_month`.

**Divergence from sheet-export.** If you sum sheet-export's `gross_rate` by `ae1 = :ae` for a given month, you'll match **except for WorldLink and House**. For those two the workbook should trust this endpoint's `booked`, not a sheet-export sum. See [§7 differences from /planning/](#7-differs-from-the-planning-page--enumerate-and-accept).

### 5. Metadata envelope

- `generated_at` — UTC ISO-8601 with `Z` suffix (same as sheet-export).
- `schema_version = "1.0"` — independent of sheet-export's `"1.1"`. Bump when the row shape changes.
- `year` — echo of the requested year (or `date.today().year` if the param was omitted).
- `row_count` — equals `len(rows)`.

No `hash_version`, no `row_hash`. Identity is `(ae1, broadcast_month)`, which is already content-free — no hashing needed for drift detection.

### 6. What the workbook does with this

1. Pull this endpoint alongside `/api/revenue/sheet-export`. One year at a time.
2. New `Planning` tab (or second pivot on Data) showing `ae1 × broadcast_month` with columns `budget / forecast / booked / expected / pipeline / vs_budget / new_accts / new_dollars`.
3. Verify Kurt's invariant: `booked + pipeline = expected` — if any row fails, raise.
4. Cross-reference against the Data tab's booked detail:
   - For a given `(ae1, month)`, the sum of `sheet-export.gross_rate` for rows where `ae1 = :ae` equals this endpoint's `booked` **for all AEs except WorldLink and House**. Don't panic on those two — see §4.

### 7. Differs from the `/planning/` page — enumerate and accept

The in-app `/planning/` page and this endpoint share the same storage but diverge on display rules. All are deliberate; none are bugs.

1. **`pipeline` sign.** Dashboard returns `forecast − booked` (signed, can go negative when overbooked). This endpoint returns `max(0, COALESCE(forecast, budget) − booked)` — floored. Kurt's rule for the workbook: "we don't go negative." The dashboard's negative-pipeline case maps to `pipeline = 0` here.

2. **`forecast` when no forecast row exists.** Dashboard auto-fills `forecast = budget` when no row, and exposes an `is_overridden` flag. This endpoint emits `forecast = null`, preserving the "no forecast was entered" signal. The `pipeline` / `expected` computations still use `budget` as the fallback internally (via `COALESCE`), so the math matches — but the column Kurt sees on the workbook will be blank where the dashboard shows a number.

3. **`vs_budget` vs. dashboard's `variance`.** These are different metrics sharing a similar name. Dashboard's `variance = forecast − budget` — "did we raise or lower the plan from budget?" This endpoint's `vs_budget = booked − budget` — "are actuals hitting budget?" The second one is what Kurt wants for pacing. **Neither is the other's rename.**

4. **`expected` vs. dashboard's `effective`.** Dashboard uses a time-based switch: `effective = booked if period.is_past else forecast`. This endpoint uses content-based max: `expected = max(booked, COALESCE(forecast, budget))`. They agree in most cases but disagree for past months where plan was missed (`booked < forecast`): dashboard shows `booked` (committed truth); this endpoint shows `forecast` (what was planned). Kurt accepted the max-based formula because the workbook's Data tab already shows `booked` separately, so `expected` can honestly reflect the target.

5. **Fields the dashboard emits that this endpoint does NOT:** `pct_booked`, `is_forecast_overridden`. Derivable client-side if wanted (`booked / expected * 100`; `forecast is not null`).

6. **Fields this endpoint emits that the dashboard does NOT:** `new_accts`, `new_dollars`. The dashboard's `/planning/api/summary` payload omits these even though they're stored in the `forecast` table. This endpoint includes them because Kurt wants them in the workbook.

If someone later needs to reconcile the two views exactly, either swap this endpoint's formulas for the dashboard's (losing Kurt's preferred semantics), or have the dashboard adopt the workbook's (requires Kurt sign-off). Until then: two views, two conventions, documented here.

### 7a. Coverage vs sheet-export — expected mismatch

Sheet-export is **all-history** (no year filter); planning-export is **year-scoped**. Don't expect their AE lists to match:

- **AEs in sheet-export but not in planning-export for year Y:** AEs whose bookings fall outside year Y. Historical-only AEs (e.g. someone who left the company) appear on the Data tab with their old spots but don't appear on the Planning tab for the current year. This is correct.
- **AEs in planning-export but not in sheet-export for year Y:** AEs with a budget row for year Y but no bookings yet (e.g. new hires, placeholder AEs like `ZTBD`). Also correct.
- **Virtual AEs:** `"WorldLink"` in planning-export aggregates spots matching `bill_code LIKE 'WorldLink%'`, regardless of their `sales_person`. On the Data tab those same spots appear under whatever `sales_person` was stored (typically `"House"`). This is how the in-app dashboard behaves; don't try to reconcile by summing Data-tab rows keyed on `ae1 = "WorldLink"`.

Reconciliation to the Data tab is reliable only for regular AEs (not WorldLink, not House), and only within the scope of the requested year.

### 8. Out of scope for v1.0

- **Write-back.** This endpoint is read-only. The dashboard (`POST /planning/api/forecast`, `.../forecast/bulk`, `.../new-business/bulk`, `.../forecast/reset`, all `@admin_required`) is the sole writer. Two writers to the `forecast` table would fight; keep the workbook read-only.
- **Multi-year range.** One year per call. Widen via `start_year` / `end_year` in v1.1 if the workbook wants multi-year history.
- **`is_active` / `is_past` period flags.** The dashboard flags the active planning window. If the workbook wants the same visual, add per-row flags in v1.1.
- **Drift detection on AE identity.** No `row_hash` / `known AEs` mechanism. If AEs get renamed (old name disappears, new name appears in the same period), the workbook just sees two different AE rows. The proposed [Workbook AE Drift Tracker](#workbook-ae-drift-tracker-proposed) handles this client-side.

### 9. Smoke test

```bash
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/planning-export \
  | jq '.metadata, .rows[0]'
```

Expected: metadata with `schema_version="1.0"` and a numeric `year`, plus a sample row with all ten fields present.

---

## Workbook AE Drift Tracker (Proposed)

> **Status:** Proposed (2026-04-21). Workbook-side only — **zero server changes required**.

Companion to `tblKnownRows` (row-level tuple acknowledgment). Catches AE-level identity changes that the row tracker misses, or flags with 300 row-level noise events when one person is renamed.

### 1. Why this exists

`tblKnownRows` fires when a new `(customer, market, revenue_class, ae1, agency_flag, sector)` tuple appears. If `ae1` changes, every tuple that AE touches gets a fresh `row_hash` → Kurt sees hundreds of `New Rows` entries instead of "one person was renamed, confirm and move on."

`tblKnownAEs` fires one level up: on the AE identity itself. It's a **coarse pre-filter** that lets Kurt confirm the AE change in one keystroke, at which point the 300 row-level flags can be batch-acknowledged or ignored.

It also surfaces AEs that the server's casing-drift normalization has quietly collapsed, so Kurt can decide whether the drift reflects a genuine new person or a DB hygiene issue the importer should catch.

### 2. What it catches

| Scenario | Example | Row-level symptom | AE-level symptom |
|---|---|---|---|
| **New AE** | Kurt hires "Alex Kim" | 50+ new `row_hash` entries on `New Rows` | One new `ae_key = alex kim` on `New AEs`. Kurt acknowledges once. |
| **Rename** | DB cleanup: `Riley van Patten` → `Riley Van Patten` (all spots) | All Riley tuples re-hash; 300+ `New Rows` entries | `ae_key = riley van patten` unchanged — **nothing fires** because the key is already acknowledged. Display may update silently. |
| **Partial-cleanup drift** | Half of Riley's spots get fixed, half don't | Server MIN picks one canonical casing; drift invisible at row level unless other identity fields also differ | Tracker compares `ae_key` against `tblKnownAEs`; if the canonical display the server returns doesn't match the acknowledged display, flag as "Casing drift — probably harmless, DB cleanup happened" |

The rename case is where this feature earns its keep. The server's casing-normalization (committed 2026-04-21) already handles the common per-spot casing drift, but a genuine rename (`Jane Smith` → `Jane Smith-Lopez` after marriage) produces a new `ae_key` and a dead old one. The tracker surfaces both.

### 3. Key design decisions

#### 3.1 Key on normalized form, not display

```
ae_key = Text.Lower(Text.Trim(Text.From(ae1)), "en-US")
```

Same algorithm as the `row_hash` applies to the `ae1` field. Stable across server-side canonical-display changes and DB cleanups. Display casing is **derived from the latest refresh** — never stored in `tblKnownAEs`.

If you key on display, every DB cleanup becomes a re-acknowledge pass for Kurt. Don't do that.

#### 3.2 Union source across both endpoints

Build the AE universe from **sheet-export's `ae1`** union **planning-export's `ae1`**:

- Sheet-export covers AEs with booking activity (all-history).
- Planning-export covers AEs with budget or forecast rows, even with no bookings (placeholder entities like `ZTBD`, new hires in planning but not yet in spots).

Either source alone misses legitimate AEs.

#### 3.3 `tblKnownAEs` schema

Hidden sheet, single table:

| Column | Type | Notes |
|---|---|---|
| `ae_key` | Text | Normalized form. Primary key. |
| `acknowledged_display` | Text | The display casing at time of ack. Used only for drift-detection reasons on `New AEs`. |
| `acknowledged_date` | Date | When Kurt added it. Optional but useful for audit. |
| `notes` | Text | Freeform. Optional. |

Kurt acknowledges by copying a row from `New AEs` → `tblKnownAEs`. Same manual workflow as `tblKnownRows` for v1. A "Ack Selected" Office Script can follow in v2 if it gets annoying.

#### 3.4 `New AEs` tab schema

Shown to Kurt. Read-only output of the PQ anti-join.

| Column | Purpose |
|---|---|
| `ae_key` | The lookup key. Kurt copies this to `tblKnownAEs` to acknowledge. |
| `ae1_display` | Current server casing. What Kurt sees on the Data tab. |
| `reason` | Auto-computed: `"New AE"`, `"Casing drift from acknowledged '<old>'"`, or `"Rename sibling — possible match for '<old>'"`. |
| `first_seen_month` | Earliest `broadcast_month` across both endpoints. Helps Kurt gauge how long this has been around. |
| `latest_month` | Latest `broadcast_month`. Helps spot stale vs active. |
| `total_gross` | Lifetime `SUM(gross_rate)` from sheet-export. `null` for planning-only AEs with no activity. |
| `source` | `"sheet-export"` / `"planning-export"` / `"both"`. Flags planning-only placeholders (`ZTBD`) vs data-only AEs (historical Riley). |

Sort by `total_gross` descending — biggest unacknowledged AEs surface first.

### 4. `reason` auto-computation

Applied row-by-row on the `New AEs` output:

1. **`"Casing drift from acknowledged '<ack_display>'"`** — if `ae_key` is found in `tblKnownAEs` but the current `ae1_display` doesn't match `acknowledged_display`. This is almost always benign (a DB cleanup happened, server's canonical-casing MIN() picked differently). Kurt can either re-acknowledge the new display (update the row in `tblKnownAEs`) or ignore.

2. **`"Rename sibling — possible match for '<old>'"`** — if `ae_key` is new AND there's an acknowledged AE whose display differs only in known name-mutation patterns (married name suffix, space variations). Heuristic, not load-bearing — false positives fine, false negatives fine. One simple check: fuzzy match on the first token of the display name. E.g., new `jane smith-lopez` flagged as possible rename of acknowledged `jane smith`.

3. **`"New AE"`** — everything else. Default reason.

Kurt reads these and decides. The tracker doesn't auto-acknowledge anything; it just saves Kurt the "wait, is this actually a new person?" cognitive pass.

### 5. PQ implementation sketch

```
qAEs_raw =
    let
        from_sheet = Table.SelectColumns(qRevenueActuals,
            {"ae1", "broadcast_month", "gross_rate"}),
        from_sheet_typed = Table.AddColumn(from_sheet, "source",
            each "sheet-export"),
        from_plan = Table.SelectColumns(qPlanningExport,
            {"ae1", "broadcast_month"}),
        from_plan_null_gross = Table.AddColumn(
            Table.AddColumn(from_plan, "gross_rate", each null),
            "source", each "planning-export"),
        combined = Table.Combine({from_sheet_typed, from_plan_null_gross}),
        with_key = Table.AddColumn(combined, "ae_key",
            each Text.Lower(Text.Trim(Text.From([ae1])), "en-US"))
    in
        with_key

qAEs_grouped =
    let
        source = qAEs_raw,
        grouped = Table.Group(source, {"ae_key"}, {
            {"ae1_display", each List.First([ae1]), type text},
            {"first_seen_month", each List.Min([broadcast_month])},
            {"latest_month", each List.Max([broadcast_month])},
            {"total_gross", each List.Sum([gross_rate])},
            {"sources", each Text.Combine(List.Distinct([source]), ",")}
        }),
        with_source_label = Table.TransformColumns(grouped, {{"sources", each
            if _ = "sheet-export,planning-export" or _ = "planning-export,sheet-export"
            then "both" else _}})
    in
        with_source_label

qNewAEs =
    let
        all_aes = qAEs_grouped,
        known = Excel.CurrentWorkbook(){[Name="tblKnownAEs"]}[Content],
        -- Anti-join to filter out acknowledged AEs (match on ae_key,
        -- but preserve drift case via separate logic).
        with_ack = Table.NestedJoin(all_aes, {"ae_key"}, known, {"ae_key"},
            "ack_match", JoinKind.LeftOuter),
        with_reason = Table.AddColumn(with_ack, "reason", each
            if Table.RowCount([ack_match]) = 0 then
                "New AE"    -- fuzzy-rename heuristic can refine this
            else if [ack_match]{0}[acknowledged_display] <> [ae1_display] then
                "Casing drift from acknowledged '" & [ack_match]{0}[acknowledged_display] & "'"
            else
                null),  -- fully acknowledged, drop from output
        filtered = Table.SelectRows(with_reason, each [reason] <> null),
        final = Table.RemoveColumns(filtered, {"ack_match"})
    in
        Table.Sort(final, {{"total_gross", Order.Descending}})
```

The fuzzy rename heuristic (first-token match) can be layered on as a second pass that adds a `rename_candidate` column, then the `reason` computation prefers the rename case over the plain "New AE". Simple to bolt on after the base tracker ships.

### 6. Acknowledgment workflow

Same muscle memory as `tblKnownRows`:

1. Refresh.
2. Open `New AEs` tab, scan top-to-bottom (sorted by total_gross).
3. For each row:
   - **"New AE"** → copy `ae_key` + current `ae1_display` to `tblKnownAEs`. Fill in `acknowledged_date = TODAY()`.
   - **"Casing drift from acknowledged '<old>'"** → either (a) update the `acknowledged_display` in the matching `tblKnownAEs` row (explicit re-ack) or (b) ignore and live with the stale display (the `reason` will keep firing until you do one or the other).
   - **"Rename sibling — possible match for '<old>'"** → Kurt's judgment. If it's really a rename, update the old `tblKnownAEs` row's `ae_key` + display; if it's a coincidence, treat as "New AE" and add a new `tblKnownAEs` row.
4. Refresh again. `New AEs` should now be empty (or close to it).

### 7. What this doesn't do (scope boundaries)

- **Doesn't auto-acknowledge anything.** All entries require Kurt action. The tracker is informational.
- **Doesn't affect `tblKnownRows`.** A fresh row_hash from an AE rename still surfaces on `New Rows`. If that's annoying after v1, a v2 enhancement could let Kurt select "AE rename — auto-acknowledge all row_hashes touching this AE" from the `New AEs` tab. Not v1.
- **Doesn't prevent the drift.** The server's casing-normalization (committed 2026-04-21) already collapses the common per-spot casing drift. The tracker catches what the server can't see — genuine renames, placeholder-to-active transitions, display-vs-ack-casing mismatches after DB cleanups.
- **Doesn't need a server endpoint.** Purely workbook-side.

### 8. Why no server-side `/api/revenue/ae-directory`

A server-side endpoint pre-computing `{ae_key, ae1_display, first_seen, last_seen, total_gross}` per AE was considered and **rejected for v1** — the workbook aggregates ~5k rows in PQ in milliseconds, and the endpoint adds one more auth surface and one more doc. Revisit if perf ever bites (unlikely).

### 9. Testing / validation (informal)

After implementing, verify three cases:

1. **Baseline.** First refresh after adding `tblKnownAEs` (empty) → `New AEs` shows every AE, sorted by total_gross. Kurt bulk-acknowledges the known-good ones.
2. **Stable refresh.** Immediately refresh again without DB changes → `New AEs` is empty.
3. **Synthetic rename.** Temporarily rename one AE in `tblKnownAEs` (change `acknowledged_display`, leave `ae_key` alone) → next refresh flags "Casing drift from acknowledged '<renamed>'". Revert the manual edit.

No automated test harness — this is workbook-side behavior, tested by Kurt exercising the UI.

---

## Canon endpoints

`/api/canon/*` — internal API backing the in-app **Customer Normalization Manager** (template `customer_normalization_manager.html`, blueprint at `src/web/routes/canon_tools.py`). Authenticated via the app's session (Tailscale-based); not callable from the workbook.

| Method | Path | Body | Purpose |
|---|---|---|---|
| `POST` | `/api/canon/agency` | `{alias_name, canonical_name}` | Insert/update row in `agency_canonical_map`; logs to `canon_audit` with `action='agency_canon'` |
| `POST` | `/api/canon/customer` | `{alias_name, canonical_name}` | Insert/update row in `customer_canonical_map`; logs to `canon_audit` with `action='customer_canon'` |
| `POST` | `/api/canon/raw-to-customer` | `{raw_text, target_entity_id}` | Map raw `bill_code` text directly to a canonical customer via `entity_aliases` (entity_type='customer'); logs to `canon_audit` with `action='raw_map'` |
| `GET` | `/api/canon/suggest/normalized?q=…` | — | Prefix-match autocomplete against `customers.normalized_name` |

Tables and view chain referenced by these endpoints (`v_raw_clean` → `v_normalized_candidates` → `v_customer_normalization_audit`, plus `entity_aliases`, `customer_canonical_map`, `agency_canonical_map`, `canon_audit`) are documented in [ARCHITECTURE.md](ARCHITECTURE.md). The monthly raw-input sync runbook (after major imports, populate `raw_customer_inputs`) lives in [RUNBOOKS.md](RUNBOOKS.md).

---

## Known gaps

Real today. Don't design around assuming they'll be fixed.

1. **Sheet-export does not emit `contract`.** Required before `AgencyPercent`, `BrokerPercent`, and `Broker (Y/N)` can be correctly computed (see [§3 PQ derivations](#3-pq-derivations-not-returned-by-api)). Workbook v1 leaves those three columns blank/null until endpoint v1.1 lands. v1.1 design needs to settle:
   - exact field name / DB source for the contract identifier
   - whether `contract` is a non-identity column (hash stays `v1`, PQ picks one contract per existing tuple for display) or a new **identity** field in the tuple (hash bumps to `v2`, `tblKnownRows` is invalidated, tuple grain narrows). The former is lower-blast-radius; the latter matches "drift creates a new row" semantics if contract changes should surface as new rows.
   - selection rule when one tuple has multiple contracts (default candidate: latest-`broadcast_month`).
2. **Sheet-export `start_month` / `end_month` query params are accepted but not applied.** The service signature takes them, echoes them into `metadata`, but `_query()` does not bind them into the SQL WHERE clause. Passing `?start_month=Jan-25&end_month=Feb-25` today returns the full dataset. **Workbook v1 does not send query params** — but don't add them.
3. **Sheet-export `include_closed` query param in spec but not implemented at all.**
4. **Hash SHA1 test-vector hex digests are not pinned.** Spec §6.6 defines the algorithm and two test vectors but leaves the hex digests as `[compute during implementation]`. When you implement the PQ hash, compute both vectors and pin them in both server-side and PQ-side unit tests.
5. **`/api/health` returns 401** because the app-wide `_require_login` before-request hook allow-lists `/health` but not `/api/health`. Use `/health` for liveness probes.

---

## Related docs

- [ARCHITECTURE.md](ARCHITECTURE.md) — system architecture, 29-column data dictionary, Canon system internals, view chain.
- [RUNBOOKS.md](RUNBOOKS.md) — token rotation, smoke-test operator procedure, env setup.
- `docs/superpowers/specs/2026-04-20-revenue-sheet-export-design.md` — full design spec for the sheet-export endpoint (preserved as historical record; §6.6 is the authoritative hash spec, §6 has the full Power Query architecture).
- `src/repositories/planning_repository.py:435-548` — server-side reference for `get_booked_revenue` semantics.
