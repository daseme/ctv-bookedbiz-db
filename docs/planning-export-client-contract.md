# Planning Export — Client Contract (Workbook Side)

**Audience:** the LLM / engineer building the Power Query side of `Revenue Master.xlsx`.
**Server:** SpotOps Flask app, `GET /api/revenue/planning-export`.
**Companion:** `docs/sheet-export-client-contract.md` (v1.1 booked detail).

This doc is the self-contained handoff for the planning rollup: one row per
`(ae1, broadcast_month)` per year, carrying budget / forecast / booked plus
three derived fields (`expected`, `pipeline`, `vs_budget`) the workbook needs
to render its planning view without re-implementing the null-handling
semantics Kurt specified.

See §8 for places this endpoint **deliberately diverges** from the in-app
`/planning/` page. None of it is accidental — but if you're cross-checking
numbers on screen vs. in the workbook, you'll see differences, and §8 is the
enumeration so nobody files it as a bug.

---

## 1. Endpoint

```
GET http://<host>:8000/api/revenue/planning-export[?year=YYYY]
Header: X-SpotOps-Token: <shared-secret>
```

- Auth: **same `X-SpotOps-Token` header as sheet-export.** No separate token.
- Query params:
  - `year` (optional, integer). Defaults to `date.today().year`.
  - Invalid `year=foo` → `400 INVALID_YEAR`.
- Port: **8000** in the current Docker compose stack.

## 2. Response shape

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

### Identity

Each row is keyed by `(ae1, broadcast_month)`. No customer / market /
revenue_class sub-grain — planning is AE × month only.

### Field reference

| Field | Type | Source / formula |
|---|---|---|
| `ae1` | string | `revenue_entities.entity_name` where `is_active = 1` |
| `broadcast_month` | ISO date | First-of-month, same format as sheet-export |
| `budget` | number \| null | `budget.budget_amount` for `(ae, year, month)` |
| `forecast` | number \| null | `forecast.forecast_amount` for `(ae, year, month)` |
| `booked` | number | `SUM(gross_rate)` from `spots`, Trade-excluded, with WorldLink/House special cases (see §4). **Never null** — `0.0` when no spots. |
| `new_accts` | integer \| null | `forecast.new_accounts_forecast`. `null` when no forecast row. |
| `new_dollars` | number \| null | `forecast.new_dollars_forecast`. `null` when no forecast row. |
| `expected` | number | `max(booked, COALESCE(forecast, budget))`. Falls back to `booked` when both plan columns are null. Never null in emitted rows. |
| `pipeline` | number \| null | `max(0, COALESCE(forecast, budget) − booked)`. **Floored** — never negative. `null` when both forecast and budget are null. |
| `vs_budget` | number \| null | `booked − budget`, signed. `null` when `budget` is null. |

### Null rules (explicit)

| Input state | `expected` | `pipeline` | `vs_budget` |
|---|---|---|---|
| All of `budget`, `forecast`, `booked>0` present | `max(booked, forecast)` | `max(0, forecast − booked)` | `booked − budget` |
| Budget + booked, no forecast | `max(booked, budget)` | `max(0, budget − booked)` | `booked − budget` |
| Forecast + booked, no budget | `max(booked, forecast)` | `max(0, forecast − booked)` | **null** |
| Booked only (no budget, no forecast) | `booked` | **null** | **null** |
| Budget only (booked = 0) | `budget` | `budget` | `−budget` |
| Forecast only (booked = 0) | `forecast` | `forecast` | null |

### Emission rule

One row per `(ae1, year, month)` where **at least one of**:
- a `budget` row exists for `(ae, year, month)`, OR
- a `forecast` row exists for `(ae, year, month)`, OR
- `booked > 0` for `(ae, year, month)`.

Months with no activity for an AE are **suppressed** (no row). For 2026 the
endpoint currently emits 62 rows across 6 AEs — fewer than 12 × 6 = 72 because
some AEs don't have activity in every month.

## 3. Kurt's workflow invariant

Kurt designed the columns so that, per `(ae, month)`:

```
booked + pipeline = expected
```

- **Underbooked month** (`booked < forecast`): `booked + (forecast − booked) = forecast`. The sum "lands on forecast" — exactly what Kurt means when he says "sum to our forecast number."
- **Overbooked month** (`booked ≥ forecast`): `booked + 0 = booked`. The sum lands on actual booked, which is the honest "operating expectation" once you've overshot the plan.

This is why `pipeline` is **floored at 0** — keeping the invariant
`booked + pipeline = expected` true in both regimes.

## 4. `booked` semantics (copied from dashboard)

The endpoint's `booked` is computed exactly like the dashboard's
`planning_repository.get_booked_revenue` (at `src/repositories/
planning_repository.py:435-548`):

| AE | SQL filter |
|---|---|
| `"WorldLink"` | `bill_code LIKE 'WorldLink%'` — **ignores `sales_person`** |
| `"House"` | `sales_person = 'House' AND bill_code NOT LIKE 'WorldLink%'` |
| All others | `sales_person = :ae1` — no `bill_code` filter |

All three branches: Trade excluded via `(revenue_type != 'Trade' OR revenue_type IS NULL)`, `SUM(gross_rate)`, grouped by `broadcast_month`.

**Divergence from sheet-export.** If you sum sheet-export's `gross_rate` by
`ae1 = :ae` for a given month, you'll match **except for WorldLink and
House**. For those two the workbook should trust this endpoint's `booked`,
not a sheet-export sum. See §8.

## 5. Metadata envelope

- `generated_at` — UTC ISO-8601 with `Z` suffix (same as sheet-export).
- `schema_version = "1.0"` — independent of sheet-export's `"1.1"`. Bump when the row shape changes.
- `year` — echo of the requested year (or `date.today().year` if the param was omitted).
- `row_count` — equals `len(rows)`.

No `hash_version`, no `row_hash`. Identity is `(ae1, broadcast_month)`, which is already content-free — no hashing needed for drift detection.

## 6. Error behaviors

Same as sheet-export:

| Situation | Behavior |
|---|---|
| Missing or wrong `X-SpotOps-Token` | `401 {"error": "Authentication required"}` |
| `SHEET_EXPORT_TOKEN` env var unset on server | `503` |
| `year` param non-integer | `400 {"error": "Invalid year: 'foo'"}` |
| Empty `rows` array on populated DB | Technically valid but suspect. Recommend the workbook error on empty for this endpoint too, with an explicit `AllowEmpty` override, same pattern as sheet-export §10. |

## 7. What the workbook does with this

1. Pull this endpoint alongside `/api/revenue/sheet-export`. One year at a time.
2. New `Planning` tab (or second pivot on Data) showing `ae1 × broadcast_month` with columns `budget / forecast / booked / expected / pipeline / vs_budget / new_accts / new_dollars`.
3. Verify Kurt's invariant: `booked + pipeline = expected` — if any row fails, raise.
4. Cross-reference against the Data tab's booked detail:
   - For a given `(ae1, month)`, the sum of `sheet-export.gross_rate` for rows where `ae1 = :ae` equals this endpoint's `booked` **for all AEs except WorldLink and House**. Don't panic on those two — see §4 and §8.

## 8. Differs from the `/planning/` page — enumerate and accept

The in-app `/planning/` page and this endpoint share the same storage but
diverge on four display rules. All four are deliberate; none are bugs.

1. **`pipeline` sign.** Dashboard returns `forecast − booked` (signed,
   can go negative when overbooked). This endpoint returns
   `max(0, COALESCE(forecast, budget) − booked)` — floored. Kurt's rule
   for the workbook: "we don't go negative." The dashboard's negative-
   pipeline case maps to `pipeline = 0` here.

2. **`forecast` when no forecast row exists.** Dashboard auto-fills
   `forecast = budget` when no row, and exposes an `is_overridden` flag
   to tell you it did so. This endpoint emits `forecast = null`,
   preserving the "no forecast was entered" signal. The `pipeline` /
   `expected` computations still use `budget` as the fallback internally
   (via `COALESCE`), so the math matches — but the column Kurt sees on
   the workbook will be blank where the dashboard shows a number.

3. **`vs_budget` vs. dashboard's `variance`.** These are different
   metrics sharing a similar name. Dashboard's `variance = forecast −
   budget` — "did we raise or lower the plan from budget?" This
   endpoint's `vs_budget = booked − budget` — "are actuals hitting
   budget?" The second one is what Kurt wants for pacing. **Neither is
   the other's rename.**

4. **`expected` vs. dashboard's `effective`.** Dashboard uses a
   time-based switch: `effective = booked if period.is_past else
   forecast`. This endpoint uses content-based max: `expected =
   max(booked, COALESCE(forecast, budget))`. They agree in most cases
   but disagree for past months where plan was missed (`booked <
   forecast`): dashboard shows `booked` (committed truth); this endpoint
   shows `forecast` (what was planned). Kurt accepted the max-based
   formula because the workbook's Data tab already shows `booked`
   separately, so `expected` can honestly reflect the target.

5. **Fields the dashboard emits that this endpoint does NOT:**
   `pct_booked`, `is_forecast_overridden`. Derivable client-side if
   wanted (`booked / expected * 100`; `forecast is not null`).

6. **Fields this endpoint emits that the dashboard does NOT:**
   `new_accts`, `new_dollars`. The dashboard's `/planning/api/summary`
   payload omits these even though they're stored in the `forecast`
   table. This endpoint includes them because Kurt wants them in the
   workbook.

If someone later needs to reconcile the two views exactly, either swap
this endpoint's formulas for the dashboard's (losing Kurt's preferred
semantics), or have the dashboard adopt the workbook's (requires Kurt
sign-off). Until then: two views, two conventions, documented here.

## 8a. Coverage vs sheet-export — expected mismatch

Sheet-export is **all-history** (no year filter); planning-export is
**year-scoped**. Don't expect their AE lists to match:

- **AEs in sheet-export but not in planning-export for year Y:** AEs
  whose bookings fall outside year Y. Historical-only AEs (e.g. someone
  who left the company) appear on the Data tab with their old spots but
  don't appear on the Planning tab for the current year. This is
  correct.
- **AEs in planning-export but not in sheet-export for year Y:** AEs
  with a budget row for year Y but no bookings yet (e.g. new hires,
  placeholder AEs like `ZTBD`). Also correct.
- **Virtual AEs:** `"WorldLink"` in planning-export aggregates spots
  matching `bill_code LIKE 'WorldLink%'`, regardless of their
  `sales_person`. On the Data tab those same spots appear under
  whatever `sales_person` was stored (typically `"House"`). This is
  how the in-app dashboard behaves; don't try to reconcile by summing
  Data-tab rows keyed on `ae1 = "WorldLink"`.

Reconciliation to the Data tab is reliable only for regular AEs (not
WorldLink, not House), and only within the scope of the requested year.

## 9. Out of scope for v1.0

- **Write-back.** This endpoint is read-only. The dashboard
  (`POST /planning/api/forecast`, `.../forecast/bulk`, `.../new-business/bulk`,
  `.../forecast/reset`, all `@admin_required`) is the sole writer. Two writers
  to the `forecast` table would fight; keep the workbook read-only.
- **Multi-year range.** One year per call. Widen via `start_year` / `end_year`
  in v1.1 if the workbook wants multi-year history.
- **`is_active` / `is_past` period flags.** The dashboard flags the active
  planning window. If the workbook wants the same visual, add per-row flags
  in v1.1 — trivial.
- **Drift detection on AE identity.** No `row_hash` / `known AEs` mechanism.
  If AEs get renamed (old name disappears, new name appears in the same
  period), the workbook just sees two different AE rows. Add a `tblKnownAEs`
  equivalent if it becomes a problem.

## 10. Smoke test

```bash
set -a; . /opt/spotops/.env; set +a
curl -sS -H "X-SpotOps-Token: $SHEET_EXPORT_TOKEN" \
  http://localhost:8000/api/revenue/planning-export \
  | jq '.metadata, .rows[0]'
```

Expected: metadata with `schema_version="1.0"` and a numeric `year`, plus a
sample row with all ten fields present.
