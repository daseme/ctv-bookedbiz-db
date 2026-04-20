# DB Schema Audit — Sheet Export

**Date:** 2026-04-20
**Author:** Server-side agent
**For:** Kurt (integrator) → Excel-side agent
**Companion doc:** `2026-04-20-revenue-sheet-export-design.md`
**Handoff doc:** `Revenue Sheet Export — Server-Side Handoff` (§4 requests this writeup)

This audit resolves the "Fields to verify during implementation" block in design doc §5. It's a pre-implementation gate — endpoint code does not start until Kurt answers the open questions at the bottom and propagates any §5 changes to the Excel-side agent.

---

## Sources reviewed

- `/opt/spotops/schema-260119-1152am.sql` — current schema snapshot
- `spots_reporting` view definition — line 1068 of the snapshot
- Migration files: `012_agency_commission_fields.sql`, `015_customer_commission_fields.sql`
- `docs/GUIDE-TwentyNineColumns.md` — Excel import column semantics
- Services: `entity_service.py`, `bill_code_parser.py`, `customer_detail_service.py`, `business_rules_service.py`

---

## Field-by-field findings

### `broker_flag`

- **Source:** not in DB.
- **Caveats:** The only broker-related column anywhere in the schema is `spots.broker_fees DECIMAL(12,2)` — a dollar amount, not a Y/N flag. There is no `brokers` table, no `customer_broker` junction, no `broker_flag` column.
- **Recommendation:** Emit `null` from the API in v1. Move to a v2 Excel-side override column Kurt maintains. No DB schema change in v1 — broker designation is clearly a human-maintained business fact, not something the import pipeline captures.

### `broker_name`

- **Source:** not in DB.
- **Caveats:** No column holds a broker name anywhere.
- **Recommendation:** Emit `null`. v2 Excel-side override. A `brokers` table + FK isn't justified by current evidence.

### `broker_percent`

- **Source:** not in DB.
- **Caveats:** Same as above.
- **Recommendation:** Emit `null`. v2 Excel-side override.

### `agency_flag`

- **Source:** `spots.agency_flag` TEXT column (line 721 of schema snapshot).
- **Storage semantics:** Stored at import from Etere Excel — **not derived** from `bill_code`. Values are `'Agency'` or `'Non-agency'` per `GUIDE-TwentyNineColumns.md`.
- **Caveats:**
    - TEXT, not boolean. Server-side conversion required: `CASE WHEN s.agency_flag = 'Agency' THEN 'Y' ELSE 'N' END`.
    - Within a (customer, market, revenue_class, broadcast_month) grouping, individual spots could in principle carry different `agency_flag` values if Etere ever produced them that way. Defensive plan: take `MAX(agency_flag)` over the grouping, or majority-vote — will need inspection of real data.
    - **Design doc §5 currently assumes `agency_flag` is derivable from `bill_code` prefix. That's wrong** — it's stored. Using the stored value is strictly better (preserves what Etere reported at import, immune to future derivation-rule changes). Design doc §5 needs a patch.
- **Recommendation:** Use the stored value. Patch design doc §5 accordingly (see Q4 below).

### `agency_percent`

- **Source:** `COALESCE(customers.commission_rate, agencies.commission_rate)`.
- **Storage semantics:**
    - `agencies.commission_rate DECIMAL(5,2)` — agency-level rate. `NULL` = **"never reviewed"** (distinct from 0%). CHECK constraint `0 ≤ rate ≤ 100`. From migration 012.
    - `customers.commission_rate DECIMAL(5,2)` — **per-customer override**. `NULL` = **"inherit from agency"**. Same CHECK constraint. From migration 015.
    - Paired with `order_rate_basis` (`'gross'` / `'net'` / NULL), but that's a separate concern not needed by v1.
- **Caveats:**
    - **Both columns are real and usable.** This is the best field in the audit.
    - **Real-world collision:** Kurt's sample sheet shows `3fold:Los Rios Community College` with `Internal Ad Sales = 15%` and `Branded Content = 0%` for the **same customer, different revenue classes**. The DB stores `commission_rate` per-customer only, **not per-customer-per-revenue-class**. The API cannot faithfully reproduce this without either (a) a per-revenue-class override table (schema change) or (b) treating per-revenue-class differences as manual v2 Excel overrides. **This needs Kurt's call — Q1 below.**
    - NULL emission: the API must emit NULL as JSON `null`, not coerce to 0. A customer under an unreviewed agency will show a blank `agency_percent` — intentional, not a bug.
- **Recommendation:** Expose `COALESCE(c.commission_rate, a.commission_rate)` as `agency_percent`. Resolve the per-revenue-class issue via Q1.

### `GrossCommission`

- **Source:** not in DB. Per-AE commission is not stored anywhere — no `ae_commission` table, no `commission_rate` column on `revenue_entities` or related AE tables.
- **Caveats:** Design doc §5 already settled this: the lookup lives in `Config!tblCommissionByAE` on the Excel side. The endpoint does not return `GrossCommission`. PQ joins `ae1` against Kurt's manual table.
- **Recommendation:** Do not expose from the API. No action beyond confirming the design doc's existing decision.

---

## Schema surprises

### 1. `spots_reporting` doesn't expose the columns the endpoint needs

`spots_reporting` (line 1068 of the schema snapshot) selects:

> `spot_id, bill_code, air_date, gross_rate, station_net, sales_person, revenue_type, broadcast_month, is_historical, import_batch_id, customer_name (= normalized_name), sector_code, sector_name, agency_name, market_code, market_display_name, region, language_code, language_name, import_mode, import_date, started_by, closed_date, closed_by`

It **does not** include `agency_flag`, `customers.commission_rate`, `agencies.commission_rate`, or `broker_fees`. Trade exclusion is the view's `WHERE` clause (fine — the endpoint inherits it).

Two implementation options:

- **A.** Query directly from `spots s` with extra joins to `customers c` and `agencies a` to pick up `agency_flag` and `commission_rate`. Apply the Trade filter manually (`WHERE s.revenue_type != 'Trade' OR s.revenue_type IS NULL`).
- **B.** Add a thin wrapper view `v_spots_reporting_sheet` that extends `spots_reporting` with the missing columns. Cleaner for reuse, one more piece of schema to maintain.

**My preference for v1: A.** One endpoint, unusual shape (wide grouping), no other consumer on the horizon. If a second consumer appears, promote to B.

### 2. `agency_flag` is stored, not derived

Design doc §5 assumes `agency_flag` comes from `bill_code` parsing. In fact it's a TEXT column on `spots`, written by the Excel importer from Etere. Using the stored value is strictly better — history survives any future rule change. Design doc §5 needs a patch (Q4).

### 3. `agencies.commission_rate` NULL ≠ 0%

Migration 012 is explicit: `NULL DEFAULT` means "never reviewed." Zero means "confirmed 0%." The API must preserve NULL (emit JSON `null`), not coerce. Forecast rows for customers under an unreviewed agency will surface as blanks in the `AgencyPercent` column — intentional, not an error.

### 4. Per-revenue-class `commission_rate` doesn't exist

Biggest gap. See Q1 below.

### 5. `spots.broker_fees` exists as an amount

Not useful for `broker_flag` / `broker_name` / `broker_percent` as the sheet defines them. Flagging it in case future work wants a "brokered revenue" metric — worth ~nothing for v1.

---

## Open questions for Kurt

**Q1 (blocking).** Per-revenue-class `agency_percent`.

The DB stores one rate per customer. Your sheet sometimes has different rates per revenue class for the same customer (the `3fold:Los Rios` case). Three options:

- **(a)** Emit the per-customer DB value for every revenue class. The Branded Content row would show 15% when you want 0% — you'd override manually in a v2 Excel table.
- **(b)** Emit `null` when the combination is ambiguous — forces a v2 per-revenue-class override table on the Excel side.
- **(c)** Add a `customer_revenue_class_commission` override table to the DB now (small schema change). The endpoint reads the per-(customer, revenue_class) override if present, else falls back to `COALESCE(customers.commission_rate, agencies.commission_rate)`.

**My recommendation:** (a) for v1. Lowest-friction path to a working endpoint. Track overrides on a v2 Excel tab (`tblAgencyPercentOverride`). If overrides accumulate past ~30 rows, promote to (c).

**Q2.** `customers.commission_rate` semantics.

Migrations 012/015 define it as an agency commission rate (0–100 DECIMAL(5,2)). Can you confirm this represents **"the percentage the agency takes from the billed amount"** — i.e., what your sheet labels as `AgencyPercent`? I read it that way but don't want to ship if the direction is actually reversed.

**Q3.** Broker fields.

Agreed to emit `null` from the API and defer to a v2 Excel override? No push for a `brokers` schema in v1?

**Q4.** Design doc §5 patch.

§5 currently says `agency_flag` is derivable from `bill_code`. It should say: *"Read from `spots.agency_flag` (TEXT; values `'Agency'` or `'Non-agency'`) and convert to `Y`/`N` in the endpoint."* Do you want me to propose a patch to the Excel-side agent through you, or wait for you to propagate?

---

## What's unblocked / blocked

**Unblocked (can start when Kurt greenlights):**

- Endpoint skeleton (route, auth header, env var, 401/503).
- Tests for auth behavior.
- Response schema scaffold using the fields that are resolved: `customer`, `market`, `revenue_class`, `ae1`, `sector`, `broadcast_month`, `gross_rate`. Plus `agency_flag` once Q4 is settled.

**Blocked on Kurt's answers:**

- Final API response shape (specifically `agency_percent` behavior and whether `broker_*` fields get emitted as null or omitted entirely).
- Design doc §5 patch (Q4).
- Integration test pivoting against `spots_reporting` — need to know the full target shape before I can hand-compute a comparison slice.

**Once Q1–Q4 are answered and design doc §5 is updated**, endpoint work is straightforward. Plan to use implementation approach A from Schema Surprise #1 unless Kurt prefers B.

---

## Resolution (2026-04-20, post-audit)

Kurt's feedback after seeing the audit:

> The percent exists for each line in the commercial log. If you prefer we can directly use the commercial log for this project. I need broker fee amount but ok not to have broker name. We can retrieve both gross and net if that is easier instead of deriving the percentage. Remember that we ignore Trade spots.

Key realization: `customers.commission_rate` and `agencies.commission_rate` are **entity-level audit fields**, not per-spot transactional rates. The per-line commission rate from the commercial log lives on `spots` implicitly — it's the gap between `gross_rate` and `station_net` (minus `broker_fees`). Emitting the raw amounts and letting PQ do arithmetic is strictly better than me picking the wrong source column.

**Data source confirmed: stay in the DB.** The `basic_import_service` already ingests every commercial-log line into `spots`. `spots.gross_rate`, `spots.station_net`, `spots.broker_fees` are all per-spot columns. No need for a second data source.

### Revised API response shape

The endpoint emits amounts; PQ computes display percentages.

```json
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
```

**Dropped from the response (vs. original design doc §5):** `broker_flag`, `broker_name`, `broker_percent`, `agency_percent`. All derivable in PQ from the three amount fields plus `agency_flag`.

**PQ-side derivations** (Excel-side agent owns these):

- `Agency` column (Y/N) ← `agency_flag` (pass-through).
- `AgencyPercent` ← `1 - SUM(station_net) / SUM(gross_rate)` over all months for the row. Zero-divide safe — if `gross_rate = 0` the cell is `null`.
- `Broker` column (Y/N) ← `"Y" if SUM(broker_fees) > 0 else "N"`.
- `BrokerName` ← always blank in v1 (Kurt OK with this; v2 Excel-side override if ever wanted).
- `BrokerPercent` ← `SUM(broker_fees) / SUM(gross_rate)` over all months for the row. Same zero-divide handling.
- `GrossCommission` ← lookup in `tblCommissionByAE` keyed by `ae1` (unchanged from design doc §5).

### Hash tuple simplified

Row identity (hash inputs) shrinks from 10 fields to 6:

| Position | Field |
|---|---|
| 1 | `customer` |
| 2 | `market` |
| 3 | `revenue_class` |
| 4 | `ae1` |
| 5 | `agency_flag` |
| 6 | `sector` |

**Dropped from hash:** `broker_flag`, `broker_name`, `broker_percent`, `agency_percent`.

**Drift semantics unchanged.** Drift still fires on AE reassignment (most important case), agency prefix change, or sector change. A customer going from brokered to non-brokered no longer triggers a new row — broker status is now a display attribute, not an identity attribute. Treat that as a feature: one row per customer/market/revclass/AE, with broker totals summed across all history.

No `hash_v1` → `hash_v2` bump needed — we're still pre-implementation, so the v1 definition just updates in place.

### Answers to Q1–Q4

- **Q1 (per-revenue-class agency_percent):** Dissolved. Percent isn't emitted by the API; PQ derives it from per-tuple gross/net. Per-revenue-class differences fall out naturally because revenue_class is part of the tuple identity.
- **Q2 (`customers.commission_rate` semantics):** Moot. Not used by this endpoint anymore. Still an open semantic question for other consumers but not blocking this work.
- **Q3 (broker fields):** Resolved. Emit `broker_fees` amount (per Kurt's requirement), skip `broker_name` in v1, derive `broker_flag` and `broker_percent` in PQ.
- **Q4 (§5 patch for agency_flag):** Still needed. `agency_flag` comes from `spots.agency_flag` (TEXT → Y/N conversion), not from `bill_code` parsing. Part of the design-doc patch below.

### Trade exclusion — explicit reminder

`WHERE s.revenue_type != 'Trade' OR s.revenue_type IS NULL` applied manually when the endpoint query builds from `spots` directly (approach A from Schema Surprise #1). `spots_reporting` already excludes Trade in its own WHERE clause — but since this endpoint bypasses the view, the filter is explicit in the service layer.

### Design doc patches needed

Before endpoint work starts, design doc (`2026-04-20-revenue-sheet-export-design.md`) needs these edits, which Kurt can propagate to the Excel-side agent:

1. **§5 response schema** — replace the 10-field row shape with the 7-field shape above (`broadcast_month` as ISO, amounts in place of derived fields). Drop `broker_flag`, `broker_name`, `broker_percent`, `agency_percent` from the sample JSON and query logic.
2. **§5 "Fields to verify"** — delete. This audit resolved it.
3. **§6 qRevenueActuals** — note the three new amount columns and the PQ-side derivations listed above (`Agency`, `AgencyPercent`, `Broker`, `BrokerPercent`).
4. **§6.6 hash specification** — reduce the tuple to 6 fields in the order above. Test vectors regenerated.
5. **§3.3 metadata drift** — update the tuple description: drift fires on AE / agency_flag / sector change. Broker status changes do NOT create new rows.

I'll prepare the design-doc patch as a PR if Kurt greenlights this revised shape.

### Unblocked

Endpoint implementation can start as soon as Kurt confirms the revised shape. The query is now simpler than the original design anticipated, which is the good kind of surprise.
