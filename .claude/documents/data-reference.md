# SpotOps Agent Context Reference

This document is the authoritative context for an LLM agent operating within
SpotOps. Read it fully before generating any query, migration, or business
logic. Rules here override intuition.

---

## 1. What SpotOps Is

A broadcast revenue management system for independent/multicultural TV stations.
It processes commercial log exports from Etere (traffic system), normalizes
customer data, and provides revenue intelligence across AEs, markets, languages,
and time periods.

Core pipeline: Etere Excel export → daily import → spots table → normalized
views → dashboards/reports.

The database is the canonical source of truth. QuickBooks handles AR/collections.
Nielsen handles ratings. SpotOps handles everything between booking and billing.

---

## 2. Critical SQLite Behaviors — Read First

These cause silent bugs if ignored.

- **Foreign keys are OFF by default.** Every connection must execute
  `PRAGMA foreign_keys = ON` or FK constraints are not enforced.

- **`broadcast_month` is TEXT in `Mmm-YY` format** (e.g., `Jan-24`, `Nov-25`).
  It sorts alphabetically, NOT chronologically. Always convert to a sortable
  form for ORDER BY. Use the canonical CASE/WHEN pattern from `v_planning_data`:
  ```sql
  CASE SUBSTR(broadcast_month, 1, 3)
      WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
      WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
      WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
      WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
  END
  ```

- **Dates are ISO 8601 TEXT** (`YYYY-MM-DD`), never Unix timestamps.

- **Air times are TEXT** (`HH:MM:SS`), not integers or time objects.

- **`modified_at` / `updated_date` are application-managed**, not trigger-managed.
  If you write a migration that bypasses the DAL, update these columns manually.

- **Triggers validate `broadcast_month` format** on INSERT and UPDATE to spots.
  Any value not matching `___-__` will ABORT. Do not attempt to insert
  partial or reformatted month values.

---

## 3. Database Access Pattern

Always use the container pattern. Never instantiate a raw connection.

```python
db = container.get("database_connection")
with db.connection() as conn:
    # your query here
```

---

## 4. The `bill_code` / Agency:Customer Pattern

`bill_code` in the spots table is a raw string from Etere. It encodes agency
and customer identity as a compound key. This is the most common source of
data quality issues.

**Parsing rules:**

```
bill_code contains no ":"   → customer = bill_code, agency = NULL (direct buy)
bill_code = "Direct:X"      → customer = X, agency = NULL (direct buy)
bill_code = "Agency:Customer" → agency = Agency, customer = Customer
bill_code = "A1:A2:Customer"  → agency1 = A1, agency2 = A2, customer = Customer
                                 (double-agency — rare but real)
```

**Normalization rules:**
- Strip leading/trailing whitespace before any comparison or grouping
- Normalize to Title Case
- Apply `agency_canonical_map` for agency name resolution
- Apply `customer_canonical_map` for customer name resolution
- Strip known suffixes: ` PRODUCTION`, ` PROD`, `- PRODUCTION`, `- PROD`

**Key constraint:** `"Admerasia:McDonald's"` and `"OtherAgency:McDonald's"` are
**different customers** with different `customer_id` values. Agency is part of
identity. Do not merge them.

**Canonical views to use instead of raw logic:**
- `v_normalized_candidates` — parses and normalizes any raw bill_code input
- `v_customer_normalization_audit` — full audit of raw→normalized→customer_id
  resolution across all spots

Never reimplement this parsing logic inline. Use the views.

---

## 5. Revenue Rules

**Recognition:** Revenue is recognized on `air_date`, not booking date or
invoice date.

**Exclusions:** Always exclude Trade revenue unless explicitly requested.
The standard filter is:
```sql
WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
```
This filter is present in all reporting views. If querying spots directly,
apply it manually.

**Rate logic by market type:**
- OTA markets: gross rate minus 15% agency commission = net
- Cable markets: rates may already be net — do not double-deduct commission
- Political pricing rules apply to OTA only — never apply to cable inventory

**The two revenue fields:**
- `gross_rate` — billed amount before agency commission
- `station_net` — what the station actually keeps

Use `gross_rate` for top-line revenue reports. Use `station_net` for margin
and profitability analysis.

---

## 6. Broadcast Month and Period Filtering

**Closed months** are locked in the `month_closures` table. Once closed, a
month's spot data is immutable — the import system will not overwrite it.

**Open months** are still receiving data from daily log imports. They are
noisy and incomplete.

**Critical YoY rule:** Never compare an open month against a closed prior-year
month in year-over-year calculations. The open month is a partial snapshot.
This produces systematically misleading deltas. Always filter to closed months
for any comparative analysis unless the user explicitly asks for current-month
data.

**Period selection:** Prefer filtering by `broadcast_month` values over date
ranges. Date-range filtering on `air_date` can produce different results than
month-based filtering when spots span month boundaries, and it bypasses the
closed-month protection logic.

---

## 7. The `is_historical` Flag

Spots imported as forward-looking placeholders (from pre-log order data) are
marked `is_historical = 1`. These represent contracted but not-yet-aired
inventory.

- Exclude from revenue actuals: `WHERE is_historical = 0`
- Include for pipeline/forecast views
- The import system sets this flag — do not modify it in ad hoc queries

---

## 8. Import System Behavior

- Daily imports process ~7,500 records from Etere Excel exports
- Etere may contain **duplicate rows** for rescheduled spots — last
  `modified_at` timestamp wins
- **Month preservation:** The importer will not delete data for closed months,
  even if they fall outside the current feed window. Closed = locked.
- **Previous month filter:** The importer ignores operational noise (last-minute
  edits, carry-over artifacts) from the currently open broadcast month on
  non-final runs
- Make-good spots may arrive without a `makegood` flag — identify by matching
  against a corresponding missed affidavit record
- Pre-empted spots (`preempted` in Etere) must never appear in revenue totals;
  they belong in reconciliation reports only

**Import batch tracking:** Every import run creates a record in `import_batches`.
The `spots.import_batch_id` FK links spots to their originating batch. Use this
for tracing data provenance.

---

## 9. Affidavit Reconciliation States

| State | Meaning | Action |
|---|---|---|
| Matched | Order + confirmed affidavit | Billable — proceed to invoice |
| Unmatched order | Booked, no affidavit | Investigate — likely pre-empted |
| Unmatched affidavit | Aired, no order | Bonus spot or data entry error |

Air time tolerance for matching: ±2 minutes. Traffic systems are imprecise.

---

## 10. Key Views — Use These, Don't Reimplement

| View | Purpose |
|---|---|
| `spots_reporting` | Primary reporting view — spots with all dimension joins, Trade excluded |
| `v_normalized_candidates` | Parse and normalize any raw bill_code |
| `v_customer_normalization_audit` | Full resolution audit: raw → normalized → customer_id |
| `v_planning_data` | AE-level budget / forecast / booked / pipeline by month |
| `v_unmatched_revenue` | Spots with no matching revenue_entity (AE assignment gaps) |
| `language_block_revenue_analysis` | Revenue by language block with customer intent |
| `spots_with_language_blocks_enhanced` | Spots joined to programming grid with intent classification |

When a view exists for the task, use it. Do not rebuild its logic in a
one-off query unless the view is demonstrably insufficient for the specific
need.

---

## 11. Customer Intent Classification

Spots are classified by why a customer chose their placement:

| Intent | Meaning |
|---|---|
| `language_specific` | Customer wanted this language block specifically |
| `time_specific` | Customer wanted this time slot regardless of language |
| `indifferent` | Customer had no placement preference |
| `no_grid_coverage` | Spot aired outside any defined programming grid |

This is stored in `spot_language_blocks.customer_intent`. It drives multicultural
analytics — which advertisers are buying language audiences vs. just buying
dayparts.

---

## 12. System Boundaries

| Domain | System | Integration |
|---|---|---|
| Traffic / scheduling | Etere | Excel export → daily import |
| Accounts receivable | QuickBooks | Manual — SpotOps does not push invoices |
| Audience ratings | Nielsen | Negotiated offline — not in DB |
| Political ad compliance | Separate tracking | SpotOps tracks revenue only |
| Programmatic/digital | Separate system | No data exchange |

Future: BXF (SMPTE ST 2021) is the target format for native Etere EDI
integration. Not implemented. Do not build against it yet.

---

## 13. Architecture Reminders

- Clean architecture: repositories for data access, services for business logic,
  no cross-layer leakage
- Dependency injection via service container — never instantiate DB connections
  or services directly
- Frozen dataclasses for domain models — treat as immutable
- Bulk query strategy: fixed number of queries regardless of entity count.
  Never N+1.
- When adding analytics: query existing views before writing new SQL.
  The views encode business rules that are easy to violate in ad hoc queries.

  For operational failure patterns and deployment gotchas, see ./tasks/lessons_learned.md