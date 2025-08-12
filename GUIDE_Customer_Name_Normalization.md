# Customer Name Matching & Review Tool

## Scope
This tool identifies and normalizes customer names from `spots.bill_code` and matches them to existing `customers` or `entity_aliases` using **blocking keys** + **RapidFuzz token scoring**.

What it does: Aggregates revenue to the canonical customer using your approved aliases in entity_aliases.

Key: We join spots → (billcode_customer_map optional) → entity_aliases → customers.

    If an alias exists, we use entity_aliases.target_entity_id.

    If no alias but the extracted name already equals a customers.normalized_name, we use that.

    Otherwise the row is tagged [UNRESOLVED] so you can triage.

billcode_customer_map (optional): A tiny helper mapping of bill_code → extracted_name produced by the analyzer (same raw string you reviewed in the UI). It ensures the SQL sees the same extracted text you approved, without trying to re-parse bill_code in SQL. If you don’t have this table yet, create it and populate it from the analyzer’s distinct bill codes + extracted names.


It supports:
- **Exact & alias matches** (via DB lookup)
- **High-confidence & review candidates** (fuzzy match)
- **Unknowns** (no viable match)
- **Review queue** with a simple approval UI to create aliases safely

**Goals:** improve match rates, catch typos/variants, reduce manual cleanup.

---

## Components

| File | Purpose |
|------|---------|
| `src/services/customer_matching/normalization.py` | Shared normalization & bill_code parsing logic |
| `src/services/customer_matching/blocking_matcher.py` | Core analyzer (blocking + fuzzy matching) |
| `src/cli/customer_names.py` | CLI wrapper to run the analyzer |
| `src/database/migrations/001_review_queue.sql` | Creates `customer_match_review` table & indexes |
| `scripts/load_review_queue.py` | Batch job to populate review queue |
| `src/web/review_ui/app.py` | Flask UI to approve/reject matches & create aliases |

---

## Quickstart

1. Analyze customer names (CLI):
python -m src.cli.customer_names --db-path data/database/production.db \
  --export-unmatched --suggest-aliases


2. Load review queue (batch):
python scripts/load_review_queue.py --db data/database/production.db --auto-approve


3. Start review UI:
export DB_PATH=data/database/production.db
export APP_PIN=1234
python -m src.web.review_ui.app --host 0.0.0.0 --port 5088

Match Statuses

| Status            | Meaning                                    |
| ----------------- | ------------------------------------------ |
| `exact`           | Exact match to `customers.normalized_name` |
| `alias`           | Direct match to `entity_aliases`           |
| `high_confidence` | Fuzzy score ≥ 0.92 & revenue ≥ \$2k        |
| `review`          | Fuzzy score ≥ 0.80 but < high confidence   |
| `unknown`         | No match or low score                      |

Recommendations

    Always run load_review_queue.py after new data loads.

    Use the review UI for alias creation; avoids direct SQL editing.

    Keep normalization.py as the single source of truth for name cleaning.

    Install dependencies:
    pip install rapidfuzz metaphone Unidecode flask


    Queries:

    Customer Totals

    -- Requires: entity_aliases (customer aliases you approved)
-- Optional but recommended: billcode_customer_map(bill_code TEXT PRIMARY KEY, alias_name TEXT NOT NULL)
--   -> alias_name = the extracted client string you reviewed (same as in the UI)

WITH base AS (
  SELECT
    s.station_net,
    s.bill_code,
    COALESCE(m.alias_name, s.bill_code) AS extracted_name  -- fallback if map not populated
  FROM spots s
  LEFT JOIN billcode_customer_map m
    ON m.bill_code = s.bill_code
),
resolved AS (
  SELECT
    -- Resolve to canonical customer via alias first, then direct name match
    COALESCE(c_by_alias.customer_id, c_by_name.customer_id)    AS customer_id,
    COALESCE(c_by_alias.normalized_name, c_by_name.normalized_name,
             '[UNRESOLVED]')                                   AS customer_name,
    b.station_net
  FROM base b
  -- 1) alias path (preferred)
  LEFT JOIN entity_aliases ea
    ON ea.entity_type = 'customer'
   AND ea.is_active = 1
   AND ea.alias_name = b.extracted_name
  LEFT JOIN customers c_by_alias
    ON c_by_alias.customer_id = ea.target_entity_id
  -- 2) direct match fallback (for exact names already in customers)
  LEFT JOIN customers c_by_name
    ON c_by_name.normalized_name = b.extracted_name
)
SELECT
  customer_id,
  customer_name,
  SUM(station_net) AS total_revenue
FROM resolved
GROUP BY customer_id, customer_name
ORDER BY total_revenue DESC;


Query (customer × month)

WITH base AS (
  SELECT
    s.station_net,
    s.bill_code,
    s.broadcast_month,
    COALESCE(m.alias_name, s.bill_code) AS extracted_name
  FROM spots s
  LEFT JOIN billcode_customer_map m
    ON m.bill_code = s.bill_code
),
resolved AS (
  SELECT
    COALESCE(c_by_alias.customer_id, c_by_name.customer_id)    AS customer_id,
    COALESCE(c_by_alias.normalized_name, c_by_name.normalized_name,
             '[UNRESOLVED]')                                   AS customer_name,
    b.broadcast_month,
    b.station_net
  FROM base b
  LEFT JOIN entity_aliases ea
    ON ea.entity_type = 'customer'
   AND ea.is_active = 1
   AND ea.alias_name = b.extracted_name
  LEFT JOIN customers c_by_alias
    ON c_by_alias.customer_id = ea.target_entity_id
  LEFT JOIN customers c_by_name
    ON c_by_name.normalized_name = b.extracted_name
)
SELECT
  customer_id,
  customer_name,
  broadcast_month,
  SUM(station_net) AS total_revenue
FROM resolved
GROUP BY customer_id, customer_name, broadcast_month
ORDER BY broadcast_month, total_revenue DESC;


AGENCY NEXT PROJECT:

# Agency Normalization – Developer Kickoff

## Current State
- **Bill codes** often have the format: `agency_name:customer_name`.
- Our current customer matching pipeline (`src/services/customer_matching/normalization.py`) now includes:
  - `extract_billcode_parts(...) -> (agency_raw, customer_raw)`
  - `extract_customer_from_bill_code(...)` for backward compatibility.
- Customer matching **only** normalizes the **customer** segment. Agency info is preserved **raw**.

## Goal for Agency Normalization Tool
- Build a parallel tool to:
  1. **Normalize agency names** consistently (casefold, punctuation removal, business suffix cleanup, etc.).
  2. Maintain a mapping of raw → normalized agencies.
  3. Detect alias/variant agencies (e.g., `H&L Agency Co` vs. `H and L Agency Company`).
  4. Provide match statistics, CSV exports, and alias suggestions (similar to customer tool).
  5. Optionally store canonical agencies in an `agencies` table with an `entity_aliases` entry for agency type.

## Schema Considerations
- Likely need:
  - `agencies` table (id, normalized_name, is_active, etc.).
  - `entity_aliases` can be reused by setting `entity_type = 'agency'`.
  - Optionally, `agency_id` foreign key in `spots` (if you want direct joins later).
- Migration would follow the pattern of `001_review_queue.sql` for customer review.

## Approach
- Create `src/services/agency_matching/normalization.py` (start from customer normalizer; adjust patterns for agency-specific suffixes like “Agency”, “Media”, “Partners”).
- Create `src/services/agency_matching/blocking_matcher.py` (clone customer blocking matcher; adapt to `agencies` table).
- Build CLI: `src/cli/agencies.py` (mirror `customer_names.py`).
- Add batch loader + review UI reusing the current review system, but filter by `entity_type='agency'`.

## Key Lessons from Customer Tool
- **Single normalization function** used everywhere.
- **Blocking keys** for performance on Pi.
- Use **RapidFuzz token scoring** for robustness.
- Store raw + normalized strings for auditability.
- Preserve original bill_code for reference.
- Keep analyzer read-only; writes go through controlled queue/approval UI.

---
**Next Steps:**
1. Copy `normalization.py` → `agency_matching/normalization.py` and update suffix/noise lists.
2. Implement blocking matcher for agencies.
3. Create migration for `agencies` table and `entity_aliases` support.
4. Hook into review queue & UI.

