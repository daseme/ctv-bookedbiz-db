# Customer Name Matching & Review Tool

## Scope
This tool identifies and normalizes customer names from `spots.bill_code` and matches them to existing `customers` or `entity_aliases` using **blocking keys** + **RapidFuzz token scoring**.

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
