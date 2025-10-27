#!/usr/bin/env python3
"""
Language Assignment CLI
========================

Purpose
-------
This tool assigns, validates, and reviews language codes for advertising spots
in the production SQLite database. It supports categorization, algorithmic
assignments, default English assignment, manual review flagging, and status
reporting. It is intended for operations teams maintaining the
`spot_language_assignments` table in sync with `spots`.

Core Workflow
-------------
1. Categorization:
   - Analyze uncategorized spots and label them:
       * LANGUAGE_ASSIGNMENT_REQUIRED — needs algorithmic detection
       * REVIEW_CATEGORY — requires manual review
       * DEFAULT_ENGLISH — should default to English
   - Categories stored in `spots.spot_category`.

2. Processing:
   - Assign actual language codes and review flags based on category.
   - Update `spot_language_assignments`.

3. Review:
   - List spots needing manual review (invalid codes, undetermined `L` codes, high-value undetermined).

4. Recategorization:
   - Force recategorize all spots if rules change (`--force-recategorize-all`).

Database Connection
-------------------
By default connects to:
    data/database/production.db
Override with:
    --database path/to/file.db

SQLite is opened with:
    * WAL mode
    * busy_timeout=5000 ms
    * foreign_keys=ON

Flags & Commands
----------------
Status & Inspection:
    --status                  Show overall review/assignment counts.
    --status-by-category      Show count of spots per category.
    --processing-status       Show processing progress per category.
    --undetermined            List spots with undetermined 'L' language code.
    --review-required         Show summary counts of review-required spots.
    --all-review              List detailed review-required spots.
    --invalid-codes           List invalid language codes (not in `languages` table).
    --uncategorized           Show count of uncategorized spots.

Categorization:
    --categorize-all          Categorize all uncategorized spots.
    --test-categorization N   Categorize N uncategorized spots (no save).
    --force-recategorize-all  Clear and re-categorize all spots.
                              Use --yes to skip confirmation.

Processing:
    --process-language-required  Assign language to LANGUAGE_ASSIGNMENT_REQUIRED spots.
    --process-review-category    Flag REVIEW_CATEGORY spots for manual review.
    --process-default-english    Assign English to DEFAULT_ENGLISH spots.
    --process-all-categories     Process all categories via orchestrator.
    --process-all-remaining      Simple: process review then default-English.

Assignment:
    --test N                  Test-run assignments on N unassigned spots (no save).
    --batch N                 Assign language to N unassigned spots (save).
    --all                     Assign all unassigned spots (save).
                              Use --yes to skip confirmation.

Global Flags:
    --database PATH           SQLite database path (default: production.db).
    --yes                     Skip prompts for destructive actions (batch/all/force-recategorize).

Examples
--------
# Categorize all uncategorized spots
python assign_languages.py --categorize-all

# Assign all unassigned spots (skip confirmation)
python assign_languages.py --all --yes

# Process all categories with orchestrator logic
python assign_languages.py --process-all-categories

# See invalid language codes
python assign_languages.py --invalid-codes

# Show review-required counts
python assign_languages.py --review-required

Notes
-----
- Trade revenue spots (`revenue_type='Trade'`) are excluded from processing.
- Review flags are set only during processing, not during categorization.
- Forcing recategorization overwrites all existing category labels.

"""

#!/usr/bin/env python3
"""
Language Assignment CLI
(see --help for full usage, commands, and examples)
"""
import argparse
import sqlite3
import logging
from typing import Dict, Any
from pathlib import Path
import sys

from tqdm import tqdm
import sys


def _make_cli_pbar(total: int, desc: str):
    return tqdm(
        total=total,
        desc=desc,
        unit="spot",
        dynamic_ncols=True,
        mininterval=0.3,
        disable=not sys.stderr.isatty(),
    )


# ----- import bootstrap (repo_root + src) -----
HERE = Path(__file__).resolve()
REPO_ROOT = HERE.parent.parent  # repo_root
SRC = REPO_ROOT / "src"  # repo_root/src

# Make both repo_root and src importable
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# ----- end bootstrap -----

from src.services.spot_categorization_service import SpotCategorizationService
from src.models.spot_category import SpotCategory
from src.services.language_processing_orchestrator import LanguageProcessingOrchestrator
from src.services.language_assignment_service import LanguageAssignmentService

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def open_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ---------- CLI actions ----------


def force_recategorize_all_spots(conn: sqlite3.Connection, assume_yes: bool) -> None:
    print("\n🔄 FORCE RECATEGORIZING ALL SPOTS...")
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM spots WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)"
    )
    total = cur.fetchone()[0]
    print(f"Found {total:,} spots to recategorize...")

    if not assume_yes:
        confirm = (
            input(
                f"\n⚠️  This will RECATEGORIZE ALL {total:,} spots.\nProceed? (yes/no): "
            )
            .strip()
            .lower()
        )
        if confirm not in ("yes", "y"):
            print("❌ Force recategorization cancelled")
            return

    print("Clearing existing categorizations...")
    cur.execute(
        "UPDATE spots SET spot_category = NULL WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)"
    )
    try:
        cur.execute("DELETE FROM spot_categorizations")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    print("✅ Existing categorizations cleared")

    print("Starting recategorization...")
    svc = SpotCategorizationService(conn)
    results = svc.categorize_all_uncategorized(batch_size=5000)

    print(f"\n🎉 FORCE RECATEGORIZATION COMPLETE:")
    print(f"   • Total spots processed: {results.get('processed', 0):,}")
    print(f"   • Successfully categorized: {results.get('categorized', 0):,}")
    show_status_by_category(conn)


def show_undetermined_spots(service: LanguageAssignmentService) -> None:
    """Show spots with undetermined language (L code)."""
    review_spots = service.get_review_required_spots(limit=200)
    undetermined = [s for s in review_spots if getattr(s, "language_code", None) == "L"]

    print(f"\n🚨 UNDETERMINED LANGUAGE SPOTS (L code, showing first 20):")
    print(f"{'Spot ID':>8} {'Bill Code':>18} {'Revenue':>10} {'Notes'}")
    print("-" * 70)

    if not undetermined:
        print("   No undetermined language spots found.")
        return

    for a in undetermined[:20]:
        sd = service.queries.get_spot_language_data(a.spot_id)
        revenue = f"${sd.gross_rate:,.0f}" if sd and sd.get("gross_rate") else "N/A"
        bill_code = (sd.get("bill_code", "N/A")[:18]) if sd else "N/A"
        print(f"{a.spot_id:>8} {bill_code:>18} {revenue:>10} {getattr(a, 'notes', '')}")


def show_review_required(service: LanguageAssignmentService) -> None:
    summary: Dict[str, Any] = service.get_review_summary()
    print(f"\n📋 SPOTS REQUIRING MANUAL REVIEW:")
    print(
        f"   • Undetermined language (L code): {summary.get('undetermined_language', 0):,}"
    )
    print(f"   • Invalid language codes: {summary.get('invalid_codes', 0):,}")
    print(
        f"   • High-value undetermined: {summary.get('high_value_undetermined', 0):,}"
    )
    print(f"   • Total requiring review: {summary.get('total_review_required', 0):,}")


def show_all_review_required_spots(service: LanguageAssignmentService) -> None:
    review_spots = service.get_review_required_spots(limit=200)
    print(f"\n🔍 ALL SPOTS REQUIRING REVIEW (showing first 50):")
    print(
        f"{'Spot ID':>8} {'Code':>6} {'Bill Code':>18} {'Revenue':>10} {'Status':>14} {'Reason'}"
    )
    print("-" * 100)

    if not review_spots:
        print("   No spots requiring review found.")
        return

    for a in review_spots[:50]:
        sd = service.queries.get_spot_language_data(a.spot_id)
        revenue = f"${sd.gross_rate:,.0f}" if sd and sd.get("gross_rate") else "N/A"
        bill_code = (sd.get("bill_code", "N/A")[:18]) if sd else "N/A"
        status_val = getattr(a.language_status, "value", a.language_status)
        reason = (
            "Undetermined" if getattr(a, "language_code", "") == "L" else "Invalid Code"
        )
        print(
            f"{a.spot_id:>8} {getattr(a, 'language_code', ''):>6} {bill_code:>18} {revenue:>10} {status_val:>14} {reason}"
        )


def show_assignment_status(service: LanguageAssignmentService) -> None:
    s = service.get_review_summary()
    print(f"\n📊 LANGUAGE ASSIGNMENT STATUS:")
    print(
        f"   • Spots needing language determination (L): {s.get('undetermined_language', 0):,}"
    )
    print(f"   • Spots with invalid language codes: {s.get('invalid_codes', 0):,}")
    print(
        f"   • High-value undetermined spots: {s.get('high_value_undetermined', 0):,}"
    )
    print(f"   • Total spots requiring review: {s.get('total_review_required', 0):,}")


def test_assignments(service: LanguageAssignmentService, count: int) -> None:
    print(f"\n🧪 TESTING assignment with {count} spots...")
    unassigned = service.queries.get_unassigned_spots(limit=count)
    if not unassigned:
        print("❌ No unassigned spots found for testing!")
        return

    print(f"Found {len(unassigned)} unassigned spots to test...")
    results = {}
    if count > 100:
        tqdm.write("Processing assignments...")
        for spot in tqdm(unassigned, desc="Testing spots", unit="spot"):
            results.update(service.batch_assign_languages([spot]))
    else:
        results = service.batch_assign_languages(unassigned)

    status_counts: Dict[str, int] = {}
    review_required = 0
    for a in results.values():
        status_val = getattr(a.language_status, "value", a.language_status)
        status_counts[status_val] = status_counts.get(status_val, 0) + 1
        if getattr(a, "requires_review", False):
            review_required += 1

    print(f"\n📊 TEST RESULTS:")
    print(f"   • Total spots tested: {len(results)}")
    print(f"   • Requiring review: {review_required}")
    for k, v in status_counts.items():
        print(f"   • {str(k).title()}: {v}")


def show_invalid_codes(service: LanguageAssignmentService) -> None:
    cur = service.db.cursor()
    cur.execute("""
        SELECT s.language_code, COUNT(*) AS spot_count, 
               SUM(s.gross_rate) AS total_revenue,
               AVG(s.gross_rate) AS avg_revenue
        FROM spots s
        LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)
        WHERE s.language_code IS NOT NULL 
          AND s.language_code != 'L'
          AND l.language_id IS NULL
          AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        GROUP BY s.language_code
        ORDER BY spot_count DESC;
    """)
    rows = cur.fetchall()
    if not rows:
        print("\n✅ No invalid language codes found!")
        return

    print(f"\n🚨 INVALID LANGUAGE CODES (not in languages table):")
    print(f"{'Code':>6} {'Count':>8} {'Total Revenue':>15} {'Avg Revenue':>12}")
    print("-" * 50)
    for code, count, total_rev, avg_rev in rows:
        total_str = f"${(total_rev or 0):,.0f}"
        avg_str = f"${(avg_rev or 0):,.0f}"
        print(f"{code:>6} {count:>8,} {total_str:>15} {avg_str:>12}")


def batch_assign(service, count: int) -> None:
    print(f"\n🚀 BATCH ASSIGNMENT of {count} spots...")
    ids = service.queries.get_unassigned_spots(limit=count)
    if not ids:
        print("✅ No unassigned spots found!")
        return
    print(f"Found {len(ids)} unassigned spots to process...")

    results = {}
    with _make_cli_pbar(len(ids), "Assigning") as pbar:
        for sid in ids:
            a = service.assign_spot_language(sid)
            results[sid] = a
            try:
                service.queries.save_language_assignment(a)
            except Exception as e:
                tqdm.write(f"Save error on spot {sid}: {e}")
            pbar.update(1)

    print(f"\n📊 BATCH RESULTS:")
    print(f"   • Processed: {len(results)}")


def assign_all(service, assume_yes: bool) -> None:
    print(f"\n🎯 ASSIGNING ALL UNASSIGNED SPOTS...")
    ids = service.queries.get_unassigned_spots()
    if not ids:
        print("✅ All spots are already assigned!")
        return
    print(f"Found {len(ids):,} unassigned spots")
    if not assume_yes:
        confirm = input("Proceed with assignment? (yes/no): ").strip().lower()
        if confirm not in ("yes", "y"):
            print("❌ Assignment cancelled")
            return

    saved = errors = 0
    with _make_cli_pbar(len(ids), "Assigning all") as pbar:
        for sid in ids:
            a = service.assign_spot_language(sid)
            try:
                service.queries.save_language_assignment(a)
                saved += 1
            except Exception as e:
                tqdm.write(f"Save error on spot {sid}: {e}")
                errors += 1
            pbar.set_postfix(saved=saved, errors=errors)
            pbar.update(1)

    print(f"\n🎉 ALL ASSIGNMENTS COMPLETE:")
    print(f"   • Successfully saved: {saved:,}")
    print(f"   • Errors: {errors:,}")


def categorize_all_spots(conn: sqlite3.Connection) -> None:
    print(f"\n🏷️  CATEGORIZING ALL UNCATEGORIZED SPOTS...")
    svc = SpotCategorizationService(conn)
    results = svc.categorize_all_uncategorized(batch_size=5000)
    print(f"\n✅ CATEGORIZATION COMPLETE:")
    print(f"   • Processed: {results.get('processed', 0):,}")
    print(f"   • Categorized: {results.get('categorized', 0):,}")


def show_status_by_category(conn: sqlite3.Connection) -> None:
    svc = SpotCategorizationService(conn)
    summary = svc.get_category_summary()
    print(f"\n📊 SPOTS BY CATEGORY:")
    total = 0
    for category in SpotCategory:
        count = int(summary.get(category.value, 0))
        total += count
        print(f"   • {category.value.replace('_', ' ').title()}: {count:,}")
    uncategorized = int(summary.get("uncategorized", 0))
    total += uncategorized
    print(f"   • Uncategorized: {uncategorized:,}")
    print(f"   • Total: {total:,}")


def test_categorization(conn: sqlite3.Connection, count: int) -> None:
    print(f"\n🧪 TESTING CATEGORIZATION with {count} spots...")
    svc = SpotCategorizationService(conn)
    sample = svc.get_uncategorized_spots(limit=count)
    if not sample:
        print("❌ No uncategorized spots found for testing!")
        return

    print(f"Found {len(sample)} uncategorized spots to test...")
    cats = svc.categorize_spots_batch(sample)  # no save
    counts: Dict[str, int] = {}
    for c in cats.values():
        counts[c.value] = counts.get(c.value, 0) + 1

    print(f"\n📊 TEST RESULTS:")
    for name, n in counts.items():
        print(f"   • {name.replace('_', ' ').title()}: {n}")


def show_uncategorized_count(conn: sqlite3.Connection) -> None:
    svc = SpotCategorizationService(conn)
    spots = svc.get_uncategorized_spots()
    print(f"\n📋 UNCATEGORIZED SPOTS: {len(spots):,}")
    if spots:
        print("💡 Run --categorize-all to categorize all spots")


def process_language_required(conn: sqlite3.Connection) -> None:
    print(f"\n🎯 PROCESSING LANGUAGE ASSIGNMENT REQUIRED SPOTS...")
    orch = LanguageProcessingOrchestrator(conn)
    res = orch.process_language_required_category()
    print(f"\n✅ LANGUAGE ASSIGNMENT COMPLETE:")
    print(f"   • Processed: {res.get('processed', 0):,}")
    print(f"   • Successfully assigned: {res.get('assigned', 0):,}")
    print(f"   • Flagged for review: {res.get('review_flagged', 0):,}")
    print(f"   • Errors: {res.get('errors', 0):,}")


def process_review_category(conn: sqlite3.Connection) -> None:
    print(f"\n📋 PROCESSING REVIEW CATEGORY SPOTS...")
    orch = LanguageProcessingOrchestrator(conn)
    res = orch.process_review_category()
    print(f"\n✅ REVIEW CATEGORY PROCESSING COMPLETE:")
    print(f"   • Processed: {res.get('processed', 0):,}")
    print(f"   • Flagged for review: {res.get('flagged_for_review', 0):,}")
    print(f"   • Errors: {res.get('errors', 0):,}")


def process_default_english(conn: sqlite3.Connection) -> None:
    print(f"\n🇺🇸 PROCESSING DEFAULT ENGLISH SPOTS...")
    orch = LanguageProcessingOrchestrator(conn)
    res = orch.process_default_english_category()
    print(f"\n✅ DEFAULT ENGLISH PROCESSING COMPLETE:")
    print(f"   • Processed: {res.get('processed', 0):,}")
    print(f"   • Assigned to English: {res.get('assigned', 0):,}")
    print(f"   • Errors: {res.get('errors', 0):,}")


def process_all_categories(conn: sqlite3.Connection) -> None:
    print(f"\n🚀 PROCESSING ALL CATEGORIES...")
    orch = LanguageProcessingOrchestrator(conn)
    res = orch.process_all_categories()
    s = res.get("summary", {})
    print(f"\n🎉 ALL CATEGORIES PROCESSING COMPLETE:")
    print(f"   • Total processed: {s.get('total_processed', 0):,}")
    print(f"   • Language assigned: {s.get('language_assigned', 0):,}")
    print(f"   • Default English assigned: {s.get('default_english_assigned', 0):,}")
    print(f"   • Flagged for review: {s.get('flagged_for_review', 0):,}")
    print(f"   • Total errors: {s.get('total_errors', 0):,}")


def show_processing_status(conn: sqlite3.Connection) -> None:
    orch = LanguageProcessingOrchestrator(conn)
    status = orch.get_processing_status()
    print(f"\n📊 PROCESSING STATUS BY CATEGORY:")
    for cat in SpotCategory:
        key = cat.value
        total = int(status.get(f"{key}_total", 0))
        processed = int(status.get(f"{key}_processed", 0))
        if total > 0:
            pct = processed / total * 100
            print(
                f"   • {key.replace('_', ' ').title()}: {processed:,}/{total:,} ({pct:.1f}%)"
            )


def process_all_categories_simple(conn: sqlite3.Connection) -> None:
    """Simple: review then default-English; prints a final rollup."""
    process_review_category(conn)
    process_default_english(conn)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM spot_language_assignments")
    total = int(cur.fetchone()[0])
    cur.execute(
        "SELECT COUNT(*) FROM spot_language_assignments WHERE requires_review = 1"
    )
    needs_review = int(cur.fetchone()[0])
    print(f"\n🎉 ALL PROCESSING COMPLETE:")
    print(f"   • Total spots processed: {total:,}")
    print(f"   • Spots requiring review: {needs_review:,}")
    print(f"   • Spots with language assigned: {total - needs_review:,}")


# ---------- main ----------


def main() -> int:
    import __main__

    p = argparse.ArgumentParser(
        description=__main__.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--database", default="data/database/production.db")
    p.add_argument(
        "--yes", action="store_true", help="Assume 'yes' for destructive prompts"
    )

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--test", type=int, help="Test with N spots")
    g.add_argument("--batch", type=int, help="Assign N spots")
    g.add_argument("--all", action="store_true", help="Assign all unassigned spots")
    g.add_argument("--status", action="store_true", help="Show assignment status")
    g.add_argument(
        "--undetermined",
        action="store_true",
        help="Show undetermined language spots (L code)",
    )
    g.add_argument(
        "--review-required", action="store_true", help="Show review required summary"
    )
    g.add_argument(
        "--all-review", action="store_true", help="Show all spots requiring review"
    )
    g.add_argument(
        "--invalid-codes", action="store_true", help="Show invalid language codes"
    )
    g.add_argument(
        "--categorize-all",
        action="store_true",
        help="Categorize all uncategorized spots",
    )
    g.add_argument(
        "--force-recategorize-all",
        action="store_true",
        help="Force recategorize ALL spots",
    )
    g.add_argument(
        "--status-by-category", action="store_true", help="Show breakdown by category"
    )
    g.add_argument(
        "--test-categorization", type=int, help="Test categorization with N spots"
    )
    g.add_argument(
        "--uncategorized", action="store_true", help="Show uncategorized spot count"
    )
    g.add_argument(
        "--process-language-required",
        action="store_true",
        help="Process language assignment required spots",
    )
    g.add_argument(
        "--process-review-category",
        action="store_true",
        help="Process review category spots",
    )
    g.add_argument(
        "--process-default-english",
        action="store_true",
        help="Process default English spots",
    )
    g.add_argument(
        "--process-all-categories", action="store_true", help="Process all categories"
    )
    g.add_argument(
        "--process-all-remaining",
        action="store_true",
        help="Process all remaining categories (simple)",
    )
    g.add_argument(
        "--processing-status",
        action="store_true",
        help="Show processing status by category",
    )

    args = p.parse_args()

    try:
        with open_sqlite(args.database) as conn:
            service = LanguageAssignmentService(conn)

            if args.status:
                show_assignment_status(service)
            elif args.undetermined:
                show_undetermined_spots(service)
            elif args.review_required:
                show_review_required(service)
            elif args.all_review:
                show_all_review_required_spots(service)
            elif args.invalid_codes:
                show_invalid_codes(service)
            elif args.test:
                test_assignments(service, args.test)
            elif args.batch:
                batch_assign(service, args.batch)
            elif args.all:
                assign_all(service, assume_yes=args.yes)
            elif args.categorize_all:
                categorize_all_spots(conn)
            elif args.force_recategorize_all:
                force_recategorize_all_spots(conn, assume_yes=args.yes)
            elif args.status_by_category:
                show_status_by_category(conn)
            elif args.test_categorization:
                test_categorization(conn, args.test_categorization)
            elif args.uncategorized:
                show_uncategorized_count(conn)
            elif args.process_language_required:
                process_language_required(conn)
            elif args.process_review_category:
                process_review_category(conn)
            elif args.process_default_english:
                process_default_english(conn)
            elif args.process_all_categories:
                process_all_categories(conn)
            elif args.process_all_remaining:
                process_all_categories_simple(conn)
            elif args.processing_status:
                show_processing_status(conn)

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
