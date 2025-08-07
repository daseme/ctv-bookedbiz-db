#!/usr/bin/env python3
"""
Simplified Language Assignment CLI
Assigns language codes based on spots.language_code column

OVERVIEW:
This CLI tool manages a two-step process for assigning language codes to advertising spots:

1. CATEGORIZATION: Analyzes spots and categorizes them based on business rules
   - LANGUAGE_ASSIGNMENT_REQUIRED: Spots that need algorithmic language detection
   - REVIEW_CATEGORY: Spots requiring manual business review
   - DEFAULT_ENGLISH: Spots that should default to English

2. PROCESSING: Processes each category to assign actual language codes and set review flags
   - Assigns specific language codes (EN, ES, etc.)
   - Flags spots requiring manual review
   - Updates spot_language_assignments table

TYPICAL WORKFLOW:

1. Initial Setup (run once):
   --categorize-all                 # Categorize all uncategorized spots
   --status-by-category            # Check category distribution

2. Process Categories:
   --process-language-required     # Process algorithmic assignments
   --process-default-english       # Assign English to appropriate spots
   --process-review-category       # Flag spots for business review
   
   OR use: --process-all-categories  # Process all categories at once

3. Review and Monitor:
   --status                        # Check assignment status
   --review-required              # See spots needing manual review
   --invalid-codes               # Check for invalid language codes

4. Force Recategorization (when rules change):
   --force-recategorize-all       # Recategorize entire dataset
   --process-all-categories       # Reprocess with new logic

TESTING OPTIONS:
--test N                          # Test assignment with N spots
--test-categorization N           # Test categorization with N spots

STATUS/INSPECTION:
--status                         # Overall assignment status
--status-by-category            # Breakdown by category
--processing-status             # Processing progress by category
--undetermined                  # Spots with 'L' language code
--all-review                    # All spots requiring review
--invalid-codes                 # Invalid language codes

EXAMPLES:
python cli_01_language_assignment.py --status-by-category
python cli_01_language_assignment.py --categorize-all
python cli_01_language_assignment.py --process-all-categories
python cli_01_language_assignment.py --review-required
python cli_01_language_assignment.py --force-recategorize-all

DATABASE:
Default: data/database/production.db
Override: --database path/to/database.db

IMPORTANT NOTES:
- Categorization determines WHICH spots get processed HOW
- Processing assigns actual language codes and review flags  
- Review flags are only updated during processing, not categorization
- Force recategorization clears ALL categories and starts fresh
- Trade revenue spots are excluded from processing
"""

import argparse
import sqlite3
import logging
import sys
import os
from tqdm import tqdm

# Add src directory to path FIRST (before other imports)
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# NOW import the modules (after path is set)
from src.services.spot_categorization_service import SpotCategorizationService
from src.models.spot_category import SpotCategory
from src.services.language_processing_orchestrator import LanguageProcessingOrchestrator
from services.language_assignment_service import LanguageAssignmentService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    parser = argparse.ArgumentParser(description="Language Assignment Tool")
    parser.add_argument("--database", default="data/database/production.db")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--test", type=int, help="Test with N spots")
    group.add_argument("--batch", type=int, help="Assign N spots") 
    group.add_argument("--all", action="store_true", help="Assign all unassigned spots")
    group.add_argument("--status", action="store_true", help="Show assignment status")
    group.add_argument("--undetermined", action="store_true", help="Show undetermined language spots (L code)")
    group.add_argument("--review-required", action="store_true", help="Show review required summary")
    group.add_argument("--all-review", action="store_true", help="Show all spots requiring review")
    group.add_argument("--invalid-codes", action="store_true", help="Show invalid language codes")
    group.add_argument("--categorize-all", action="store_true", help="Categorize all uncategorized spots")
    group.add_argument("--force-recategorize-all", action="store_true", help="Force recategorize ALL spots (including already categorized)")
    group.add_argument("--status-by-category", action="store_true", help="Show breakdown by category") 
    group.add_argument("--test-categorization", type=int, help="Test categorization with N spots")
    group.add_argument("--uncategorized", action="store_true", help="Show uncategorized spot count")
    group.add_argument("--process-language-required", action="store_true", help="Process language assignment required spots")
    group.add_argument("--process-review-category", action="store_true", help="Process review category spots")
    group.add_argument("--process-default-english", action="store_true", help="Process default English spots")
    group.add_argument("--process-all-categories", action="store_true", help="Process all categories")
    group.add_argument("--process-all-remaining", action="store_true", help="Process all remaining categories (simple)")
    group.add_argument("--processing-status", action="store_true", help="Show processing status by category")

    args = parser.parse_args()
    
    try:
        with sqlite3.connect(args.database) as conn:
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
                assign_all(service)
            elif args.categorize_all:
                categorize_all_spots(conn)
            elif args.force_recategorize_all:
                force_recategorize_all_spots(conn)
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

def force_recategorize_all_spots(conn):
    """Force recategorize ALL spots (including already categorized ones)"""
    print(f"\n🔄 FORCE RECATEGORIZING ALL SPOTS...")
    
    # Get total count first
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM spots WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)")
    total_count = cursor.fetchone()[0]
    
    print(f"Found {total_count:,} spots to recategorize...")
    
    # Confirm with user since this is destructive
    confirm = input(f"\n⚠️  This will RECATEGORIZE ALL {total_count:,} spots (overwriting existing categories).\nProceed? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("❌ Force recategorization cancelled")
        return
    
    # Step 1: Clear all existing categorizations to make them "uncategorized"
    print("Clearing existing categorizations...")
    cursor.execute("UPDATE spots SET spot_category = NULL WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)")
    
    # Also clear separate categorizations table if it exists
    try:
        cursor.execute("DELETE FROM spot_categorizations")
    except sqlite3.OperationalError:
        # Table doesn't exist, that's fine
        pass
    
    conn.commit()
    print("✅ Existing categorizations cleared")
    
    # Step 2: Use the existing working categorization method
    print("Starting recategorization...")
    service = SpotCategorizationService(conn)
    results = service.categorize_all_uncategorized(batch_size=5000)
    
    print(f"\n🎉 FORCE RECATEGORIZATION COMPLETE:")
    print(f"   • Total spots processed: {results['processed']:,}")
    print(f"   • Successfully categorized: {results['categorized']:,}")
    
    # Show new category distribution
    show_status_by_category(conn)

def show_undetermined_spots(service):
    """Show spots with undetermined language (L code) - now part of review required"""
    review_spots = service.get_review_required_spots(limit=20)
    
    # Filter for just L codes to maintain the specific undetermined view
    undetermined_spots = [spot for spot in review_spots if spot.language_code == 'L']
    
    print(f"\n🚨 UNDETERMINED LANGUAGE SPOTS (L code, showing first 20):")
    print(f"{'Spot ID':>8} {'Bill Code':>15} {'Revenue':>10} {'Notes'}")
    print("-" * 60)
    
    if not undetermined_spots:
        print("   No undetermined language spots found.")
        return
    
    for assignment in undetermined_spots[:20]:
        spot_data = service.queries.get_spot_language_data(assignment.spot_id)
        revenue = f"${spot_data.gross_rate:,.0f}" if spot_data and spot_data.gross_rate else "N/A"
        bill_code = spot_data.bill_code[:15] if spot_data and spot_data.bill_code else "N/A"
        
        print(f"{assignment.spot_id:>8} {bill_code:>15} {revenue:>10} {assignment.notes}")

def show_review_required(service):
    """Show summary of all spots requiring manual review"""
    summary = service.get_review_summary()
    
    print(f"\n📋 SPOTS REQUIRING MANUAL REVIEW:")
    print(f"   • Undetermined language (L code): {summary['undetermined_language']:,}")
    print(f"   • Invalid language codes: {summary['invalid_codes']:,}")
    print(f"   • High-value undetermined: {summary['high_value_undetermined']:,}")
    print(f"   • Total requiring review: {summary['total_review_required']:,}")

def show_all_review_required_spots(service):
    """Show all spots requiring review with details"""
    review_spots = service.get_review_required_spots(limit=50)
    
    print(f"\n🔍 ALL SPOTS REQUIRING REVIEW (showing first 50):")
    print(f"{'Spot ID':>8} {'Code':>6} {'Bill Code':>15} {'Revenue':>10} {'Status':>12} {'Reason'}")
    print("-" * 85)
    
    if not review_spots:
        print("   No spots requiring review found.")
        return
    
    for assignment in review_spots:
        spot_data = service.queries.get_spot_language_data(assignment.spot_id)
        revenue = f"${spot_data.gross_rate:,.0f}" if spot_data and spot_data.gross_rate else "N/A"
        bill_code = spot_data.bill_code[:15] if spot_data and spot_data.bill_code else "N/A"
        
        reason = "Undetermined" if assignment.language_code == 'L' else "Invalid Code"
        
        print(f"{assignment.spot_id:>8} {assignment.language_code:>6} {bill_code:>15} {revenue:>10} {assignment.language_status.value:>12} {reason}")

def show_assignment_status(service):
    """Show overall assignment status"""
    review_summary = service.get_review_summary()
    
    print(f"\n📊 LANGUAGE ASSIGNMENT STATUS:")
    print(f"   • Spots needing language determination (L): {review_summary['undetermined_language']:,}")
    print(f"   • Spots with invalid language codes: {review_summary['invalid_codes']:,}")
    print(f"   • High-value undetermined spots: {review_summary['high_value_undetermined']:,}")
    print(f"   • Total spots requiring review: {review_summary['total_review_required']:,}")

def test_assignments(service, count):
    """Test language assignment"""
    print(f"\n🧪 TESTING assignment with {count} spots...")
    
    unassigned_spots = service.queries.get_unassigned_spots(limit=count)
    if not unassigned_spots:
        print("❌ No unassigned spots found for testing!")
        return
    
    print(f"Found {len(unassigned_spots)} unassigned spots to test...")
    
    # Show progress if testing a significant number of spots
    if count > 100:
        tqdm.write("Processing assignments...")
        results = {}
        for spot in tqdm(unassigned_spots, desc="Testing spots", unit="spot"):
            batch_results = service.batch_assign_languages([spot])
            results.update(batch_results)
    else:
        results = service.batch_assign_languages(unassigned_spots)
    
    # Analyze results
    status_counts = {}
    review_required = 0
    
    for assignment in results.values():
        status = assignment.language_status.value
        status_counts[status] = status_counts.get(status, 0) + 1
        if assignment.requires_review:
            review_required += 1
    
    print(f"\n📊 TEST RESULTS:")
    print(f"   • Total spots tested: {len(results)}")
    print(f"   • Requiring review: {review_required}")
    for status, count in status_counts.items():
        print(f"   • {status.title()}: {count}")

def show_invalid_codes(service):
    """Show spots with invalid language codes (not in languages table)"""
    cursor = service.db.cursor()
    cursor.execute("""
        SELECT s.language_code, COUNT(*) as spot_count, 
               SUM(s.gross_rate) as total_revenue,
               AVG(s.gross_rate) as avg_revenue
        FROM spots s
        LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)
        WHERE s.language_code IS NOT NULL 
        AND s.language_code != 'L'
        AND l.language_id IS NULL
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        GROUP BY s.language_code
        ORDER BY spot_count DESC;
    """)
    
    results = cursor.fetchall()
    
    if not results:
        print("\n✅ No invalid language codes found!")
        return
    
    print(f"\n🚨 INVALID LANGUAGE CODES (not in languages table):")
    print(f"{'Code':>6} {'Count':>8} {'Total Revenue':>15} {'Avg Revenue':>12}")
    print("-" * 50)
    
    for code, count, total_rev, avg_rev in results:
        total_str = f"${total_rev:,.0f}" if total_rev else "$0"
        avg_str = f"${avg_rev:,.0f}" if avg_rev else "$0"
        print(f"{code:>6} {count:>8,} {total_str:>15} {avg_str:>12}")

def batch_assign(service, count):
    """Batch assign languages"""
    print(f"\n🚀 BATCH ASSIGNMENT of {count} spots...")
    
    unassigned_spots = service.queries.get_unassigned_spots(limit=count)
    if not unassigned_spots:
        print("✅ No unassigned spots found!")
        return
    
    print(f"Found {len(unassigned_spots)} unassigned spots to process...")
    
    results = service.batch_assign_languages(unassigned_spots)
    
    # Save assignments with progress bar
    saved_count = 0
    error_count = 0
    
    for assignment in tqdm(results.values(), desc="Saving assignments", unit="spot"):
        try:
            service.queries.save_language_assignment(assignment)
            saved_count += 1
        except Exception as e:
            print(f"Error saving assignment for spot {assignment.spot_id}: {e}")
            error_count += 1
    
    print(f"\n📊 BATCH RESULTS:")
    print(f"   • Processed: {len(results)}")
    print(f"   • Saved successfully: {saved_count}")
    print(f"   • Errors: {error_count}")

def assign_all(service):
    """Assign all unassigned spots"""
    print(f"\n🎯 ASSIGNING ALL UNASSIGNED SPOTS...")
    
    unassigned_spots = service.queries.get_unassigned_spots()
    if not unassigned_spots:
        print("✅ All spots are already assigned!")
        return
    
    print(f"Found {len(unassigned_spots):,} unassigned spots")
    
    confirm = input(f"\nProceed with assignment? (yes/no): ").strip().lower()
    if confirm not in ["yes", "y"]:
        print("❌ Assignment cancelled")
        return
    
    # Process in batches of 1000
    batch_size = 1000
    total_saved = 0
    total_errors = 0
    
    for i in tqdm(range(0, len(unassigned_spots), batch_size), 
                  desc="Processing batches", unit="batch"):
        batch = unassigned_spots[i:i + batch_size]
        results = service.batch_assign_languages(batch)
        
        # Save batch with inner progress bar
        for assignment in tqdm(results.values(), 
                              desc="Saving assignments", 
                              leave=False, unit="spot"):
            try:
                service.queries.save_language_assignment(assignment)
                total_saved += 1
            except Exception as e:
                print(f"Error saving assignment for spot {assignment.spot_id}: {e}")
                total_errors += 1
    
    print(f"\n🎉 ALL ASSIGNMENTS COMPLETE:")
    print(f"   • Total processed: {len(unassigned_spots):,}")
    print(f"   • Successfully saved: {total_saved:,}")
    print(f"   • Errors: {total_errors:,}")

def categorize_all_spots(conn):
    """Categorize all uncategorized spots"""
    print(f"\n🏷️  CATEGORIZING ALL UNCATEGORIZED SPOTS...")
    
    service = SpotCategorizationService(conn)
    results = service.categorize_all_uncategorized(batch_size=5000)
    
    print(f"\n✅ CATEGORIZATION COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Categorized: {results['categorized']:,}")

def show_status_by_category(conn):
    """Show spot breakdown by category"""
    service = SpotCategorizationService(conn)
    summary = service.get_category_summary()
    
    print(f"\n📊 SPOTS BY CATEGORY:")
    
    for category in SpotCategory:
        count = summary.get(category.value, 0)
        print(f"   • {category.value.replace('_', ' ').title()}: {count:,}")
    
    uncategorized = summary.get('uncategorized', 0)
    print(f"   • Uncategorized: {uncategorized:,}")
    
    total = sum(summary.values())
    print(f"   • Total: {total:,}")

def test_categorization(conn, count):
    """Test categorization with sample spots"""
    print(f"\n🧪 TESTING CATEGORIZATION with {count} spots...")
    
    service = SpotCategorizationService(conn)
    uncategorized_spots = service.get_uncategorized_spots(limit=count)
    
    if not uncategorized_spots:
        print("❌ No uncategorized spots found for testing!")
        return
    
    print(f"Found {len(uncategorized_spots)} uncategorized spots to test...")
    
    # Categorize without saving
    categorizations = service.categorize_spots_batch(uncategorized_spots)
    
    # Count by category
    category_counts = {}
    for category in categorizations.values():
        category_counts[category.value] = category_counts.get(category.value, 0) + 1
    
    print(f"\n📊 TEST RESULTS:")
    for category_name, count in category_counts.items():
        print(f"   • {category_name.replace('_', ' ').title()}: {count}")

def show_uncategorized_count(conn):
    """Show count of uncategorized spots"""
    service = SpotCategorizationService(conn)
    uncategorized_spots = service.get_uncategorized_spots()
    
    print(f"\n📋 UNCATEGORIZED SPOTS: {len(uncategorized_spots):,}")
    
    if len(uncategorized_spots) > 0:
        print(f"💡 Run --categorize-all to categorize all spots")

def process_language_required(conn):
    """Process language assignment required spots"""
    print(f"\n🎯 PROCESSING LANGUAGE ASSIGNMENT REQUIRED SPOTS...")
    
    orchestrator = LanguageProcessingOrchestrator(conn)
    results = orchestrator.process_language_required_category()
    
    print(f"\n✅ LANGUAGE ASSIGNMENT COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Successfully assigned: {results['assigned']:,}")
    print(f"   • Flagged for review: {results['review_flagged']:,}")
    print(f"   • Errors: {results['errors']:,}")

def process_review_category(conn):
    """Process review category spots"""
    print(f"\n📋 PROCESSING REVIEW CATEGORY SPOTS...")
    
    orchestrator = LanguageProcessingOrchestrator(conn)
    results = orchestrator.process_review_category()
    
    print(f"\n✅ REVIEW CATEGORY PROCESSING COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Flagged for review: {results['flagged_for_review']:,}")
    print(f"   • Errors: {results['errors']:,}")

def process_default_english(conn):
    """Process default English spots"""
    print(f"\n🇺🇸 PROCESSING DEFAULT ENGLISH SPOTS...")
    
    orchestrator = LanguageProcessingOrchestrator(conn)
    results = orchestrator.process_default_english_category()
    
    print(f"\n✅ DEFAULT ENGLISH PROCESSING COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Assigned to English: {results['assigned']:,}")
    print(f"   • Errors: {results['errors']:,}")

def process_all_categories(conn):
    """Process all categories"""
    print(f"\n🚀 PROCESSING ALL CATEGORIES...")
    
    orchestrator = LanguageProcessingOrchestrator(conn)
    results = orchestrator.process_all_categories()
    
    summary = results['summary']
    print(f"\n🎉 ALL CATEGORIES PROCESSING COMPLETE:")
    print(f"   • Total processed: {summary['total_processed']:,}")
    print(f"   • Language assigned: {summary['language_assigned']:,}")
    print(f"   • Default English assigned: {summary['default_english_assigned']:,}")
    print(f"   • Flagged for review: {summary['flagged_for_review']:,}")
    print(f"   • Total errors: {summary['total_errors']:,}")

def show_processing_status(conn):
    """Show processing status by category"""
    orchestrator = LanguageProcessingOrchestrator(conn)
    status = orchestrator.get_processing_status()
    
    print(f"\n📊 PROCESSING STATUS BY CATEGORY:")
    
    for category in SpotCategory:
        category_key = category.value
        total = status.get(f"{category_key}_total", 0)
        processed = status.get(f"{category_key}_processed", 0)
        
        if total > 0:
            percentage = (processed / total * 100) if total > 0 else 0
            print(f"   • {category.value.replace('_', ' ').title()}: {processed:,}/{total:,} ({percentage:.1f}%)")

def process_review_category_simple(conn):
    """Process review category spots (simple version)"""
    print(f"\n📋 PROCESSING REVIEW CATEGORY SPOTS...")
    
    import sys
    import os
    sys.path.append('src')
    from services.language_assignment_service import LanguageAssignmentService
    from models.spot_category import SpotCategory
    
    service = LanguageAssignmentService(conn)
    spot_ids = service.get_spots_by_category(SpotCategory.REVIEW_CATEGORY)
    
    if not spot_ids:
        print("❌ No review category spots found!")
        return
    
    print(f"Found {len(spot_ids):,} spots requiring business review...")
    results = service.process_review_category_spots(spot_ids)
    
    print(f"\n✅ REVIEW CATEGORY PROCESSING COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Flagged for review: {results['flagged_for_review']:,}")
    print(f"   • Errors: {results['errors']:,}")

def process_default_english_simple(conn):
    """Process default English spots (simple version)"""
    print(f"\n🇺🇸 PROCESSING DEFAULT ENGLISH SPOTS...")
    
    import sys
    import os
    sys.path.append('src')
    from services.language_assignment_service import LanguageAssignmentService
    from models.spot_category import SpotCategory
    
    service = LanguageAssignmentService(conn)
    spot_ids = service.get_spots_by_category(SpotCategory.DEFAULT_ENGLISH)
    
    if not spot_ids:
        print("❌ No default English spots found!")
        return
    
    print(f"Found {len(spot_ids):,} spots for default English assignment...")
    results = service.process_default_english_spots(spot_ids)
    
    print(f"\n✅ DEFAULT ENGLISH PROCESSING COMPLETE:")
    print(f"   • Processed: {results['processed']:,}")
    print(f"   • Assigned to English: {results['assigned']:,}")
    print(f"   • Errors: {results['errors']:,}")

def process_all_categories_simple(conn):
    """Process all remaining categories"""
    print(f"\n🚀 PROCESSING ALL REMAINING CATEGORIES...")
    
    # Process Review Category first (smaller)
    process_review_category_simple(conn)
    
    # Process Default English (larger)  
    process_default_english_simple(conn)
    
    # Show final summary
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM spot_language_assignments")
    total_processed = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM spot_language_assignments WHERE requires_review = 1")
    total_review = cursor.fetchone()[0]
    
    print(f"\n🎉 ALL PROCESSING COMPLETE:")
    print(f"   • Total spots processed: {total_processed:,}")
    print(f"   • Spots requiring review: {total_review:,}")
    print(f"   • Spots with language assigned: {total_processed - total_review:,}")

if __name__ == "__main__":
    exit(main())