"""
Business Rules CLI Tool (Fixed Imports)
=======================================

Command-line interface for managing and testing business rules in the
language block assignment system. Fixed import paths to work from project root.
"""

import argparse
import sqlite3
import sys
import os
import json
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.services.business_rules_service import BusinessRulesService

# Note: Enhanced service import will be added when we fix that file too
from src.models.business_rules_models import BusinessRuleType, SpotData


class BusinessRulesCLI:
    """Command-line interface for business rules management"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self.business_rules_service = None
        # self.enhanced_service = None  # Will add when enhanced service is fixed

    def connect(self) -> bool:
        """Connect to database and initialize services"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.business_rules_service = BusinessRulesService(self.conn)
            # self.enhanced_service = EnhancedLanguageBlockService(self.conn)  # Will add later
            print(f"✅ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"❌ Failed to connect: {e}")
            return False

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def test_rules(self, limit: int = 100) -> None:
        """Test business rules on a sample of spots"""
        print(f"\n🧪 Testing business rules on {limit} spots...")

        # Get sample spots
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT spot_id FROM spots 
            WHERE customer_id IS NOT NULL
            AND time_in IS NOT NULL
            AND time_out IS NOT NULL
            ORDER BY RANDOM()
            LIMIT ?
        """,
            (limit,),
        )

        spot_ids = [row[0] for row in cursor.fetchall()]

        if not spot_ids:
            print("❌ No spots found to test")
            return

        results = []
        for spot_id in spot_ids:
            spot_data = self.business_rules_service.get_spot_data_from_db(spot_id)
            if spot_data:
                result = self.business_rules_service.evaluate_spot(spot_data)
                results.append((spot_data, result))

        # Print results
        self._print_test_results(results)

    def estimate_impact(self) -> None:
        """Estimate impact of business rules on all spots"""
        print(f"\n📊 Estimating business rules impact...")

        estimates = self.business_rules_service.estimate_total_impact()

        if not estimates:
            print("❌ Unable to estimate impact")
            return

        # Get total spots for context
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM spots")
        total_spots = cursor.fetchone()[0]

        print(f"\n📈 ESTIMATED IMPACT:")
        print(f"{'Rule Type':<30} {'Spots':<12} {'% of Total':<12}")
        print("-" * 55)

        total_affected = 0
        for rule_type, count in estimates.items():
            percentage = (count / total_spots * 100) if total_spots > 0 else 0
            total_affected += count
            print(f"{rule_type:<30} {count:<12,} {percentage:<12.1f}%")

        overall_percentage = (
            (total_affected / total_spots * 100) if total_spots > 0 else 0
        )
        print("-" * 55)
        print(
            f"{'TOTAL AFFECTED':<30} {total_affected:<12,} {overall_percentage:<12.1f}%"
        )
        print(f"\n💡 Estimated edge case reduction: {overall_percentage:.1f}%")

    def show_rules(self) -> None:
        """Show all configured business rules"""
        print(f"\n📋 BUSINESS RULES CONFIGURATION:")
        print("=" * 60)

        rules = self.business_rules_service.get_rules_summary()

        for i, rule in enumerate(rules, 1):
            print(f"\n{i}. {rule['name']}")
            print(f"   Type: {rule['rule_type']}")
            print(f"   Description: {rule['description']}")
            print(
                f"   Sectors: {rule['sector_codes'] if rule['sector_codes'] else 'Any'}"
            )
            if rule["min_duration_minutes"]:
                print(f"   Min Duration: {rule['min_duration_minutes']} minutes")
            if rule["max_duration_minutes"]:
                print(f"   Max Duration: {rule['max_duration_minutes']} minutes")
            print(f"   Auto-resolve: {'Yes' if rule['auto_resolve'] else 'No'}")
            print(f"   Priority: {rule['priority']}")

    def validate_rules(self) -> None:
        """Validate business rules configuration"""
        print(f"\n🔍 Validating business rules configuration...")

        # Check if sectors exist
        cursor = self.conn.cursor()
        cursor.execute("SELECT sector_code FROM sectors WHERE is_active = 1")
        active_sectors = {row[0] for row in cursor.fetchall()}

        print(f"Active sectors: {sorted(active_sectors)}")

        rules = self.business_rules_service.get_rules_summary()

        all_valid = True
        for rule in rules:
            print(f"\n✅ Validating: {rule['name']}")

            # Check sectors
            if rule["sector_codes"]:
                missing_sectors = [
                    s for s in rule["sector_codes"] if s not in active_sectors
                ]
                if missing_sectors:
                    print(f"   ❌ Missing sectors: {missing_sectors}")
                    all_valid = False
                else:
                    print(f"   ✅ Sectors valid: {rule['sector_codes']}")
            else:
                print(f"   ✅ Applies to all sectors")

            # Check duration constraints
            if rule["min_duration_minutes"] and rule["max_duration_minutes"]:
                if rule["min_duration_minutes"] >= rule["max_duration_minutes"]:
                    print(f"   ❌ Invalid duration range")
                    all_valid = False
                else:
                    print(f"   ✅ Duration range valid")

        if all_valid:
            print(f"\n✅ All business rules are valid!")
        else:
            print(f"\n❌ Some business rules have issues")

    def show_stats(self) -> None:
        """Show business rules statistics"""
        print(f"\n📊 BUSINESS RULES STATISTICS:")
        print("=" * 50)

        stats = self.business_rules_service.get_stats()

        if stats["total_evaluated"] == 0:
            print("No spots have been evaluated yet")
            return

        print(f"Total evaluated: {stats['total_evaluated']}")
        print(
            f"Auto-resolved: {stats['auto_resolved']} ({stats['auto_resolve_rate']:.1%})"
        )
        print(f"Flagged for review: {stats['flagged_for_review']}")

        if stats["rules_applied"]:
            print(f"\nRules applied:")
            for rule_type, count in stats["rules_applied"].items():
                print(f"  • {rule_type}: {count}")

    def export_rules(self, output_file: str) -> None:
        """Export business rules to JSON file"""
        rules = self.business_rules_service.get_rules_summary()

        try:
            with open(output_file, "w") as f:
                json.dump(rules, f, indent=2)
            print(f"✅ Rules exported to {output_file}")
        except Exception as e:
            print(f"❌ Error exporting rules: {e}")

    def _print_test_results(self, results: List[tuple]) -> None:
        """Print test results in a formatted way"""
        if not results:
            print("No results to display")
            return

        total = len(results)
        auto_resolved = sum(1 for _, result in results if result.auto_resolved)
        flagged = sum(1 for _, result in results if result.requires_attention)
        no_rule = total - auto_resolved - flagged

        print(f"\n📊 TEST RESULTS:")
        print(f"   • Total spots tested: {total}")
        print(
            f"   • Auto-resolved: {auto_resolved} ({auto_resolved / total * 100:.1f}%)"
        )
        print(f"   • Flagged for review: {flagged} ({flagged / total * 100:.1f}%)")
        print(f"   • No rule matched: {no_rule} ({no_rule / total * 100:.1f}%)")

        # Show rule breakdown
        rule_counts = {}
        for _, result in results:
            if result.rule_applied:
                rule_type = result.rule_applied.rule_type.value
                rule_counts[rule_type] = rule_counts.get(rule_type, 0) + 1

        if rule_counts:
            print(f"\n🎯 RULES TRIGGERED:")
            for rule_type, count in rule_counts.items():
                print(f"   • {rule_type}: {count}")

        # Show sample matches
        print(f"\n📋 SAMPLE MATCHES:")
        shown = 0
        for spot_data, result in results:
            if result.rule_applied and shown < 5:
                print(
                    f"   • Spot {spot_data.spot_id}: {spot_data.customer_name[:40]}..."
                )
                print(
                    f"     Sector: {spot_data.sector_code}, Duration: {spot_data.duration_minutes}min"
                )
                print(f"     Rule: {result.rule_applied.name}")
                shown += 1


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description="Business Rules CLI Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.cli.business_rules_cli --rules                    # Show all rules
  python -m src.cli.business_rules_cli --test 100                 # Test on 100 spots
  python -m src.cli.business_rules_cli --estimate                 # Estimate impact
  python -m src.cli.business_rules_cli --validate                 # Validate rules
  python -m src.cli.business_rules_cli --stats                    # Show statistics
  python -m src.cli.business_rules_cli --export rules.json       # Export rules
        """,
    )

    parser.add_argument(
        "--database",
        default="./data/database/production.db",
        help="Database path (default: ./data/database/production.db)",
    )

    # Action group (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument(
        "--rules", action="store_true", help="Show all business rules"
    )
    action_group.add_argument(
        "--test", type=int, metavar="N", help="Test rules on N spots"
    )
    action_group.add_argument(
        "--estimate", action="store_true", help="Estimate impact of rules"
    )
    action_group.add_argument(
        "--validate", action="store_true", help="Validate rules configuration"
    )
    action_group.add_argument("--stats", action="store_true", help="Show statistics")
    action_group.add_argument(
        "--export", metavar="FILE", help="Export rules to JSON file"
    )

    args = parser.parse_args()

    # Initialize CLI
    cli = BusinessRulesCLI(args.database)

    if not cli.connect():
        return 1

    try:
        if args.rules:
            cli.show_rules()
        elif args.test:
            cli.test_rules(args.test)
        elif args.estimate:
            cli.estimate_impact()
        elif args.validate:
            cli.validate_rules()
        elif args.stats:
            cli.show_stats()
        elif args.export:
            cli.export_rules(args.export)

        return 0
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    finally:
        cli.close()


if __name__ == "__main__":
    exit(main())
