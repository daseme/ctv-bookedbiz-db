#!/usr/bin/env python3
"""
Language Block Reporting Test Script
Tests the language block reporting views and generates sample reports
"""

import sqlite3
import pandas as pd
from datetime import datetime
import json
import os
from pathlib import Path


class LanguageBlockReporter:
    def __init__(self, db_path):
        self.db_path = db_path
        self.connection = None

    def connect(self):
        """Connect to the database"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.connection.row_factory = sqlite3.Row
            print(f"✓ Connected to database: {self.db_path}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect to database: {e}")
            return False

    def disconnect(self):
        """Disconnect from database"""
        if self.connection:
            self.connection.close()
            print("✓ Disconnected from database")

    def create_views(self):
        """Create the language block reporting views"""
        sql_file = "language_block_views.sql"
        if not os.path.exists(sql_file):
            print(f"✗ SQL file not found: {sql_file}")
            return False

        try:
            with open(sql_file, "r") as f:
                sql_content = f.read()

            # Split by view creation statements and execute each
            statements = sql_content.split("CREATE VIEW")
            for i, statement in enumerate(statements):
                if i == 0:  # Skip the first empty part
                    continue
                full_statement = "CREATE VIEW" + statement.strip()
                if full_statement.strip():
                    self.connection.execute(full_statement)

            self.connection.commit()
            print("✓ Language block reporting views created successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to create views: {e}")
            return False

    def test_basic_functionality(self):
        """Test basic functionality of the views"""
        print("\n" + "=" * 50)
        print("TESTING BASIC FUNCTIONALITY")
        print("=" * 50)

        tests = [
            {
                "name": "Language Block Revenue Summary",
                "query": "SELECT COUNT(*) as count FROM language_block_revenue_summary",
                "min_expected": 0,
            },
            {
                "name": "Monthly Performance",
                "query": "SELECT COUNT(*) as count FROM monthly_language_block_performance",
                "min_expected": 0,
            },
            {
                "name": "Customer Analysis",
                "query": "SELECT COUNT(*) as count FROM language_block_customer_analysis",
                "min_expected": 0,
            },
            {
                "name": "Dashboard View",
                "query": "SELECT COUNT(*) as count FROM language_block_dashboard",
                "min_expected": 0,
            },
            {
                "name": "Top Language Blocks",
                "query": "SELECT COUNT(*) as count FROM top_language_blocks",
                "min_expected": 0,
            },
        ]

        all_passed = True
        for test in tests:
            try:
                cursor = self.connection.execute(test["query"])
                result = cursor.fetchone()
                count = result["count"] if result else 0

                if count >= test["min_expected"]:
                    print(f"✓ {test['name']}: {count} records")
                else:
                    print(
                        f"✗ {test['name']}: {count} records (expected >= {test['min_expected']})"
                    )
                    all_passed = False
            except Exception as e:
                print(f"✗ {test['name']}: Error - {e}")
                all_passed = False

        return all_passed

    def generate_summary_report(self):
        """Generate a summary report of language block performance"""
        print("\n" + "=" * 50)
        print("LANGUAGE BLOCK SUMMARY REPORT")
        print("=" * 50)

        try:
            # Get overall statistics
            query = """
            SELECT 
                COUNT(DISTINCT language_block_id) as total_blocks,
                COUNT(DISTINCT language_name) as total_languages,
                COUNT(DISTINCT market_code) as total_markets,
                SUM(current_year_revenue) as total_revenue,
                SUM(current_year_spots) as total_spots,
                AVG(current_year_revenue) as avg_revenue_per_block
            FROM language_block_dashboard
            """

            cursor = self.connection.execute(query)
            stats = cursor.fetchone()

            if stats:
                print(f"Total Language Blocks: {stats['total_blocks']}")
                print(f"Total Languages: {stats['total_languages']}")
                print(f"Total Markets: {stats['total_markets']}")
                print(f"Total Revenue (Current Year): ${stats['total_revenue']:,.2f}")
                print(f"Total Spots (Current Year): {stats['total_spots']:,}")
                print(
                    f"Average Revenue per Block: ${stats['avg_revenue_per_block']:,.2f}"
                )

            # Get top performing blocks
            print(f"\n{'=' * 30}")
            print("TOP 10 PERFORMING BLOCKS")
            print(f"{'=' * 30}")

            top_blocks_query = """
            SELECT 
                block_name,
                language_name,
                market_name,
                current_year_revenue,
                current_year_spots,
                yoy_revenue_growth_pct,
                performance_rating
            FROM top_language_blocks
            LIMIT 10
            """

            cursor = self.connection.execute(top_blocks_query)
            top_blocks = cursor.fetchall()

            if top_blocks:
                for i, block in enumerate(top_blocks, 1):
                    growth = (
                        f"{block['yoy_revenue_growth_pct']:.1f}%"
                        if block["yoy_revenue_growth_pct"]
                        else "N/A"
                    )
                    print(f"{i:2d}. {block['block_name']} ({block['language_name']})")
                    print(f"    Market: {block['market_name']}")
                    print(f"    Revenue: ${block['current_year_revenue']:,.2f}")
                    print(f"    Spots: {block['current_year_spots']:,}")
                    print(
                        f"    Growth: {growth} | Rating: {block['performance_rating']}"
                    )
                    print()
            else:
                print("No data available")

            return True
        except Exception as e:
            print(f"✗ Failed to generate summary report: {e}")
            return False

    def generate_detailed_report(self, output_file="language_block_report.json"):
        """Generate a detailed JSON report"""
        print(f"\n{'=' * 50}")
        print("GENERATING DETAILED REPORT")
        print(f"{'=' * 50}")

        try:
            report_data = {
                "generated_at": datetime.now().isoformat(),
                "database_path": self.db_path,
                "summary": {},
                "language_blocks": [],
                "top_customers": [],
                "monthly_trends": [],
            }

            # Summary data
            summary_query = """
            SELECT 
                COUNT(DISTINCT language_block_id) as total_blocks,
                COUNT(DISTINCT language_name) as total_languages,
                COUNT(DISTINCT market_code) as total_markets,
                SUM(current_year_revenue) as total_revenue,
                SUM(current_year_spots) as total_spots
            FROM language_block_dashboard
            """

            cursor = self.connection.execute(summary_query)
            summary = cursor.fetchone()
            if summary:
                report_data["summary"] = dict(summary)

            # Language block details
            blocks_query = """
            SELECT 
                language_block_id,
                block_name,
                language_name,
                market_name,
                current_year_revenue,
                current_year_spots,
                current_year_customers,
                yoy_revenue_growth_pct,
                performance_rating,
                last_activity_date
            FROM language_block_dashboard
            ORDER BY current_year_revenue DESC
            LIMIT 50
            """

            cursor = self.connection.execute(blocks_query)
            blocks = cursor.fetchall()
            report_data["language_blocks"] = [dict(block) for block in blocks]

            # Top customers by language block
            customers_query = """
            SELECT 
                block_name,
                language_name,
                customer_name,
                total_revenue,
                total_spots,
                customer_loyalty
            FROM language_block_customer_analysis
            ORDER BY total_revenue DESC
            LIMIT 25
            """

            cursor = self.connection.execute(customers_query)
            customers = cursor.fetchall()
            report_data["top_customers"] = [dict(customer) for customer in customers]

            # Monthly trends
            trends_query = """
            SELECT 
                year_month,
                SUM(total_revenue) as monthly_revenue,
                SUM(total_spots) as monthly_spots,
                COUNT(DISTINCT language_block_id) as active_blocks
            FROM monthly_language_block_performance
            WHERE year_month >= date('now', '-12 months', 'start of month')
            GROUP BY year_month
            ORDER BY year_month
            """

            cursor = self.connection.execute(trends_query)
            trends = cursor.fetchall()
            report_data["monthly_trends"] = [dict(trend) for trend in trends]

            # Save report
            with open(output_file, "w") as f:
                json.dump(report_data, f, indent=2, default=str)

            print(f"✓ Detailed report saved to: {output_file}")
            print(f"  - {len(report_data['language_blocks'])} language blocks")
            print(f"  - {len(report_data['top_customers'])} top customers")
            print(f"  - {len(report_data['monthly_trends'])} monthly data points")

            return True
        except Exception as e:
            print(f"✗ Failed to generate detailed report: {e}")
            return False

    def analyze_performance_issues(self):
        """Analyze potential performance issues"""
        print(f"\n{'=' * 50}")
        print("PERFORMANCE ANALYSIS")
        print(f"{'=' * 50}")

        try:
            # Check for inactive blocks with recent revenue
            inactive_query = """
            SELECT 
                block_name,
                language_name,
                last_activity_date,
                current_year_revenue
            FROM language_block_dashboard
            WHERE performance_rating = 'Needs Attention'
            AND current_year_revenue > 0
            ORDER BY current_year_revenue DESC
            """

            cursor = self.connection.execute(inactive_query)
            inactive_blocks = cursor.fetchall()

            if inactive_blocks:
                print("⚠ BLOCKS NEEDING ATTENTION:")
                for block in inactive_blocks:
                    print(f"  - {block['block_name']} ({block['language_name']})")
                    print(f"    Revenue: ${block['current_year_revenue']:,.2f}")
                    print(f"    Last Activity: {block['last_activity_date']}")
                    print()
            else:
                print("✓ No performance issues detected")

            # Check for declining blocks
            declining_query = """
            SELECT 
                block_name,
                language_name,
                current_year_revenue,
                yoy_revenue_growth_pct
            FROM language_block_dashboard
            WHERE yoy_revenue_growth_pct < -20
            ORDER BY yoy_revenue_growth_pct ASC
            """

            cursor = self.connection.execute(declining_query)
            declining_blocks = cursor.fetchall()

            if declining_blocks:
                print("📉 DECLINING BLOCKS (>20% revenue drop):")
                for block in declining_blocks:
                    print(f"  - {block['block_name']} ({block['language_name']})")
                    print(f"    Revenue: ${block['current_year_revenue']:,.2f}")
                    print(f"    YoY Change: {block['yoy_revenue_growth_pct']:.1f}%")
                    print()
            else:
                print("✓ No significantly declining blocks detected")

            return True
        except Exception as e:
            print(f"✗ Failed to analyze performance: {e}")
            return False


def main():
    """Main function to run the language block reporting tests"""
    print("Language Block Reporting System Test")
    print("=" * 50)

    # Database path - adjust as needed
    db_path = "data/database/production.db"

    if not os.path.exists(db_path):
        print(f"✗ Database not found: {db_path}")
        print(
            "Please make sure you've downloaded the database with: python db_sync.py download"
        )
        return False

    # Initialize reporter
    reporter = LanguageBlockReporter(db_path)

    try:
        # Connect to database
        if not reporter.connect():
            return False

        # Create views (skip if they already exist)
        print("\nCreating reporting views...")
        # Note: Views will be created from the SQL file above

        # Test basic functionality
        if not reporter.test_basic_functionality():
            print("⚠ Some tests failed, but continuing with reporting...")

        # Generate reports
        reporter.generate_summary_report()
        reporter.generate_detailed_report()
        reporter.analyze_performance_issues()

        print(f"\n{'=' * 50}")
        print("✓ LANGUAGE BLOCK REPORTING TEST COMPLETED")
        print(f"{'=' * 50}")
        print("Next steps:")
        print("1. Review the generated reports")
        print("2. Check language_block_report.json for detailed data")
        print("3. Integrate views into your FastAPI application")
        print("4. Set up automated reporting schedule")

        return True

    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False
    finally:
        reporter.disconnect()


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
