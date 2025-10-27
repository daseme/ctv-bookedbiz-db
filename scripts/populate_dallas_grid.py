#!/usr/bin/env python3
"""
CLI script to populate Dallas Grid language blocks
Place this in: scripts/populate_dallas_grid.py
"""

import sys
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection
from services.dallas_grid_populator import DallasGridPopulator


def main():
    """CLI interface for Dallas Grid population."""
    import argparse

    parser = argparse.ArgumentParser(description="Populate Dallas Grid Language Blocks")
    parser.add_argument(
        "--db-path", default="data/database/production.db", help="Database path"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--summary-only", action="store_true", help="Show summary without populating"
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Validate existing population"
    )

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    # Initialize service
    db_connection = DatabaseConnection(args.db_path)
    populator = DallasGridPopulator(db_connection)

    try:
        if args.summary_only:
            # Show current population summary
            summary = populator.get_population_summary()
            print(f"\n{'=' * 60}")
            print(f"DALLAS GRID POPULATION SUMMARY")
            print(f"{'=' * 60}")
            print(f"Total Blocks: {summary['total_blocks']}")
            print(f"Days with Blocks: {summary['days_with_blocks']}")
            print(f"Markets Covered: {summary['market_coverage']}")

            if summary["language_distribution"]:
                print(f"\nLanguage Distribution:")
                for lang_code, info in summary["language_distribution"].items():
                    print(f"  {lang_code} ({info['name']}): {info['blocks']} blocks")

        elif args.validate_only:
            # Validate existing population
            print(f"\n{'=' * 60}")
            print(f"VALIDATING DALLAS GRID POPULATION")
            print(f"{'=' * 60}")

            validation_result = populator.validate_coverage()

            if validation_result["success"]:
                print(f"✅ Validation successful!")
                print(f"   Total blocks: {validation_result['total_blocks']}")

                if "language_distribution" in validation_result:
                    print(f"   Language distribution:")
                    for lang_code, info in validation_result["language_distribution"][
                        "distribution"
                    ].items():
                        print(
                            f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)"
                        )
            else:
                print(f"❌ Validation failed!")
                for error in validation_result["errors"]:
                    print(f"   • {error}")

        else:
            # Populate Dallas Grid
            print(f"\n{'=' * 60}")
            print(f"DALLAS GRID LANGUAGE BLOCK POPULATION")
            print(f"{'=' * 60}")

            result = populator.populate_dallas_grid_blocks()

            if result["success"]:
                print(f"✅ Population successful!")
                print(f"   Blocks created: {result['blocks_created']}")
                print(f"   Days processed: {result['days_processed']}")

                if result["validation_result"]:
                    val_result = result["validation_result"]
                    print(
                        f"   Validation: {'✅ Passed' if val_result['success'] else '❌ Failed'}"
                    )

                    if "language_distribution" in val_result:
                        print(f"   Language distribution:")
                        for lang_code, info in val_result["language_distribution"][
                            "distribution"
                        ].items():
                            print(
                                f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)"
                            )
            else:
                print(f"❌ Population failed!")
                for error in result["errors"]:
                    print(f"   • {error}")
                sys.exit(1)

    finally:
        db_connection.close()


if __name__ == "__main__":
    main()
