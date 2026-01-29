#!/usr/bin/env python3
"""
Dallas Grid Language Block Populator - BASED ON DALLAS VISUAL SCHEDULE
Populates the Dallas Grid with the actual Dallas Crossings TV programming schedule.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService


class DallasGridPopulator(BaseService):
    """Populates Dallas Grid with language blocks matching Dallas visual programming schedule."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.schedule_id = 2  # Dallas Grid
        self.language_mappings = {}
        self._load_language_mappings()

    def _load_language_mappings(self):
        """Load language ID mappings from src.database."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute(
                    "SELECT language_id, language_code, language_name FROM languages"
                )
                for row in cursor.fetchall():
                    language_id, language_code, language_name = row
                    self.language_mappings[language_code] = {
                        "id": language_id,
                        "name": language_name,
                    }

            logging.info(f"Loaded {len(self.language_mappings)} language mappings")

        except Exception as e:
            logging.error(f"Failed to load language mappings: {e}")
            raise

    def populate_dallas_grid_blocks(self) -> Dict[str, Any]:
        """Populate Dallas Grid with language blocks for all 7 days."""
        start_time = datetime.now()
        result = {
            "success": False,
            "blocks_created": 0,
            "days_processed": 0,
            "errors": [],
            "validation_result": None,
        }

        logging.info("Starting Dallas Grid language block population...")

        try:
            # Check if Dallas Grid exists
            if not self._verify_dallas_grid_exists():
                raise Exception("Dallas Grid (schedule_id=2) not found")

            # Clear existing blocks for Dallas Grid
            blocks_cleared = self._clear_existing_blocks()
            if blocks_cleared > 0:
                logging.info(f"Cleared {blocks_cleared} existing Dallas Grid blocks")

            days_of_week = [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ]

            with self.db.transaction() as conn:
                total_blocks = 0

                for day in days_of_week:
                    daily_blocks = self._create_daily_blocks(day)

                    # Insert blocks for this day
                    for block in daily_blocks:
                        conn.execute(
                            """
                            INSERT INTO language_blocks (
                                schedule_id, day_of_week, time_start, time_end,
                                language_id, block_name, block_type, day_part, display_order
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                self.schedule_id,
                                block["day_of_week"],
                                block["time_start"],
                                block["time_end"],
                                block["language_id"],
                                block["block_name"],
                                block["block_type"],
                                block["day_part"],
                                block["display_order"],
                            ),
                        )
                        total_blocks += 1

                    result["days_processed"] += 1
                    logging.info(
                        f"Created {len(daily_blocks)} blocks for {day.title()}"
                    )

                result["blocks_created"] = total_blocks
                logging.info(f"Successfully created {total_blocks} language blocks")

            # Validate the population
            validation_result = self.validate_coverage()
            result["validation_result"] = validation_result

            if not validation_result["success"]:
                result["errors"].extend(validation_result["errors"])
                raise Exception("Coverage validation failed")

            result["success"] = True
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(
                f"Dallas Grid population completed successfully in {duration:.2f} seconds"
            )

        except Exception as e:
            error_msg = f"Dallas Grid population failed: {str(e)}"
            result["errors"].append(error_msg)
            logging.error(error_msg)

        return result

    def _verify_dallas_grid_exists(self) -> bool:
        """Verify Dallas Grid exists in database."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM programming_schedules 
                    WHERE schedule_id = ? AND schedule_name = 'Dallas Grid' AND is_active = 1
                """,
                    (self.schedule_id,),
                )
                return cursor.fetchone()[0] > 0
        except Exception as e:
            logging.error(f"Error verifying Dallas Grid: {e}")
            return False

    def _clear_existing_blocks(self) -> int:
        """Clear existing language blocks for Dallas Grid."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute(
                    """
                    DELETE FROM language_blocks WHERE schedule_id = ?
                """,
                    (self.schedule_id,),
                )
                return cursor.rowcount
        except Exception as e:
            logging.error(f"Error clearing existing blocks: {e}")
            return 0

    def _create_daily_blocks(self, day_of_week: str) -> List[Dict]:
        """Create language blocks for a specific day based on Dallas visual programming schedule."""

        # Dallas Schedule (same for all days based on visual schedule)
        daily_schedule = [
            # 6:00-7:00 AM: Mandarin News
            {
                "time_start": "06:00:00",
                "time_end": "07:00:00",
                "language_code": "M",
                "block_name": "Phoenix Evening Express",
                "block_type": "News",
                "day_part": "Early Morning",
                "display_order": 1,
            },
            # 7:00-8:00 AM: Mandarin Drama
            {
                "time_start": "07:00:00",
                "time_end": "08:00:00",
                "language_code": "M",
                "block_name": "The Starry Love",
                "block_type": "Drama",
                "day_part": "Early Morning",
                "display_order": 2,
            },
            # 8:00-9:00 AM: Mandarin Variety
            {
                "time_start": "08:00:00",
                "time_end": "09:00:00",
                "language_code": "M",
                "block_name": "Mandarin Morning Variety",
                "block_type": "Variety",
                "day_part": "Morning",
                "display_order": 3,
            },
            # 9:00-10:00 AM: Mandarin Programming
            {
                "time_start": "09:00:00",
                "time_end": "10:00:00",
                "language_code": "M",
                "block_name": "Mandarin Morning Block",
                "block_type": "General",
                "day_part": "Morning",
                "display_order": 4,
            },
            # 10:00-11:00 AM: Japanese Programming
            {
                "time_start": "10:00:00",
                "time_end": "11:00:00",
                "language_code": "J",
                "block_name": "Japanese Morning Block",
                "block_type": "General",
                "day_part": "Morning",
                "display_order": 5,
            },
            # 11:00 AM-12:00 PM: Korean Programming
            {
                "time_start": "11:00:00",
                "time_end": "12:00:00",
                "language_code": "K",
                "block_name": "Korean Midday Block",
                "block_type": "General",
                "day_part": "Morning",
                "display_order": 6,
            },
            # 12:00-1:00 PM: Marketplace (English)
            {
                "time_start": "12:00:00",
                "time_end": "13:00:00",
                "language_code": "E",
                "block_name": "Marketplace",
                "block_type": "General",
                "day_part": "Midday",
                "display_order": 7,
            },
            # 1:00-2:00 PM: Mandarin Programming
            {
                "time_start": "13:00:00",
                "time_end": "14:00:00",
                "language_code": "M",
                "block_name": "Mandarin Afternoon Block",
                "block_type": "General",
                "day_part": "Afternoon",
                "display_order": 8,
            },
            # 2:00-3:00 PM: Mandarin News
            {
                "time_start": "14:00:00",
                "time_end": "15:00:00",
                "language_code": "M",
                "block_name": "Primetime News",
                "block_type": "News",
                "day_part": "Afternoon",
                "display_order": 9,
            },
            # 3:00-4:00 PM: Mandarin Programming
            {
                "time_start": "15:00:00",
                "time_end": "16:00:00",
                "language_code": "M",
                "block_name": "Mandarin Afternoon Block 2",
                "block_type": "General",
                "day_part": "Afternoon",
                "display_order": 10,
            },
            # 4:00-5:00 PM: Mandarin Children
            {
                "time_start": "16:00:00",
                "time_end": "17:00:00",
                "language_code": "M",
                "block_name": "Mandarin Children Block",
                "block_type": "Children",
                "day_part": "Afternoon",
                "display_order": 11,
            },
            # 5:00-6:00 PM: Cantonese Programming
            {
                "time_start": "17:00:00",
                "time_end": "18:00:00",
                "language_code": "C",
                "block_name": "Cantonese Evening Block",
                "block_type": "General",
                "day_part": "Early Evening",
                "display_order": 12,
            },
            # 6:00-7:00 PM: Cantonese News/Talk
            {
                "time_start": "18:00:00",
                "time_end": "19:00:00",
                "language_code": "C",
                "block_name": "Cantonese News Block",
                "block_type": "News",
                "day_part": "Early Evening",
                "display_order": 13,
            },
            # 7:00-8:00 PM: Mandarin News
            {
                "time_start": "19:00:00",
                "time_end": "20:00:00",
                "language_code": "M",
                "block_name": "Phoenix Evening Express",
                "block_type": "News",
                "day_part": "Early Evening",
                "display_order": 14,
            },
            # 8:00-9:00 PM: Mandarin Drama
            {
                "time_start": "20:00:00",
                "time_end": "21:00:00",
                "language_code": "M",
                "block_name": "The Starry Love",
                "block_type": "Drama",
                "day_part": "Prime",
                "display_order": 15,
            },
            # 9:00-10:00 PM: Mandarin Programming
            {
                "time_start": "21:00:00",
                "time_end": "22:00:00",
                "language_code": "M",
                "block_name": "Mandarin Prime Block 1",
                "block_type": "Prime",
                "day_part": "Prime",
                "display_order": 16,
            },
            # 10:00-11:00 PM: Mandarin Programming
            {
                "time_start": "22:00:00",
                "time_end": "23:00:00",
                "language_code": "M",
                "block_name": "Mandarin Prime Block 2",
                "block_type": "Prime",
                "day_part": "Prime",
                "display_order": 17,
            },
            # 11:00 PM-12:00 AM: Korean Programming
            {
                "time_start": "23:00:00",
                "time_end": "24:00:00",
                "language_code": "K",
                "block_name": "Korean Late Night Block",
                "block_type": "General",
                "day_part": "Late Night",
                "display_order": 18,
            },
            # 12:00-1:00 AM: Mandarin Late Night
            {
                "time_start": "00:00:00",
                "time_end": "01:00:00",
                "language_code": "M",
                "block_name": "Mandarin Late Night Block",
                "block_type": "General",
                "day_part": "Late Night",
                "display_order": 19,
            },
            # 1:00-2:00 AM: Cantonese Late Night
            {
                "time_start": "01:00:00",
                "time_end": "02:00:00",
                "language_code": "C",
                "block_name": "Cantonese Late Night Block",
                "block_type": "General",
                "day_part": "Late Night",
                "display_order": 20,
            },
            # 2:00-3:00 AM: Mandarin News
            {
                "time_start": "02:00:00",
                "time_end": "03:00:00",
                "language_code": "M",
                "block_name": "Primetime News Late",
                "block_type": "News",
                "day_part": "Overnight",
                "display_order": 21,
            },
            # 3:00-4:00 AM: Mandarin Overnight
            {
                "time_start": "03:00:00",
                "time_end": "04:00:00",
                "language_code": "M",
                "block_name": "Mandarin Overnight Block 1",
                "block_type": "General",
                "day_part": "Overnight",
                "display_order": 22,
            },
            # 4:00-5:00 AM: Mandarin Overnight
            {
                "time_start": "04:00:00",
                "time_end": "05:00:00",
                "language_code": "M",
                "block_name": "Mandarin Overnight Block 2",
                "block_type": "General",
                "day_part": "Overnight",
                "display_order": 23,
            },
            # 5:00-6:00 AM: Mandarin Overnight
            {
                "time_start": "05:00:00",
                "time_end": "06:00:00",
                "language_code": "M",
                "block_name": "Mandarin Overnight Block 3",
                "block_type": "General",
                "day_part": "Overnight",
                "display_order": 24,
            },
        ]

        # Convert to database format
        blocks = []
        for schedule_block in daily_schedule:
            # Get language ID
            language_code = schedule_block["language_code"]
            if language_code not in self.language_mappings:
                raise Exception(
                    f"Language code '{language_code}' not found in mappings"
                )

            language_id = self.language_mappings[language_code]["id"]

            block = {
                "day_of_week": day_of_week,
                "time_start": schedule_block["time_start"],
                "time_end": schedule_block["time_end"],
                "language_id": language_id,
                "block_name": schedule_block["block_name"],
                "block_type": schedule_block["block_type"],
                "day_part": schedule_block["day_part"],
                "display_order": schedule_block["display_order"],
            }
            blocks.append(block)

        return blocks

    def validate_coverage(self) -> Dict[str, Any]:
        """Validate complete coverage with no gaps or overlaps."""
        result = {
            "success": True,
            "total_blocks": 0,
            "coverage_by_day": {},
            "errors": [],
        }

        try:
            with self.db.connect() as conn:
                # Check total blocks
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM language_blocks WHERE schedule_id = ?
                """,
                    (self.schedule_id,),
                )
                result["total_blocks"] = cursor.fetchone()[0]

                # Expected: 168 blocks (24 per day × 7 days)
                expected_total = 168
                if result["total_blocks"] != expected_total:
                    result["success"] = False
                    result["errors"].append(
                        f"Expected {expected_total} blocks, found {result['total_blocks']}"
                    )

                # Check coverage by day
                days_of_week = [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ]

                for day in days_of_week:
                    day_coverage = self._validate_day_coverage(conn, day)
                    result["coverage_by_day"][day] = day_coverage

                    if not day_coverage["success"]:
                        result["success"] = False
                        result["errors"].extend(day_coverage["errors"])

                # Check for language distribution
                language_distribution = self._validate_language_distribution(conn)
                if not language_distribution["success"]:
                    result["success"] = False
                    result["errors"].extend(language_distribution["errors"])

                result["language_distribution"] = language_distribution

        except Exception as e:
            result["success"] = False
            result["errors"].append(f"Validation error: {str(e)}")

        return result

    def _validate_day_coverage(self, conn, day_of_week: str) -> Dict[str, Any]:
        """Validate coverage for a specific day."""
        result = {
            "success": True,
            "blocks_count": 0,
            "time_coverage": "0 hours",
            "errors": [],
        }

        try:
            # Get blocks for this day
            cursor = conn.execute(
                """
                SELECT time_start, time_end, block_name 
                FROM language_blocks 
                WHERE schedule_id = ? AND day_of_week = ?
                ORDER BY time_start
            """,
                (self.schedule_id, day_of_week),
            )

            blocks = cursor.fetchall()
            result["blocks_count"] = len(blocks)

            if len(blocks) != 24:
                result["success"] = False
                result["errors"].append(
                    f"{day_of_week}: Expected 24 blocks, found {len(blocks)}"
                )
                return result

            # Check for 24-hour coverage
            total_minutes = 0

            for time_start, time_end, block_name in blocks:
                start_minutes = self._time_to_minutes(time_start)
                end_minutes = self._time_to_minutes(time_end)

                # Handle midnight rollover for overnight blocks
                if start_minutes == 0:  # Midnight blocks
                    if end_minutes <= 360:  # 00:00-06:00 range
                        total_minutes += end_minutes
                    else:
                        result["success"] = False
                        result["errors"].append(
                            f"{day_of_week}: Invalid overnight time range for {block_name}"
                        )
                elif start_minutes >= end_minutes:
                    result["success"] = False
                    result["errors"].append(
                        f"{day_of_week}: Invalid time range for {block_name}"
                    )
                else:
                    total_minutes += end_minutes - start_minutes

            result["time_coverage"] = f"{total_minutes // 60} hours"

            # Should cover full 24 hours (1440 minutes)
            if total_minutes != 1440:
                result["success"] = False
                result["errors"].append(
                    f"{day_of_week}: Coverage is {total_minutes // 60} hours, expected 24 hours"
                )

        except Exception as e:
            result["success"] = False
            result["errors"].append(f"{day_of_week} validation error: {str(e)}")

        return result

    def _validate_language_distribution(self, conn) -> Dict[str, Any]:
        """Validate language distribution across all blocks."""
        result = {"success": True, "distribution": {}, "errors": []}

        try:
            cursor = conn.execute(
                """
                SELECT l.language_code, l.language_name, COUNT(lb.block_id) as block_count
                FROM language_blocks lb
                JOIN languages l ON lb.language_id = l.language_id
                WHERE lb.schedule_id = ?
                GROUP BY l.language_code, l.language_name
                ORDER BY block_count DESC
            """,
                (self.schedule_id,),
            )

            for language_code, language_name, block_count in cursor.fetchall():
                result["distribution"][language_code] = {
                    "name": language_name,
                    "blocks": block_count,
                    "percentage": round((block_count / 168) * 100, 1),
                }

            # Expected distribution based on Dallas visual schedule
            # Daily blocks per language × 7 days
            expected = {
                "M": 119,  # Mandarin: 17 blocks per day × 7 days = 119 blocks
                "C": 21,  # Cantonese: 3 blocks per day × 7 days = 21 blocks
                "K": 14,  # Korean: 2 blocks per day × 7 days = 14 blocks
                "J": 7,  # Japanese: 1 block per day × 7 days = 7 blocks
                "E": 7,  # English: 1 block per day × 7 days = 7 blocks
            }

            for lang_code, expected_count in expected.items():
                if lang_code not in result["distribution"]:
                    result["success"] = False
                    result["errors"].append(f"Language {lang_code} not found in blocks")
                elif result["distribution"][lang_code]["blocks"] != expected_count:
                    actual_count = result["distribution"][lang_code]["blocks"]
                    result["success"] = False
                    result["errors"].append(
                        f"Language {lang_code}: expected {expected_count} blocks, found {actual_count}"
                    )

        except Exception as e:
            result["success"] = False
            result["errors"].append(f"Language distribution validation error: {str(e)}")

        return result

    def _time_to_minutes(self, time_str: str) -> int:
        """Convert time string to minutes since midnight."""
        try:
            # Handle 24:00:00 as end of day (1440 minutes)
            if time_str == "24:00:00":
                return 1440

            parts = time_str.split(":")
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except Exception:
            return 0

    def get_population_summary(self) -> Dict[str, Any]:
        """Get summary of current Dallas Grid population."""
        summary = {
            "schedule_name": "Dallas Grid",
            "schedule_id": self.schedule_id,
            "total_blocks": 0,
            "days_with_blocks": 0,
            "language_distribution": {},
            "market_coverage": 0,
        }

        try:
            with self.db.connect() as conn:
                # Total blocks
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM language_blocks WHERE schedule_id = ?
                """,
                    (self.schedule_id,),
                )
                summary["total_blocks"] = cursor.fetchone()[0]

                # Days with blocks
                cursor = conn.execute(
                    """
                    SELECT COUNT(DISTINCT day_of_week) FROM language_blocks WHERE schedule_id = ?
                """,
                    (self.schedule_id,),
                )
                summary["days_with_blocks"] = cursor.fetchone()[0]

                # Language distribution
                cursor = conn.execute(
                    """
                    SELECT l.language_code, l.language_name, COUNT(lb.block_id) as block_count
                    FROM language_blocks lb
                    JOIN languages l ON lb.language_id = l.language_id
                    WHERE lb.schedule_id = ?
                    GROUP BY l.language_code, l.language_name
                """,
                    (self.schedule_id,),
                )

                for language_code, language_name, block_count in cursor.fetchall():
                    summary["language_distribution"][language_code] = {
                        "name": language_name,
                        "blocks": block_count,
                    }

                # Market coverage
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) FROM schedule_market_assignments WHERE schedule_id = ?
                """,
                    (self.schedule_id,),
                )
                summary["market_coverage"] = cursor.fetchone()[0]

        except Exception as e:
            logging.error(f"Error getting population summary: {e}")

        return summary


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
            print("DALLAS GRID POPULATION SUMMARY")
            print(f"{'=' * 60}")
            print(f"Total Blocks: {summary['total_blocks']}")
            print(f"Days with Blocks: {summary['days_with_blocks']}")
            print(f"Markets Covered: {summary['market_coverage']}")

            if summary["language_distribution"]:
                print("\nLanguage Distribution:")
                for lang_code, info in summary["language_distribution"].items():
                    print(f"  {lang_code} ({info['name']}): {info['blocks']} blocks")

        elif args.validate_only:
            # Validate existing population
            print(f"\n{'=' * 60}")
            print("VALIDATING DALLAS GRID POPULATION")
            print(f"{'=' * 60}")

            validation_result = populator.validate_coverage()

            if validation_result["success"]:
                print("✅ Validation successful!")
                print(f"   Total blocks: {validation_result['total_blocks']}")

                if "language_distribution" in validation_result:
                    print("   Language distribution:")
                    for lang_code, info in validation_result["language_distribution"][
                        "distribution"
                    ].items():
                        print(
                            f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)"
                        )
            else:
                print("❌ Validation failed!")
                for error in validation_result["errors"]:
                    print(f"   • {error}")

        else:
            # Populate Dallas Grid
            print(f"\n{'=' * 60}")
            print("DALLAS GRID LANGUAGE BLOCK POPULATION")
            print(f"{'=' * 60}")

            result = populator.populate_dallas_grid_blocks()

            if result["success"]:
                print("✅ Population successful!")
                print(f"   Blocks created: {result['blocks_created']}")
                print(f"   Days processed: {result['days_processed']}")

                if result["validation_result"]:
                    val_result = result["validation_result"]
                    print(
                        f"   Validation: {'✅ Passed' if val_result['success'] else '❌ Failed'}"
                    )

                    if "language_distribution" in val_result:
                        print("   Language distribution:")
                        for lang_code, info in val_result["language_distribution"][
                            "distribution"
                        ].items():
                            print(
                                f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)"
                            )
            else:
                print("❌ Population failed!")
                for error in result["errors"]:
                    print(f"   • {error}")

    finally:
        db_connection.close()


if __name__ == "__main__":
    main()
