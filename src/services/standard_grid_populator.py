#!/usr/bin/env python3
"""
Standard Grid Language Block Populator - WEEKDAY vs WEEKEND SCHEDULING
Populates the Standard Grid with different schedules for weekdays vs weekends.
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection
from services.base_service import BaseService

class StandardGridPopulator(BaseService):
    """Populates Standard Grid with language blocks with weekday/weekend differentiation."""
    
    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)
        self.schedule_id = 1  # Standard Grid
        self.language_mappings = {}
        self._load_language_mappings()
    
    def _load_language_mappings(self):
        """Load language ID mappings from database."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute("SELECT language_id, language_code, language_name FROM languages")
                for row in cursor.fetchall():
                    language_id, language_code, language_name = row
                    self.language_mappings[language_code] = {
                        'id': language_id,
                        'name': language_name
                    }
            
            logging.info(f"Loaded {len(self.language_mappings)} language mappings")
            
        except Exception as e:
            logging.error(f"Failed to load language mappings: {e}")
            raise
    
    def populate_standard_grid_blocks(self) -> Dict[str, Any]:
        """Populate Standard Grid with language blocks for all 7 days."""
        start_time = datetime.now()
        result = {
            'success': False,
            'blocks_created': 0,
            'days_processed': 0,
            'errors': [],
            'validation_result': None
        }
        
        logging.info("Starting Standard Grid language block population...")
        
        try:
            # Check if Standard Grid exists
            if not self._verify_standard_grid_exists():
                raise Exception("Standard Grid (schedule_id=1) not found")
            
            # Clear existing blocks for Standard Grid
            blocks_cleared = self._clear_existing_blocks()
            if blocks_cleared > 0:
                logging.info(f"Cleared {blocks_cleared} existing Standard Grid blocks")
            
            days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            
            with self.db.transaction() as conn:
                total_blocks = 0
                
                for day in days_of_week:
                    daily_blocks = self._create_daily_blocks(day)
                    
                    # Insert blocks for this day
                    for block in daily_blocks:
                        conn.execute("""
                            INSERT INTO language_blocks (
                                schedule_id, day_of_week, time_start, time_end,
                                language_id, block_name, block_type, day_part, display_order
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            self.schedule_id,
                            block['day_of_week'],
                            block['time_start'],
                            block['time_end'],
                            block['language_id'],
                            block['block_name'],
                            block['block_type'],
                            block['day_part'],
                            block['display_order']
                        ))
                        total_blocks += 1
                    
                    result['days_processed'] += 1
                    logging.info(f"Created {len(daily_blocks)} blocks for {day.title()}")
                
                result['blocks_created'] = total_blocks
                logging.info(f"Successfully created {total_blocks} language blocks")
            
            # Validate the population
            validation_result = self.validate_coverage()
            result['validation_result'] = validation_result
            
            if not validation_result['success']:
                result['errors'].extend(validation_result['errors'])
                raise Exception("Coverage validation failed")
            
            result['success'] = True
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f"Standard Grid population completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            error_msg = f"Standard Grid population failed: {str(e)}"
            result['errors'].append(error_msg)
            logging.error(error_msg)
        
        return result
    
    def _verify_standard_grid_exists(self) -> bool:
        """Verify Standard Grid exists in database."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM programming_schedules 
                    WHERE schedule_id = ? AND schedule_name = 'Standard Grid' AND is_active = 1
                """, (self.schedule_id,))
                return cursor.fetchone()[0] > 0
        except Exception as e:
            logging.error(f"Error verifying Standard Grid: {e}")
            return False
    
    def _clear_existing_blocks(self) -> int:
        """Clear existing language blocks for Standard Grid."""
        try:
            with self.db.connect() as conn:
                cursor = conn.execute("""
                    DELETE FROM language_blocks WHERE schedule_id = ?
                """, (self.schedule_id,))
                return cursor.rowcount
        except Exception as e:
            logging.error(f"Error clearing existing blocks: {e}")
            return 0
    
    def _create_daily_blocks(self, day_of_week: str) -> List[Dict]:
        """Create language blocks for a specific day with weekday/weekend differentiation."""
        
        # Determine if this is a weekday or weekend
        is_weekend = day_of_week in ['saturday', 'sunday']
        
        if is_weekend:
            # WEEKEND SCHEDULE (Saturday & Sunday)
            daily_schedule = [
                # 6:00-6:30 AM: English Religious (Kingdom of God)
                {
                    'time_start': '06:00:00',
                    'time_end': '06:30:00',
                    'language_code': 'E',
                    'block_name': 'English Religious Programming',
                    'block_type': 'Religious',
                    'day_part': 'Early Morning',
                    'display_order': 1
                },
                # 6:30-8:00 AM: Mandarin Morning
                {
                    'time_start': '06:30:00',
                    'time_end': '08:00:00',
                    'language_code': 'M',
                    'block_name': 'Mandarin Morning Block',
                    'block_type': 'General',
                    'day_part': 'Early Morning',
                    'display_order': 2
                },
                # 8:00-10:00 AM: Korean Block
                {
                    'time_start': '08:00:00',
                    'time_end': '10:00:00',
                    'language_code': 'K',
                    'block_name': 'Korean Morning Block',
                    'block_type': 'News',
                    'day_part': 'Morning',
                    'display_order': 3
                },
                # 10:00-11:00 AM: Japanese Block
                {
                    'time_start': '10:00:00',
                    'time_end': '11:00:00',
                    'language_code': 'J',
                    'block_name': 'Japanese Morning Block',
                    'block_type': 'General',
                    'day_part': 'Morning',
                    'display_order': 4
                },
                # 11:00 AM-1:00 PM: Vietnamese Block
                {
                    'time_start': '11:00:00',
                    'time_end': '13:00:00',
                    'language_code': 'V',
                    'block_name': 'Vietnamese Midday Block',
                    'block_type': 'General',
                    'day_part': 'Midday',
                    'display_order': 5
                },
                # 1:00-4:00 PM: South Asian Block
                {
                    'time_start': '13:00:00',
                    'time_end': '16:00:00',
                    'language_code': 'SA',
                    'block_name': 'South Asian Afternoon Block',
                    'block_type': 'General',
                    'day_part': 'Afternoon',
                    'display_order': 6
                },
                # 4:00-6:00 PM: Filipino Block
                {
                    'time_start': '16:00:00',
                    'time_end': '18:00:00',
                    'language_code': 'T',
                    'block_name': 'Filipino Afternoon Block',
                    'block_type': 'General',
                    'day_part': 'Afternoon',
                    'display_order': 7
                },
                # 6:00-7:00 PM: Hmong Block (WEEKEND ONLY!)
                {
                    'time_start': '18:00:00',
                    'time_end': '19:00:00',
                    'language_code': 'Hm',
                    'block_name': 'Hmong Weekend Block',
                    'block_type': 'General',
                    'day_part': 'Early Evening',
                    'display_order': 8
                },
                # 7:00-8:00 PM: Cantonese Block
                {
                    'time_start': '19:00:00',
                    'time_end': '20:00:00',
                    'language_code': 'C',
                    'block_name': 'Cantonese Evening Block',
                    'block_type': 'General',
                    'day_part': 'Early Evening',
                    'display_order': 9
                },
                # 8:00-11:30 PM: Mandarin Prime Block
                {
                    'time_start': '20:00:00',
                    'time_end': '23:30:00',
                    'language_code': 'M',
                    'block_name': 'Mandarin Prime Block',
                    'block_type': 'Prime',
                    'day_part': 'Prime',
                    'display_order': 10
                },
                # 11:30 PM-12:00 AM: Cantonese Late Night
                {
                    'time_start': '23:30:00',
                    'time_end': '24:00:00',
                    'language_code': 'C',
                    'block_name': 'Cantonese Late Night Block',
                    'block_type': 'General',
                    'day_part': 'Late Night',
                    'display_order': 11
                },
                # 12:00-6:00 AM: English Shopping Block
                {
                    'time_start': '00:00:00',
                    'time_end': '06:00:00',
                    'language_code': 'E',
                    'block_name': 'English Shopping Block',
                    'block_type': 'Shopping',
                    'day_part': 'Overnight',
                    'display_order': 12
                }
            ]
        else:
            # WEEKDAY SCHEDULE (Monday - Friday)
            daily_schedule = [
                # 6:00-8:00 AM: Mandarin Morning Block (Full 2 hours)
                {
                    'time_start': '06:00:00',
                    'time_end': '08:00:00',
                    'language_code': 'M',
                    'block_name': 'Mandarin Morning Block',
                    'block_type': 'News',
                    'day_part': 'Early Morning',
                    'display_order': 1
                },
                # 8:00-10:00 AM: Korean Block
                {
                    'time_start': '08:00:00',
                    'time_end': '10:00:00',
                    'language_code': 'K',
                    'block_name': 'Korean Morning Block',
                    'block_type': 'News',
                    'day_part': 'Morning',
                    'display_order': 2
                },
                # 10:00-11:00 AM: Japanese Block
                {
                    'time_start': '10:00:00',
                    'time_end': '11:00:00',
                    'language_code': 'J',
                    'block_name': 'Japanese Morning Block',
                    'block_type': 'General',
                    'day_part': 'Morning',
                    'display_order': 3
                },
                # 11:00 AM-1:00 PM: Vietnamese Block
                {
                    'time_start': '11:00:00',
                    'time_end': '13:00:00',
                    'language_code': 'V',
                    'block_name': 'Vietnamese Midday Block',
                    'block_type': 'General',
                    'day_part': 'Midday',
                    'display_order': 4
                },
                # 1:00-4:00 PM: South Asian Block
                {
                    'time_start': '13:00:00',
                    'time_end': '16:00:00',
                    'language_code': 'SA',
                    'block_name': 'South Asian Afternoon Block',
                    'block_type': 'General',
                    'day_part': 'Afternoon',
                    'display_order': 5
                },
                # 4:00-6:00 PM: Filipino Block
                {
                    'time_start': '16:00:00',
                    'time_end': '18:00:00',
                    'language_code': 'T',
                    'block_name': 'Filipino Afternoon Block',
                    'block_type': 'General',
                    'day_part': 'Afternoon',
                    'display_order': 6
                },
                # 6:00-7:00 PM: Gap/Different Programming (NO HMONG on weekdays)
                # Based on visual schedule, this appears to be extended Filipino or other content
                {
                    'time_start': '18:00:00',
                    'time_end': '19:00:00',
                    'language_code': 'T',
                    'block_name': 'Filipino Extended Block',
                    'block_type': 'General',
                    'day_part': 'Early Evening',
                    'display_order': 7
                },
                # 7:00-8:00 PM: Cantonese Block
                {
                    'time_start': '19:00:00',
                    'time_end': '20:00:00',
                    'language_code': 'C',
                    'block_name': 'Cantonese Evening Block',
                    'block_type': 'General',
                    'day_part': 'Early Evening',
                    'display_order': 8
                },
                # 8:00-11:30 PM: Mandarin Prime Block
                {
                    'time_start': '20:00:00',
                    'time_end': '23:30:00',
                    'language_code': 'M',
                    'block_name': 'Mandarin Prime Block',
                    'block_type': 'Prime',
                    'day_part': 'Prime',
                    'display_order': 9
                },
                # 11:30 PM-12:00 AM: Cantonese Late Night
                {
                    'time_start': '23:30:00',
                    'time_end': '24:00:00',
                    'language_code': 'C',
                    'block_name': 'Cantonese Late Night Block',
                    'block_type': 'General',
                    'day_part': 'Late Night',
                    'display_order': 10
                },
                # 12:00-6:00 AM: English Shopping Block
                {
                    'time_start': '00:00:00',
                    'time_end': '06:00:00',
                    'language_code': 'E',
                    'block_name': 'English Shopping Block',
                    'block_type': 'Shopping',
                    'day_part': 'Overnight',
                    'display_order': 11
                }
            ]
        
        # Convert to database format
        blocks = []
        for schedule_block in daily_schedule:
            # Get language ID
            language_code = schedule_block['language_code']
            if language_code not in self.language_mappings:
                raise Exception(f"Language code '{language_code}' not found in mappings")
            
            language_id = self.language_mappings[language_code]['id']
            
            block = {
                'day_of_week': day_of_week,
                'time_start': schedule_block['time_start'],
                'time_end': schedule_block['time_end'],
                'language_id': language_id,
                'block_name': schedule_block['block_name'],
                'block_type': schedule_block['block_type'],
                'day_part': schedule_block['day_part'],
                'display_order': schedule_block['display_order']
            }
            blocks.append(block)
        
        return blocks
    
    def validate_coverage(self) -> Dict[str, Any]:
        """Validate complete coverage with no gaps or overlaps."""
        result = {
            'success': True,
            'total_blocks': 0,
            'coverage_by_day': {},
            'errors': []
        }
        
        try:
            with self.db.connect() as conn:
                # Check total blocks
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM language_blocks WHERE schedule_id = ?
                """, (self.schedule_id,))
                result['total_blocks'] = cursor.fetchone()[0]
                
                # Expected: Weekdays (11 blocks × 5 days) + Weekends (12 blocks × 2 days) = 55 + 24 = 79 blocks
                expected_total = 79
                if result['total_blocks'] != expected_total:
                    result['success'] = False
                    result['errors'].append(f"Expected {expected_total} blocks, found {result['total_blocks']}")
                
                # Check coverage by day
                days_of_week = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                
                for day in days_of_week:
                    day_coverage = self._validate_day_coverage(conn, day)
                    result['coverage_by_day'][day] = day_coverage
                    
                    if not day_coverage['success']:
                        result['success'] = False
                        result['errors'].extend(day_coverage['errors'])
                
                # Check for language distribution
                language_distribution = self._validate_language_distribution(conn)
                if not language_distribution['success']:
                    result['success'] = False
                    result['errors'].extend(language_distribution['errors'])
                
                result['language_distribution'] = language_distribution
                
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Validation error: {str(e)}")
        
        return result
    
    def _validate_day_coverage(self, conn, day_of_week: str) -> Dict[str, Any]:
        """Validate coverage for a specific day."""
        is_weekend = day_of_week in ['saturday', 'sunday']
        expected_blocks = 12 if is_weekend else 11
        
        result = {
            'success': True,
            'blocks_count': 0,
            'time_coverage': '0 hours',
            'errors': []
        }
        
        try:
            # Get blocks for this day
            cursor = conn.execute("""
                SELECT time_start, time_end, block_name 
                FROM language_blocks 
                WHERE schedule_id = ? AND day_of_week = ?
                ORDER BY time_start
            """, (self.schedule_id, day_of_week))
            
            blocks = cursor.fetchall()
            result['blocks_count'] = len(blocks)
            
            if len(blocks) != expected_blocks:
                result['success'] = False
                result['errors'].append(f"{day_of_week}: Expected {expected_blocks} blocks, found {len(blocks)}")
                return result
            
            # Check for 24-hour coverage
            total_minutes = 0
            
            for time_start, time_end, block_name in blocks:
                start_minutes = self._time_to_minutes(time_start)
                end_minutes = self._time_to_minutes(time_end)
                
                # Handle midnight rollover for overnight block
                if start_minutes == 0 and end_minutes == 360:  # 00:00-06:00
                    total_minutes += 360  # 6 hours
                elif start_minutes >= end_minutes:
                    result['success'] = False
                    result['errors'].append(f"{day_of_week}: Invalid time range for {block_name}")
                else:
                    total_minutes += (end_minutes - start_minutes)
            
            result['time_coverage'] = f"{total_minutes // 60} hours"
            
            # Should cover full 24 hours (1440 minutes)
            if total_minutes != 1440:
                result['success'] = False
                result['errors'].append(f"{day_of_week}: Coverage is {total_minutes//60} hours, expected 24 hours")
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"{day_of_week} validation error: {str(e)}")
        
        return result
    
    def _validate_language_distribution(self, conn) -> Dict[str, Any]:
        """Validate language distribution across all blocks."""
        result = {
            'success': True,
            'distribution': {},
            'errors': []
        }
        
        try:
            cursor = conn.execute("""
                SELECT l.language_code, l.language_name, COUNT(lb.block_id) as block_count
                FROM language_blocks lb
                JOIN languages l ON lb.language_id = l.language_id
                WHERE lb.schedule_id = ?
                GROUP BY l.language_code, l.language_name
                ORDER BY block_count DESC
            """, (self.schedule_id,))
            
            for language_code, language_name, block_count in cursor.fetchall():
                result['distribution'][language_code] = {
                    'name': language_name,
                    'blocks': block_count,
                    'percentage': round((block_count / 79) * 100, 1)
                }
            
            # Expected distribution based on actual visual schedule with weekday/weekend differences
            expected = {
                'M': 14,  # Mandarin: 2 blocks per day × 7 days = 14 blocks (morning + prime)
                'K': 7,   # Korean: 1 block per day × 7 days = 7 blocks
                'J': 7,   # Japanese: 1 block per day × 7 days = 7 blocks
                'V': 7,   # Vietnamese: 1 block per day × 7 days = 7 blocks
                'SA': 7,  # South Asian: 1 block per day × 7 days = 7 blocks
                'T': 12,  # Tagalog/Filipino: 2 blocks per weekday × 5 + 1 block per weekend × 2 = 12 blocks
                'Hm': 2,  # Hmong: 1 block per weekend day × 2 days = 2 blocks (WEEKEND ONLY)
                'C': 14,  # Cantonese: 2 blocks per day × 7 days = 14 blocks
                'E': 9    # English: 1 overnight + 0.5 weekend religious × 2 = 7 + 2 = 9 blocks
            }
            
            for lang_code, expected_count in expected.items():
                if lang_code not in result['distribution']:
                    result['success'] = False
                    result['errors'].append(f"Language {lang_code} not found in blocks")
                elif result['distribution'][lang_code]['blocks'] != expected_count:
                    actual_count = result['distribution'][lang_code]['blocks']
                    result['success'] = False
                    result['errors'].append(f"Language {lang_code}: expected {expected_count} blocks, found {actual_count}")
            
        except Exception as e:
            result['success'] = False
            result['errors'].append(f"Language distribution validation error: {str(e)}")
        
        return result
    
    def _time_to_minutes(self, time_str: str) -> int:
        """Convert time string to minutes since midnight."""
        try:
            if time_str == '23:59:59':
                return 1439
            
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except Exception:
            return 0
    
    def get_population_summary(self) -> Dict[str, Any]:
        """Get summary of current Standard Grid population."""
        summary = {
            'schedule_name': 'Standard Grid',
            'schedule_id': self.schedule_id,
            'total_blocks': 0,
            'days_with_blocks': 0,
            'language_distribution': {},
            'market_coverage': 0
        }
        
        try:
            with self.db.connect() as conn:
                # Total blocks
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM language_blocks WHERE schedule_id = ?
                """, (self.schedule_id,))
                summary['total_blocks'] = cursor.fetchone()[0]
                
                # Days with blocks
                cursor = conn.execute("""
                    SELECT COUNT(DISTINCT day_of_week) FROM language_blocks WHERE schedule_id = ?
                """, (self.schedule_id,))
                summary['days_with_blocks'] = cursor.fetchone()[0]
                
                # Language distribution
                cursor = conn.execute("""
                    SELECT l.language_code, l.language_name, COUNT(lb.block_id) as block_count
                    FROM language_blocks lb
                    JOIN languages l ON lb.language_id = l.language_id
                    WHERE lb.schedule_id = ?
                    GROUP BY l.language_code, l.language_name
                """, (self.schedule_id,))
                
                for language_code, language_name, block_count in cursor.fetchall():
                    summary['language_distribution'][language_code] = {
                        'name': language_name,
                        'blocks': block_count
                    }
                
                # Market coverage
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM schedule_market_assignments WHERE schedule_id = ?
                """, (self.schedule_id,))
                summary['market_coverage'] = cursor.fetchone()[0]
        
        except Exception as e:
            logging.error(f"Error getting population summary: {e}")
        
        return summary

def main():
    """CLI interface for Standard Grid population."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Populate Standard Grid Language Blocks")
    parser.add_argument("--db-path", default="data/database/production.db", help="Database path")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--summary-only", action="store_true", help="Show summary without populating")
    parser.add_argument("--validate-only", action="store_true", help="Validate existing population")
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Initialize service
    db_connection = DatabaseConnection(args.db_path)
    populator = StandardGridPopulator(db_connection)
    
    try:
        if args.summary_only:
            # Show current population summary
            summary = populator.get_population_summary()
            print(f"\n{'='*60}")
            print(f"STANDARD GRID POPULATION SUMMARY")
            print(f"{'='*60}")
            print(f"Total Blocks: {summary['total_blocks']}")
            print(f"Days with Blocks: {summary['days_with_blocks']}")
            print(f"Markets Covered: {summary['market_coverage']}")
            
            if summary['language_distribution']:
                print(f"\nLanguage Distribution:")
                for lang_code, info in summary['language_distribution'].items():
                    print(f"  {lang_code} ({info['name']}): {info['blocks']} blocks")
            
        elif args.validate_only:
            # Validate existing population
            print(f"\n{'='*60}")
            print(f"VALIDATING STANDARD GRID POPULATION")
            print(f"{'='*60}")
            
            validation_result = populator.validate_coverage()
            
            if validation_result['success']:
                print(f"✅ Validation successful!")
                print(f"   Total blocks: {validation_result['total_blocks']}")
                
                if 'language_distribution' in validation_result:
                    print(f"   Language distribution:")
                    for lang_code, info in validation_result['language_distribution']['distribution'].items():
                        print(f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)")
            else:
                print(f"❌ Validation failed!")
                for error in validation_result['errors']:
                    print(f"   • {error}")
        
        else:
            # Populate Standard Grid
            print(f"\n{'='*60}")
            print(f"STANDARD GRID LANGUAGE BLOCK POPULATION")
            print(f"{'='*60}")
            
            result = populator.populate_standard_grid_blocks()
            
            if result['success']:
                print(f"✅ Population successful!")
                print(f"   Blocks created: {result['blocks_created']}")
                print(f"   Days processed: {result['days_processed']}")
                
                if result['validation_result']:
                    val_result = result['validation_result']
                    print(f"   Validation: {'✅ Passed' if val_result['success'] else '❌ Failed'}")
                    
                    if 'language_distribution' in val_result:
                        print(f"   Language distribution:")
                        for lang_code, info in val_result['language_distribution']['distribution'].items():
                            print(f"     {lang_code}: {info['blocks']} blocks ({info['percentage']}%)")
            else:
                print(f"❌ Population failed!")
                for error in result['errors']:
                    print(f"   • {error}")
    
    finally:
        db_connection.close()

if __name__ == "__main__":
    main()