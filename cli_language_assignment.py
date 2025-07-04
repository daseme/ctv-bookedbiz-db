#!/usr/bin/env python3
"""
Standalone Language Block Assignment Script
===========================================

This script includes the Language Block Service directly to avoid import issues.
Run this to assign your 338K unassigned 2025 spots to language blocks!

Usage:
    python cli_language_assignment.py --test 10           # Test with 10 spots
    python cli_language_assignment.py --batch 1000        # Assign 1000 spots
    python cli_language_assignment.py --all-2025          # Assign all unassigned 2025 spots
    python cli_language_assignment.py --all-2024          # Assign all unassigned 2024 spots
    python cli_language_assignment.py --status            # Show assignment status by year
"""

import sqlite3
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class CustomerIntent(Enum):
    """Customer intent classification for spot placement"""
    LANGUAGE_SPECIFIC = "language_specific"  # Single block, language-targeted
    TIME_SPECIFIC = "time_specific"          # Multi-block, same day-part
    INDIFFERENT = "indifferent"              # Multi-block, customer flexible
    NO_GRID_COVERAGE = "no_grid_coverage"    # Market has no programming grid


class AssignmentMethod(Enum):
    """Method used for spot assignment"""
    AUTO_COMPUTED = "auto_computed"
    MANUAL_OVERRIDE = "manual_override"
    NO_GRID_AVAILABLE = "no_grid_available"


@dataclass
class SpotData:
    """Simplified spot data structure"""
    spot_id: int
    market_id: int
    air_date: str
    day_of_week: str
    time_in: str
    time_out: str
    language_id: Optional[int] = None
    customer_id: Optional[int] = None


@dataclass
class LanguageBlock:
    """Language block data structure"""
    block_id: int
    schedule_id: int
    day_of_week: str
    time_start: str
    time_end: str
    language_id: int
    block_name: str
    block_type: str
    day_part: str


@dataclass
class AssignmentResult:
    """Result of spot assignment"""
    spot_id: int
    success: bool
    schedule_id: Optional[int] = None
    block_id: Optional[int] = None
    customer_intent: Optional[CustomerIntent] = None
    spans_multiple_blocks: bool = False
    blocks_spanned: Optional[List[int]] = None
    primary_block_id: Optional[int] = None
    requires_attention: bool = False
    alert_reason: Optional[str] = None
    error_message: Optional[str] = None


class LanguageBlockService:
    """Language Block Assignment Service"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Simple stats tracking
        self.stats = {
            'processed': 0,
            'assigned': 0,
            'no_coverage': 0,
            'multi_block': 0,
            'errors': 0
        }
    
    def assign_single_spot(self, spot_id: int) -> AssignmentResult:
        """Assign a single spot to appropriate language block(s)"""
        try:
            # Step 1: Get spot data
            spot_data = self._get_spot_data(spot_id)
            if not spot_data:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=False,
                    error_message="Spot not found or invalid"
                )
            
            # Step 2: Check if spot has market assignment
            if not spot_data.market_id:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="Spot has no market assignment"
                )
            
            # Step 3: Find applicable programming schedule
            schedule_id = self._get_applicable_schedule(spot_data.market_id, spot_data.air_date)
            if not schedule_id:
                result = AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No programming grid for market"
                )
                self.stats['no_coverage'] += 1
                return result
            
            # Step 4: Find overlapping language blocks
            blocks = self._get_overlapping_blocks(
                schedule_id, spot_data.day_of_week, spot_data.time_in, spot_data.time_out
            )
            
            if not blocks:
                result = AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No language blocks cover spot time"
                )
                self._save_assignment(result)
                self.stats['no_coverage'] += 1
                return result
            
            # Step 5: Analyze intent and create assignment
            result = self._create_assignment(spot_data, schedule_id, blocks)
            self._save_assignment(result)
            
            # Update stats
            if result.spans_multiple_blocks:
                self.stats['multi_block'] += 1
            else:
                self.stats['assigned'] += 1
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error assigning spot {spot_id}: {e}")
            self.stats['errors'] += 1
            return AssignmentResult(
                spot_id=spot_id,
                success=False,
                error_message=str(e)
            )
        finally:
            self.stats['processed'] += 1
    
    def assign_spots_batch(self, spot_ids: List[int] = None, limit: int = None) -> Dict[str, Any]:
        """Assign multiple spots to language blocks"""
        self.logger.info("Starting batch spot assignment")
        
        # Reset stats
        self.stats = {
            'processed': 0,
            'assigned': 0,
            'no_coverage': 0,
            'multi_block': 0,
            'errors': 0
        }
        
        try:
            # Get spots to process
            if spot_ids:
                spots_to_process = spot_ids
            else:
                spots_to_process = self._get_unassigned_spot_ids(limit)
            
            self.logger.info(f"Processing {len(spots_to_process)} spots")
            
            # Process each spot
            for i, spot_id in enumerate(spots_to_process):
                try:
                    result = self.assign_single_spot(spot_id)
                    
                    # Log progress every 100 spots
                    if (i + 1) % 100 == 0:
                        self.logger.info(f"Processed {i + 1}/{len(spots_to_process)} spots...")
                        
                except Exception as e:
                    self.logger.error(f"Failed to process spot {spot_id}: {e}")
                    self.stats['errors'] += 1
            
            self.logger.info(f"Batch assignment completed: {self.stats}")
            return self.stats
            
        except Exception as e:
            self.logger.error(f"Error in batch assignment: {e}")
            self.stats['errors'] += 1
            return self.stats
    
    def test_assignment(self, limit: int = 10) -> Dict[str, Any]:
        """Test assignment with a small number of spots"""
        self.logger.info(f"Running test assignment with {limit} spots")
        
        # Get test spots
        test_spot_ids = self._get_unassigned_spot_ids(limit)
        
        if not test_spot_ids:
            return {
                'success': False,
                'message': 'No unassigned spots found for testing',
                'spots_tested': 0,
                'stats': self.stats,
                'success_rate': 0.0,
                'spot_details': []
            }
        
        # Run assignment
        results = self.assign_spots_batch(test_spot_ids)
        
        # Get detailed results for analysis
        test_results = {
            'success': True,
            'spots_tested': len(test_spot_ids),
            'stats': results,
            'success_rate': (results['assigned'] + results['no_coverage'] + results['multi_block']) / results['processed'] if results['processed'] > 0 else 0,
            'spot_details': []
        }
        
        # Get details for each assigned spot
        for spot_id in test_spot_ids:
            spot_details = self._get_assignment_details(spot_id)
            if spot_details:
                test_results['spot_details'].append(spot_details)
        
        return test_results
    
    def _get_spot_data(self, spot_id: int) -> Optional[SpotData]:
        """Get spot data from database"""
        cursor = self.db.cursor()
        
        query = """
        SELECT spot_id, market_id, air_date, day_of_week, time_in, time_out, 
               language_id, customer_id
        FROM spots
        WHERE spot_id = ?
          AND market_id IS NOT NULL
          AND time_in IS NOT NULL 
          AND time_out IS NOT NULL
          AND day_of_week IS NOT NULL
        """
        
        cursor.execute(query, (spot_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return SpotData(
            spot_id=row[0],
            market_id=row[1],
            air_date=row[2],
            day_of_week=row[3],
            time_in=row[4],
            time_out=row[5],
            language_id=row[6],
            customer_id=row[7]
        )
    
    def _get_applicable_schedule(self, market_id: int, air_date: str) -> Optional[int]:
        """Find applicable programming schedule for market and date"""
        cursor = self.db.cursor()
        
        # First try the original query
        query = """
        SELECT ps.schedule_id, ps.schedule_name
        FROM programming_schedules ps
        JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
        WHERE sma.market_id = ?
          AND DATE(sma.effective_start_date) <= DATE(?)
          AND (sma.effective_end_date IS NULL OR DATE(sma.effective_end_date) >= DATE(?))
          AND ps.is_active = 1
        ORDER BY sma.assignment_priority DESC, sma.effective_start_date DESC
        LIMIT 1
        """
        
        cursor.execute(query, (market_id, air_date, air_date))
        row = cursor.fetchone()
        
        if row:
            return row[0]
        
        # If no result, try fallback approach
        fallback_query = """
        SELECT ps.schedule_id, ps.schedule_name
        FROM programming_schedules ps
        JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
        WHERE sma.market_id = ? AND ps.is_active = 1
        ORDER BY sma.assignment_priority DESC
        LIMIT 1
        """
        
        cursor.execute(fallback_query, (market_id,))
        row = cursor.fetchone()
        
        if row:
            self.logger.debug(f"Using fallback schedule for market {market_id}")
            return row[0]
        
        return None
    
    def _get_overlapping_blocks(self, schedule_id: int, day_of_week: str, 
                              time_in: str, time_out: str) -> List[LanguageBlock]:
        """Find language blocks that overlap with spot time"""
        cursor = self.db.cursor()
        
        query = """
        SELECT block_id, schedule_id, day_of_week, time_start, time_end,
               language_id, block_name, block_type, day_part
        FROM language_blocks
        WHERE schedule_id = ?
          AND LOWER(day_of_week) = LOWER(?)
          AND is_active = 1
        ORDER BY time_start
        """
        
        cursor.execute(query, (schedule_id, day_of_week))
        rows = cursor.fetchall()
        
        # Check for time overlap
        overlapping_blocks = []
        spot_start_minutes = self._time_to_minutes(time_in)
        spot_end_minutes = self._time_to_minutes(time_out)
        
        for row in rows:
            block_start_minutes = self._time_to_minutes(row[3])
            block_end_minutes = self._time_to_minutes(row[4])
            
            # Check for overlap
            if self._times_overlap(
                spot_start_minutes, spot_end_minutes,
                block_start_minutes, block_end_minutes
            ):
                block = LanguageBlock(
                    block_id=row[0],
                    schedule_id=row[1],
                    day_of_week=row[2],
                    time_start=row[3],
                    time_end=row[4],
                    language_id=row[5],
                    block_name=row[6],
                    block_type=row[7],
                    day_part=row[8]
                )
                overlapping_blocks.append(block)
        
        return overlapping_blocks
    
    def _create_assignment(self, spot: SpotData, schedule_id: int, 
                          blocks: List[LanguageBlock]) -> AssignmentResult:
        """Create assignment based on spot and overlapping blocks"""
        
        if len(blocks) == 1:
            # Single block assignment
            block = blocks[0]
            intent = self._analyze_single_block_intent(spot, block)
            
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=block.block_id,
                customer_intent=intent,
                spans_multiple_blocks=False,
                primary_block_id=block.block_id,
                requires_attention=intent == CustomerIntent.TIME_SPECIFIC
            )
        
        else:
            # Multi-block assignment
            intent = self._analyze_multi_block_intent(spot, blocks)
            primary_block = self._select_primary_block(spot, blocks)
            
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=None,  # NULL for multi-block
                customer_intent=intent,
                spans_multiple_blocks=True,
                blocks_spanned=[b.block_id for b in blocks],
                primary_block_id=primary_block.block_id if primary_block else None,
                requires_attention=len(blocks) > 3
            )
    
    def _analyze_single_block_intent(self, spot: SpotData, block: LanguageBlock) -> CustomerIntent:
        """Analyze customer intent for single block assignment"""
        if spot.language_id and spot.language_id == block.language_id:
            return CustomerIntent.LANGUAGE_SPECIFIC
        elif spot.language_id and spot.language_id != block.language_id:
            return CustomerIntent.TIME_SPECIFIC
        else:
            return CustomerIntent.LANGUAGE_SPECIFIC
    
    def _analyze_multi_block_intent(self, spot: SpotData, blocks: List[LanguageBlock]) -> CustomerIntent:
        """Analyze customer intent for multi-block assignment"""
        unique_languages = set(b.language_id for b in blocks)
        unique_day_parts = set(b.day_part for b in blocks)
        
        if len(unique_languages) == 1:
            if len(unique_day_parts) == 1:
                return CustomerIntent.TIME_SPECIFIC
            else:
                return CustomerIntent.INDIFFERENT
        else:
            return CustomerIntent.INDIFFERENT
    
    def _select_primary_block(self, spot: SpotData, blocks: List[LanguageBlock]) -> Optional[LanguageBlock]:
        """Select primary block for multi-block assignment"""
        if not blocks:
            return None
        
        # Prefer language match if available
        if spot.language_id:
            matching_blocks = [b for b in blocks if b.language_id == spot.language_id]
            if matching_blocks:
                return matching_blocks[0]
        
        return blocks[0]
    
    def _save_assignment(self, result: AssignmentResult):
        """Save assignment to database"""
        cursor = self.db.cursor()
        
        # Only insert if we have a valid schedule_id
        if result.schedule_id:
            # Delete existing assignment if exists
            cursor.execute("DELETE FROM spot_language_blocks WHERE spot_id = ?", (result.spot_id,))
            
            cursor.execute("""
                INSERT INTO spot_language_blocks (
                    spot_id, schedule_id, block_id, customer_intent, intent_confidence,
                    spans_multiple_blocks, blocks_spanned, primary_block_id,
                    assignment_method, assigned_date, assigned_by,
                    requires_attention, alert_reason, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                result.spot_id,
                result.schedule_id,
                result.block_id,
                result.customer_intent.value if result.customer_intent else None,
                1.0,  # Default confidence
                result.spans_multiple_blocks,
                str(result.blocks_spanned) if result.blocks_spanned else None,
                result.primary_block_id,
                AssignmentMethod.AUTO_COMPUTED.value,
                datetime.now().isoformat(),
                'system',
                result.requires_attention,
                result.alert_reason,
                result.error_message
            ))
            
            self.db.commit()
    
    def _get_unassigned_spot_ids(self, limit: int = None) -> List[int]:
        """Get spot IDs that don't have language block assignments"""
        cursor = self.db.cursor()
        
        query = """
        SELECT s.spot_id
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE slb.spot_id IS NULL
          AND s.market_id IS NOT NULL
          AND s.time_in IS NOT NULL
          AND s.time_out IS NOT NULL
          AND s.day_of_week IS NOT NULL
        ORDER BY s.spot_id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    
    def _get_assignment_details(self, spot_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed assignment information for a spot"""
        cursor = self.db.cursor()
        
        query = """
        SELECT 
            s.spot_id, s.bill_code, s.air_date, s.time_in, s.time_out,
            m.market_code, 
            slb.customer_intent, slb.spans_multiple_blocks, slb.requires_attention,
            slb.alert_reason, lb.block_name, lb.day_part
        FROM spots s
        JOIN markets m ON s.market_id = m.market_id
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
        WHERE s.spot_id = ?
        """
        
        cursor.execute(query, (spot_id,))
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return {
            'spot_id': row[0],
            'bill_code': row[1],
            'air_date': row[2],
            'time_in': row[3],
            'time_out': row[4],
            'market_code': row[5],
            'customer_intent': row[6],
            'spans_multiple_blocks': bool(row[7]) if row[7] is not None else False,
            'requires_attention': bool(row[8]) if row[8] is not None else False,
            'alert_reason': row[9],
            'block_name': row[10],
            'day_part': row[11]
        }
    
    def _time_to_minutes(self, time_str: str) -> int:
        """Convert HH:MM:SS time string to minutes since midnight"""
        try:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except (ValueError, IndexError):
            return 0
    
    def _times_overlap(self, start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two time ranges overlap"""
        return start1 < end2 and end1 > start2
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current assignment statistics"""
        return self.stats.copy()


def get_unassigned_2025_count(db_connection):
    """Get count of unassigned 2025 spots"""
    cursor = db_connection.cursor()
    
    query = """
    SELECT COUNT(*) as unassigned_count,
           SUM(gross_rate) as unassigned_revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '2025%'
      AND slb.spot_id IS NULL
      AND s.market_id IS NOT NULL
      AND s.time_in IS NOT NULL
      AND s.time_out IS NOT NULL
      AND s.day_of_week IS NOT NULL
      AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    return row[0], row[1] if row else (0, 0)


def get_unassigned_2024_count(db_connection):
    """Get count of unassigned 2024 spots"""
    cursor = db_connection.cursor()
    
    query = """
    SELECT COUNT(*) as unassigned_count,
           SUM(gross_rate) as unassigned_revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '2024%'
      AND slb.spot_id IS NULL
      AND s.market_id IS NOT NULL
      AND s.time_in IS NOT NULL
      AND s.time_out IS NOT NULL
      AND s.day_of_week IS NOT NULL
      AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    """
    
    cursor.execute(query)
    row = cursor.fetchone()
    return row[0], row[1] if row else (0, 0)


def get_unassigned_by_year_summary(db_connection):
    """Get summary of unassigned spots by year"""
    cursor = db_connection.cursor()
    
    query = """
    SELECT 
        SUBSTR(s.broadcast_month, 1, 4) as year,
        COUNT(*) as total_spots,
        COUNT(slb.spot_id) as assigned_spots,
        COUNT(*) - COUNT(slb.spot_id) as unassigned_spots,
        SUM(s.gross_rate) as total_revenue,
        SUM(CASE WHEN slb.spot_id IS NOT NULL THEN s.gross_rate ELSE 0 END) as assigned_revenue,
        SUM(CASE WHEN slb.spot_id IS NULL THEN s.gross_rate ELSE 0 END) as unassigned_revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.market_id IS NOT NULL
      AND s.time_in IS NOT NULL
      AND s.time_out IS NOT NULL
      AND s.day_of_week IS NOT NULL
      AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.broadcast_month IS NOT NULL
    GROUP BY year
    ORDER BY year DESC
    """
    
    cursor.execute(query)
    return cursor.fetchall()


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Language Block Assignment Tool")
    parser.add_argument("--database", default="data/database/production.db", help="Database path")
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--test", type=int, metavar="N", help="Test assignment with N spots")
    mode_group.add_argument("--batch", type=int, metavar="N", help="Assign N unassigned spots")
    mode_group.add_argument("--all-2025", action="store_true", help="Assign all unassigned 2025 spots")
    mode_group.add_argument("--all-2024", action="store_true", help="Assign all unassigned 2024 spots") 
    mode_group.add_argument("--status", action="store_true", help="Show current assignment status")
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = sqlite3.connect(args.database)
        print(f"✅ Connected to database: {args.database}")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return 1
    
    try:
        service = LanguageBlockService(conn)
        
        if args.status:
            # Show current status for all years
            year_summary = get_unassigned_by_year_summary(conn)
            
            print(f"\n📊 ASSIGNMENT STATUS BY YEAR:")
            print(f"{'Year':6} {'Total Spots':>12} {'Assigned':>10} {'Unassigned':>12} {'Assigned %':>10} {'Unassigned Revenue':>18}")
            print("-" * 85)
            
            for row in year_summary:
                year = row[0]
                total_spots = row[1]
                assigned_spots = row[2]
                unassigned_spots = row[3]
                unassigned_revenue = row[6]
                assigned_pct = (assigned_spots / total_spots * 100) if total_spots > 0 else 0
                
                print(f"{year:6} {total_spots:>12,} {assigned_spots:>10,} {unassigned_spots:>12,} {assigned_pct:>9.1f}% ${unassigned_revenue:>17,.0f}")
            
            print(f"\n💡 Use --all-2024 or --all-2025 to assign all spots for a specific year")
            
        elif args.test:
            # Test mode
            print(f"\n🧪 TESTING assignment with {args.test} spots...")
            results = service.test_assignment(args.test)
            
            print(f"\n📊 TEST RESULTS:")
            print(f"   • Spots tested: {results['spots_tested']}")
            print(f"   • Success rate: {results['success_rate']:.1%}")
            print(f"   • Stats: {results['stats']}")
            
            if results['spot_details'] and len(results['spot_details']) > 0:
                print(f"\n📋 SAMPLE ASSIGNMENTS:")
                for detail in results['spot_details'][:5]:  # Show first 5
                    intent = detail['customer_intent'] or 'None'
                    block = detail['block_name'] or 'No block'
                    print(f"   • Spot {detail['spot_id']} ({detail['bill_code']}): {intent} → {block}")
            
        elif args.batch:
            # Batch mode
            print(f"\n🚀 BATCH ASSIGNMENT of {args.batch} spots...")
            
            # Get unassigned spot IDs (limited)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.spot_id 
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.day_of_week IS NOT NULL
                ORDER BY s.spot_id
                LIMIT ?
            """, (args.batch,))
            
            spot_ids = [row[0] for row in cursor.fetchall()]
            
            if not spot_ids:
                print("❌ No unassigned spots found!")
                return 1
                
            print(f"Found {len(spot_ids)} unassigned spots to process...")
            results = service.assign_spots_batch(spot_ids)
            
            print(f"\n📊 BATCH RESULTS:")
            print(f"   • Processed: {results['processed']}")
            print(f"   • Assigned: {results['assigned']}")
            print(f"   • Multi-block: {results['multi_block']}")
            print(f"   • No coverage: {results['no_coverage']}")
            print(f"   • Errors: {results['errors']}")
            
        elif args.all_2025:
            # All 2025 mode
            unassigned_count, unassigned_revenue = get_unassigned_2025_count(conn)
            
            if unassigned_count == 0:
                print("✅ All 2025 spots are already assigned!")
                return 0
            
            print(f"\n🎯 ASSIGNING ALL 2025 SPOTS:")
            print(f"   • Found {unassigned_count:,} unassigned 2025 spots")
            print(f"   • Total unassigned revenue: ${unassigned_revenue:,.2f}")
            
            confirm = input(f"\nProceed with assignment? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("❌ Assignment cancelled")
                return 0
            
            # Get all unassigned 2025 spot IDs
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.spot_id 
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE '2025%'
                  AND slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.day_of_week IS NOT NULL
                ORDER BY s.spot_id
            """)
            
            spot_ids = [row[0] for row in cursor.fetchall()]
            
            print(f"🚀 Processing {len(spot_ids):,} spots...")
            results = service.assign_spots_batch(spot_ids)
            
            print(f"\n🎉 2025 ASSIGNMENT COMPLETE:")
            print(f"   • Processed: {results['processed']:,}")
            print(f"   • Assigned: {results['assigned']:,}")
            print(f"   • Multi-block: {results['multi_block']:,}")
            print(f"   • No coverage: {results['no_coverage']:,}")
            print(f"   • Errors: {results['errors']:,}")
            
            success_rate = (results['assigned'] + results['multi_block']) / results['processed'] * 100 if results['processed'] > 0 else 0
            print(f"   • Success rate: {success_rate:.1f}%")
            
        elif args.all_2024:
            # All 2024 mode
            unassigned_count, unassigned_revenue = get_unassigned_2024_count(conn)
            
            if unassigned_count == 0:
                print("✅ All 2024 spots are already assigned!")
                return 0
            
            print(f"\n🎯 ASSIGNING ALL 2024 SPOTS:")
            print(f"   • Found {unassigned_count:,} unassigned 2024 spots")
            print(f"   • Total unassigned revenue: ${unassigned_revenue:,.2f}")
            print(f"   • Note: This will overwrite any existing 2024 assignments")
            
            confirm = input(f"\nProceed with assignment? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("❌ Assignment cancelled")
                return 0
            
            # Get all unassigned 2024 spot IDs
            cursor = conn.cursor()
            cursor.execute("""
                SELECT s.spot_id 
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE s.broadcast_month LIKE '2024%'
                  AND slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.day_of_week IS NOT NULL
                ORDER BY s.spot_id
            """)
            
            spot_ids = [row[0] for row in cursor.fetchall()]
            
            print(f"🚀 Processing {len(spot_ids):,} spots...")
            results = service.assign_spots_batch(spot_ids)
            
            print(f"\n🎉 2024 ASSIGNMENT COMPLETE:")
            print(f"   • Processed: {results['processed']:,}")
            print(f"   • Assigned: {results['assigned']:,}")
            print(f"   • Multi-block: {results['multi_block']:,}")
            print(f"   • No coverage: {results['no_coverage']:,}")
            print(f"   • Errors: {results['errors']:,}")
            
            success_rate = (results['assigned'] + results['multi_block']) / results['processed'] * 100 if results['processed'] > 0 else 0
            print(f"   • Success rate: {success_rate:.1f}%")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during assignment: {e}")
        return 1
        
    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())