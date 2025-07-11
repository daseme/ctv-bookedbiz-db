#!/usr/bin/env python3
"""
Standalone Language Block Assignment Script
===========================================

This script includes the Language Block Service directly to avoid import issues.
Run this to assign your unassigned spots to language blocks for any year!

Features:
- Dynamic year support (2023, 2024, 2025, 2026+)
- Chinese language family recognition (Cantonese + Mandarin = Chinese intention)
- Same-language multi-block assignment (e.g., Filipino 16:00-19:00)
- Perfect grid coverage with intelligent intent analysis
- Comprehensive error handling and progress tracking

Usage:
    python cli_01_assign_language_blocks.py --test 100           # Test with 100 spots
    python cli_01_assign_language_blocks.py --batch 1000        # Assign 1000 spots
    python cli_01_assign_language_blocks.py --all-year 2025     # Assign all unassigned 2025 spots
    python cli_01_assign_language_blocks.py --all-year 2024     # Assign all unassigned 2024 spots
    python cli_01_assign_language_blocks.py --all-year 2023     # Assign all unassigned 2023 spots
    python cli_01_assign_language_blocks.py --status            # Show assignment status by year
    python cli_01_assign_language_blocks.py --force-year 2024   # Force reassign all 2024 spots (deletes existing)

Assignment Logic:
- language_specific: Spots targeting single language or Chinese family (Cantonese+Mandarin)
- indifferent: Long-duration spots spanning multiple different language families
- no_coverage: Spots with no language block coverage (minimal with proper grid)

Database Requirements:
- SQLite 3.x compatible
- Requires language_blocks, spots, and spot_language_blocks tables
- Broadcast month format: MMM-YY (e.g., Jan-25, Feb-25)
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
    """Result of spot assignment with enhanced business rule tracking"""
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
    campaign_type: str = 'language_specific'
    
    # NEW: Enhanced business rule tracking
    business_rule_applied: Optional[str] = None    # 'tagalog_pattern', 'chinese_pattern', 'ros_duration', 'ros_time'
    auto_resolved_date: Optional[datetime] = None  # When enhanced rule was applied


class LanguageBlockService:
    """Language Block Assignment Service with Enhanced Business Rules"""
    
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
    
    def _create_assignment(self, spot: SpotData, schedule_id: int, 
                            blocks: List[LanguageBlock]) -> AssignmentResult:
        """Create assignment with enhanced business rules (additive layer)"""
        
        # Step 1: Apply base logic (unchanged)
        base_result = self._analyze_base_assignment(spot, schedule_id, blocks)
        
        # Step 2: Apply enhanced rules only if base logic returns 'indifferent'
        if base_result.customer_intent == CustomerIntent.INDIFFERENT:
            enhanced_result = self._apply_enhanced_business_rules(spot, base_result)
            return enhanced_result
        
        return base_result
    
    def _analyze_base_assignment(self, spot: SpotData, schedule_id: int, 
                                blocks: List[LanguageBlock]) -> AssignmentResult:
        """Original assignment logic (unchanged)"""
        
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
                requires_attention=intent == CustomerIntent.TIME_SPECIFIC,
                campaign_type='language_specific'
            )
        
        else:
            # Multi-block assignment
            intent = self._analyze_multi_block_intent(spot, blocks)
            primary_block = self._select_primary_block(spot, blocks)
            
            # Calculate spot duration and campaign type
            spot_duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
            campaign_type = self._determine_campaign_type(intent, spot_duration, len(blocks))
            
            # Handle different intents
            if intent == CustomerIntent.INDIFFERENT:
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=None,
                    customer_intent=intent,
                    spans_multiple_blocks=True,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=True,
                    alert_reason=f"Multi-language campaign ({campaign_type}) - candidate for enhanced rules",
                    campaign_type=campaign_type
                )
            
            elif intent == CustomerIntent.LANGUAGE_SPECIFIC:
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=primary_block.block_id if primary_block else None,
                    customer_intent=intent,
                    spans_multiple_blocks=False,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=len(blocks) > 3,
                    campaign_type='language_specific'
                )
            
            else:  # TIME_SPECIFIC
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=primary_block.block_id if primary_block else None,
                    customer_intent=intent,
                    spans_multiple_blocks=False,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=len(blocks) > 3,
                    campaign_type='language_specific'
                )

    def _apply_enhanced_business_rules(self, spot: SpotData, base_result: AssignmentResult) -> AssignmentResult:
        """Apply enhanced business rules (additive layer)"""
        
        # Rule 1: Duration-based ROS detection (> 4 hours)
        if self._is_ros_by_duration(spot):
            return self._create_ros_assignment(spot, base_result, 'ros_duration')
        
        # Rule 2: Time-based ROS detection (13:00-23:59)
        if self._is_ros_by_time(spot):
            return self._create_ros_assignment(spot, base_result, 'ros_time')
        
        # Rule 3: Tagalog pattern recognition (16:00-19:00 + "T")
        if self._is_tagalog_pattern(spot):
            return self._create_tagalog_assignment(spot, base_result)
        
        # Rule 4: Chinese pattern recognition (19:00-23:59 + "M"/"M/C")
        if self._is_chinese_pattern(spot):
            return self._create_chinese_assignment(spot, base_result)
        
        # No enhanced rule applies, return original result
        return base_result
    
    def _is_ros_by_duration(self, spot: SpotData) -> bool:
        """Check if spot duration > 4 hours (240 minutes)"""
        duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
        return duration > 360  # 6 hours (360 minutes)
    
    def _is_ros_by_time(self, spot: SpotData) -> bool:
        """Check if spot runs 13:00-23:59 (ROS time slot)"""
        return spot.time_in == "13:00:00" and spot.time_out == "23:59:00"
    
    def _is_tagalog_pattern(self, spot: SpotData) -> bool:
        """Check if spot matches Tagalog pattern (16:00-19:00 + language hint "T")"""
        time_match = spot.time_in == "16:00:00" and spot.time_out == "19:00:00"
        language_hint = self._get_language_hint(spot) == "T"
        return time_match and language_hint
    
    def _is_chinese_pattern(self, spot: SpotData) -> bool:
        """Check if spot matches Chinese pattern (19:00-23:59 + language hint "M"/"M/C")"""
        time_match = spot.time_in == "19:00:00" and spot.time_out == "23:59:00"
        language_hint = self._get_language_hint(spot) in ["M", "M/C"]
        return time_match and language_hint
    
    def _get_language_hint(self, spot: SpotData) -> Optional[str]:
        """Get original language hint from spots.language_code"""
        cursor = self.db.cursor()
        cursor.execute("SELECT language_code FROM spots WHERE spot_id = ?", (spot.spot_id,))
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    
    def _create_ros_assignment(self, spot: SpotData, base_result: AssignmentResult, 
                              rule_type: str) -> AssignmentResult:
        """Create ROS assignment from enhanced rule"""
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=base_result.schedule_id,
            block_id=None,  # ROS has no specific block
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            blocks_spanned=base_result.blocks_spanned,
            primary_block_id=base_result.primary_block_id,
            requires_attention=False,  # ROS is resolved
            alert_reason=None,
            campaign_type='ros',  # ROS campaign type
            business_rule_applied=rule_type,
            auto_resolved_date=datetime.now()
        )
    
    def _create_tagalog_assignment(self, spot: SpotData, base_result: AssignmentResult) -> AssignmentResult:
        """Create Tagalog assignment from enhanced rule"""
        # Find Tagalog block from the spanned blocks
        tagalog_block = self._find_tagalog_block(base_result.blocks_spanned)
        
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=base_result.schedule_id,
            block_id=tagalog_block.block_id if tagalog_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,  # Now language-specific
            blocks_spanned=base_result.blocks_spanned,
            primary_block_id=tagalog_block.block_id if tagalog_block else None,
            requires_attention=False,  # Resolved by rule
            alert_reason=None,
            campaign_type='language_specific',
            business_rule_applied='tagalog_pattern',
            auto_resolved_date=datetime.now()
        )
    
    def _create_chinese_assignment(self, spot: SpotData, base_result: AssignmentResult) -> AssignmentResult:
        """Create Chinese family assignment from enhanced rule"""
        # Find Chinese block from the spanned blocks (Mandarin or Cantonese)
        chinese_block = self._find_chinese_block(base_result.blocks_spanned)
        
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=base_result.schedule_id,
            block_id=chinese_block.block_id if chinese_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,  # Now language-specific
            blocks_spanned=base_result.blocks_spanned,
            primary_block_id=chinese_block.block_id if chinese_block else None,
            requires_attention=False,  # Resolved by rule
            alert_reason=None,
            campaign_type='language_specific',
            business_rule_applied='chinese_pattern',
            auto_resolved_date=datetime.now()
        )
    
    def _find_tagalog_block(self, block_ids: List[int]) -> Optional[LanguageBlock]:
        """Find Tagalog block from list of block IDs"""
        if not block_ids:
            return None
        
        cursor = self.db.cursor()
        placeholders = ','.join(['?'] * len(block_ids))
        query = f"""
        SELECT lb.block_id, lb.schedule_id, lb.day_of_week, lb.time_start, lb.time_end,
            lb.language_id, lb.block_name, lb.block_type, lb.day_part
        FROM language_blocks lb
        JOIN languages l ON lb.language_id = l.language_id
        WHERE lb.block_id IN ({placeholders})
        AND l.language_code = 'T'
        LIMIT 1
        """
        
        cursor.execute(query, block_ids)
        row = cursor.fetchone()
        
        if row:
            return LanguageBlock(
                block_id=row[0], schedule_id=row[1], day_of_week=row[2],
                time_start=row[3], time_end=row[4], language_id=row[5],
                block_name=row[6], block_type=row[7], day_part=row[8]
            )
        return None
    
    def _find_chinese_block(self, block_ids: List[int]) -> Optional[LanguageBlock]:
        """Find Chinese block (Mandarin or Cantonese) from list of block IDs"""
        if not block_ids:
            return None
        
        cursor = self.db.cursor()
        placeholders = ','.join(['?'] * len(block_ids))
        query = f"""
        SELECT block_id, schedule_id, day_of_week, time_start, time_end,
               language_id, block_name, block_type, day_part
        FROM language_blocks lb
        WHERE lb.block_id IN ({placeholders})
        AND lb.language_id IN (2, 3)  -- Mandarin=2, Cantonese=3
        ORDER BY lb.language_id  -- Prefer Mandarin (2) over Cantonese (3)
        LIMIT 1
        """
        
        cursor.execute(query, block_ids)
        row = cursor.fetchone()
        
        if row:
            return LanguageBlock(
                block_id=row[0], schedule_id=row[1], day_of_week=row[2],
                time_start=row[3], time_end=row[4], language_id=row[5],
                block_name=row[6], block_type=row[7], day_part=row[8]
            )
        return None
    
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
        
        # Check for same language
        if len(unique_languages) == 1:
            return CustomerIntent.LANGUAGE_SPECIFIC
        
        # Check for Chinese language family (Mandarin + Cantonese)
        chinese_languages = {2, 3}  # Mandarin=2, Cantonese=3 (from your language table)
        if unique_languages.issubset(chinese_languages):
            return CustomerIntent.LANGUAGE_SPECIFIC  # Chinese intention
        
        # Multiple different language families = truly indifferent
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
    
    def _calculate_spot_duration(self, time_in: str, time_out: str) -> int:
        """Calculate spot duration in minutes"""
        try:
            start_minutes = self._time_to_minutes(time_in)
            end_minutes = self._time_to_minutes(time_out)
            
            if end_minutes >= start_minutes:
                return end_minutes - start_minutes
            else:
                # Handle midnight rollover
                return (24 * 60) - start_minutes + end_minutes
        except:
            return 0

    def _determine_campaign_type(self, intent: CustomerIntent, duration_minutes: int, block_count: int) -> str:
        """Determine campaign type based on intent, duration, and block count"""
        
        if intent == CustomerIntent.LANGUAGE_SPECIFIC:
            return 'language_specific'
        
        elif intent == CustomerIntent.INDIFFERENT:
            # ROS detection: 17+ hours (1020+ minutes) or 15+ blocks
            if duration_minutes >= 1020 or block_count >= 15:
                return 'ros'
            else:
                return 'multi_language'
        
        else:  # TIME_SPECIFIC
            return 'language_specific'
    
    def _save_assignment(self, result: AssignmentResult):
        """Save assignment to database with enhanced rule tracking"""
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
                    requires_attention, alert_reason, notes, campaign_type,
                    business_rule_applied, auto_resolved_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                result.error_message,
                result.campaign_type,
                result.business_rule_applied,
                result.auto_resolved_date.isoformat() if result.auto_resolved_date else None
            ))
            
            self.db.commit()
    
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
            slb.alert_reason, lb.block_name, lb.day_part,
            slb.business_rule_applied, slb.auto_resolved_date
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
            'day_part': row[11],
            'business_rule_applied': row[12],
            'auto_resolved_date': row[13]
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
    
    def get_enhanced_rule_stats(self) -> Dict[str, Any]:
        """Get statistics on enhanced business rule applications"""
        cursor = self.db.cursor()
        
        query = """
        SELECT 
            business_rule_applied,
            COUNT(*) as count,
            AVG(intent_confidence) as avg_confidence,
            MIN(auto_resolved_date) as first_applied,
            MAX(auto_resolved_date) as last_applied
        FROM spot_language_blocks
        WHERE business_rule_applied IS NOT NULL
        GROUP BY business_rule_applied
        ORDER BY count DESC
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        stats = {
            'enhanced_rules': {},
            'total_enhanced': 0,
            'total_standard': 0
        }
        
        for row in results:
            rule_name = row[0]
            stats['enhanced_rules'][rule_name] = {
                'count': row[1],
                'avg_confidence': row[2],
                'first_applied': row[3],
                'last_applied': row[4]
            }
            stats['total_enhanced'] += row[1]
        
        # Get total standard assignments
        cursor.execute("SELECT COUNT(*) FROM spot_language_blocks WHERE business_rule_applied IS NULL")
        stats['total_standard'] = cursor.fetchone()[0]
        
        return stats


def get_unassigned_by_year_summary(db_connection):
    """Get summary of unassigned spots by year (updated for any year)"""
    cursor = db_connection.cursor()
    
    query = """
    SELECT 
        '20' || SUBSTR(s.broadcast_month, -2) as year,
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
    GROUP BY SUBSTR(s.broadcast_month, -2)
    ORDER BY year DESC
    """
    
    cursor.execute(query)
    return cursor.fetchall()

def get_unassigned_year_count(db_connection, year: int):
    """Get count of unassigned spots for any year"""
    cursor = db_connection.cursor()
    year_suffix = str(year)[-2:]  # Get last 2 digits (e.g., 2023 -> 23)
    
    query = """
    SELECT COUNT(*) as unassigned_count,
        SUM(gross_rate) as unassigned_revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE ?
    AND slb.spot_id IS NULL
    AND s.market_id IS NOT NULL
    AND s.time_in IS NOT NULL
    AND s.time_out IS NOT NULL
    AND s.day_of_week IS NOT NULL
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    """
    
    cursor.execute(query, (f'%-{year_suffix}',))
    row = cursor.fetchone()
    return row[0], row[1] if row else (0, 0)


def get_available_years(db_connection):
    """Get list of available years in the database"""
    cursor = db_connection.cursor()
    
    query = """
    SELECT DISTINCT '20' || SUBSTR(broadcast_month, -2) as year
    FROM spots 
    WHERE broadcast_month IS NOT NULL
    ORDER BY year DESC
    """
    
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


def get_unassigned_spot_ids_for_year(db_connection, year: int, limit: int = None):
    """Get spot IDs that don't have language block assignments for specific year"""
    cursor = db_connection.cursor()
    year_suffix = str(year)[-2:]  # Get last 2 digits
    
    query = """
    SELECT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE ?
    AND slb.spot_id IS NULL
    AND s.market_id IS NOT NULL
    AND s.time_in IS NOT NULL
    AND s.time_out IS NOT NULL
    AND s.day_of_week IS NOT NULL
    ORDER BY s.spot_id
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query, (f'%-{year_suffix}',))
    return [row[0] for row in cursor.fetchall()]


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Language Block Assignment Tool")
    parser.add_argument("--database", default="data/database/production.db", help="Database path")
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--test", type=int, metavar="N", help="Test assignment with N spots")
    mode_group.add_argument("--batch", type=int, metavar="N", help="Assign N unassigned spots")
    mode_group.add_argument("--all-year", type=int, metavar="YYYY", help="Assign all unassigned spots for specific year (e.g., 2023, 2024, 2025)")
    mode_group.add_argument("--status", action="store_true", help="Show current assignment status")
    mode_group.add_argument("--force-year", type=int, metavar="YYYY", help="Force reassignment of all spots for specific year")
    
    args = parser.parse_args()
    
    # Connect to database
    try:
        conn = sqlite3.connect(args.database)
        print(f"✅ Connected to database: {args.database}")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return 1
    
    try:
        service = LanguageBlockService(conn)  # ← FIXED: Pass conn parameter
        
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
            
            available_years = get_available_years(conn)
            print(f"Available years: {', '.join(available_years)}")
            print(f"\n💡 Use --all-year YYYY to assign all spots for a specific year")
            print(f"💡 Example: --all-year 2023 or --all-year 2025")
            
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

        elif args.force_year:
            # Force reassignment mode
            year = args.force_year
            
            # Delete existing assignments for the year
            year_suffix = str(year)[-2:]
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM spot_language_blocks 
                WHERE spot_id IN (
                    SELECT spot_id FROM spots 
                    WHERE broadcast_month LIKE ?
                )
            """, (f'%-{year_suffix}',))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            print(f"🗑️ Deleted {deleted_count:,} existing assignments for {year}")
            
            # Now re-assign
            spot_ids = get_unassigned_spot_ids_for_year(conn, year)
            print(f"🚀 Re-assigning {len(spot_ids):,} spots for {year}")
            
            results = service.assign_spots_batch(spot_ids)
            print(f"✅ Re-assignment completed: {results}")
            
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
        
        elif args.all_year:
            # All year mode
            year = args.all_year
            unassigned_count, unassigned_revenue = get_unassigned_year_count(conn, year)
            
            if unassigned_count == 0:
                print(f"✅ All {year} spots are already assigned!")
                return 0
            
            print(f"\n🎯 ASSIGNING ALL {year} SPOTS:")
            print(f"   • Found {unassigned_count:,} unassigned {year} spots")
            print(f"   • Total unassigned revenue: ${unassigned_revenue:,.2f}")
            
            confirm = input(f"\nProceed with assignment? (yes/no): ").strip().lower()
            if confirm not in ['yes', 'y']:
                print("❌ Assignment cancelled")
                return 0
            
            # Get all unassigned spot IDs for the year
            spot_ids = get_unassigned_spot_ids_for_year(conn, year)
            
            print(f"🚀 Processing {len(spot_ids):,} spots...")
            results = service.assign_spots_batch(spot_ids)
            
            print(f"\n🎉 {year} ASSIGNMENT COMPLETE:")
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