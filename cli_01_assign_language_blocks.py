#!/usr/bin/env python3
"""
Standalone Language Block Assignment Script - UPDATED DOCUMENTATION
=================================================================

This script includes the Language Block Service directly to avoid import issues.
Run this to assign your unassigned spots to language blocks for any year!

It is designed to be run from the command line with various options for testing, batch assignment, and status reporting.
Usage:
    python cli_01_assign_language_blocks.py --test 100           # Test with 100 spots
    python cli_01_assign_language_blocks.py --batch 1000        # Assign 1000 spots
    python cli_01_assign_language_blocks.py --all-year 2025     # Assign all unassigned 2025 spots
    python cli_01_assign_language_blocks.py --all-year 2024     # Assign all unassigned 2024 spots
    python cli_01_assign_language_blocks.py --all-year 2023     # Assign all unassigned 2023 spots
    python cli_01_assign_language_blocks.py --status            # Show assignment status by year
    python cli_01_assign_language_blocks.py --force-year 2024   # Force reassign all 2024 spots (deletes existing)

Assignment Logic - UPDATED WITH CAMPAIGN_TYPE FIELD:
- language_specific: Spots targeting single language or Chinese family (Cantonese+Mandarin)
- ros: Long-duration spots or broadcast sponsorships (Run on Schedule)
- multi_language: Spots spanning multiple different language families
- no_coverage: Spots with no language block coverage (minimal with proper grid)

"""

import sqlite3
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CustomerIntent(Enum):
    """Customer intent classification for spot placement"""

    LANGUAGE_SPECIFIC = "language_specific"  # Single block, language-targeted
    TIME_SPECIFIC = "time_specific"  # Multi-block, same day-part
    INDIFFERENT = "indifferent"  # Multi-block, customer flexible
    NO_GRID_COVERAGE = "no_grid_coverage"  # Market has no programming grid


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
    campaign_type: str = "language_specific"

    # NEW: Enhanced business rule tracking
    business_rule_applied: Optional[str] = (
        None  # 'tagalog_pattern', 'chinese_pattern', 'ros_duration', 'ros_time'
    )
    auto_resolved_date: Optional[datetime] = None  # When enhanced rule was applied


class LanguageBlockService:
    """Language Block Assignment Service with Enhanced Business Rules"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

        # Simple stats tracking
        self.stats = {
            "processed": 0,
            "assigned": 0,
            "no_coverage": 0,
            "multi_block": 0,
            "errors": 0,
        }

    def _normalize_time_out(self, time_out: str) -> str:
        """FIXED: Convert '1 day, 0:00:00' to '00:00:00' (midnight) for proper comparison"""
        if time_out and "day" in str(time_out) and "0:00:00" in str(time_out):
            return "00:00:00"  # FIXED: Use 00:00:00 instead of 24:00:00
        return time_out

    def assign_single_spot(self, spot_id: int) -> AssignmentResult:
        """COMPLETE: Assign a single spot with proper variable definitions"""
        try:
            # Step 1: Get spot data
            spot_data = self._get_spot_data(spot_id)
            if not spot_data:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=False,
                    error_message="Spot not found or invalid",
                )

            # Step 2: Check if spot has market assignment
            if not spot_data.market_id:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="Spot has no market assignment",
                )

            # Step 3: Apply precedence rules first
            self.logger.info(f"DEBUG: Applying precedence rules for spot {spot_id}")
            precedence_result = self._apply_precedence_rules(spot_data)

            if precedence_result:
                self.logger.info(
                    f"DEBUG: Precedence rule applied for spot {spot_id}: {precedence_result.business_rule_applied}"
                )
                self._save_assignment(precedence_result)
                if precedence_result.campaign_type == "ros":
                    self.stats["multi_block"] += 1
                else:
                    self.stats["assigned"] += 1
                return precedence_result
            else:
                self.logger.info(
                    f"DEBUG: No precedence rules matched for spot {spot_id}"
                )

            # Step 4: Find programming schedule
            schedule_id = self._get_applicable_schedule(
                spot_data.market_id, spot_data.air_date
            )
            if not schedule_id:
                self.logger.info(
                    f"DEBUG: No programming schedule found for spot {spot_id}"
                )
                result = AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No programming grid for market",
                )
                self._save_assignment(result)
                self.stats["no_coverage"] += 1
                return result

            self.logger.info(
                f"DEBUG: Found schedule_id {schedule_id} for spot {spot_id}"
            )

            # Step 5: Find overlapping language blocks
            blocks = self._get_overlapping_blocks(
                schedule_id,
                spot_data.day_of_week,
                spot_data.time_in,
                spot_data.time_out,
            )

            if not blocks:
                self.logger.info(
                    f"DEBUG: No overlapping blocks found for spot {spot_id}"
                )
                result = AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No language blocks cover spot time",
                )
                self._save_assignment(result)
                self.stats["no_coverage"] += 1
                return result

            self.logger.info(
                f"DEBUG: Found {len(blocks)} overlapping blocks for spot {spot_id}"
            )

            # Step 6: Apply normal language block assignment
            # Add this right before calling _analyze_base_assignment
            self.logger.info(f"DEBUG: About to call _analyze_base_assignment method")
            self.logger.info(
                f"DEBUG: Method exists: {hasattr(self, '_analyze_base_assignment')}"
            )
            self.logger.info(
                f"DEBUG: Calling _analyze_base_assignment for spot {spot_id}"
            )
            result = self._analyze_base_assignment(spot_data, schedule_id, blocks)

            # CRITICAL FIX: Check if result is None and fix database constraints
            if result is None:
                self.logger.error(
                    f"ERROR: _analyze_base_assignment returned None for spot {spot_id}"
                )

                # Create a fallback result that respects database constraints
                if len(blocks) == 1:
                    # Single block: spans_multiple_blocks = False, block_id = NOT NULL
                    result = AssignmentResult(
                        spot_id=spot_id,
                        success=True,
                        schedule_id=schedule_id,
                        block_id=blocks[0].block_id,
                        customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                        spans_multiple_blocks=False,
                        blocks_spanned=[blocks[0].block_id],
                        primary_block_id=blocks[0].block_id,
                        requires_attention=True,
                        alert_reason="Fallback assignment - _analyze_base_assignment returned None",
                        campaign_type="language_specific",
                        error_message="Assignment method returned None",
                    )
                else:
                    # Multiple blocks: spans_multiple_blocks = True, block_id = NULL
                    result = AssignmentResult(
                        spot_id=spot_id,
                        success=True,
                        schedule_id=schedule_id,
                        block_id=None,  # Must be None when spans_multiple_blocks = True
                        customer_intent=CustomerIntent.INDIFFERENT,
                        spans_multiple_blocks=True,
                        blocks_spanned=[b.block_id for b in blocks],
                        primary_block_id=blocks[0].block_id,
                        requires_attention=True,
                        alert_reason="Fallback assignment - _analyze_base_assignment returned None",
                        campaign_type="multi_language",
                        error_message="Assignment method returned None",
                    )

            self.logger.info(
                f"DEBUG: Assignment result for spot {spot_id}: success={result.success}, block_id={result.block_id}, spans_multiple={result.spans_multiple_blocks}"
            )

            # Save the assignment
            self._save_assignment(result)

            # Update stats
            if result.spans_multiple_blocks:
                self.stats["multi_block"] += 1
            else:
                self.stats["assigned"] += 1

            return result

        except Exception as e:
            self.logger.error(f"Error assigning spot {spot_id}: {e}")
            self.stats["errors"] += 1
            return AssignmentResult(
                spot_id=spot_id, success=False, error_message=str(e)
            )
        finally:
            self.stats["processed"] += 1

    def assign_spots_batch(
        self, spot_ids: List[int] = None, limit: int = None
    ) -> Dict[str, Any]:
        """Assign multiple spots to language blocks"""
        self.logger.info("Starting batch spot assignment")

        # Reset stats
        self.stats = {
            "processed": 0,
            "assigned": 0,
            "no_coverage": 0,
            "multi_block": 0,
            "errors": 0,
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
                        self.logger.info(
                            f"Processed {i + 1}/{len(spots_to_process)} spots..."
                        )

                except Exception as e:
                    self.logger.error(f"Failed to process spot {spot_id}: {e}")
                    self.stats["errors"] += 1

            self.logger.info(f"Batch assignment completed: {self.stats}")
            return self.stats

        except Exception as e:
            self.logger.error(f"Error in batch assignment: {e}")
            self.stats["errors"] += 1
            return self.stats

    def test_assignment(self, limit: int = 10) -> Dict[str, Any]:
        """Test assignment with a small number of spots"""
        self.logger.info(f"Running test assignment with {limit} spots")

        # Get test spots
        test_spot_ids = self._get_unassigned_spot_ids(limit)

        if not test_spot_ids:
            return {
                "success": False,
                "message": "No unassigned spots found for testing",
                "spots_tested": 0,
                "stats": self.stats,
                "success_rate": 0.0,
                "spot_details": [],
            }

        # Run assignment
        results = self.assign_spots_batch(test_spot_ids)

        # Get detailed results for analysis
        test_results = {
            "success": True,
            "spots_tested": len(test_spot_ids),
            "stats": results,
            "success_rate": (
                results["assigned"] + results["no_coverage"] + results["multi_block"]
            )
            / results["processed"]
            if results["processed"] > 0
            else 0,
            "spot_details": [],
        }

        # Get details for each assigned spot
        for spot_id in test_spot_ids:
            spot_details = self._get_assignment_details(spot_id)
            if spot_details:
                test_results["spot_details"].append(spot_details)

        return test_results

    def _analyze_base_assignment(self, spot: SpotData, schedule_id: int,
                                blocks: List[LanguageBlock]) -> AssignmentResult:
        """FIXED: Proper language analysis with enhanced Chinese family detection"""
        
        # ENHANCED: Check for Chinese family pattern (19:00-00:00 spanning Chinese blocks)
        if self._is_chinese_family_span(spot, blocks):
            self.logger.info(f"DEBUG: Chinese family span detected for spot {spot.spot_id}")
            
            # Find the primary Chinese block (prefer Mandarin Prime Block)
            chinese_block = self._find_primary_chinese_block(blocks)
            
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=None,
                customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                spans_multiple_blocks=len(blocks) > 1,
                blocks_spanned=[b.block_id for b in blocks],
                primary_block_id=chinese_block.block_id if chinese_block else blocks[0].block_id,
                requires_attention=False,
                campaign_type='language_specific',
                business_rule_applied='chinese_family_span',
                auto_resolved_date=datetime.now()
            )

        # Check for expanded Tagalog pattern (16:00-19:00 OR 17:00-19:00)
        if self._is_tagalog_pattern(spot):
            self.logger.info(f"DEBUG: Tagalog pattern detected for spot {spot.spot_id}")

            # Find Tagalog block from available blocks
            tagalog_block = self._find_tagalog_block([b.block_id for b in blocks])

            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=tagalog_block.block_id
                if tagalog_block
                else blocks[0].block_id,
                customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                spans_multiple_blocks=False,
                blocks_spanned=[blocks[0].block_id],
                primary_block_id=tagalog_block.block_id
                if tagalog_block
                else blocks[0].block_id,
                requires_attention=False,
                campaign_type="language_specific",
                business_rule_applied="tagalog_pattern",
                auto_resolved_date=datetime.now(),
            )

        # COMPREHENSIVE FIX: Proper analysis for ALL assignments
        if len(blocks) == 1:
            # Single block assignment
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=blocks[0].block_id,
                customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                spans_multiple_blocks=False,
                blocks_spanned=[blocks[0].block_id],
                primary_block_id=blocks[0].block_id,
                requires_attention=False,
                campaign_type="language_specific",
            )

        else:
            # FIXED: Multi-block assignment with PROPER language analysis
            language_analysis = self._analyze_block_languages(blocks)
            primary_block = self._select_primary_block(spot, blocks)
            spot_duration = self._calculate_spot_duration(spot.time_in, spot.time_out)

            # Log the analysis for debugging
            self.logger.info(
                f"DEBUG: Multi-block analysis for spot {spot.spot_id}: {language_analysis}"
            )

            # Decision logic based on language analysis
            if language_analysis["classification"] == "same_language":
                # Multiple blocks, same language → language_specific
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=None,  # FIXED: Must be None when spans_multiple_blocks = True
                    customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                    spans_multiple_blocks=True,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=False,
                    campaign_type="language_specific",
                    alert_reason=f"Multi-block same language: {language_analysis['primary_language']}",
                )

            elif language_analysis["classification"] == "same_family":
                # Multiple blocks, same language family → language_specific
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=None,  # FIXED: Must be None when spans_multiple_blocks = True
                    customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                    spans_multiple_blocks=True,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=False,
                    campaign_type="language_specific",
                    alert_reason=f"Multi-block same family: {language_analysis['family_name']}",
                )

            else:
                # Multiple blocks, different language families → check for ROS vs multi_language
                if (
                    spot_duration >= 1020 or len(blocks) >= 15
                ):  # 17+ hours or 15+ blocks
                    campaign_type = "ros"
                    requires_attention = False
                    alert_reason = f"ROS assignment: {spot_duration}min across {len(blocks)} blocks"
                else:
                    campaign_type = "multi_language"
                    requires_attention = True
                    alert_reason = (
                        f"True multi-language: {language_analysis['unique_languages']}"
                    )

                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=None,
                    customer_intent=CustomerIntent.INDIFFERENT,
                    spans_multiple_blocks=True,
                    blocks_spanned=[b.block_id for b in blocks],
                    primary_block_id=primary_block.block_id if primary_block else None,
                    requires_attention=requires_attention,
                    alert_reason=alert_reason,
                    campaign_type=campaign_type,
                )

    def _analyze_block_languages(self, blocks: List[LanguageBlock]) -> Dict[str, Any]:
        """COMPREHENSIVE: Analyze the languages of multiple blocks"""

        unique_languages = set(b.language_id for b in blocks)
        language_names = [self._get_language_name(b.language_id) for b in blocks]

        # Define language families (confirmed correct groupings)
        language_families = {
            "Chinese": {2, 3},  # Mandarin=2, Cantonese=3 (SAME family)
            "Filipino": {4},  # Tagalog=4 (single language in current DB)
            "South Asian": {6},  # South Asian=6 (represents the family)
            "English": {1},  # English=1 (single language)
            "Vietnamese": {7},  # Vietnamese=7 (single language)
            "Korean": {8},  # Korean=8 (single language)
            "Japanese": {9},  # Japanese=9 (single language)
            "Hmong": {5},  # Hmong=5 (single language)
        }

        # Check for same language
        if len(unique_languages) == 1:
            primary_language = language_names[0]
            return {
                "classification": "same_language",
                "unique_languages": list(unique_languages),
                "primary_language": primary_language,
                "family_name": primary_language,
                "block_count": len(blocks),
            }

        # Check for same language family
        for family_name, family_languages in language_families.items():
            if unique_languages.issubset(family_languages):
                return {
                    "classification": "same_family",
                    "unique_languages": list(unique_languages),
                    "primary_language": None,
                    "family_name": family_name,
                    "block_count": len(blocks),
                }

        # Different language families
        return {
            "classification": "different_families",
            "unique_languages": list(unique_languages),
            "primary_language": None,
            "family_name": None,
            "block_count": len(blocks),
            "language_names": language_names,
        }

    def _get_language_name(self, language_id: int) -> str:
        """Get language name from language_id"""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT language_name FROM languages WHERE language_id = ?", (language_id,)
        )
        result = cursor.fetchone()
        return result[0] if result else f"Unknown_{language_id}"

    def _apply_precedence_rules(self, spot: SpotData) -> Optional[AssignmentResult]:
        """ENHANCED: Apply precedence rules with operational time-based language rules"""

        # Rule 1: WorldLink Direct Response (highest priority)
        if self._is_worldlink_spot(spot):
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=1,
                block_id=None,
                customer_intent=CustomerIntent.INDIFFERENT,
                spans_multiple_blocks=True,
                blocks_spanned=[],
                primary_block_id=None,
                requires_attention=False,
                alert_reason=None,
                campaign_type="direct_response",
                business_rule_applied="worldlink_direct_response",
                auto_resolved_date=datetime.now(),
            )

        # Rule 2: Paid Programming
        if self._is_paid_programming(spot):
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=1,
                block_id=None,
                customer_intent=CustomerIntent.INDIFFERENT,
                spans_multiple_blocks=True,
                blocks_spanned=[],
                primary_block_id=None,
                requires_attention=False,
                alert_reason=None,
                campaign_type="paid_programming",
                business_rule_applied="revenue_type_paid_programming",
                auto_resolved_date=datetime.now(),
            )

        # NEW Rule 3: Operational Time-Based Language Rules
        operational_language = self._get_operational_language_assignment(spot)

        if operational_language == 'chinese':
            # Get schedule for this spot
            schedule_id = self._get_applicable_schedule(
                spot.market_id, spot.air_date
            )
            
            if schedule_id:
                # Find actual Chinese blocks that overlap with this spot
                blocks = self._get_overlapping_blocks(
                    schedule_id,
                    spot.day_of_week,
                    spot.time_in,
                    spot.time_out
                )
                
                # Filter for Chinese blocks (Mandarin=2, Cantonese=3)
                chinese_blocks = [b for b in blocks if b.language_id in [2, 3]]
                
                if chinese_blocks:
                    # Use the existing Chinese block finding logic
                    primary_block = self._find_primary_chinese_block(chinese_blocks)
                    
                    if len(chinese_blocks) == 1:
                        # Single Chinese block
                        return AssignmentResult(
                            spot_id=spot.spot_id,
                            success=True,
                            schedule_id=schedule_id,
                            block_id=chinese_blocks[0].block_id,
                            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                            spans_multiple_blocks=False,
                            blocks_spanned=[chinese_blocks[0].block_id],
                            primary_block_id=chinese_blocks[0].block_id,
                            requires_attention=False,
                            campaign_type="language_specific",
                            business_rule_applied="operational_chinese_time",
                            auto_resolved_date=datetime.now(),
                        )
                    else:
                        # Multiple Chinese blocks
                        return AssignmentResult(
                            spot_id=spot.spot_id,
                            success=True,
                            schedule_id=schedule_id,
                            block_id=None,  # Must be None when spans_multiple_blocks = True
                            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                            spans_multiple_blocks=True,
                            blocks_spanned=[b.block_id for b in chinese_blocks],
                            primary_block_id=primary_block.block_id if primary_block else chinese_blocks[0].block_id,
                            requires_attention=False,
                            campaign_type="language_specific",
                            business_rule_applied="operational_chinese_time",
                            auto_resolved_date=datetime.now(),
                        )
            
            # Fallback if no Chinese blocks found - let normal flow handle it
            self.logger.warning(f"No Chinese blocks found for operational rule on spot {spot.spot_id}")
            return None
        
        # For Hmong and Tagalog, let the normal flow handle them
        # This preserves existing pattern matching
        
        # Rule 4: Enhanced Chinese Pattern (backwards compatibility + extended hours)
        if self._is_chinese_pattern_enhanced(spot):
            schedule_id = self._get_applicable_schedule(
                spot.market_id, spot.air_date
            )
            if not schedule_id:
                schedule_id = 1
                
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=None,
                customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                spans_multiple_blocks=True,
                blocks_spanned=[],
                primary_block_id=None,
                requires_attention=False,
                alert_reason=None,
                campaign_type="language_specific",
                business_rule_applied="chinese_pattern",
                auto_resolved_date=datetime.now(),
            )

        # Rule 5: ROS by Duration (> 6 hours = 360 minutes)
        # BUT exclude Chinese and Tagalog patterns first
        if not self._is_tagalog_pattern(spot):
            duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
            if duration > 360:
                return AssignmentResult(
                    spot_id=spot.spot_id,
                    success=True,
                    schedule_id=1,
                    block_id=None,
                    customer_intent=CustomerIntent.INDIFFERENT,
                    spans_multiple_blocks=True,
                    blocks_spanned=[],
                    primary_block_id=None,
                    requires_attention=False,
                    alert_reason=None,
                    campaign_type="ros",
                    business_rule_applied="ros_duration",
                    auto_resolved_date=datetime.now(),
                )

        # Rule 6: ROS by Time Pattern (exclude Chinese and Tagalog patterns)
        if self._is_ros_by_time(spot):
            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=1,
                block_id=None,
                customer_intent=CustomerIntent.INDIFFERENT,
                spans_multiple_blocks=True,
                blocks_spanned=[],
                primary_block_id=None,
                requires_attention=False,
                alert_reason=None,
                campaign_type="ros",
                business_rule_applied="ros_time",
                auto_resolved_date=datetime.now(),
            )

        return None

    def _is_chinese_pattern(self, spot: SpotData) -> bool:
        """FIXED: Check if spot matches Chinese pattern (19:00 or 20:00 start)"""

        # Chinese patterns can start at 19:00:00 OR 20:00:00
        if spot.time_in not in ["19:00:00", "20:00:00"]:
            return False

        # Must end at 23:59:00 OR contain "day" (for "1 day, 0:00:00")
        time_out_match = (
            spot.time_out == "23:59:00"
            or spot.time_out == "24:00:00"
            or "day" in str(spot.time_out)
        )

        if not time_out_match:
            return False

        # Must have Chinese language code
        language_hint = self._get_language_hint(spot)
        language_match = language_hint in ["M", "C", "M/C"]

        # Debug logging
        if time_out_match and language_match:
            self.logger.info(
                f"DEBUG: Chinese pattern detected for spot {spot.spot_id} - time_in: {spot.time_in}, time_out: {spot.time_out}, language: {language_hint}"
            )

        return time_out_match and language_match

        if self._is_tagalog_pattern(spot):
            self.logger.info(
                f"DEBUG: Tagalog pattern detected in normal assignment for spot {spot.spot_id}"
            )

            # Find Tagalog block from available blocks
            tagalog_block = self._find_tagalog_block([b.block_id for b in blocks])

            return AssignmentResult(
                spot_id=spot.spot_id,
                success=True,
                schedule_id=schedule_id,
                block_id=tagalog_block.block_id if tagalog_block else None,
                customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                spans_multiple_blocks=False,
                blocks_spanned=[b.block_id for b in blocks],
                primary_block_id=tagalog_block.block_id if tagalog_block else None,
                requires_attention=False,
                alert_reason=None,
                campaign_type="language_specific",
                business_rule_applied="tagalog_pattern",
                auto_resolved_date=datetime.now(),
            )

        # THEN: Continue with original assignment logic for other spots
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
                campaign_type="language_specific",
            )

        else:
            # Multi-block assignment
            intent = self._analyze_multi_block_intent(spot, blocks)
            primary_block = self._select_primary_block(spot, blocks)

            # Calculate spot duration and campaign type
            spot_duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
            campaign_type = self._determine_campaign_type(
                intent, spot_duration, len(blocks)
            )

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
                    campaign_type=campaign_type,
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
                    campaign_type="language_specific",
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
                    campaign_type="language_specific",
                )

    def _is_chinese_pattern_enhanced(self, spot: SpotData) -> bool:
        """Enhanced Chinese pattern detection including extended evening hours"""
        
        # Keep original exact pattern matching for backwards compatibility
        if spot.time_in in ["19:00:00", "20:00:00"]:
            # Must end at 23:59:00 OR contain "day" (for "1 day, 0:00:00")
            time_out_match = (
                spot.time_out == "23:59:00"
                or spot.time_out == "24:00:00"
                or "day" in str(spot.time_out)
            )
            
            if time_out_match:
                # Must have Chinese language code
                language_hint = self._get_language_hint(spot)
                if language_hint in ["M", "C", "M/C"]:
                    self.logger.info(
                        f"DEBUG: Original Chinese pattern detected for spot {spot.spot_id} - "
                        f"time_in: {spot.time_in}, time_out: {spot.time_out}, language: {language_hint}"
                    )
                    return True
        
        # Extended pattern: Any spot starting 19:00-23:30 with Chinese language code
        try:
            hour = int(spot.time_in.split(':')[0])
            minute = int(spot.time_in.split(':')[1])
            
            # 19:00:00 through 23:30:00
            if (hour == 19) or (hour >= 20 and hour <= 23):
                if hour == 23 and minute > 30:
                    return False
                    
                language_hint = self._get_language_hint(spot)
                if language_hint in ["M", "C", "M/C"]:
                    self.logger.info(
                        f"DEBUG: Extended Chinese pattern detected for spot {spot.spot_id} - "
                        f"time_in: {spot.time_in}, language: {language_hint}"
                    )
                    return True
        except:
            pass
        
        return False

    def _is_chinese_family_span(self, spot: SpotData, blocks: List[LanguageBlock]) -> bool:
        """ENHANCED: Check if spot spans Chinese family blocks (Mandarin + Cantonese)"""
        
        # Check if spot time pattern matches Chinese evening (19:00-00:00)
        normalized_time_out = self._normalize_time_out(spot.time_out)
        time_pattern_match = (
            spot.time_in == "19:00:00" and 
            normalized_time_out == "00:00:00"
        )
        
        if not time_pattern_match:
            return False
        
        # Check if blocks contain Chinese family languages
        block_languages = set(b.language_id for b in blocks)
        chinese_languages = {2, 3}  # Mandarin=2, Cantonese=3
        
        # Must have at least one Chinese language block
        has_chinese_blocks = bool(block_languages & chinese_languages)
        
        if has_chinese_blocks:
            self.logger.info(f"DEBUG: Chinese family span detected for spot {spot.spot_id} - "
                        f"time: {spot.time_in}-{normalized_time_out}, "
                        f"blocks: {[b.block_name for b in blocks]}")
            return True
        
        return False

    def _is_worldlink_spot(self, spot: SpotData) -> bool:
        """Check if spot is from WorldLink agency (DEBUG VERSION)"""
        cursor = self.db.cursor()
        cursor.execute(
            """
            SELECT a.agency_name, s.bill_code
            FROM spots s
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.spot_id = ?
        """,
            (spot.spot_id,),
        )

        row = cursor.fetchone()
        if not row:
            self.logger.info(f"DEBUG: No data found for spot {spot.spot_id}")
            return False

        agency_name = row[0] or ""
        bill_code = row[1] or ""

        self.logger.info(
            f"DEBUG: Spot {spot.spot_id} - agency_name: '{agency_name}', bill_code: '{bill_code}'"
        )

        is_worldlink = "WorldLink" in agency_name or "WorldLink" in bill_code
        self.logger.info(
            f"DEBUG: WorldLink detection result for spot {spot.spot_id}: {is_worldlink}"
        )

        return is_worldlink

    def _is_ros_by_duration(self, spot: SpotData) -> bool:
        """Check if spot duration > 6 hours (DEBUG VERSION)"""
        duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
        self.logger.info(
            f"DEBUG: Spot {spot.spot_id} duration: {duration} minutes (threshold: 360)"
        )

        is_ros = duration > 360
        self.logger.info(
            f"DEBUG: ROS duration result for spot {spot.spot_id}: {is_ros}"
        )

        return is_ros

    def _is_ros_by_time(self, spot: SpotData) -> bool:
        """FIXED: ROS time check that excludes Chinese and Tagalog patterns"""

        # CRITICAL: If this is a Chinese or Tagalog pattern, it should NOT be ROS
        if self._is_chinese_pattern(spot) or self._is_tagalog_pattern(spot):
            return False

        # Pattern 1: 13:00-23:59 (standard ROS)
        if spot.time_in == "13:00:00" and spot.time_out == "23:59:00":
            return True

        # Pattern 2: Late night to next day patterns (excluding Chinese patterns)
        if "day" in str(spot.time_out):
            start_hour = int(spot.time_in.split(":")[0])

            # Late night starts (after 20:00) running to next day
            # NOTE: 19:00 and 20:00 are handled by Chinese pattern detection
            if start_hour >= 21:
                return True

            # Very early morning starts (before 6:00) running to next day
            if start_hour <= 6:
                return True

        # Pattern 3: Full day patterns
        if spot.time_in == "06:00:00" and spot.time_out == "23:59:00":
            return True

        return False

    def _is_paid_programming(self, spot: SpotData) -> bool:
        """Check if spot is Paid Programming"""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT revenue_type FROM spots WHERE spot_id = ?", (spot.spot_id,)
        )
        row = cursor.fetchone()
        return row and row[0] == "Paid Programming"

    def _get_operational_language_assignment(self, spot: SpotData) -> Optional[str]:
        """Apply operational time-based language rules from master control"""
        
        # CRITICAL: Check duration first - long spots should be ROS, not language-specific
        duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
        
        # If spot is longer than 6 hours (360 minutes), it's likely ROS
        # This prevents all-day and overnight spots from being miscategorized
        if duration > 360:
            self.logger.info(f"DEBUG: Spot {spot.spot_id} duration {duration}min > 360min, skipping operational rules for ROS")
            return None
        
        # Also check for specific ROS time pattern
        if spot.time_in == "13:00:00" and spot.time_out == "23:59:00":
            self.logger.info(f"DEBUG: Spot {spot.spot_id} matches ROS time pattern, skipping operational rules")
            return None
        
        try:
            hour = int(spot.time_in.split(':')[0])
        except:
            return None
        
        # Morning Chinese block: 06:00-08:00
        if 6 <= hour < 8:
            self.logger.info(f"DEBUG: Operational morning Chinese rule applied for spot {spot.spot_id} at {spot.time_in}")
            return 'chinese'
        
        # Evening Chinese block: 19:00-23:59
        if 19 <= hour <= 23:
            # Check for weekend Hmong exception (18:00-20:00)
            if spot.day_of_week in ['Saturday', 'Sunday'] and hour < 20:
                language_hint = self._get_language_hint(spot)
                if language_hint == 'H':
                    self.logger.info(f"DEBUG: Hmong weekend exception for spot {spot.spot_id}")
                    return 'hmong'
            
            # Check if this might be a Tagalog spot that shouldn't be Chinese
            if hour < 19:  # 18:00 hour
                language_hint = self._get_language_hint(spot)
                if language_hint == 'T':
                    return None  # Let normal flow handle it
            
            self.logger.info(f"DEBUG: Operational evening Chinese rule applied for spot {spot.spot_id} at {spot.time_in}")
            return 'chinese'
        
        return None

    def _is_tagalog_pattern(self, spot: SpotData) -> bool:
        """EXPANDED: Check if spot matches Tagalog pattern (16:00-19:00 OR 17:00-19:00 + T)"""

        # EXPANDED: Accept both 16:00:00-19:00:00 AND 17:00:00-19:00:00
        time_match = (spot.time_in == "16:00:00" and spot.time_out == "19:00:00") or (
            spot.time_in == "17:00:00" and spot.time_out == "19:00:00"
        )

        if not time_match:
            return False

        # Must have Tagalog language code
        language_hint = self._get_language_hint(spot)
        language_match = language_hint == "T"

        # Debug logging
        if time_match and language_match:
            self.logger.info(
                f"DEBUG: Tagalog pattern detected for spot {spot.spot_id} - time_in: {spot.time_in}, time_out: {spot.time_out}, language: {language_hint}"
            )

        return time_match and language_match

    def _is_chinese_pattern(self, spot: SpotData) -> bool:
        """FIXED: Check if spot matches Chinese pattern - now includes 20:00:00 start times"""

        # Chinese patterns can start at 19:00:00 OR 20:00:00
        if spot.time_in not in ["19:00:00", "20:00:00"]:
            return False

        # Must end at 23:59:00 OR contain "day" (for "1 day, 0:00:00")
        time_out_match = (
            spot.time_out == "23:59:00"
            or spot.time_out == "24:00:00"
            or "day" in str(spot.time_out)
        )

        if not time_out_match:
            return False

        # Must have Chinese language code
        language_hint = self._get_language_hint(spot)
        language_match = language_hint in ["M", "C", "M/C"]

        # Debug logging
        if time_out_match and language_match:
            self.logger.info(
                f"DEBUG: Chinese pattern detected for spot {spot.spot_id} - time_in: {spot.time_in}, time_out: {spot.time_out}, language: {language_hint}"
            )

        return time_out_match and language_match

    def _get_language_hint(self, spot: SpotData) -> Optional[str]:
        """Get original language hint from spots.language_code"""
        cursor = self.db.cursor()
        cursor.execute(
            "SELECT language_code FROM spots WHERE spot_id = ?", (spot.spot_id,)
        )
        row = cursor.fetchone()
        return row[0] if row and row[0] else None

    def _create_chinese_assignment(
        self, spot: SpotData, schedule_id: int, blocks: List[LanguageBlock]
    ) -> AssignmentResult:
        """ENHANCED: Create Chinese family assignment from enhanced rule"""
        # Find Chinese block from the available blocks (Mandarin or Cantonese)
        chinese_block = self._find_chinese_block([b.block_id for b in blocks])

        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=chinese_block.block_id if chinese_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,
            blocks_spanned=[b.block_id for b in blocks],
            primary_block_id=chinese_block.block_id if chinese_block else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="language_specific",
            business_rule_applied="chinese_pattern",
            auto_resolved_date=datetime.now(),
        )

    def _create_tagalog_assignment(
        self, spot: SpotData, schedule_id: int, blocks: List[LanguageBlock]
    ) -> AssignmentResult:
        """ENHANCED: Create Tagalog assignment from enhanced rule"""
        # Find Tagalog block from the available blocks
        tagalog_block = self._find_tagalog_block([b.block_id for b in blocks])

        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=tagalog_block.block_id if tagalog_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,
            blocks_spanned=[b.block_id for b in blocks],
            primary_block_id=tagalog_block.block_id if tagalog_block else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="language_specific",
            business_rule_applied="tagalog_pattern",
            auto_resolved_date=datetime.now(),
        )

    def _create_direct_response_assignment(
        self, spot: SpotData, schedule_id: int, blocks: List[LanguageBlock]
    ) -> AssignmentResult:
        """FIXED: Create direct_response assignment with proper constraint values"""
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            blocks_spanned=[b.block_id for b in blocks]
            if blocks
            else [],  # FIXED: Empty list instead of None
            primary_block_id=blocks[0].block_id if blocks else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="direct_response",
            business_rule_applied="worldlink_direct_response",
            auto_resolved_date=datetime.now(),
        )

    def _create_ros_assignment(
        self,
        spot: SpotData,
        schedule_id: int,
        blocks: List[LanguageBlock],
        rule_type: str,
    ) -> AssignmentResult:
        """FIXED: Create ROS assignment with proper constraint values"""
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            blocks_spanned=[b.block_id for b in blocks]
            if blocks
            else [],  # FIXED: Empty list instead of None
            primary_block_id=blocks[0].block_id if blocks else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="ros",
            business_rule_applied=rule_type,
            auto_resolved_date=datetime.now(),
        )

    def _create_tagalog_assignment(
        self, spot: SpotData, schedule_id: int, blocks: List[LanguageBlock]
    ) -> AssignmentResult:
        """Create Tagalog assignment from enhanced rule"""
        # Find Tagalog block from the available blocks
        tagalog_block = self._find_tagalog_block([b.block_id for b in blocks])

        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=tagalog_block.block_id if tagalog_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,
            blocks_spanned=[b.block_id for b in blocks],
            primary_block_id=tagalog_block.block_id if tagalog_block else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="language_specific",
            business_rule_applied="tagalog_pattern",
            auto_resolved_date=datetime.now(),
        )

    def _create_chinese_assignment(
        self, spot: SpotData, schedule_id: int, blocks: List[LanguageBlock]
    ) -> AssignmentResult:
        """Create Chinese family assignment from enhanced rule"""
        # Find Chinese block from the available blocks (Mandarin or Cantonese)
        chinese_block = self._find_chinese_block([b.block_id for b in blocks])

        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=chinese_block.block_id if chinese_block else None,
            customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
            spans_multiple_blocks=False,
            blocks_spanned=[b.block_id for b in blocks],
            primary_block_id=chinese_block.block_id if chinese_block else None,
            requires_attention=False,
            alert_reason=None,
            campaign_type="language_specific",
            business_rule_applied="chinese_pattern",
            auto_resolved_date=datetime.now(),
        )

    def _find_tagalog_block(self, block_ids: List[int]) -> Optional[LanguageBlock]:
        """Find Tagalog block from list of block IDs"""
        if not block_ids:
            return None

        cursor = self.db.cursor()
        placeholders = ",".join(["?"] * len(block_ids))
        query = f"""
        SELECT lb.block_id, lb.schedule_id, lb.day_of_week, lb.time_start, lb.time_end,
            lb.language_id, lb.block_name, lb.block_type, lb.day_part
        FROM language_blocks lb
        JOIN languages l ON lb.language_id = l.language_id
        WHERE lb.block_id IN ({placeholders})
        AND l.language_code = 'T'
        LIMIT 1
        """

        try:
            cursor.execute(query, block_ids)
            row = cursor.fetchone()

            if row:
                return LanguageBlock(
                    block_id=row[0],
                    schedule_id=row[1],
                    day_of_week=row[2],
                    time_start=row[3],
                    time_end=row[4],
                    language_id=row[5],
                    block_name=row[6],
                    block_type=row[7],
                    day_part=row[8],
                )
        except Exception as e:
            self.logger.error(f"Error finding Tagalog block: {e}")

        return None

    def _find_chinese_block(self, block_ids: List[int]) -> Optional[LanguageBlock]:
        """Find Chinese block (Mandarin or Cantonese) from list of block IDs"""
        if not block_ids:
            return None

        cursor = self.db.cursor()
        placeholders = ",".join(["?"] * len(block_ids))
        query = f"""
        SELECT block_id, schedule_id, day_of_week, time_start, time_end,
            language_id, block_name, block_type, day_part
        FROM language_blocks lb
        WHERE lb.block_id IN ({placeholders})
        AND lb.language_id IN (2, 3)  -- Mandarin=2, Cantonese=3
        ORDER BY lb.language_id  -- Prefer Mandarin (2) over Cantonese (3)
        LIMIT 1
        """

        try:
            cursor.execute(query, block_ids)
            row = cursor.fetchone()

            if row:
                return LanguageBlock(
                    block_id=row[0],
                    schedule_id=row[1],
                    day_of_week=row[2],
                    time_start=row[3],
                    time_end=row[4],
                    language_id=row[5],
                    block_name=row[6],
                    block_type=row[7],
                    day_part=row[8],
                )
        except Exception as e:
            self.logger.error(f"Error finding Chinese block: {e}")

        return None

    def _find_primary_chinese_block(self, blocks: List[LanguageBlock]) -> Optional[LanguageBlock]:
        """ENHANCED: Find primary Chinese block, preferring Mandarin Prime Block"""
        
        # First, try to find Mandarin Prime Block
        mandarin_prime = None
        mandarin_blocks = []
        cantonese_blocks = []
        
        for block in blocks:
            if block.language_id == 2:  # Mandarin
                mandarin_blocks.append(block)
                if 'Prime' in block.block_name:
                    mandarin_prime = block
            elif block.language_id == 3:  # Cantonese
                cantonese_blocks.append(block)
        
        # Priority order:
        # 1. Mandarin Prime Block (if exists)
        if mandarin_prime:
            return mandarin_prime
        
        # 2. Any Mandarin block
        if mandarin_blocks:
            return mandarin_blocks[0]
        
        # 3. Any Cantonese block
        if cantonese_blocks:
            return cantonese_blocks[0]
        
        # 4. First block as fallback
        return blocks[0] if blocks else None

    def _analyze_single_block_intent(
        self, spot: SpotData, block: LanguageBlock
    ) -> CustomerIntent:
        """Analyze customer intent for single block assignment"""
        if spot.language_id and spot.language_id == block.language_id:
            return CustomerIntent.LANGUAGE_SPECIFIC
        elif spot.language_id and spot.language_id != block.language_id:
            return CustomerIntent.TIME_SPECIFIC
        else:
            return CustomerIntent.LANGUAGE_SPECIFIC

    def _analyze_multi_block_intent(
        self, spot: SpotData, blocks: List[LanguageBlock]
    ) -> CustomerIntent:
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

    def _select_primary_block(
        self, spot: SpotData, blocks: List[LanguageBlock]
    ) -> Optional[LanguageBlock]:
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
        """FIXED: Calculate spot duration in minutes, handling "1 day, 0:00:00" format properly"""
        try:
            # FIXED: Normalize time_out first
            normalized_time_out = self._normalize_time_out(time_out)
            
            start_minutes = self._time_to_minutes(time_in)
            
            # FIXED: Handle midnight (00:00:00) as next day
            if normalized_time_out == '00:00:00':
                end_minutes = 1440  # 24 * 60 = next day midnight
            else:
                end_minutes = self._time_to_minutes(normalized_time_out)
            
            # Calculate duration
            if end_minutes >= start_minutes:
                duration = end_minutes - start_minutes
            else:
                # Handle midnight rollover for other cases
                duration = (1440 - start_minutes) + end_minutes
            
            return duration
        except:
            return 0

    def _determine_campaign_type(
        self, intent: CustomerIntent, duration_minutes: int, block_count: int
    ) -> str:
        """Determine campaign type based on intent, duration, and block count"""

        if intent == CustomerIntent.LANGUAGE_SPECIFIC:
            return "language_specific"

        elif intent == CustomerIntent.INDIFFERENT:
            # ROS detection: 17+ hours (1020+ minutes) or 15+ blocks
            if duration_minutes >= 1020 or block_count >= 15:
                return "ros"
            else:
                return "multi_language"

        else:  # TIME_SPECIFIC
            return "language_specific"

    def _save_assignment(self, result: AssignmentResult):
        """FIXED: Save assignment to database - handle None schedule_id properly"""
        cursor = self.db.cursor()

        # Handle case where no schedule_id exists (no grid coverage)
        if result.schedule_id is None:
            # For spots with no grid coverage, we can still save the assignment
            # but without schedule_id or block_id
            cursor.execute(
                "DELETE FROM spot_language_blocks WHERE spot_id = ?", (result.spot_id,)
            )

            cursor.execute(
                """
                INSERT INTO spot_language_blocks (
                    spot_id, schedule_id, block_id, customer_intent, intent_confidence,
                    spans_multiple_blocks, blocks_spanned, primary_block_id,
                    assignment_method, assigned_date, assigned_by,
                    requires_attention, alert_reason, notes, campaign_type,
                    business_rule_applied, auto_resolved_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    result.spot_id,
                    None,  # schedule_id can be None for no grid coverage
                    None,  # block_id will be None for no grid coverage
                    result.customer_intent.value if result.customer_intent else None,
                    1.0,  # Default confidence
                    result.spans_multiple_blocks,
                    str(result.blocks_spanned)
                    if result.blocks_spanned is not None
                    else None,
                    result.primary_block_id,
                    AssignmentMethod.AUTO_COMPUTED.value,
                    datetime.now().isoformat(),
                    "system",
                    result.requires_attention,
                    result.alert_reason,
                    result.error_message,
                    result.campaign_type,
                    result.business_rule_applied,
                    result.auto_resolved_date.isoformat()
                    if result.auto_resolved_date
                    else None,
                ),
            )
        else:
            # Normal assignment with schedule_id
            cursor.execute(
                "DELETE FROM spot_language_blocks WHERE spot_id = ?", (result.spot_id,)
            )

            cursor.execute(
                """
                INSERT INTO spot_language_blocks (
                    spot_id, schedule_id, block_id, customer_intent, intent_confidence,
                    spans_multiple_blocks, blocks_spanned, primary_block_id,
                    assignment_method, assigned_date, assigned_by,
                    requires_attention, alert_reason, notes, campaign_type,
                    business_rule_applied, auto_resolved_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    result.spot_id,
                    result.schedule_id,
                    result.block_id,
                    result.customer_intent.value if result.customer_intent else None,
                    1.0,  # Default confidence
                    result.spans_multiple_blocks,
                    str(result.blocks_spanned)
                    if result.blocks_spanned is not None
                    else None,
                    result.primary_block_id,
                    AssignmentMethod.AUTO_COMPUTED.value,
                    datetime.now().isoformat(),
                    "system",
                    result.requires_attention,
                    result.alert_reason,
                    result.error_message,
                    result.campaign_type,
                    result.business_rule_applied,
                    result.auto_resolved_date.isoformat()
                    if result.auto_resolved_date
                    else None,
                ),
            )

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
            customer_id=row[7],
        )

    def _get_applicable_schedule(self, market_id: int, air_date: str) -> Optional[int]:
        """ENHANCED: Find applicable programming schedule with better error handling"""
        cursor = self.db.cursor()

        self.logger.info(
            f"DEBUG: Looking for schedule for market_id={market_id}, air_date={air_date}"
        )

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
            self.logger.info(
                f"DEBUG: Found schedule {row[0]} ({row[1]}) for market {market_id}"
            )
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
            self.logger.info(
                f"DEBUG: Using fallback schedule {row[0]} ({row[1]}) for market {market_id}"
            )
            return row[0]

        self.logger.info(f"DEBUG: No schedule found for market {market_id}")
        return None

    def _get_overlapping_blocks(self, schedule_id: int, day_of_week: str, 
                            time_in: str, time_out: str) -> List[LanguageBlock]:
        """FIXED: Find language blocks that overlap with spot time, handling midnight rollover"""
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

        # FIXED: Normalize time_out before processing
        normalized_time_out = self._normalize_time_out(time_out)
        
        cursor.execute(query, (schedule_id, day_of_week))
        rows = cursor.fetchall()
        
        # Check for time overlap
        overlapping_blocks = []
        spot_start_minutes = self._time_to_minutes(time_in)
        spot_end_minutes = self._time_to_minutes(normalized_time_out)
        
        # FIXED: Handle midnight rollover (00:00:00 = 1440 minutes)
        if normalized_time_out == '00:00:00':
            spot_end_minutes = 1440  # 24 * 60 = next day midnight
        
        for row in rows:
            block_start_minutes = self._time_to_minutes(row[3])
            block_end_minutes = self._time_to_minutes(row[4])
            
            # FIXED: Handle midnight rollover in block times
            if row[4] == '00:00:00':
                block_end_minutes = 1440
            
            # Check for overlap with midnight rollover support
            if self._times_overlap_with_midnight(
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
            "spot_id": row[0],
            "bill_code": row[1],
            "air_date": row[2],
            "time_in": row[3],
            "time_out": row[4],
            "market_code": row[5],
            "customer_intent": row[6],
            "spans_multiple_blocks": bool(row[7]) if row[7] is not None else False,
            "requires_attention": bool(row[8]) if row[8] is not None else False,
            "alert_reason": row[9],
            "block_name": row[10],
            "day_part": row[11],
            "business_rule_applied": row[12],
            "auto_resolved_date": row[13],
        }

    def _time_to_minutes(self, time_str: str) -> int:
        """Convert HH:MM:SS time string to minutes since midnight"""
        try:
            parts = time_str.split(":")
            hours = int(parts[0])
            minutes = int(parts[1])
            return hours * 60 + minutes
        except (ValueError, IndexError):
            return 0

    def _times_overlap(self, start1: int, end1: int, start2: int, end2: int) -> bool:
        """Check if two time ranges overlap"""
        return start1 < end2 and end1 > start2

    def _times_overlap_with_midnight(self, start1: int, end1: int, start2: int, end2: int) -> bool:
        """FIXED: Check if two time ranges overlap, handling midnight rollover"""
        # Standard overlap check
        if start1 < end2 and end1 > start2:
            return True
        
        # Special case: if either range crosses midnight (end > 1440)
        if end1 > 1440 or end2 > 1440:
            # Convert to handle midnight rollover
            if end1 > 1440:
                end1 = end1 - 1440
            if end2 > 1440:
                end2 = end2 - 1440
            
            # Check overlap considering rollover
            return start1 < end2 and end1 > start2
        
        return False

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

        stats = {"enhanced_rules": {}, "total_enhanced": 0, "total_standard": 0}

        for row in results:
            rule_name = row[0]
            stats["enhanced_rules"][rule_name] = {
                "count": row[1],
                "avg_confidence": row[2],
                "first_applied": row[3],
                "last_applied": row[4],
            }
            stats["total_enhanced"] += row[1]

        # Get total standard assignments
        cursor.execute(
            "SELECT COUNT(*) FROM spot_language_blocks WHERE business_rule_applied IS NULL"
        )
        stats["total_standard"] = cursor.fetchone()[0]

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

    cursor.execute(query, (f"%-{year_suffix}",))
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

    cursor.execute(query, (f"%-{year_suffix}",))
    return [row[0] for row in cursor.fetchall()]


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Language Block Assignment Tool")
    parser.add_argument(
        "--database", default="data/database/production.db", help="Database path"
    )

    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--test", type=int, metavar="N", help="Test assignment with N spots"
    )
    mode_group.add_argument(
        "--batch", type=int, metavar="N", help="Assign N unassigned spots"
    )
    mode_group.add_argument(
        "--all-year",
        type=int,
        metavar="YYYY",
        help="Assign all unassigned spots for specific year (e.g., 2023, 2024, 2025)",
    )
    mode_group.add_argument(
        "--status", action="store_true", help="Show current assignment status"
    )
    mode_group.add_argument(
        "--force-year",
        type=int,
        metavar="YYYY",
        help="Force reassignment of all spots for specific year",
    )

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
            print(
                f"{'Year':6} {'Total Spots':>12} {'Assigned':>10} {'Unassigned':>12} {'Assigned %':>10} {'Unassigned Revenue':>18}"
            )
            print("-" * 85)

            for row in year_summary:
                year = row[0]
                total_spots = row[1]
                assigned_spots = row[2]
                unassigned_spots = row[3]
                unassigned_revenue = row[6]
                assigned_pct = (
                    (assigned_spots / total_spots * 100) if total_spots > 0 else 0
                )

                print(
                    f"{year:6} {total_spots:>12,} {assigned_spots:>10,} {unassigned_spots:>12,} {assigned_pct:>9.1f}% ${unassigned_revenue:>17,.0f}"
                )

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

            if results["spot_details"] and len(results["spot_details"]) > 0:
                print(f"\n📋 SAMPLE ASSIGNMENTS:")
                for detail in results["spot_details"][:5]:  # Show first 5
                    intent = detail["customer_intent"] or "None"
                    block = detail["block_name"] or "No block"
                    print(
                        f"   • Spot {detail['spot_id']} ({detail['bill_code']}): {intent} → {block}"
                    )

        elif args.force_year:
            # Force reassignment mode
            year = args.force_year

            # Delete existing assignments for the year
            year_suffix = str(year)[-2:]
            cursor = conn.cursor()
            cursor.execute(
                """
                DELETE FROM spot_language_blocks 
                WHERE spot_id IN (
                    SELECT spot_id FROM spots 
                    WHERE broadcast_month LIKE ?
                )
            """,
                (f"%-{year_suffix}",),
            )

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
            cursor.execute(
                """
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
            """,
                (args.batch,),
            )

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
            if confirm not in ["yes", "y"]:
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

            success_rate = (
                (results["assigned"] + results["multi_block"])
                / results["processed"]
                * 100
                if results["processed"] > 0
                else 0
            )
            print(f"   • Success rate: {success_rate:.1f}%")

        return 0

    except Exception as e:
        print(f"❌ Error during assignment: {e}")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())
