"""
Enhanced Language Block Service (Updated for CHECK Constraint Compatibility)
===========================================================================

Updated to work with existing CHECK constraint on assignment_method.
Uses 'auto_computed' for assignment_method and business_rule_applied column to track rules.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from .business_rules_service import BusinessRulesService
from ..models.business_rules_models import (
    BusinessRuleResult,
    SpotData,
    AssignmentResult,
    AssignmentMethod,
    CustomerIntent,
)


class EnhancedLanguageBlockService:
    """Enhanced language block service with business rules integration"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.business_rules = BusinessRulesService(db_connection)
        self.logger = logging.getLogger(__name__)

        # Statistics tracking
        self.stats = {
            "total_processed": 0,
            "business_rule_resolved": 0,
            "business_rule_flagged": 0,
            "standard_assignment": 0,
            "errors": 0,
        }

    def assign_single_spot(self, spot_id: int) -> AssignmentResult:
        """
        Assign a single spot with business rules integration

        Args:
            spot_id: ID of the spot to assign

        Returns:
            AssignmentResult with assignment details
        """
        self.stats["total_processed"] += 1

        try:
            # Step 1: Get spot data for business rule evaluation
            spot_data = self.business_rules.get_spot_data_from_db(spot_id)
            if not spot_data:
                return self._create_error_result(
                    spot_id, "Spot not found or invalid data"
                )

            # Step 2: Apply business rules first
            rule_result = self.business_rules.evaluate_spot(spot_data)

            # Step 3: Handle business rule results
            if rule_result.auto_resolved:
                # Auto-resolve based on business rule
                self.stats["business_rule_resolved"] += 1
                assignment_result = self._create_business_rule_assignment(
                    spot_data, rule_result
                )
                self._save_assignment(assignment_result)
                return assignment_result

            elif rule_result.requires_attention:
                # Flag for manual review
                self.stats["business_rule_flagged"] += 1
                assignment_result = self._create_flagged_assignment(
                    spot_data, rule_result
                )
                self._save_assignment(assignment_result)
                return assignment_result

            else:
                # No business rule matched - use standard assignment logic
                self.stats["standard_assignment"] += 1
                return self._run_standard_assignment(spot_id, spot_data)

        except Exception as e:
            self.logger.error(f"Error in enhanced assignment for spot {spot_id}: {e}")
            self.stats["errors"] += 1
            return self._create_error_result(spot_id, str(e))

    def assign_spots_batch(
        self, spot_ids: List[int] = None, limit: int = None
    ) -> Dict[str, Any]:
        """
        Assign multiple spots to language blocks with business rules

        Args:
            spot_ids: List of specific spot IDs to assign
            limit: Maximum number of spots to assign if spot_ids not provided

        Returns:
            Dictionary with batch assignment statistics
        """
        self.logger.info("Starting batch spot assignment with business rules")

        # Reset batch stats
        batch_stats = {
            "processed": 0,
            "business_rule_resolved": 0,
            "business_rule_flagged": 0,
            "standard_assignment": 0,
            "errors": 0,
            "start_time": datetime.now(),
        }

        try:
            # Get spots to process
            if spot_ids:
                spots_to_process = spot_ids
            else:
                spots_to_process = self._get_unassigned_spot_ids(limit)

            self.logger.info(
                f"Processing {len(spots_to_process)} spots with business rules"
            )

            # Process each spot
            for i, spot_id in enumerate(spots_to_process):
                try:
                    result = self.assign_single_spot(spot_id)

                    # Update batch stats
                    batch_stats["processed"] += 1
                    if result.business_rule_applied:
                        if result.requires_attention:
                            batch_stats["business_rule_flagged"] += 1
                        else:
                            batch_stats["business_rule_resolved"] += 1
                    elif result.success:
                        batch_stats["standard_assignment"] += 1
                    else:
                        batch_stats["errors"] += 1

                    # Log progress every 100 spots
                    if (i + 1) % 100 == 0:
                        self.logger.info(
                            f"Processed {i + 1}/{len(spots_to_process)} spots..."
                        )

                except Exception as e:
                    self.logger.error(f"Failed to process spot {spot_id}: {e}")
                    batch_stats["errors"] += 1

            batch_stats["end_time"] = datetime.now()
            batch_stats["duration"] = (
                batch_stats["end_time"] - batch_stats["start_time"]
            ).total_seconds()

            self.logger.info(f"Batch assignment completed: {batch_stats}")
            return batch_stats

        except Exception as e:
            self.logger.error(f"Error in batch assignment: {e}")
            batch_stats["errors"] += 1
            return batch_stats

    def _create_business_rule_assignment(
        self, spot_data: SpotData, rule_result: BusinessRuleResult
    ) -> AssignmentResult:
        """Create assignment result for auto-resolved business rule"""
        schedule_id = self._get_applicable_schedule(
            spot_data.market_id, spot_data.air_date
        )

        return AssignmentResult(
            spot_id=spot_data.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=None,  # Business rules typically span multiple blocks
            customer_intent=rule_result.customer_intent,
            spans_multiple_blocks=True,
            requires_attention=False,
            alert_reason=rule_result.alert_reason,
            assignment_method=AssignmentMethod.AUTO_COMPUTED,  # Use existing CHECK constraint value
            business_rule_applied=rule_result.rule_applied.rule_type.value,
            confidence=rule_result.confidence,
            notes=rule_result.notes,
            assigned_date=datetime.now(),
        )

    def _create_flagged_assignment(
        self, spot_data: SpotData, rule_result: BusinessRuleResult
    ) -> AssignmentResult:
        """Create assignment result for flagged business rule"""
        schedule_id = self._get_applicable_schedule(
            spot_data.market_id, spot_data.air_date
        )

        return AssignmentResult(
            spot_id=spot_data.spot_id,
            success=True,
            schedule_id=schedule_id,
            block_id=None,
            customer_intent=rule_result.customer_intent,
            spans_multiple_blocks=False,
            requires_attention=True,
            alert_reason=rule_result.alert_reason,
            assignment_method=AssignmentMethod.AUTO_COMPUTED,  # Use existing CHECK constraint value
            business_rule_applied=rule_result.rule_applied.rule_type.value
            if rule_result.rule_applied
            else None,
            confidence=rule_result.confidence,
            notes=rule_result.notes,
            assigned_date=datetime.now(),
        )

    def _run_standard_assignment(
        self, spot_id: int, spot_data: SpotData
    ) -> AssignmentResult:
        """Run standard assignment logic for spots that don't match business rules"""
        try:
            # Get applicable schedule
            schedule_id = self._get_applicable_schedule(
                spot_data.market_id, spot_data.air_date
            )

            if not schedule_id:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No programming grid for market",
                    assignment_method=AssignmentMethod.NO_GRID_AVAILABLE,
                    notes="No programming schedule found for market",
                )

            # Find overlapping blocks (simplified - integrate with your existing logic)
            blocks = self._get_overlapping_blocks(schedule_id, spot_data)

            if not blocks:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    customer_intent=CustomerIntent.NO_GRID_COVERAGE,
                    requires_attention=True,
                    alert_reason="No language blocks cover spot time",
                    assignment_method=AssignmentMethod.AUTO_COMPUTED,
                    notes="No overlapping language blocks found",
                )

            # Determine assignment based on block count
            if len(blocks) == 1:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=blocks[0]["block_id"],
                    customer_intent=CustomerIntent.LANGUAGE_SPECIFIC,
                    spans_multiple_blocks=False,
                    assignment_method=AssignmentMethod.AUTO_COMPUTED,
                    notes="Single block assignment",
                )
            else:
                return AssignmentResult(
                    spot_id=spot_id,
                    success=True,
                    schedule_id=schedule_id,
                    block_id=None,
                    customer_intent=CustomerIntent.INDIFFERENT,
                    spans_multiple_blocks=True,
                    blocks_spanned=[b["block_id"] for b in blocks],
                    primary_block_id=blocks[0]["block_id"],
                    requires_attention=len(blocks) > 3,
                    assignment_method=AssignmentMethod.AUTO_COMPUTED,
                    notes=f"Multi-block assignment ({len(blocks)} blocks)",
                )

        except Exception as e:
            self.logger.error(f"Error in standard assignment for spot {spot_id}: {e}")
            return self._create_error_result(spot_id, str(e))

    def _get_applicable_schedule(self, market_id: int, air_date: str) -> Optional[int]:
        """Find applicable programming schedule for market and date"""
        cursor = self.db_connection.cursor()

        query = """
        SELECT ps.schedule_id
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

        return row[0] if row else None

    def _get_overlapping_blocks(
        self, schedule_id: int, spot_data: SpotData
    ) -> List[Dict[str, Any]]:
        """Find language blocks that overlap with spot time (simplified)"""
        cursor = self.db_connection.cursor()

        query = """
        SELECT block_id, block_name, language_id, time_start, time_end
        FROM language_blocks
        WHERE schedule_id = ?
          AND LOWER(day_of_week) = LOWER(?)
          AND is_active = 1
        ORDER BY time_start
        """

        # Extract day of week from spot data (this would come from your existing logic)
        day_of_week = (
            spot_data.air_date
        )  # Simplified - you'd extract day of week properly

        cursor.execute(query, (schedule_id, "monday"))  # Simplified
        rows = cursor.fetchall()

        # Return simplified block data
        return [
            {
                "block_id": row[0],
                "block_name": row[1],
                "language_id": row[2],
                "time_start": row[3],
                "time_end": row[4],
            }
            for row in rows
        ]

    def _save_assignment(self, result: AssignmentResult):
        """Save assignment to database (compatible with existing CHECK constraint)"""
        cursor = self.db_connection.cursor()

        # Delete existing assignment if exists
        cursor.execute(
            "DELETE FROM spot_language_blocks WHERE spot_id = ?", (result.spot_id,)
        )

        # Map AssignmentMethod enum to string values compatible with CHECK constraint
        assignment_method_value = self._map_assignment_method(result.assignment_method)

        # Insert new assignment
        cursor.execute(
            """
            INSERT INTO spot_language_blocks (
                spot_id, schedule_id, block_id, customer_intent, intent_confidence,
                spans_multiple_blocks, blocks_spanned, primary_block_id,
                assignment_method, assigned_date, assigned_by,
                requires_attention, alert_reason, notes,
                business_rule_applied, auto_resolved_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                result.spot_id,
                result.schedule_id,
                result.block_id,
                result.customer_intent.value if result.customer_intent else None,
                result.confidence,
                result.spans_multiple_blocks,
                str(result.blocks_spanned) if result.blocks_spanned else None,
                result.primary_block_id,
                assignment_method_value,
                result.assigned_date.isoformat(),
                "enhanced_service",
                result.requires_attention,
                result.alert_reason,
                result.notes,
                result.business_rule_applied,
                result.assigned_date.isoformat()
                if result.business_rule_applied
                else None,
            ),
        )

        self.db_connection.commit()

    def _map_assignment_method(self, method: AssignmentMethod) -> str:
        """Map AssignmentMethod enum to values compatible with CHECK constraint"""
        # Map our enum values to the allowed CHECK constraint values
        mapping = {
            AssignmentMethod.AUTO_COMPUTED: "auto_computed",
            AssignmentMethod.MANUAL_OVERRIDE: "manual_override",
            AssignmentMethod.NO_GRID_AVAILABLE: "no_grid_available",
            # Map our new business rule methods to existing allowed values
            AssignmentMethod.BUSINESS_RULE_AUTO_RESOLVED: "auto_computed",
            AssignmentMethod.BUSINESS_RULE_FLAGGED: "auto_computed",
        }

        return mapping.get(method, "auto_computed")

    def _get_unassigned_spot_ids(self, limit: int = None) -> List[int]:
        """Get spot IDs that don't have language block assignments"""
        cursor = self.db_connection.cursor()

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

    def _create_error_result(
        self, spot_id: int, error_message: str
    ) -> AssignmentResult:
        """Create error assignment result"""
        return AssignmentResult(
            spot_id=spot_id,
            success=False,
            error_message=error_message,
            assignment_method=AssignmentMethod.AUTO_COMPUTED,
            assigned_date=datetime.now(),
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics"""
        return {
            "service_stats": self.stats,
            "business_rules_stats": self.business_rules.get_stats(),
        }

    def get_business_rules_stats(self) -> Dict[str, Any]:
        """Get business rules statistics"""
        return self.business_rules.get_stats()

    def estimate_business_rules_impact(self) -> Dict[str, Any]:
        """Estimate the impact of business rules on all spots"""
        return self.business_rules.estimate_total_impact()

    def reset_stats(self):
        """Reset statistics"""
        self.stats = {
            "total_processed": 0,
            "business_rule_resolved": 0,
            "business_rule_flagged": 0,
            "standard_assignment": 0,
            "errors": 0,
        }
        self.business_rules.reset_stats()

    def get_rules_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all business rules"""
        return self.business_rules.get_rules_summary()
