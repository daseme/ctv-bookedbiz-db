# src/services/pipeline_decay.py
"""
Pipeline Decay System - Real-time pipeline adjustment between manual calibration sessions.

This system automatically adjusts pipeline values as revenue is booked or removed,
creating a dynamic baseline that makes bi-weekly reviews more meaningful.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DecayEventType(Enum):
    """Types of decay events that can occur."""

    REVENUE_BOOKED = "revenue_booked"
    REVENUE_REMOVED = "revenue_removed"
    MANUAL_ADJUSTMENT = "manual_adjustment"
    CALIBRATION_RESET = "calibration_reset"


@dataclass
class DecayEvent:
    """Represents a single pipeline decay event."""

    timestamp: str
    event_type: DecayEventType
    ae_id: str
    month: str
    amount: float  # Negative for bookings, positive for removals
    old_pipeline: float
    new_pipeline: float
    customer: Optional[str] = None
    description: Optional[str] = None
    created_by: str = "system"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        data["event_type"] = self.event_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DecayEvent":
        """Create from dictionary."""
        data = data.copy()
        data["event_type"] = DecayEventType(data["event_type"])
        return cls(**data)


@dataclass
class DecaySummary:
    """Summary of pipeline decay for a specific month."""

    ae_id: str
    month: str
    calibrated_pipeline: float
    current_pipeline: float
    calibration_date: str
    days_since_calibration: int
    total_decay: float
    decay_events: List[DecayEvent]
    calibration_session_id: Optional[str] = None

    @property
    def decay_rate_per_day(self) -> float:
        """Calculate average decay per day since calibration."""
        if self.days_since_calibration <= 0:
            return 0.0
        return self.total_decay / self.days_since_calibration

    @property
    def decay_percentage(self) -> float:
        """Calculate decay as percentage of calibrated pipeline."""
        if self.calibrated_pipeline == 0:
            return 0.0
        return (self.total_decay / self.calibrated_pipeline) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ae_id": self.ae_id,
            "month": self.month,
            "calibrated_pipeline": self.calibrated_pipeline,
            "current_pipeline": self.current_pipeline,
            "calibration_date": self.calibration_date,
            "days_since_calibration": self.days_since_calibration,
            "total_decay": self.total_decay,
            "decay_rate_per_day": self.decay_rate_per_day,
            "decay_percentage": self.decay_percentage,
            "calibration_session_id": self.calibration_session_id,
            "decay_events": [event.to_dict() for event in self.decay_events],
        }


class PipelineDecayEngine:
    """
    Core engine for pipeline decay calculations and management.

    Handles:
    - Real-time pipeline adjustments
    - Decay event tracking
    - Calibration baseline management
    - Decay analytics and reporting
    """

    def __init__(self, pipeline_service):
        """Initialize decay engine with pipeline service."""
        self.pipeline_service = pipeline_service
        self.data_path = pipeline_service.data_path
        self.decay_file = os.path.join(self.data_path, "pipeline_decay.json")

        # Ensure decay tracking file exists
        self._ensure_decay_file_exists()

    def _ensure_decay_file_exists(self):
        """Ensure pipeline decay tracking file exists."""
        if not os.path.exists(self.decay_file):
            default_decay_data = {
                "schema_version": "1.0",
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "decay_tracking": {},
                "metadata": {
                    "created_by": "pipeline_decay_engine",
                    "created_date": datetime.utcnow().isoformat() + "Z",
                    "description": "Pipeline decay event tracking and analytics",
                },
            }
            self._write_decay_data(default_decay_data)

    def _read_decay_data(self) -> Dict[str, Any]:
        """Read decay tracking data safely."""
        return self.pipeline_service._read_json_safely(self.decay_file)

    def _write_decay_data(self, data: Dict[str, Any]):
        """Write decay tracking data safely."""
        self.pipeline_service._write_json_safely(self.decay_file, data)

    def apply_pipeline_decay(
        self,
        ae_id: str,
        month: str,
        revenue_change: float,
        event_type: DecayEventType,
        customer: str = None,
        description: str = None,
        created_by: str = "system",
    ) -> bool:
        """
        Apply pipeline decay based on revenue changes.

        Args:
            ae_id: Account Executive ID
            month: Month in YYYY-MM format
            revenue_change: Amount of change (negative for bookings, positive for removals)
            event_type: Type of decay event
            customer: Customer name (optional)
            description: Description of the change
            created_by: Who initiated the change

        Returns:
            bool: Success status
        """
        try:
            # Get current pipeline data
            current_data = self.pipeline_service.get_pipeline_data(ae_id, month)
            if not current_data:
                logger.warning(f"No pipeline data found for {ae_id} {month}")
                return False

            # Calculate new pipeline value
            old_pipeline = current_data.get("current_pipeline", 0.0)
            new_pipeline = max(
                0.0, old_pipeline + revenue_change
            )  # Pipeline can't go negative

            # Create decay event
            decay_event = DecayEvent(
                timestamp=datetime.utcnow().isoformat() + "Z",
                event_type=event_type,
                ae_id=ae_id,
                month=month,
                amount=revenue_change,
                old_pipeline=old_pipeline,
                new_pipeline=new_pipeline,
                customer=customer,
                description=description or f"{event_type.value}: {revenue_change:+.0f}",
                created_by=created_by,
            )

            # Update pipeline data
            update_success = self.pipeline_service.update_pipeline_data(
                ae_id=ae_id,
                month=month,
                pipeline_update={"current_pipeline": new_pipeline},
                updated_by=f"decay_engine_{created_by}",
            )

            if not update_success:
                logger.error(f"Failed to update pipeline data for decay event")
                return False

            # Record decay event
            self._record_decay_event(decay_event)

            logger.info(
                f"Pipeline decay applied: {ae_id} {month} {revenue_change:+.0f} -> {new_pipeline:.0f}"
            )
            return True

        except Exception as e:
            logger.error(f"Error applying pipeline decay: {e}")
            return False

    def _record_decay_event(self, decay_event: DecayEvent):
        """Record a decay event in the tracking system."""
        try:
            decay_data = self._read_decay_data()

            # Initialize tracking for AE if not exists
            if decay_event.ae_id not in decay_data["decay_tracking"]:
                decay_data["decay_tracking"][decay_event.ae_id] = {}

            # Initialize tracking for month if not exists
            if decay_event.month not in decay_data["decay_tracking"][decay_event.ae_id]:
                decay_data["decay_tracking"][decay_event.ae_id][decay_event.month] = {
                    "decay_events": [],
                    "calibration_baseline": None,
                    "calibration_date": None,
                    "last_updated": decay_event.timestamp,
                }

            # Add event to tracking
            month_tracking = decay_data["decay_tracking"][decay_event.ae_id][
                decay_event.month
            ]
            month_tracking["decay_events"].append(decay_event.to_dict())
            month_tracking["last_updated"] = decay_event.timestamp

            # Update file metadata
            decay_data["last_updated"] = decay_event.timestamp

            # Write back to file
            self._write_decay_data(decay_data)

        except Exception as e:
            logger.error(f"Error recording decay event: {e}")

    def set_calibration_baseline(
        self,
        ae_id: str,
        month: str,
        pipeline_value: float,
        calibrated_by: str,
        session_id: str = None,
    ) -> bool:
        """
        Set a new calibration baseline for pipeline decay tracking.

        Args:
            ae_id: Account Executive ID
            month: Month in YYYY-MM format
            pipeline_value: New calibrated pipeline value
            calibrated_by: Who performed the calibration
            session_id: Review session ID (optional)

        Returns:
            bool: Success status
        """
        try:
            # Update pipeline data with new baseline
            update_success = self.pipeline_service.update_pipeline_data(
                ae_id=ae_id,
                month=month,
                pipeline_update={
                    "current_pipeline": pipeline_value,
                    "calibrated_pipeline": pipeline_value,
                    "calibration_date": datetime.utcnow().isoformat() + "Z",
                    "calibration_session_id": session_id,
                },
                updated_by=calibrated_by,
                review_session_id=session_id,
            )

            if not update_success:
                logger.error(f"Failed to update pipeline data for calibration baseline")
                return False

            # Record calibration event
            calibration_event = DecayEvent(
                timestamp=datetime.utcnow().isoformat() + "Z",
                event_type=DecayEventType.CALIBRATION_RESET,
                ae_id=ae_id,
                month=month,
                amount=0.0,  # No change amount for calibrations
                old_pipeline=pipeline_value,  # Both old and new are the same
                new_pipeline=pipeline_value,
                description=f"Calibration baseline set to {pipeline_value:.0f}",
                created_by=calibrated_by,
            )

            # Update decay tracking
            decay_data = self._read_decay_data()

            # Initialize tracking for AE if not exists
            if ae_id not in decay_data["decay_tracking"]:
                decay_data["decay_tracking"][ae_id] = {}

            # Reset tracking for month with new baseline
            decay_data["decay_tracking"][ae_id][month] = {
                "decay_events": [calibration_event.to_dict()],
                "calibration_baseline": pipeline_value,
                "calibration_date": calibration_event.timestamp,
                "calibration_session_id": session_id,
                "last_updated": calibration_event.timestamp,
            }

            # Update file metadata
            decay_data["last_updated"] = calibration_event.timestamp

            # Write back to file
            self._write_decay_data(decay_data)

            logger.info(
                f"Calibration baseline set: {ae_id} {month} -> {pipeline_value:.0f}"
            )
            return True

        except Exception as e:
            logger.error(f"Error setting calibration baseline: {e}")
            return False

    def get_decay_summary(self, ae_id: str, month: str) -> Optional[DecaySummary]:
        """
        Get comprehensive decay summary for a month.

        Args:
            ae_id: Account Executive ID
            month: Month in YYYY-MM format

        Returns:
            DecaySummary object or None if no data
        """
        try:
            # Get current pipeline data
            pipeline_data = self.pipeline_service.get_pipeline_data(ae_id, month)
            if not pipeline_data:
                return None

            # Get decay tracking data
            decay_data = self._read_decay_data()
            tracking_data = (
                decay_data.get("decay_tracking", {}).get(ae_id, {}).get(month, {})
            )

            # Extract values
            current_pipeline = pipeline_data.get("current_pipeline", 0.0)
            calibrated_pipeline = (
                tracking_data.get("calibration_baseline") or current_pipeline
            )
            calibration_date = (
                tracking_data.get("calibration_date")
                or datetime.utcnow().isoformat() + "Z"
            )

            # Calculate days since calibration
            try:
                cal_date = datetime.fromisoformat(
                    calibration_date.replace("Z", "+00:00")
                )
                days_since_calibration = (
                    datetime.utcnow() - cal_date.replace(tzinfo=None)
                ).days
            except:
                days_since_calibration = 0

            # Parse decay events
            decay_events = []
            for event_data in tracking_data.get("decay_events", []):
                try:
                    decay_events.append(DecayEvent.from_dict(event_data))
                except Exception as e:
                    logger.warning(f"Failed to parse decay event: {e}")

            # Calculate total decay
            total_decay = current_pipeline - calibrated_pipeline

            return DecaySummary(
                ae_id=ae_id,
                month=month,
                calibrated_pipeline=calibrated_pipeline,
                current_pipeline=current_pipeline,
                calibration_date=calibration_date,
                days_since_calibration=days_since_calibration,
                total_decay=total_decay,
                decay_events=decay_events,
                calibration_session_id=tracking_data.get("calibration_session_id"),
            )

        except Exception as e:
            logger.error(f"Error getting decay summary: {e}")
            return None

    def get_decay_analytics(
        self, ae_id: str, months: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive decay analytics for an AE.

        Args:
            ae_id: Account Executive ID
            months: List of months to analyze (optional)

        Returns:
            Dictionary with decay analytics
        """
        try:
            analytics = {
                "ae_id": ae_id,
                "analysis_date": datetime.utcnow().isoformat() + "Z",
                "monthly_summaries": {},
                "overall_metrics": {},
                "trends": {},
                "recommendations": [],
            }

            # Get decay data
            decay_data = self._read_decay_data()
            ae_tracking = decay_data.get("decay_tracking", {}).get(ae_id, {})

            if not ae_tracking:
                return analytics

            # Analyze each month
            monthly_summaries = {}
            all_decay_rates = []
            all_decay_percentages = []

            target_months = months or list(ae_tracking.keys())

            for month in target_months:
                summary = self.get_decay_summary(ae_id, month)
                if summary:
                    monthly_summaries[month] = summary.to_dict()
                    all_decay_rates.append(summary.decay_rate_per_day)
                    all_decay_percentages.append(summary.decay_percentage)

            analytics["monthly_summaries"] = monthly_summaries

            # Calculate overall metrics
            if all_decay_rates:
                analytics["overall_metrics"] = {
                    "avg_daily_decay_rate": sum(all_decay_rates) / len(all_decay_rates),
                    "max_daily_decay_rate": max(all_decay_rates),
                    "min_daily_decay_rate": min(all_decay_rates),
                    "avg_decay_percentage": sum(all_decay_percentages)
                    / len(all_decay_percentages),
                    "months_analyzed": len(target_months),
                    "total_decay_events": sum(
                        len(s["decay_events"]) for s in monthly_summaries.values()
                    ),
                }

            # Generate trends and recommendations
            analytics["trends"] = self._analyze_decay_trends(monthly_summaries)
            analytics["recommendations"] = self._generate_decay_recommendations(
                analytics
            )

            return analytics

        except Exception as e:
            logger.error(f"Error getting decay analytics: {e}")
            return {"error": str(e)}

    def _analyze_decay_trends(
        self, monthly_summaries: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Analyze decay trends across months."""
        trends = {"direction": "stable", "volatility": "low", "patterns": []}

        if len(monthly_summaries) < 2:
            return trends

        # Analyze decay rate trends
        decay_rates = [s["decay_rate_per_day"] for s in monthly_summaries.values()]

        if len(decay_rates) >= 2:
            recent_avg = sum(decay_rates[-2:]) / 2
            earlier_avg = (
                sum(decay_rates[:-2]) / len(decay_rates[:-2])
                if len(decay_rates) > 2
                else decay_rates[0]
            )

            if recent_avg > earlier_avg * 1.2:
                trends["direction"] = "increasing"
            elif recent_avg < earlier_avg * 0.8:
                trends["direction"] = "decreasing"

        # Analyze volatility
        if len(decay_rates) >= 3:
            import statistics

            volatility = statistics.stdev(decay_rates) if len(decay_rates) > 1 else 0
            mean_rate = statistics.mean(decay_rates)

            if mean_rate != 0:
                coefficient_of_variation = volatility / abs(mean_rate)
                if coefficient_of_variation > 0.5:
                    trends["volatility"] = "high"
                elif coefficient_of_variation > 0.2:
                    trends["volatility"] = "medium"

        return trends

    def _generate_decay_recommendations(self, analytics: Dict[str, Any]) -> List[str]:
        """Generate actionable recommendations based on decay analysis."""
        recommendations = []

        overall_metrics = analytics.get("overall_metrics", {})
        trends = analytics.get("trends", {})

        # Recommendation based on decay rate
        avg_decay_rate = overall_metrics.get("avg_daily_decay_rate", 0)
        if avg_decay_rate < -100:  # Significant negative decay (pipeline shrinking)
            recommendations.append(
                "Strong revenue performance is reducing pipeline needs faster than expected. Consider lowering future pipeline targets."
            )
        elif avg_decay_rate > 100:  # Significant positive decay (pipeline growing)
            recommendations.append(
                "Pipeline is growing due to cancellations or removals. Investigate causes and consider mitigation strategies."
            )

        # Recommendation based on trends
        if trends.get("direction") == "increasing" and avg_decay_rate > 0:
            recommendations.append(
                "Increasing decay trend suggests growing issues with revenue retention. Review recent losses."
            )
        elif trends.get("direction") == "decreasing" and avg_decay_rate < 0:
            recommendations.append(
                "Decreasing decay trend indicates improving revenue performance. Consider raising targets."
            )

        # Recommendation based on volatility
        if trends.get("volatility") == "high":
            recommendations.append(
                "High decay volatility suggests unpredictable revenue patterns. Consider more frequent calibration sessions."
            )

        # Default recommendation if no specific patterns
        if not recommendations:
            recommendations.append(
                "Decay patterns appear normal. Continue regular bi-weekly calibration sessions."
            )

        return recommendations

    def cleanup_old_decay_events(self, days_to_keep: int = 90) -> int:
        """
        Clean up old decay events to prevent file bloat.

        Args:
            days_to_keep: Number of days of events to retain

        Returns:
            Number of events cleaned up
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            cutoff_timestamp = cutoff_date.isoformat() + "Z"

            decay_data = self._read_decay_data()
            events_removed = 0

            for ae_id, ae_tracking in decay_data.get("decay_tracking", {}).items():
                for month, month_data in ae_tracking.items():
                    original_count = len(month_data.get("decay_events", []))

                    # Keep events newer than cutoff date
                    month_data["decay_events"] = [
                        event
                        for event in month_data.get("decay_events", [])
                        if event.get("timestamp", "") > cutoff_timestamp
                    ]

                    events_removed += original_count - len(month_data["decay_events"])

            if events_removed > 0:
                decay_data["last_updated"] = datetime.utcnow().isoformat() + "Z"
                self._write_decay_data(decay_data)
                logger.info(f"Cleaned up {events_removed} old decay events")

            return events_removed

        except Exception as e:
            logger.error(f"Error cleaning up decay events: {e}")
            return 0


# Integration functions for revenue booking system
def on_revenue_booked(
    ae_id: str,
    month: str,
    amount: float,
    customer: str = None,
    description: str = None,
    decay_engine: PipelineDecayEngine = None,
) -> bool:
    """
    Hook function triggered when revenue is booked.
    Automatically applies pipeline decay.

    Args:
        ae_id: Account Executive ID
        month: Month in YYYY-MM format
        amount: Revenue amount booked
        customer: Customer name
        description: Description of the booking
        decay_engine: PipelineDecayEngine instance

    Returns:
        bool: Success status
    """
    if not decay_engine:
        logger.warning("No decay engine provided for revenue booking hook")
        return False

    return decay_engine.apply_pipeline_decay(
        ae_id=ae_id,
        month=month,
        revenue_change=-amount,  # Negative because pipeline decreases when revenue is booked
        event_type=DecayEventType.REVENUE_BOOKED,
        customer=customer,
        description=description or f"Revenue booked: ${amount:,.0f}",
        created_by="revenue_booking_system",
    )


def on_revenue_removed(
    ae_id: str,
    month: str,
    amount: float,
    customer: str = None,
    reason: str = None,
    decay_engine: PipelineDecayEngine = None,
) -> bool:
    """
    Hook function triggered when revenue is removed/cancelled.
    Automatically applies pipeline decay.

    Args:
        ae_id: Account Executive ID
        month: Month in YYYY-MM format
        amount: Revenue amount removed
        customer: Customer name
        reason: Reason for removal
        decay_engine: PipelineDecayEngine instance

    Returns:
        bool: Success status
    """
    if not decay_engine:
        logger.warning("No decay engine provided for revenue removal hook")
        return False

    return decay_engine.apply_pipeline_decay(
        ae_id=ae_id,
        month=month,
        revenue_change=amount,  # Positive because pipeline increases when revenue is removed
        event_type=DecayEventType.REVENUE_REMOVED,
        customer=customer,
        description=reason or f"Revenue removed: ${amount:,.0f}",
        created_by="revenue_booking_system",
    )
