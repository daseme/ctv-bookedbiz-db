# src/services/db_sync_decay_service.py
"""
Database-Sync Decay Service
Reads from database when reports load, automatically detects changes, applies decay
NO CHANGES to your Excel import system required
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class DatabaseSyncDecayService:
    """
    Smart decay system that syncs with database on report access.

    How it works:
    1. When a report loads, we check what's in the database
    2. Compare with our last known state (stored in JSON)
    3. Calculate what changed since last check
    4. Apply decay adjustments automatically
    5. Update our tracking data

    Your Excel imports remain completely unchanged!
    """

    def __init__(self, ae_service, pipeline_service):
        """Initialize with required services."""
        self.ae_service = ae_service
        self.pipeline_service = pipeline_service
        self.last_sync_file = (
            pipeline_service.data_path + "/last_sync_state.json"
            if pipeline_service.data_path
            else None
        )

    def sync_and_apply_decay(self, ae_id: str = None) -> Dict[str, Any]:
        """
        Main sync function - call this when loading reports.

        Args:
            ae_id: Specific AE to sync, or None for all AEs

        Returns:
            Sync results with applied decay adjustments
        """
        sync_results = {
            "sync_timestamp": datetime.utcnow().isoformat() + "Z",
            "aes_synced": 0,
            "changes_detected": 0,
            "decay_adjustments_applied": 0,
            "errors": [],
            "change_details": [],
        }

        try:
            # Get AEs to sync
            if ae_id:
                ae_list = [self.ae_service.get_ae_by_id(ae_id)]
                ae_list = [ae for ae in ae_list if ae is not None]
            else:
                ae_list = self.ae_service.get_filtered_ae_list()

            # Load last known state
            last_state = self._load_last_sync_state()

            for ae in ae_list:
                try:
                    ae_changes = self._sync_ae_revenue_data(ae, last_state)
                    sync_results["aes_synced"] += 1
                    sync_results["changes_detected"] += len(ae_changes)
                    sync_results["change_details"].extend(ae_changes)

                    # Apply decay for each change
                    for change in ae_changes:
                        if self._apply_decay_for_change(change):
                            sync_results["decay_adjustments_applied"] += 1
                        else:
                            sync_results["errors"].append(
                                f"Failed to apply decay for {change['ae_name']} {change['month']}"
                            )

                except Exception as e:
                    error_msg = f"Error syncing {ae['name']}: {str(e)}"
                    logger.error(error_msg)
                    sync_results["errors"].append(error_msg)

            # Save current state for next sync
            self._save_current_sync_state(ae_list)

            logger.info(
                f"Sync complete: {sync_results['aes_synced']} AEs, {sync_results['decay_adjustments_applied']} decay adjustments"
            )

        except Exception as e:
            error_msg = f"Sync process error: {str(e)}"
            logger.error(error_msg)
            sync_results["errors"].append(error_msg)

        return sync_results

    def _sync_ae_revenue_data(
        self, ae: Dict[str, Any], last_state: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Sync revenue data for a specific AE and detect changes.

        Args:
            ae: AE information
            last_state: Last known revenue state

        Returns:
            List of detected changes
        """
        changes = []
        ae_name = ae["name"]
        ae_id = ae["ae_id"]

        # Get current year's months
        current_year = datetime.now().year
        months = [f"{current_year}-{month:02d}" for month in range(1, 13)]

        # Check each month for changes
        for month in months:
            try:
                # Get current revenue from database
                current_revenue = self.ae_service.get_monthly_revenue(ae_name, month)

                # Get last known revenue
                state_key = f"{ae_name}_{month}"
                last_revenue = last_state.get(state_key, 0)

                # Calculate change
                revenue_change = current_revenue - last_revenue

                # If there's a significant change, record it
                if abs(revenue_change) >= 1000:  # $1000 threshold
                    change = {
                        "ae_id": ae_id,
                        "ae_name": ae_name,
                        "month": month,
                        "old_revenue": last_revenue,
                        "new_revenue": current_revenue,
                        "revenue_change": revenue_change,
                        "change_type": "revenue_increase"
                        if revenue_change > 0
                        else "revenue_decrease",
                        "detected_at": datetime.utcnow().isoformat() + "Z",
                        "source": "database_sync",
                    }
                    changes.append(change)

                    logger.info(
                        f"Revenue change detected: {ae_name} {month} {last_revenue} -> {current_revenue} ({revenue_change:+.0f})"
                    )

            except Exception as e:
                logger.error(f"Error checking {ae_name} {month}: {e}")

        return changes

    def _apply_decay_for_change(self, change: Dict[str, Any]) -> bool:
        """
        Apply decay adjustment for a detected revenue change.

        Args:
            change: Change information

        Returns:
            Success status
        """
        try:
            ae_id = change["ae_id"]
            month = change["month"]
            revenue_change = change["revenue_change"]
            ae_name = change["ae_name"]

            # Apply decay based on change type
            if revenue_change > 0:
                # Revenue increased - apply booking decay (reduce pipeline)
                success = self.pipeline_service.apply_revenue_booking(
                    ae_id=ae_id,
                    month=month,
                    amount=revenue_change,
                    customer="DB Sync Detection",
                    description=f"Database sync detected +${revenue_change:,.0f} revenue increase",
                )
            else:
                # Revenue decreased - apply removal decay (increase pipeline)
                success = self.pipeline_service.apply_revenue_removal(
                    ae_id=ae_id,
                    month=month,
                    amount=abs(revenue_change),
                    customer="DB Sync Detection",
                    reason=f"Database sync detected -${abs(revenue_change):,.0f} revenue decrease",
                )

            if success:
                logger.info(f"Applied decay: {ae_name} {month} {revenue_change:+.0f}")

            return success

        except Exception as e:
            logger.error(f"Error applying decay for change: {e}")
            return False

    def _load_last_sync_state(self) -> Dict[str, float]:
        """Load the last known revenue state."""
        if not self.last_sync_file:
            return {}

        try:
            import json
            import os

            if os.path.exists(self.last_sync_file):
                with open(self.last_sync_file, "r") as f:
                    data = json.load(f)
                    return data.get("revenue_state", {})
        except Exception as e:
            logger.warning(f"Could not load last sync state: {e}")

        return {}

    def _save_current_sync_state(self, ae_list: List[Dict[str, Any]]):
        """Save current revenue state for next sync."""
        if not self.last_sync_file:
            return

        try:
            import json
            import os

            # Build current state
            current_state = {}
            current_year = datetime.now().year
            months = [f"{current_year}-{month:02d}" for month in range(1, 13)]

            for ae in ae_list:
                ae_name = ae["name"]
                for month in months:
                    try:
                        revenue = self.ae_service.get_monthly_revenue(ae_name, month)
                        state_key = f"{ae_name}_{month}"
                        current_state[state_key] = revenue
                    except Exception as e:
                        logger.warning(
                            f"Could not get revenue for {ae_name} {month}: {e}"
                        )

            # Save state
            sync_data = {
                "last_sync_timestamp": datetime.utcnow().isoformat() + "Z",
                "revenue_state": current_state,
                "ae_count": len(ae_list),
                "schema_version": "1.0",
            }

            os.makedirs(os.path.dirname(self.last_sync_file), exist_ok=True)

            with open(self.last_sync_file, "w") as f:
                json.dump(sync_data, f, indent=2)

            logger.debug(f"Saved sync state with {len(current_state)} revenue records")

        except Exception as e:
            logger.error(f"Error saving sync state: {e}")

    def get_sync_status(self) -> Dict[str, Any]:
        """Get current sync status information."""
        status = {
            "sync_enabled": True,
            "last_sync_timestamp": None,
            "last_sync_ae_count": 0,
            "sync_file_exists": False,
            "decay_system_enabled": hasattr(self.pipeline_service, "decay_engine"),
        }

        try:
            if self.last_sync_file:
                import json
                import os

                if os.path.exists(self.last_sync_file):
                    status["sync_file_exists"] = True

                    with open(self.last_sync_file, "r") as f:
                        data = json.load(f)
                        status["last_sync_timestamp"] = data.get("last_sync_timestamp")
                        status["last_sync_ae_count"] = data.get("ae_count", 0)

                        # Calculate time since last sync
                        if status["last_sync_timestamp"]:
                            last_sync = datetime.fromisoformat(
                                status["last_sync_timestamp"].replace("Z", "+00:00")
                            )
                            time_since = datetime.utcnow() - last_sync.replace(
                                tzinfo=None
                            )
                            status["hours_since_last_sync"] = (
                                time_since.total_seconds() / 3600
                            )

        except Exception as e:
            logger.error(f"Error getting sync status: {e}")
            status["error"] = str(e)

        return status

    def force_full_sync(self) -> Dict[str, Any]:
        """Force a full sync of all AEs (useful for initial setup)."""
        logger.info("Starting forced full sync...")

        # Clear last state to force detection of all current revenue as "changes"
        try:
            if self.last_sync_file:
                import os

                if os.path.exists(self.last_sync_file):
                    os.remove(self.last_sync_file)
                    logger.info("Cleared previous sync state")
        except Exception as e:
            logger.warning(f"Could not clear sync state: {e}")

        # Run full sync
        return self.sync_and_apply_decay()


# Enhanced AE Service that auto-syncs
class AutoSyncAEService:
    """
    Wrapper around your existing AE service that auto-syncs with decay system.
    Drop-in replacement that adds decay functionality without changing your code.
    """

    def __init__(self, original_ae_service, pipeline_service):
        """
        Wrap your existing AE service.

        Args:
            original_ae_service: Your existing AE service
            pipeline_service: Pipeline service with decay
        """
        self.original_ae_service = original_ae_service
        self.pipeline_service = pipeline_service
        self.decay_sync = DatabaseSyncDecayService(
            original_ae_service, pipeline_service
        )

        # Pass through all original methods
        for attr_name in dir(original_ae_service):
            if not attr_name.startswith("_") and not hasattr(self, attr_name):
                setattr(self, attr_name, getattr(original_ae_service, attr_name))

    def get_ae_by_id(self, ae_id: str, auto_sync: bool = True):
        """Enhanced get_ae_by_id with optional auto-sync."""
        # Auto-sync this AE's data if requested
        if auto_sync:
            try:
                sync_results = self.decay_sync.sync_and_apply_decay(ae_id)
                if sync_results["decay_adjustments_applied"] > 0:
                    logger.info(
                        f"Auto-sync applied {sync_results['decay_adjustments_applied']} decay adjustments for {ae_id}"
                    )
            except Exception as e:
                logger.warning(f"Auto-sync failed for {ae_id}: {e}")

        # Get the AE data using original service
        ae_data = self.original_ae_service.get_ae_by_id(ae_id)

        # Add decay information if available
        if ae_data and hasattr(self.pipeline_service, "decay_engine"):
            try:
                # Get decay analytics
                decay_analytics = self.pipeline_service.get_decay_analytics(ae_id)
                ae_data["decay_analytics"] = decay_analytics
                ae_data["decay_enabled"] = True

                # Check for recent decay activity
                recent_activity = decay_analytics.get("overall_metrics", {}).get(
                    "total_decay_events", 0
                )
                ae_data["has_decay_activity"] = recent_activity > 0

            except Exception as e:
                logger.warning(f"Could not get decay analytics for {ae_id}: {e}")
                ae_data["decay_enabled"] = False
                ae_data["has_decay_activity"] = False

        return ae_data

    def get_filtered_ae_list(self, auto_sync: bool = False):
        """Enhanced get_filtered_ae_list with optional auto-sync."""
        # Auto-sync all AEs if requested (use sparingly - can be slow)
        if auto_sync:
            try:
                sync_results = self.decay_sync.sync_and_apply_decay()
                if sync_results["decay_adjustments_applied"] > 0:
                    logger.info(
                        f"Auto-sync applied {sync_results['decay_adjustments_applied']} total decay adjustments"
                    )
            except Exception as e:
                logger.warning(f"Auto-sync failed: {e}")

        # Get AE list using original service
        ae_list = self.original_ae_service.get_filtered_ae_list()

        # Add decay information to each AE
        for ae in ae_list:
            if hasattr(self.pipeline_service, "decay_engine"):
                try:
                    # Quick check for decay activity
                    decay_summary = self.pipeline_service.get_pipeline_decay_summary(
                        ae["ae_id"], datetime.now().strftime("%Y-%m")
                    )
                    ae["has_decay_activity"] = bool(
                        decay_summary and decay_summary.get("decay_events")
                    )
                    ae["decay_enabled"] = True
                except Exception:
                    ae["has_decay_activity"] = False
                    ae["decay_enabled"] = False

        return ae_list


# Simple integration function
def enhance_ae_service_with_auto_sync(original_ae_service, pipeline_service):
    """
    Enhance your existing AE service with auto-sync decay functionality.

    Usage:
        ae_service = enhance_ae_service_with_auto_sync(original_ae_service, pipeline_service)
        # Now your ae_service automatically syncs and applies decay!
    """
    return AutoSyncAEService(original_ae_service, pipeline_service)


# ADD THIS TO THE END of your existing db_sync_decay_service.py file
# Don't replace anything, just add these functions at the bottom:

# Simple integration functions for minimal changes to existing code
_global_decay_service = None

# src/services/db_sync_decay_service.py - Replace the simple_ae_decay_check function


def simple_ae_decay_check(ae_service, pipeline_service, ae_id: str) -> Dict[str, Any]:
    """
    Ultra-simple function to add to your existing routes.
    Fixed to work with ReportDataService instead of expecting get_ae_by_id method.
    """
    global _global_decay_service

    try:
        # Check if pipeline service has decay capabilities
        if (
            not hasattr(pipeline_service, "decay_engine")
            or not pipeline_service.decay_engine
        ):
            logger.info(
                "Decay system not available - pipeline service has no decay engine"
            )
            return {
                "success": True,
                "decay_applied": 0,
                "changes_found": 0,
                "message": "Decay system not enabled",
            }

        # Initialize the full decay service if not already done
        if _global_decay_service is None:
            try:
                # Create a compatible AE service wrapper
                class AEServiceWrapper:
                    def __init__(self, report_service):
                        self.report_service = report_service

                    def get_ae_by_id(self, ae_id):
                        # Convert ae_id to ae_name
                        ae_name = ae_id.replace("ae_", "").replace("_", " ").title()
                        return {"ae_id": ae_id, "name": ae_name, "territory": "Unknown"}

                    def get_filtered_ae_list(self):
                        # Return a basic AE list
                        return [
                            {"ae_id": "ae_charmaine_lane", "name": "Charmaine Lane"},
                            {"ae_id": "ae_house", "name": "House"},
                            {
                                "ae_id": "ae_riley_van_patten",
                                "name": "Riley Van Patten",
                            },
                            {
                                "ae_id": "ae_white_horse_international",
                                "name": "White Horse International",
                            },
                        ]

                    def get_monthly_revenue(self, ae_name, month):
                        # Try to get monthly revenue - simplified version
                        try:
                            year, month_num = month.split("-")

                            from src.models.report_data import ReportFilters

                            filters = ReportFilters(year=int(year), ae_name=ae_name)

                            monthly_data = (
                                self.report_service.get_monthly_revenue_report_data(
                                    int(year), filters
                                )
                            )

                            if monthly_data and hasattr(monthly_data, "to_dict"):
                                revenue_data = monthly_data.to_dict().get(
                                    "revenue_data", []
                                )
                                total_revenue = 0

                                for item in revenue_data:
                                    if item.get("ae") == ae_name:
                                        month_key = f"month_{int(month_num)}"
                                        if month_key in item:
                                            total_revenue += float(item[month_key] or 0)

                                return total_revenue

                        except Exception as e:
                            logger.warning(
                                f"Could not get monthly revenue for {ae_name} {month}: {e}"
                            )

                        return 0

                ae_service_wrapper = AEServiceWrapper(ae_service)
                _global_decay_service = DatabaseSyncDecayService(
                    ae_service_wrapper, pipeline_service
                )
                logger.info("Initialized decay sync service with wrapper")

            except Exception as e:
                logger.error(f"Could not initialize decay service: {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "decay_applied": 0,
                    "message": "Decay service initialization failed",
                }

        # Run sync for this specific AE
        try:
            results = _global_decay_service.sync_and_apply_decay(ae_id)

            # Return simplified results
            return {
                "success": True,
                "decay_applied": results.get("decay_adjustments_applied", 0),
                "changes_found": results.get("changes_detected", 0),
                "message": f"Applied {results.get('decay_adjustments_applied', 0)} decay adjustments",
            }

        except Exception as e:
            logger.error(f"Decay check failed for {ae_id}: {e}")
            return {
                "success": False,
                "error": str(e),
                "decay_applied": 0,
                "message": "Decay check failed",
            }

    except Exception as e:
        logger.error(f"Decay system error for {ae_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "decay_applied": 0,
            "message": "Decay system error",
        }


def get_ae_with_decay_info(
    ae_service, pipeline_service, ae_id: str
) -> Optional[Dict[str, Any]]:
    """
    Get AE info enhanced with decay information.
    Drop-in replacement for ae_service.get_ae_by_id() that adds decay info.

    Usage:
    # Instead of: ae_info = ae_service.get_ae_by_id(ae_id)
    # Use: ae_info = get_ae_with_decay_info(ae_service, pipeline_service, ae_id)
    """
    try:
        # Run decay check first
        decay_results = simple_ae_decay_check(ae_service, pipeline_service, ae_id)

        # Get AE info
        ae_info = ae_service.get_ae_by_id(ae_id)
        if not ae_info:
            return None

        # Add decay information
        ae_info["decay_enabled"] = hasattr(pipeline_service, "decay_engine")
        ae_info["decay_last_check"] = datetime.utcnow().isoformat() + "Z"
        ae_info["decay_adjustments_applied"] = decay_results.get("decay_applied", 0)
        ae_info["has_decay_activity"] = decay_results.get("decay_applied", 0) > 0

        # Try to get decay analytics if available
        if hasattr(pipeline_service, "get_decay_analytics"):
            try:
                analytics = pipeline_service.get_decay_analytics(ae_id)
                ae_info["decay_analytics"] = analytics
            except:
                pass

        return ae_info

    except Exception as e:
        logger.error(f"Error getting AE with decay info: {e}")
        # Fallback to original AE service
        return ae_service.get_ae_by_id(ae_id)


def enhance_monthly_summary_with_decay(
    monthly_summary: List[Dict], ae_id: str, pipeline_service
) -> List[Dict]:
    """
    Enhance monthly summary data with decay information.

    Usage:
    monthly_summary = [...] # your existing monthly summary
    monthly_summary = enhance_monthly_summary_with_decay(monthly_summary, ae_id, pipeline_service)
    """
    if (
        not hasattr(pipeline_service, "decay_engine")
        or not pipeline_service.decay_engine
    ):
        return monthly_summary

    try:
        for month_data in monthly_summary:
            month = month_data.get("month")
            if not month:
                continue

            # Get decay summary for this month
            try:
                decay_summary = pipeline_service.get_pipeline_decay_summary(
                    ae_id, month
                )
                if decay_summary:
                    month_data.update(
                        {
                            "has_decay_activity": len(
                                decay_summary.get("decay_events", [])
                            )
                            > 1,
                            "total_decay": decay_summary.get("total_decay", 0),
                            "days_since_calibration": decay_summary.get(
                                "days_since_calibration", 0
                            ),
                            "decay_events_count": len(
                                decay_summary.get("decay_events", [])
                            ),
                            "calibrated_pipeline": decay_summary.get(
                                "calibrated_pipeline"
                            ),
                            "calibration_date": decay_summary.get("calibration_date"),
                        }
                    )
            except Exception as e:
                logger.warning(f"Could not get decay info for {ae_id} {month}: {e}")

    except Exception as e:
        logger.error(f"Error enhancing monthly summary with decay: {e}")

    return monthly_summary


# Convenience function for full system status
def get_decay_system_status(ae_service, pipeline_service) -> Dict[str, Any]:
    """Get comprehensive decay system status."""
    global _global_decay_service

    if _global_decay_service is None:
        _global_decay_service = DatabaseSyncDecayService(ae_service, pipeline_service)

    try:
        return _global_decay_service.get_sync_status()
    except Exception as e:
        return {"error": str(e), "status": "error"}
