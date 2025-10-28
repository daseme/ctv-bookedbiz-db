# Integration with Your Existing Structure
# Modify your existing pipeline_app_v2.py

"""Modular Pipeline Revenue Management App - Enhanced with Decay System."""

from flask import Flask, render_template, jsonify, request
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path for imports
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, PROJECT_ROOT)

# Import your existing services
from src.services.budget_service import BudgetService
from src.services.ae_service import AEService
from src.services.customer_service import CustomerService
from src.services.pipeline_service import PipelineService

# NEW: Import the decay sync service
from src.services.db_sync_decay_service import (
    DatabaseSyncDecayService,
    enhance_ae_service_with_auto_sync,
)

app = Flask(__name__)

# Configuration (same as before)
DB_PATH = os.path.join(PROJECT_ROOT, "data/database/production.db")
DATA_PATH = os.path.join(PROJECT_ROOT, "data/processed")
CONFIG_PATH = os.path.join(os.path.dirname(__file__))

# Initialize services (mostly same as before)
budget_service = BudgetService(DATA_PATH)
customer_service = CustomerService(DB_PATH)
pipeline_service = PipelineService(DATA_PATH)

# ENHANCED: Create AE service with decay integration
original_ae_service = AEService(DB_PATH, CONFIG_PATH)  # Your original
ae_service = enhance_ae_service_with_auto_sync(
    original_ae_service, pipeline_service
)  # Enhanced

# NEW: Create decay sync service for manual operations
decay_sync_service = DatabaseSyncDecayService(original_ae_service, pipeline_service)


# Keep all your existing helper classes unchanged
class MonthDisplayHelper:
    """Helper for month display formatting."""

    MONTH_NAMES = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }

    @classmethod
    def format_month_display(cls, month_str: str) -> str:
        """Convert YYYY-MM to 'Month YYYY'."""
        year, month_num = month_str.split("-")
        return f"{cls.MONTH_NAMES[month_num]} {year}"


class PipelineCalculator:
    """Handles pipeline calculations and business logic."""

    @staticmethod
    def calculate_pipeline_values(
        budget: float,
        booked_revenue: float = 0,
        month_str: str = None,
        assigned_pipeline: float = None,
    ) -> Dict[str, float]:
        """Calculate pipeline values using assigned monthly pipeline amounts."""
        # Check if this is a past month (before current month)
        if month_str:
            from datetime import datetime

            current_date = datetime.now()
            current_year = current_date.year
            current_month = current_date.month

            year, month = month_str.split("-")
            if int(year) < current_year or (
                int(year) == current_year and int(month) < current_month
            ):
                return {"current_pipeline": 0.0, "expected_pipeline": 0.0}

        # Use assigned pipeline if provided, otherwise fall back to budget calculation
        if assigned_pipeline is not None:
            current_pipeline = assigned_pipeline
            expected_pipeline = assigned_pipeline
        else:
            # Fallback to budget-based calculation for backwards compatibility
            current_pipeline = max(0, budget - booked_revenue)
            expected_pipeline = current_pipeline

        return {
            "current_pipeline": current_pipeline,
            "expected_pipeline": expected_pipeline,
        }

    @staticmethod
    def calculate_gaps(
        booked_revenue: float,
        current_pipeline: float,
        expected_pipeline: float,
        budget: float,
    ) -> Dict[str, float]:
        """Calculate pipeline and budget gaps."""
        return {
            "pipeline_gap": current_pipeline - expected_pipeline,
            "budget_gap": (booked_revenue + current_pipeline) - budget,
        }

    @staticmethod
    def determine_status(pipeline_gap: float) -> str:
        """Determine pipeline status based on gap."""
        if pipeline_gap >= 0:
            return "ahead"
        elif pipeline_gap >= -10000:
            return "on_track"
        else:
            return "behind"


# Keep all your existing routes, just enhance the AE summary route
@app.route("/")
def index():
    """Landing page."""
    return render_template("index.html", title="CTV Reports")


@app.route("/pipeline-revenue")
def pipeline_revenue_management():
    """Pipeline Revenue Management page."""
    try:
        # Get filtered AE list (now with decay info!)
        ae_list = ae_service.get_filtered_ae_list()

        data = {
            "session_date": datetime.now().strftime("%B %d, %Y"),
            "ae_list": ae_list,
            "session": {"completed_aes": [], "total_aes": len(ae_list)},
        }

        return render_template(
            "pipeline_revenue.html", title="Pipeline Revenue Management", data=data
        )
    except Exception as e:
        print(f"ERROR in pipeline_revenue_management: {e}")
        # Return minimal data structure to prevent template errors
        data = {
            "session_date": datetime.now().strftime("%B %d, %Y"),
            "ae_list": [],
            "session": {"completed_aes": [], "total_aes": 0},
        }
        return render_template(
            "pipeline_revenue.html", title="Pipeline Revenue Management", data=data
        )


@app.route("/api/aes")
def get_aes():
    """Get list of Account Executives."""
    try:
        aes = ae_service.get_filtered_ae_list()
        return jsonify({"success": True, "data": aes})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/ae/<ae_id>/summary")
def get_ae_summary(ae_id: str):
    """Get summary for specific AE - ENHANCED with auto-sync decay."""
    try:
        from src.services.db_sync_decay_service import simple_ae_decay_check

        decay_results = simple_ae_decay_check(ae_service, pipeline_service, ae_id)

        # This now auto-syncs and applies decay!
        ae_info = ae_service.get_ae_by_id(ae_id)
        if not ae_info:
            return jsonify({"success": False, "error": "AE not found"})

        ae_name = ae_info["name"]

        # Generate monthly summary for current year
        monthly_summary = []
        current_year = datetime.now().year

        for month_num in range(1, 13):  # All 12 months
            month_str = f"{current_year}-{month_num:02d}"

            # Get actual revenue from database
            booked_revenue = ae_service.get_monthly_revenue(ae_name, month_str)

            # Get budget from budget service
            budget = budget_service.get_monthly_budget(ae_name, month_str)

            # Get assigned pipeline from pipeline service (now with decay!)
            assigned_pipeline = None
            decay_info = None
            try:
                pipeline_data = pipeline_service.get_pipeline_data(ae_id, month_str)
                if pipeline_data:
                    assigned_pipeline = pipeline_data.get("current_pipeline")

                # Get decay information
                if (
                    hasattr(pipeline_service, "decay_engine")
                    and pipeline_service.decay_engine
                ):
                    decay_summary = pipeline_service.get_pipeline_decay_summary(
                        ae_id, month_str
                    )
                    if decay_summary:
                        decay_info = {
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
            except:
                pass  # Fall back to budget calculation if pipeline service fails

            # Calculate pipeline values using assigned amounts or budget fallback
            pipeline_values = PipelineCalculator.calculate_pipeline_values(
                budget, booked_revenue, month_str, assigned_pipeline
            )

            # Calculate gaps
            gaps = PipelineCalculator.calculate_gaps(
                booked_revenue,
                pipeline_values["current_pipeline"],
                pipeline_values["expected_pipeline"],
                budget,
            )

            # Determine status
            status = PipelineCalculator.determine_status(gaps["pipeline_gap"])

            month_summary = {
                "month": month_str,
                "month_display": MonthDisplayHelper.format_month_display(month_str),
                "is_current_month": month_num == datetime.now().month,
                "booked_revenue": float(booked_revenue),
                "current_pipeline": pipeline_values["current_pipeline"],
                "expected_pipeline": pipeline_values["expected_pipeline"],
                "budget": budget,
                "pipeline_gap": gaps["pipeline_gap"],
                "budget_gap": gaps["budget_gap"],
                "pipeline_status": status,
                "notes": "",
                "last_updated": "",
            }

            # Add decay information if available
            if decay_info:
                month_summary.update(decay_info)

            monthly_summary.append(month_summary)

        # Calculate quarterly summary (same as before)
        quarterly_summary = []
        quarters = [
            {"name": f"Q1 {current_year}", "months": [1, 2, 3]},
            {"name": f"Q2 {current_year}", "months": [4, 5, 6]},
            {"name": f"Q3 {current_year}", "months": [7, 8, 9]},
            {"name": f"Q4 {current_year}", "months": [10, 11, 12]},
        ]

        for quarter in quarters:
            quarter_months = [
                m
                for m in monthly_summary
                if int(m["month"].split("-")[1]) in quarter["months"]
            ]

            quarter_booked = sum(m["booked_revenue"] for m in quarter_months)
            quarter_budget = sum(m["budget"] for m in quarter_months)
            quarter_pipeline = sum(m["current_pipeline"] for m in quarter_months)

            quarter_summary = {
                "quarter_name": quarter["name"],
                "month_count": len(quarter["months"]),
                "booked_revenue": quarter_booked,
                "current_pipeline": quarter_pipeline,
                "expected_pipeline": quarter_pipeline,
                "budget": quarter_budget,
                "pipeline_gap": 0,
                "budget_gap": quarter_booked - quarter_budget,
            }

            quarterly_summary.append(quarter_summary)

        # Calculate YTD target for AE info
        ytd_target = budget_service.get_annual_target(ae_name)

        response_data = {
            "ae_info": {
                "ae_id": ae_id,
                "name": ae_name,
                "territory": ae_info["territory"],
                "ytd_target": int(ytd_target),
                "ytd_actual": ae_info["ytd_actual"],
                "avg_deal_size": ae_info["avg_deal_size"],
                "active": True,
                # NEW: Add decay information
                "decay_enabled": ae_info.get("decay_enabled", False),
                "has_decay_activity": ae_info.get("has_decay_activity", False),
                "decay_analytics": ae_info.get("decay_analytics"),
            },
            "monthly_summary": monthly_summary,
            "quarterly_summary": quarterly_summary,
        }

        return jsonify({"success": True, "data": response_data})

    except Exception as e:
        print(f"ERROR in get_ae_summary: {e}")
        return jsonify({"success": False, "error": str(e)})


# NEW: Add decay-specific routes
@app.route("/api/pipeline/decay/sync", methods=["POST"])
def manual_sync():
    """Manually trigger decay sync (useful for testing)."""
    try:
        data = request.get_json() or {}
        ae_id = data.get("ae_id")  # None = sync all

        results = decay_sync_service.sync_and_apply_decay(ae_id)

        return jsonify(
            {
                "success": True,
                "message": f"Applied {results['decay_adjustments_applied']} decay adjustments",
                "results": results,
            }
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/pipeline/decay/status")
def decay_status():
    """Get decay system status."""
    try:
        status = decay_sync_service.get_sync_status()
        return jsonify({"success": True, "data": status})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


# Keep all your existing routes unchanged
@app.route("/api/customers/<ae_id>/<month>")
def get_customer_details(ae_id: str, month: str):
    """Get customer deal details for specific AE and month."""
    try:
        # Get AE info to get the name
        ae_info = ae_service.get_ae_by_id(ae_id)
        if not ae_info:
            return jsonify({"success": False, "error": "AE not found"})

        ae_name = ae_info["name"]

        # Get customer deals using the customer service
        deals = customer_service.get_customer_deals(ae_name, month)

        # Categorize deals
        categorized_data = customer_service.categorize_deals(deals)

        # Add additional metadata
        response_data = {"ae_id": ae_id, "month": month, **categorized_data}

        return jsonify({"success": True, "data": response_data})

    except Exception as e:
        print(f"ERROR in get_customer_details: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route("/api/pipeline/<ae_id>/<month>", methods=["PUT"])
def update_pipeline(ae_id: str, month: str):
    """Update pipeline data - ENHANCED with decay calibration."""
    try:
        data = request.get_json()
        new_pipeline = data.get("current_pipeline")

        if new_pipeline is not None:
            # Apply calibration through decay system
            success = pipeline_service.set_pipeline_calibration(
                ae_id=ae_id,
                month=month,
                pipeline_value=float(new_pipeline),
                calibrated_by="manual_calibration",
                session_id=f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            )

            if success:
                return jsonify(
                    {
                        "success": True,
                        "message": "Pipeline calibrated successfully",
                        "calibration_applied": True,
                    }
                )

        return jsonify({"success": True, "message": "Pipeline updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)})


if __name__ == "__main__":
    print(f"Database path: {DB_PATH}")
    print(f"Database exists: {os.path.exists(DB_PATH)}")
    print(f"Data path: {DATA_PATH}")
    print(f"Budget service initialized: {budget_service.budget_file}")
    print(f"Decay system enabled: {hasattr(pipeline_service, 'decay_engine')}")

    # Optional: Run initial sync on startup
    try:
        print("Running initial decay sync...")
        results = decay_sync_service.sync_and_apply_decay()
        print(
            f"Initial sync: {results['decay_adjustments_applied']} decay adjustments applied"
        )
    except Exception as e:
        print(f"Initial sync failed (this is normal on first run): {e}")

    app.run(debug=True, host="0.0.0.0", port=5001)
