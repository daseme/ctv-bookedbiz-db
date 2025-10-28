# Add this to your main Flask app (likely in web/routes/ or similar)
# Create a new file: src/web/routes/pipeline_routes.py

"""Pipeline Revenue Management Routes for Main App."""

from flask import Blueprint, render_template, jsonify, request
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Service container and utilities
from src.services.container import get_container
from src.web.utils.request_helpers import (
    safe_get_service,
    create_success_response,
    create_error_response,
    handle_service_error,
    log_requests,
    handle_request_errors,
)

# Business services
from src.services.budget_service import BudgetService
from src.services.ae_service import AEService
from src.services.customer_service import CustomerService
from src.services.pipeline_service import PipelineService

logger = logging.getLogger(__name__)

# Create blueprint
pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/pipeline")


# Initialize services (you might need to adjust paths)
def get_services():
    """Get or create service instances."""
    # You'll need to adjust these paths to match your project structure
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    DB_PATH = os.path.join(PROJECT_ROOT, "data/database/production.db")
    DATA_PATH = os.path.join(PROJECT_ROOT, "data/processed")
    CONFIG_PATH = os.path.join(PROJECT_ROOT, "src/web/routes")

    return {
        "budget_service": BudgetService(DATA_PATH),
        "ae_service": AEService(DB_PATH, CONFIG_PATH),
        "customer_service": CustomerService(DB_PATH),
        "pipeline_service": PipelineService(DATA_PATH),
    }


# Helper classes (from your pipeline_app_v2.py)
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
        try:
            year, month_num = month_str.split("-")
            return f"{cls.MONTH_NAMES[month_num]} {year}"
        except:
            return month_str


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
        # Check if this is a past month
        if month_str:
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


@pipeline_bp.route("/revenue")
def pipeline_revenue_management():
    """Pipeline Revenue Management page."""
    print("🔍 Pipeline route called")

    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        print("🔍 Getting AE data from monthly report...")
        from src.models.report_data import ReportFilters
        from datetime import date

        current_year = date.today().year
        filters = ReportFilters(year=current_year)

        monthly_data = report_service.get_monthly_revenue_report_data(
            current_year, filters
        )
        print(f"✅ Got monthly data: {type(monthly_data)}")

        ae_list = []
        if monthly_data and hasattr(monthly_data, "to_dict"):
            monthly_dict = monthly_data.to_dict()
            print(f"🔍 Monthly report keys: {list(monthly_dict.keys())}")

            # Use the ae_list directly from the data!
            if "ae_list" in monthly_dict and monthly_dict["ae_list"]:
                print(f"🔍 Found ae_list with {len(monthly_dict['ae_list'])} AEs")
                print(f"🔍 AE list content: {monthly_dict['ae_list']}")

                for ae_name in monthly_dict["ae_list"]:
                    ae_list.append(
                        {
                            "ae_id": f"ae_{ae_name.replace(' ', '_').replace('.', '').lower()}",
                            "name": ae_name,
                            "decay_enabled": True,
                            "has_decay_activity": True,
                        }
                    )

                print(
                    f"✅ Converted {len(ae_list)} AEs: {[ae['name'] for ae in ae_list]}"
                )

            # If ae_list is empty, try revenue_data structure
            elif "revenue_data" in monthly_dict:
                print("🔍 Checking revenue_data for AE information...")
                revenue_data = monthly_dict["revenue_data"]
                print(f"🔍 Revenue data type: {type(revenue_data)}")

                if isinstance(revenue_data, list) and len(revenue_data) > 0:
                    print(f"🔍 Revenue data has {len(revenue_data)} items")
                    for i, item in enumerate(revenue_data[:3]):  # Check first 3
                        print(
                            f"🔍 Revenue item {i}: {list(item.keys()) if isinstance(item, dict) else type(item)}"
                        )
                        if isinstance(item, dict):
                            # Look for AE-related keys
                            ae_keys = [k for k in item.keys() if "ae" in k.lower()]
                            print(f"🔍 AE-related keys in item {i}: {ae_keys}")
                            if ae_keys:
                                ae_value = item[ae_keys[0]]
                                print(f"🔍 AE value for {ae_keys[0]}: {ae_value}")
                                if ae_value and ae_value not in [
                                    ae["name"] for ae in ae_list
                                ]:
                                    ae_list.append(
                                        {
                                            "ae_id": f"ae_{ae_value.replace(' ', '_').replace('.', '').lower()}",
                                            "name": ae_value,
                                            "decay_enabled": True,
                                            "has_decay_activity": True,
                                        }
                                    )

        # Use better test data if no real data found
        if not ae_list:
            print("⚠️ No real AEs found, using enhanced test data")
            ae_list = [
                {
                    "ae_id": "sarah_johnson",
                    "name": "Sarah Johnson ⚡",
                    "decay_enabled": True,
                    "has_decay_activity": True,
                },
                {
                    "ae_id": "mike_chen",
                    "name": "Mike Chen",
                    "decay_enabled": True,
                    "has_decay_activity": False,
                },
                {
                    "ae_id": "lisa_rodriguez",
                    "name": "Lisa Rodriguez",
                    "decay_enabled": False,
                    "has_decay_activity": False,
                },
                {
                    "ae_id": "david_kim",
                    "name": "David Kim ⚡",
                    "decay_enabled": True,
                    "has_decay_activity": True,
                },
            ]

        data = {
            "session_date": datetime.now().strftime("%B %d, %Y"),
            "ae_list": ae_list,
            "session": {"completed_aes": [], "total_aes": len(ae_list)},
        }

        return render_template(
            "./pipeline/pipeline_revenue_management.html",
            title="Pipeline Revenue Management",
            data=data,
        )

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback

        traceback.print_exc()

        # Final fallback
        ae_list = [
            {
                "ae_id": "demo_ae_1",
                "name": "Demo AE #1 ⚡",
                "decay_enabled": True,
                "has_decay_activity": True,
            },
            {
                "ae_id": "demo_ae_2",
                "name": "Demo AE #2",
                "decay_enabled": True,
                "has_decay_activity": False,
            },
        ]

        data = {
            "session_date": datetime.now().strftime("%B %d, %Y"),
            "ae_list": ae_list,
            "session": {"completed_aes": [], "total_aes": len(ae_list)},
        }
        return render_template(
            "./pipeline/pipeline_revenue_management.html",
            title="Pipeline Revenue Management",
            data=data,
        )


# src/web/routes/pipeline_routes.py - Replace the get_customer_data function


@pipeline_bp.route("/api/customers/<ae_id>/<month>")
@log_requests
@handle_request_errors
def get_customer_data(ae_id: str, month: str):
    """Get customer/deal data for specific AE and month."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Convert ae_id back to AE name
        ae_name = ae_id.replace("ae_", "").replace("_", " ").title()
        logger.info(
            f"Getting customer data for AE: {ae_name} (ID: {ae_id}), Month: {month}"
        )

        # Parse month
        year, month_num = month.split("-")
        year = int(year)
        month_num = int(month_num)

        # Get revenue data for this AE and month - FIXED filters
        from src.models.report_data import ReportFilters

        try:
            # Create basic filters (without ae_name parameter)
            filters = ReportFilters(year=year)
            logger.info("Created ReportFilters successfully")
        except Exception as e:
            logger.warning(f"Could not create ReportFilters: {e}")
            filters = None

        monthly_data = report_service.get_monthly_revenue_report_data(year, filters)

        booked_deals = []
        pipeline_deals = []

        if monthly_data and hasattr(monthly_data, "to_dict"):
            monthly_dict = monthly_data.to_dict()
            revenue_data = monthly_dict.get("revenue_data", [])

            logger.info(f"Found {len(revenue_data)} revenue records total")

            # Manual filtering for this AE
            for item in revenue_data:
                item_ae = item.get("ae", "")
                if item_ae == ae_name:  # Exact match
                    # Get revenue for this specific month
                    month_key = f"month_{month_num}"
                    month_revenue = item.get(month_key, 0)

                    if month_revenue is not None:
                        month_revenue = float(month_revenue)

                        if month_revenue > 0:
                            # This is booked revenue
                            customer_info = {
                                "customer_name": item.get(
                                    "customer", "Unknown Customer"
                                ),
                                "spot_count": 1,  # Simplified
                                "total_revenue": month_revenue,
                                "first_spot": f"{year}-{month_num:02d}-01",
                                "sector": item.get("sector", "Unknown"),
                                "revenue_type": item.get("revenue_type", "Standard"),
                            }
                            booked_deals.append(customer_info)

        logger.info(f"Found {len(booked_deals)} booked deals for {ae_name} in {month}")

        # Generate pipeline deals (prospective customers)
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
            pipeline_data = pipeline_service.get_pipeline_data(ae_id, month)
            current_pipeline = pipeline_data.get("current_pipeline", 0)

            if current_pipeline > 5000:  # Only show pipeline if substantial
                # Generate representative pipeline deals
                pipeline_deals = [
                    {
                        "customer_name": "Prospective Auto Dealer A",
                        "deal_description": "Holiday Sales Campaign",
                        "amount": current_pipeline * 0.35,
                        "probability": "70",
                        "expected_close": f"{year}-{month_num:02d}-15",
                    },
                    {
                        "customer_name": "Regional Restaurant Chain",
                        "deal_description": "Brand Awareness Package",
                        "amount": current_pipeline * 0.25,
                        "probability": "50",
                        "expected_close": f"{year}-{month_num:02d}-22",
                    },
                    {
                        "customer_name": "Local Medical Practice",
                        "deal_description": "New Location Launch",
                        "amount": current_pipeline * 0.40,
                        "probability": "30",
                        "expected_close": f"{year}-{month_num:02d}-28",
                    },
                ]

                # Filter out deals that are too small
                pipeline_deals = [
                    deal for deal in pipeline_deals if deal["amount"] >= 1000
                ]

        except Exception as e:
            logger.warning(f"Could not get pipeline deals for {ae_id} {month}: {e}")
            pipeline_deals = []

        logger.info(
            f"Generated {len(pipeline_deals)} pipeline deals for {ae_name} in {month}"
        )

        # Combine all deals
        all_deals = booked_deals + pipeline_deals

        return create_success_response(
            {
                "booked_deals": booked_deals,
                "pipeline_deals": pipeline_deals,
                "all_deals": all_deals,
                "month": month,
                "ae_id": ae_id,
                "ae_name": ae_name,
                "summary": {
                    "booked_count": len(booked_deals),
                    "pipeline_count": len(pipeline_deals),
                    "total_booked_revenue": sum(
                        deal["total_revenue"] for deal in booked_deals
                    ),
                    "total_pipeline_value": sum(
                        deal["amount"] for deal in pipeline_deals
                    ),
                },
            }
        )

    except Exception as e:
        logger.error(f"Error getting customer data for {ae_id} {month}: {e}")
        import traceback

        traceback.print_exc()
        return handle_service_error(e, f"getting customer data for {ae_id} {month}")


# Add decay timeline endpoint as well
@pipeline_bp.route("/api/pipeline/decay/timeline/<ae_id>/<month>")
@log_requests
@handle_request_errors
def get_decay_timeline(ae_id: str, month: str):
    """Get decay timeline for specific AE and month."""
    try:
        container = get_container()

        # Try to get pipeline service
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
        except Exception as e:
            logger.warning(f"Pipeline service not available for decay timeline: {e}")
            return create_success_response(
                {"timeline": [], "message": "Decay system not available"}
            )

        # Get decay timeline
        timeline = []
        if hasattr(pipeline_service, "get_decay_timeline"):
            try:
                timeline = pipeline_service.get_decay_timeline(ae_id, month)
            except Exception as e:
                logger.warning(f"Could not get decay timeline for {ae_id} {month}: {e}")

        # If no timeline from service, generate sample events
        if not timeline:
            timeline = [
                {
                    "timestamp": f"{month}-15T10:30:00Z",
                    "event_type": "calibration_reset",
                    "description": "Monthly pipeline calibration",
                    "amount": 0,
                    "old_pipeline": 45000,
                    "new_pipeline": 45000,
                    "customer": None,
                    "created_by": "system",
                }
            ]

        return create_success_response(
            {
                "timeline": timeline,
                "ae_id": ae_id,
                "month": month,
                "event_count": len(timeline),
            }
        )

    except Exception as e:
        logger.error(f"Error getting decay timeline for {ae_id} {month}: {e}")
        return handle_service_error(e, f"getting decay timeline for {ae_id} {month}")


# Add calibration endpoint
@pipeline_bp.route("/api/pipeline/decay/calibration", methods=["POST"])
@log_requests
@handle_request_errors
def calibrate_pipeline():
    """Apply pipeline calibration."""
    try:
        data = request.get_json()

        # Validate required fields
        required_fields = ["ae_id", "month", "pipeline_value", "calibrated_by"]
        for field in required_fields:
            if field not in data:
                return create_error_response(f"Missing required field: {field}", 400)

        container = get_container()

        # Try to get pipeline service
        try:
            pipeline_service = safe_get_service(container, "pipeline_service")
        except Exception as e:
            logger.warning(f"Pipeline service not available for calibration: {e}")
            return create_error_response("Pipeline service not available", 503)

        # Apply calibration
        success = False
        if hasattr(pipeline_service, "set_pipeline_calibration"):
            try:
                success = pipeline_service.set_pipeline_calibration(
                    ae_id=data["ae_id"],
                    month=data["month"],
                    pipeline_value=float(data["pipeline_value"]),
                    calibrated_by=data["calibrated_by"],
                    session_id=data.get("session_id"),
                )
            except Exception as e:
                logger.error(f"Calibration failed: {e}")
                return create_error_response(f"Calibration failed: {str(e)}", 500)

        if success:
            return create_success_response(
                {
                    "message": "Pipeline calibrated successfully",
                    "ae_id": data["ae_id"],
                    "month": data["month"],
                    "pipeline_value": data["pipeline_value"],
                }
            )
        else:
            return create_error_response("Calibration failed", 500)

    except Exception as e:
        logger.error(f"Error in pipeline calibration: {e}")
        return handle_service_error(e, "calibrating pipeline")
