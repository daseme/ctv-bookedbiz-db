# src/web/routes/pricing.py
"""
Pricing analysis routes with drill-down navigation.
Uses broadcast_month periods from month_closures for accurate YoY comparison.
"""

import logging
from flask import Blueprint, render_template, request, jsonify
from src.services.container import get_container
from src.services.pricing_analysis_service import (
    DRILL_PATHS,
    DIMENSION_LABELS,
)

logger = logging.getLogger(__name__)

pricing_bp = Blueprint("pricing", __name__, url_prefix="/pricing")


def safe_get_service(container, name):
    """Safely get a service from the container."""
    try:
        return container.get(name)
    except Exception as e:
        logger.error(f"Failed to get service {name}: {e}")
        raise


@pricing_bp.route("/")
def pricing_index():
    """Pricing analysis landing page with trend and quarterly summary."""
    try:
        container = get_container()
        service = safe_get_service(container, "pricing_analysis_service")

        # Get trailing 12 closed months
        period = service.get_trailing_12_closed_months()

        if not period.months:
            return render_template(
                "pricing/index.html",
                error="No closed months found. Close at least one month to use pricing analysis.",
                overall=None,
                monthly_trend=[],
                quarterly=[],
                filter_options={},
                entry_points=list(DRILL_PATHS.keys()),
                dimension_labels=DIMENSION_LABELS,
                period=period,
            )

        # Get overall summary
        overall = service.get_overall_summary(period)

        # Get monthly trend for chart
        monthly_trend = service.get_monthly_trend(period)

        # Get quarterly summary
        quarterly = service.get_quarterly_summary(period)

        # Get filter options
        filter_options = service.get_filter_options()

        return render_template(
            "pricing/index.html",
            overall=overall,
            monthly_trend=monthly_trend,
            quarterly=quarterly,
            filter_options=filter_options,
            entry_points=list(DRILL_PATHS.keys()),
            dimension_labels=DIMENSION_LABELS,
            period=period,
        )
    except Exception as e:
        logger.error(f"Error in pricing_index: {e}", exc_info=True)
        return render_template("error_500.html"), 500


@pricing_bp.route("/analyze/<dimension>")
def analyze_dimension(dimension: str):
    """Analyze pricing by a specific dimension with drill-down support."""
    try:
        container = get_container()
        service = safe_get_service(container, "pricing_analysis_service")

        # Get period
        period = service.get_trailing_12_closed_months()

        if not period.months:
            return render_template(
                "pricing/drilldown.html",
                error="No closed months found.",
                dimension=dimension,
                dimension_label=DIMENSION_LABELS.get(dimension, dimension),
                data=[],
                overall=None,
                context=None,
                filter_options={},
                is_monthly=False,
                entry_point=dimension,
                filters={},
                min_spots=10,
                period=period,
                dimension_labels=DIMENSION_LABELS,
            )

        # Build filters from query params
        filters = {}
        for key in DRILL_PATHS.get(dimension, []):
            value = request.args.get(key)
            if value:
                filters[key] = value

        # Also check for global filters
        day_part = request.args.get('day_part')
        day_type = request.args.get('day_type')
        min_spots = request.args.get('min_spots', 10, type=int)

        if day_part:
            filters['day_part'] = day_part
        if day_type:
            filters['day_type'] = day_type

        # Create drill-down context
        context = service.create_drilldown_context(dimension, filters)

        # Get the current dimension to analyze
        current_dim = context.path[context.current_level]

        # Get dimension data
        if current_dim == 'monthly':
            # Terminal: show monthly trend
            data = service.get_monthly_trend(period, filters)
            is_monthly = True
        else:
            data = service.get_dimension_summary(
                current_dim, period, filters, min_spots
            )
            is_monthly = False

        # Get overall summary for context
        overall = service.get_overall_summary(period, filters)

        # Get filter options
        filter_options = service.get_filter_options()

        return render_template(
            "pricing/drilldown.html",
            dimension=current_dim,
            dimension_label=DIMENSION_LABELS.get(current_dim, current_dim),
            data=data,
            overall=overall,
            context=context,
            filter_options=filter_options,
            is_monthly=is_monthly,
            entry_point=dimension,
            filters=filters,
            min_spots=min_spots,
            period=period,
            dimension_labels=DIMENSION_LABELS,
        )
    except Exception as e:
        logger.error(f"Error in analyze_dimension: {e}", exc_info=True)
        return render_template("error_500.html"), 500


@pricing_bp.route("/api/trend")
def api_monthly_trend():
    """API endpoint for monthly trend data (for charts)."""
    try:
        container = get_container()
        service = safe_get_service(container, "pricing_analysis_service")

        period = service.get_trailing_12_closed_months()

        # Build filters
        filters = {}
        for key in ['ae', 'market', 'sector', 'customer', 'language', 'day_part', 'day_type']:
            value = request.args.get(key)
            if value:
                filters[key] = value

        trend = service.get_monthly_trend(period, filters if filters else None)

        return jsonify({
            "success": True,
            "period": {
                "display_range": period.display_range,
                "months": period.months,
            },
            "data": [
                {
                    "broadcast_month": t.broadcast_month,
                    "avg_rate": t.avg_rate,
                    "spot_count": t.spot_count,
                    "total_revenue": t.total_revenue
                }
                for t in trend
            ]
        })
    except Exception as e:
        logger.error(f"Error in api_monthly_trend: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@pricing_bp.route("/api/dimension/<dimension>")
def api_dimension_data(dimension: str):
    """API endpoint for dimension summary data."""
    try:
        container = get_container()
        service = safe_get_service(container, "pricing_analysis_service")

        period = service.get_trailing_12_closed_months()
        min_spots = request.args.get('min_spots', 10, type=int)

        # Build filters
        filters = {}
        for key in ['ae', 'market', 'sector', 'customer', 'language', 'day_part', 'day_type', 'tenure']:
            value = request.args.get(key)
            if value:
                filters[key] = value

        data = service.get_dimension_summary(
            dimension, period, filters if filters else None, min_spots
        )

        return jsonify({
            "success": True,
            "dimension": dimension,
            "period": {
                "display_range": period.display_range,
                "months": period.months,
            },
            "data": [
                {
                    "dimension_value": d.dimension_value,
                    "avg_rate": d.avg_rate,
                    "spot_count": d.spot_count,
                    "total_revenue": d.total_revenue,
                    "yoy_rate_change": d.yoy_rate_change,
                    "yoy_volume_change": d.yoy_volume_change,
                }
                for d in data
            ]
        })
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logger.error(f"Error in api_dimension_data: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500


@pricing_bp.route("/api/overall")
def api_overall_summary():
    """API endpoint for overall summary."""
    try:
        container = get_container()
        service = safe_get_service(container, "pricing_analysis_service")

        period = service.get_trailing_12_closed_months()

        # Build filters
        filters = {}
        for key in ['ae', 'market', 'sector', 'customer', 'language', 'day_part', 'day_type', 'tenure']:
            value = request.args.get(key)
            if value:
                filters[key] = value

        overall = service.get_overall_summary(period, filters if filters else None)

        return jsonify({
            "success": True,
            "period": {
                "display_range": period.display_range,
                "months": period.months,
                "prior_months": period.prior_months,
            },
            "data": {
                "avg_rate": overall.avg_rate,
                "spot_count": overall.spot_count,
                "total_revenue": overall.total_revenue,
                "prior_avg_rate": overall.prior_avg_rate,
                "prior_spot_count": overall.prior_spot_count,
                "prior_total_revenue": overall.prior_total_revenue,
                "yoy_rate_change": overall.yoy_rate_change,
                "yoy_volume_change": overall.yoy_volume_change,
            }
        })
    except Exception as e:
        logger.error(f"Error in api_overall_summary: {e}", exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500