# src/web/routes/language_blocks.py
"""
Language Block Reporting blueprint with Year/Month filtering support
"""

import logging
from flask import Blueprint, request
from datetime import datetime, date
from src.services.container import get_container
from src.web.utils.request_helpers import (
    create_json_response,
    create_error_response,
    handle_service_error,
    log_requests,
    handle_request_errors,
)

logger = logging.getLogger(__name__)

# Create blueprint
language_blocks_bp = Blueprint(
    "language_blocks", __name__, url_prefix="/api/language-blocks"
)


def _get_db():
    """Get DatabaseConnection from container."""
    return get_container().get("database_connection")


def build_date_filter(year=None, month=None):
    """Build SQL date filter based on year and month parameters"""
    filters = []
    params = []

    if year:
        if month:
            # Specific month
            filters.append("year_month = ?")
            params.append(f"{year}-{month:02d}")
        else:
            # Entire year
            filters.append("year = ?")
            params.append(year)

    return filters, params


def get_filter_params():
    """Extract year and month parameters from request"""
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    # Validate month
    if month and (month < 1 or month > 12):
        month = None

    return year, month


@language_blocks_bp.route("/metadata/available-periods", methods=["GET"])
@log_requests
@handle_request_errors
def get_available_periods():
    """Get available years and months for filtering"""
    try:
        with _get_db().connection_ro() as conn:
            years = [
                row["year"]
                for row in conn.execute("""
                    SELECT DISTINCT year
                    FROM language_block_revenue_summary
                    WHERE year IS NOT NULL
                    ORDER BY year DESC
                """).fetchall()
            ]

            months_data = conn.execute("""
                SELECT DISTINCT year,
                       CAST(substr(year_month, 6, 2) AS INTEGER) as month,
                       year_month
                FROM language_block_revenue_summary
                WHERE year_month IS NOT NULL
                ORDER BY year DESC, month ASC
            """).fetchall()

        months_by_year = {}
        for row in months_data:
            y = row["year"]
            m = row["month"]
            if y not in months_by_year:
                months_by_year[y] = []
            if m not in months_by_year[y]:
                months_by_year[y].append(m)

        return create_json_response(
            {
                "available_years": years,
                "months_by_year": months_by_year,
                "current_year": date.today().year,
                "current_month": date.today().month,
            }
        )

    except Exception as e:
        logger.error(f"Error getting available periods: {str(e)}")
        return handle_service_error(e, "Error getting available periods")


@language_blocks_bp.route("/test", methods=["GET"])
def test_connection():
    """Test database connection"""
    try:
        year, month = get_filter_params()
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(date_filters) if date_filters else "1=1"

        query = f"SELECT COUNT(*) as count FROM language_block_revenue_summary WHERE {where_clause}"

        with _get_db().connection_ro() as conn:
            result = conn.execute(query, params).fetchone()
        count = result["count"] if result else 0

        return create_json_response(
            {
                "status": "success",
                "message": "Database connection successful",
                "record_count": count,
                "filters_applied": {"year": year, "month": month},
                "timestamp": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        return create_json_response(
            {
                "status": "error",
                "message": str(e),
                "timestamp": datetime.now().isoformat(),
            }
        )


@language_blocks_bp.route("/summary", methods=["GET"])
@log_requests
@handle_request_errors
def get_language_block_summary():
    """Get overall language block performance summary with date filtering"""
    try:
        year, month = get_filter_params()
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)"] + date_filters
        )

        query = f"""
        SELECT
            COUNT(DISTINCT block_id) as total_blocks,
            COUNT(DISTINCT language_name) as total_languages,
            COUNT(DISTINCT market_code) as total_markets,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block_month,
            MIN(first_air_date) as data_start_date,
            MAX(last_air_date) as data_end_date
        FROM language_block_revenue_summary
        WHERE {where_clause}
        """

        with _get_db().connection_ro() as conn:
            result = conn.execute(query, params).fetchone()

        if not result:
            return create_error_response("No language block data found", 404)

        return create_json_response(
            {
                "summary": dict(result),
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving language block summary: {str(e)}")
        return handle_service_error(e, "Error retrieving summary")


@language_blocks_bp.route("/top-performers", methods=["GET"])
@log_requests
@handle_request_errors
def get_top_performing_blocks():
    """Get top performing language blocks by revenue with date filtering"""
    try:
        year, month = get_filter_params()
        limit = request.args.get("limit", 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10

        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)", "total_revenue IS NOT NULL"]
            + date_filters
        )
        params.append(limit)

        query = f"""
        SELECT
            block_name, language_name, market_display_name, day_part,
            time_start || '-' || time_end as time_slot,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COUNT(DISTINCT year_month) as active_months,
            COALESCE(AVG(total_revenue), 0) as avg_monthly_revenue
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY block_id, block_name, language_name, market_display_name,
                 day_part, time_start, time_end
        ORDER BY total_revenue DESC
        LIMIT ?
        """

        with _get_db().connection_ro() as conn:
            results = conn.execute(query, params).fetchall()

        return create_json_response(
            {
                "top_performers": [dict(row) for row in results],
                "limit": limit,
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving top performers: {str(e)}")
        return handle_service_error(e, "Error retrieving top performers")


@language_blocks_bp.route("/language-performance", methods=["GET"])
@log_requests
@handle_request_errors
def get_language_performance():
    """Get performance metrics by language with date filtering"""
    try:
        year, month = get_filter_params()
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)"] + date_filters
        )

        query = f"""
        SELECT
            language_name,
            COUNT(DISTINCT block_id) as block_count,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block,
            CASE
                WHEN COALESCE(SUM(total_spots), 0) > 0
                THEN COALESCE(SUM(total_revenue), 0) / COALESCE(SUM(total_spots), 1)
                ELSE 0
            END as revenue_per_spot
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY language_name
        ORDER BY total_revenue DESC
        """

        with _get_db().connection_ro() as conn:
            results = conn.execute(query, params).fetchall()

        return create_json_response(
            {
                "language_performance": [dict(row) for row in results],
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving language performance: {str(e)}")
        return handle_service_error(e, "Error retrieving language performance")


@language_blocks_bp.route("/market-performance", methods=["GET"])
@log_requests
@handle_request_errors
def get_market_performance():
    """Get performance metrics by market with date filtering"""
    try:
        year, month = get_filter_params()
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)"] + date_filters
        )

        query = f"""
        SELECT
            market_display_name, market_code,
            COUNT(DISTINCT block_id) as block_count,
            COUNT(DISTINCT language_name) as language_count,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY market_display_name, market_code
        ORDER BY total_revenue DESC
        """

        with _get_db().connection_ro() as conn:
            results = conn.execute(query, params).fetchall()

        return create_json_response(
            {
                "market_performance": [dict(row) for row in results],
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving market performance: {str(e)}")
        return handle_service_error(e, "Error retrieving market performance")


@language_blocks_bp.route("/time-slot-performance", methods=["GET"])
@log_requests
@handle_request_errors
def get_time_slot_performance():
    """Get performance metrics by time slot with date filtering"""
    try:
        year, month = get_filter_params()
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(
            [
                "(is_active = 1 OR is_active IS NULL)",
                "day_part IS NOT NULL AND day_part != ''",
            ]
            + date_filters
        )

        query = f"""
        SELECT
            day_part,
            COUNT(DISTINCT block_id) as block_count,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block,
            CASE
                WHEN COALESCE(SUM(total_spots), 0) > 0
                THEN COALESCE(SUM(total_revenue), 0) / COALESCE(SUM(total_spots), 1)
                ELSE 0
            END as revenue_per_spot
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY day_part
        ORDER BY total_revenue DESC
        """

        with _get_db().connection_ro() as conn:
            results = conn.execute(query, params).fetchall()

        return create_json_response(
            {
                "time_slot_performance": [dict(row) for row in results],
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving time slot performance: {str(e)}")
        return handle_service_error(e, "Error retrieving time slot performance")


@language_blocks_bp.route("/recent-activity", methods=["GET"])
@log_requests
@handle_request_errors
def get_recent_activity():
    """Get recent activity trends for language blocks with date filtering"""
    try:
        year, month = get_filter_params()
        months_back = request.args.get("months", 6, type=int)
        if months_back < 1 or months_back > 24:
            months_back = 6

        date_filters, params = build_date_filter(year, month)

        if not date_filters:
            where_clause = (
                "(is_active = 1 OR is_active IS NULL) "
                f"AND year_month >= date('now', '-{months_back} months', 'start of month') "
                "AND year_month IS NOT NULL AND year_month != ''"
            )
            params = []
        elif year and not month:
            where_clause = (
                "(is_active = 1 OR is_active IS NULL) AND year = ? "
                "AND year_month IS NOT NULL AND year_month != ''"
            )
        else:
            where_clause = " AND ".join(
                [
                    "(is_active = 1 OR is_active IS NULL)",
                    "year_month IS NOT NULL AND year_month != ''",
                ]
                + date_filters
            )

        query = f"""
        SELECT
            year_month,
            COUNT(DISTINCT block_id) as active_blocks,
            COALESCE(SUM(total_revenue), 0) as monthly_revenue,
            COALESCE(SUM(total_spots), 0) as monthly_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY year_month
        ORDER BY year_month DESC
        """

        with _get_db().connection_ro() as conn:
            results = conn.execute(query, params).fetchall()

        return create_json_response(
            {
                "recent_activity": [dict(row) for row in results],
                "months_requested": months_back,
                "filters": {"year": year, "month": month},
                "generated_at": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        logger.error(f"Error retrieving recent activity: {str(e)}")
        return handle_service_error(e, "Error retrieving recent activity")


@language_blocks_bp.route("/insights", methods=["GET"])
@log_requests
@handle_request_errors
def get_performance_insights():
    """Get key performance insights and recommendations with date filtering"""
    try:
        year, month = get_filter_params()
        insights = {}

        date_filters, params = build_date_filter(year, month)
        base_where = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)"] + date_filters
        )

        with _get_db().connection_ro() as conn:
            most_profitable = conn.execute(f"""
                SELECT language_name,
                       COALESCE(SUM(total_revenue), 0)
                           / COALESCE(SUM(total_spots), 1) as revenue_per_spot,
                       COALESCE(SUM(total_spots), 0) as total_spots,
                       COALESCE(SUM(total_revenue), 0) as total_revenue
                FROM language_block_revenue_summary
                WHERE {base_where}
                GROUP BY language_name
                HAVING COALESCE(SUM(total_spots), 0) >= 10
                ORDER BY revenue_per_spot DESC
                LIMIT 1
            """, params).fetchone()
            if most_profitable:
                insights["most_profitable_language"] = dict(most_profitable)

            busiest_slot = conn.execute(f"""
                SELECT day_part,
                       COALESCE(SUM(total_spots), 0) as total_spots,
                       COALESCE(SUM(total_revenue), 0) as total_revenue,
                       COUNT(DISTINCT block_id) as block_count
                FROM language_block_revenue_summary
                WHERE {base_where}
                  AND day_part IS NOT NULL AND day_part != ''
                GROUP BY day_part
                ORDER BY total_spots DESC
                LIMIT 1
            """, params).fetchone()
            if busiest_slot:
                insights["busiest_time_slot"] = dict(busiest_slot)

            opportunities = conn.execute(f"""
                SELECT language_name,
                       COUNT(DISTINCT market_code) as current_markets,
                       COALESCE(SUM(total_revenue), 0) as current_revenue,
                       COALESCE(SUM(total_spots), 0) as total_spots,
                       COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block
                FROM language_block_revenue_summary
                WHERE {base_where}
                GROUP BY language_name
                HAVING current_markets < 5 AND current_revenue > 1000
                ORDER BY current_revenue DESC
                LIMIT 3
            """, params).fetchall()
            if opportunities:
                insights["growth_opportunities"] = [dict(row) for row in opportunities]

        insights["filters"] = {"year": year, "month": month}
        insights["generated_at"] = datetime.now().isoformat()
        return create_json_response(insights)

    except Exception as e:
        logger.error(f"Error retrieving insights: {str(e)}")
        return handle_service_error(e, "Error retrieving insights")


@language_blocks_bp.route("/report", methods=["GET"])
@log_requests
@handle_request_errors
def get_comprehensive_report():
    """Get comprehensive language block report with date filtering"""
    try:
        year, month = get_filter_params()
        report_data = {}

        date_filters, params = build_date_filter(year, month)
        base_where = " AND ".join(
            ["(is_active = 1 OR is_active IS NULL)"] + date_filters
        )

        with _get_db().connection_ro() as conn:
            summary_result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT block_id) as total_blocks,
                    COUNT(DISTINCT language_name) as total_languages,
                    COUNT(DISTINCT market_code) as total_markets,
                    COALESCE(SUM(total_revenue), 0) as total_revenue,
                    COALESCE(SUM(total_spots), 0) as total_spots,
                    COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block_month
                FROM language_block_revenue_summary
                WHERE {base_where}
            """, params).fetchone()
            if summary_result:
                report_data["summary"] = dict(summary_result)

            top_performers = conn.execute(f"""
                SELECT block_name, language_name, market_display_name, day_part,
                       COALESCE(SUM(total_revenue), 0) as total_revenue,
                       COALESCE(SUM(total_spots), 0) as total_spots,
                       COUNT(DISTINCT year_month) as active_months
                FROM language_block_revenue_summary
                WHERE {base_where}
                GROUP BY block_id, block_name, language_name,
                         market_display_name, day_part
                ORDER BY total_revenue DESC
                LIMIT 10
            """, params).fetchall()
            report_data["top_performers"] = [dict(row) for row in top_performers]

            language_performance = conn.execute(f"""
                SELECT language_name,
                       COUNT(DISTINCT block_id) as block_count,
                       COALESCE(SUM(total_revenue), 0) as total_revenue,
                       COALESCE(SUM(total_spots), 0) as total_spots,
                       CASE
                           WHEN COALESCE(SUM(total_spots), 0) > 0
                           THEN COALESCE(SUM(total_revenue), 0)
                               / COALESCE(SUM(total_spots), 1)
                           ELSE 0
                       END as revenue_per_spot
                FROM language_block_revenue_summary
                WHERE {base_where}
                GROUP BY language_name
                ORDER BY total_revenue DESC
            """, params).fetchall()
            report_data["language_performance"] = [
                dict(row) for row in language_performance
            ]

        report_data["filters"] = {"year": year, "month": month}
        report_data["generated_at"] = datetime.now().isoformat()
        return create_json_response(report_data)

    except Exception as e:
        logger.error(f"Error generating comprehensive report: {str(e)}")
        return handle_service_error(e, "Error generating comprehensive report")
