# src/web/routes/language_blocks.py
"""
Language Block Reporting blueprint with Year/Month filtering support
"""
import logging
import sqlite3
from flask import Blueprint, request
from datetime import datetime, date
from src.services.container import get_container
from src.web.utils.request_helpers import (
    create_json_response, create_success_response, create_error_response,
    handle_service_error, safe_get_service, log_requests, handle_request_errors
)

logger = logging.getLogger(__name__)

# Create blueprint
language_blocks_bp = Blueprint('language_blocks', __name__, url_prefix='/api/language-blocks')

def get_database_connection():
    """Get database connection using your existing system"""
    try:
        container = get_container()
        
        # Try to get database path from container/config
        try:
            # First try to get from config
            db_path = container.get_config('DB_PATH', None)
            if not db_path:
                # Try alternative config names
                db_path = container.get_config('DATABASE_PATH', None)
            if not db_path:
                db_path = container.get_config('SQLITE_DB_PATH', None)
            if not db_path:
                # Default path based on your system
                db_path = '/opt/apps/ctv-bookedbiz-db/data/database/production.db'
                
        except Exception as e:
            logger.warning(f"Could not get DB path from config: {e}")
            db_path = '/opt/apps/ctv-bookedbiz-db/data/database/production.db'
        
        # Create connection
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # This allows dict-like access
        return conn
        
    except Exception as e:
        logger.error(f"Failed to get database connection: {e}")
        raise

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
    year = request.args.get('year', type=int)
    month = request.args.get('month', type=int)
    
    # Validate month
    if month and (month < 1 or month > 12):
        month = None
    
    return year, month

@language_blocks_bp.route('/metadata/available-periods', methods=['GET'])
@log_requests
@handle_request_errors
def get_available_periods():
    """Get available years and months for filtering"""
    try:
        conn = get_database_connection()
        
        # Get available years
        year_query = """
        SELECT DISTINCT year 
        FROM language_block_revenue_summary 
        WHERE year IS NOT NULL 
        ORDER BY year DESC
        """
        cursor = conn.execute(year_query)
        years = [row['year'] for row in cursor.fetchall()]
        
        # Get available months for each year
        month_query = """
        SELECT DISTINCT year, 
               CAST(substr(year_month, 6, 2) AS INTEGER) as month,
               year_month
        FROM language_block_revenue_summary 
        WHERE year_month IS NOT NULL 
        ORDER BY year DESC, month ASC
        """
        cursor = conn.execute(month_query)
        months_data = cursor.fetchall()
        
        # Group months by year
        months_by_year = {}
        for row in months_data:
            year = row['year']
            month = row['month']
            if year not in months_by_year:
                months_by_year[year] = []
            if month not in months_by_year[year]:
                months_by_year[year].append(month)
        
        conn.close()
        
        return create_json_response({
            "available_years": years,
            "months_by_year": months_by_year,
            "current_year": date.today().year,
            "current_month": date.today().month
        })
        
    except Exception as e:
        logger.error(f"Error getting available periods: {str(e)}")
        return handle_service_error(e, "Error getting available periods")

@language_blocks_bp.route('/test', methods=['GET'])
def test_connection():
    """Test database connection"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        # Build test query with filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(date_filters) if date_filters else "1=1"
        
        query = f"SELECT COUNT(*) as count FROM language_block_revenue_summary WHERE {where_clause}"
        cursor = conn.execute(query, params)
        result = cursor.fetchone()
        count = result['count'] if result else 0
        
        conn.close()
        
        return create_json_response({
            "status": "success",
            "message": "Database connection successful",
            "record_count": count,
            "filters_applied": {
                "year": year,
                "month": month
            },
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return create_json_response({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        })

@language_blocks_bp.route('/summary', methods=['GET'])
@log_requests
@handle_request_errors
def get_language_block_summary():
    """Get overall language block performance summary with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)"] + date_filters)
        
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
        
        cursor = conn.execute(query, params)
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            return create_error_response("No language block data found", 404)
        
        # Convert to dict
        result_dict = dict(result)
        
        response_data = {
            "summary": result_dict,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        }
        
        return create_json_response(response_data)
        
    except Exception as e:
        logger.error(f"Error retrieving language block summary: {str(e)}")
        return handle_service_error(e, "Error retrieving summary")

@language_blocks_bp.route('/top-performers', methods=['GET'])
@log_requests
@handle_request_errors
def get_top_performing_blocks():
    """Get top performing language blocks by revenue with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        limit = request.args.get('limit', 10, type=int)
        if limit < 1 or limit > 50:
            limit = 10
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)", "total_revenue IS NOT NULL"] + date_filters)
        
        # Add limit parameter
        params.append(limit)
            
        query = f"""
        SELECT 
            block_name,
            language_name,
            market_display_name,
            day_part,
            time_start || '-' || time_end as time_slot,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COUNT(DISTINCT year_month) as active_months,
            COALESCE(AVG(total_revenue), 0) as avg_monthly_revenue
        FROM language_block_revenue_summary
        WHERE {where_clause}
        GROUP BY block_id, block_name, language_name, market_display_name, day_part, time_start, time_end
        ORDER BY total_revenue DESC
        LIMIT ?
        """
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert results to list of dicts
        results_list = [dict(row) for row in results]
        
        return create_json_response({
            "top_performers": results_list,
            "limit": limit,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving top performers: {str(e)}")
        return handle_service_error(e, "Error retrieving top performers")

@language_blocks_bp.route('/language-performance', methods=['GET'])
@log_requests
@handle_request_errors
def get_language_performance():
    """Get performance metrics by language with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)"] + date_filters)
        
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
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert results to list of dicts
        results_list = [dict(row) for row in results]
        
        return create_json_response({
            "language_performance": results_list,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving language performance: {str(e)}")
        return handle_service_error(e, "Error retrieving language performance")

@language_blocks_bp.route('/market-performance', methods=['GET'])
@log_requests
@handle_request_errors
def get_market_performance():
    """Get performance metrics by market with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)"] + date_filters)
        
        query = f"""
        SELECT 
            market_display_name,
            market_code,
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
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        results_list = [dict(row) for row in results]
        
        return create_json_response({
            "market_performance": results_list,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving market performance: {str(e)}")
        return handle_service_error(e, "Error retrieving market performance")

@language_blocks_bp.route('/time-slot-performance', methods=['GET'])
@log_requests
@handle_request_errors
def get_time_slot_performance():
    """Get performance metrics by time slot with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)", "day_part IS NOT NULL AND day_part != ''"] + date_filters)
        
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
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        results_list = [dict(row) for row in results]
        
        return create_json_response({
            "time_slot_performance": results_list,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving time slot performance: {str(e)}")
        return handle_service_error(e, "Error retrieving time slot performance")

@language_blocks_bp.route('/recent-activity', methods=['GET'])
@log_requests
@handle_request_errors
def get_recent_activity():
    """Get recent activity trends for language blocks with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        
        months_back = request.args.get('months', 6, type=int)
        if months_back < 1 or months_back > 24:
            months_back = 6
        
        # Build date filters - for recent activity, we might want to show trends even with year filter
        date_filters, params = build_date_filter(year, month)
        
        if not date_filters:
            # No specific filters, show recent months
            where_clause = f"(is_active = 1 OR is_active IS NULL) AND year_month >= date('now', '-{months_back} months', 'start of month') AND year_month IS NOT NULL AND year_month != ''"
            params = []
        else:
            # With filters, adjust the recent activity logic
            if year and not month:
                # Show all months for the year
                where_clause = f"(is_active = 1 OR is_active IS NULL) AND year = ? AND year_month IS NOT NULL AND year_month != ''"
            else:
                # Specific month or other filters
                where_clause = " AND ".join(["(is_active = 1 OR is_active IS NULL)", "year_month IS NOT NULL AND year_month != ''"] + date_filters)
        
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
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        results_list = [dict(row) for row in results]
        
        return create_json_response({
            "recent_activity": results_list,
            "months_requested": months_back,
            "filters": {
                "year": year,
                "month": month
            },
            "generated_at": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error retrieving recent activity: {str(e)}")
        return handle_service_error(e, "Error retrieving recent activity")

@language_blocks_bp.route('/insights', methods=['GET'])
@log_requests
@handle_request_errors 
def get_performance_insights():
    """Get key performance insights and recommendations with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        insights = {}
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        base_where = " AND ".join(["(is_active = 1 OR is_active IS NULL)"] + date_filters)
        
        # Most profitable language per spot
        query1 = f"""
        SELECT 
            language_name,
            COALESCE(SUM(total_revenue), 0) / COALESCE(SUM(total_spots), 1) as revenue_per_spot,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(SUM(total_revenue), 0) as total_revenue
        FROM language_block_revenue_summary
        WHERE {base_where}
        GROUP BY language_name
        HAVING COALESCE(SUM(total_spots), 0) >= 10
        ORDER BY revenue_per_spot DESC
        LIMIT 1
        """
        
        cursor = conn.execute(query1, params)
        most_profitable = cursor.fetchone()
        if most_profitable:
            insights['most_profitable_language'] = dict(most_profitable)
        
        # Busiest time slot
        query2 = f"""
        SELECT 
            day_part,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COUNT(DISTINCT block_id) as block_count
        FROM language_block_revenue_summary
        WHERE {base_where} AND day_part IS NOT NULL AND day_part != ''
        GROUP BY day_part
        ORDER BY total_spots DESC
        LIMIT 1
        """
        
        cursor = conn.execute(query2, params)
        busiest_slot = cursor.fetchone()
        if busiest_slot:
            insights['busiest_time_slot'] = dict(busiest_slot)
        
        # Growth opportunities
        query3 = f"""
        SELECT 
            language_name,
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
        """
        
        cursor = conn.execute(query3, params)
        opportunities = cursor.fetchall()
        if opportunities:
            insights['growth_opportunities'] = [dict(row) for row in opportunities]
        
        conn.close()
        insights['filters'] = {
            "year": year,
            "month": month
        }
        insights['generated_at'] = datetime.now().isoformat()
        
        return create_json_response(insights)
        
    except Exception as e:
        logger.error(f"Error retrieving insights: {str(e)}")
        return handle_service_error(e, "Error retrieving insights")

@language_blocks_bp.route('/report', methods=['GET'])
@log_requests
@handle_request_errors
def get_comprehensive_report():
    """Get comprehensive language block report with date filtering"""
    try:
        conn = get_database_connection()
        year, month = get_filter_params()
        report_data = {}
        
        # Build date filters
        date_filters, params = build_date_filter(year, month)
        base_where = " AND ".join(["(is_active = 1 OR is_active IS NULL)"] + date_filters)
        
        # Get summary
        summary_query = f"""
        SELECT 
            COUNT(DISTINCT block_id) as total_blocks,
            COUNT(DISTINCT language_name) as total_languages,
            COUNT(DISTINCT market_code) as total_markets,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COALESCE(AVG(total_revenue), 0) as avg_revenue_per_block_month
        FROM language_block_revenue_summary
        WHERE {base_where}
        """
        cursor = conn.execute(summary_query, params)
        summary_result = cursor.fetchone()
        if summary_result:
            report_data['summary'] = dict(summary_result)
        
        # Get top performers
        top_performers_query = f"""
        SELECT 
            block_name,
            language_name,
            market_display_name,
            day_part,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            COUNT(DISTINCT year_month) as active_months
        FROM language_block_revenue_summary
        WHERE {base_where}
        GROUP BY block_id, block_name, language_name, market_display_name, day_part
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        cursor = conn.execute(top_performers_query, params)
        top_performers = cursor.fetchall()
        report_data['top_performers'] = [dict(row) for row in top_performers]
        
        # Get language performance
        language_query = f"""
        SELECT 
            language_name,
            COUNT(DISTINCT block_id) as block_count,
            COALESCE(SUM(total_revenue), 0) as total_revenue,
            COALESCE(SUM(total_spots), 0) as total_spots,
            CASE 
                WHEN COALESCE(SUM(total_spots), 0) > 0 
                THEN COALESCE(SUM(total_revenue), 0) / COALESCE(SUM(total_spots), 1)
                ELSE 0
            END as revenue_per_spot
        FROM language_block_revenue_summary
        WHERE {base_where}
        GROUP BY language_name
        ORDER BY total_revenue DESC
        """
        cursor = conn.execute(language_query, params)
        language_performance = cursor.fetchall()
        report_data['language_performance'] = [dict(row) for row in language_performance]
        
        conn.close()
        report_data['filters'] = {
            "year": year,
            "month": month
        }
        report_data['generated_at'] = datetime.now().isoformat()
        
        return create_json_response(report_data)
        
    except Exception as e:
        logger.error(f"Error generating comprehensive report: {str(e)}")
        return handle_service_error(e, "Error generating comprehensive report")