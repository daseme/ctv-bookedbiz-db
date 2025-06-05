"""Flask web application for reporting UI."""

from flask import Flask, render_template, jsonify
from typing import Dict, List, Any, Optional
import sqlite3
import logging
from datetime import date, datetime
from decimal import Decimal
import json
import os
import sys

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database.connection import DatabaseConnection
from repositories.sqlite_repositories import SQLiteSpotRepository, SQLiteCustomerRepository, ReferenceDataRepository

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database connection setup
DB_PATH = os.path.join(os.path.dirname(__file__), '../../data/database/test.db')
db_connection = DatabaseConnection(DB_PATH)

# Repository instances
spot_repo = SQLiteSpotRepository(db_connection)
customer_repo = SQLiteCustomerRepository(db_connection)
reference_repo = ReferenceDataRepository(db_connection)


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal objects."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


def get_monthly_revenue_summary() -> List[Dict[str, Any]]:
    """Get monthly revenue summary data using the SQL query."""
    conn = db_connection.connect()
    
    # Read the SQL query from the file
    query_path = os.path.join(os.path.dirname(__file__), '../../queries/monthly-revenue-summary.sql')
    with open(query_path, 'r') as f:
        query = f.read()
    
    cursor = conn.execute(query)
    results = [dict(row) for row in cursor.fetchall()]
    
    return results


def get_quarterly_performance_data() -> Dict[str, Any]:
    """Get quarterly performance data for reporting."""
    conn = db_connection.connect()
    
    # Get current year quarterly data
    query = """
    SELECT 
        CASE 
            WHEN strftime('%m', broadcast_month) IN ('01', '02', '03') THEN 'Q1'
            WHEN strftime('%m', broadcast_month) IN ('04', '05', '06') THEN 'Q2'
            WHEN strftime('%m', broadcast_month) IN ('07', '08', '09') THEN 'Q3'
            WHEN strftime('%m', broadcast_month) IN ('10', '11', '12') THEN 'Q4'
        END as quarter,
        strftime('%Y', broadcast_month) as year,
        COUNT(*) as spot_count,
        ROUND(SUM(gross_rate), 2) as total_revenue,
        ROUND(AVG(gross_rate), 2) as avg_rate
    FROM spots
    WHERE broadcast_month IS NOT NULL 
    AND gross_rate IS NOT NULL
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY quarter, year
    ORDER BY year DESC, quarter
    """
    
    cursor = conn.execute(query)
    quarterly_data = [dict(row) for row in cursor.fetchall()]
    
    return {
        'current_year': datetime.now().year,
        'quarterly_data': quarterly_data
    }


def get_ae_performance_data() -> List[Dict[str, Any]]:
    """Get AE (Account Executive) performance data."""
    conn = db_connection.connect()
    
    query = """
    SELECT 
        sales_person as ae_name,
        COUNT(*) as spot_count,
        ROUND(SUM(gross_rate), 2) as total_revenue,
        ROUND(AVG(gross_rate), 2) as avg_rate,
        MIN(air_date) as first_spot_date,
        MAX(air_date) as last_spot_date
    FROM spots
    WHERE sales_person IS NOT NULL 
    AND sales_person != ''
    AND gross_rate IS NOT NULL
    AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    GROUP BY sales_person
    ORDER BY total_revenue DESC
    """
    
    cursor = conn.execute(query)
    return [dict(row) for row in cursor.fetchall()]


def get_sector_performance_data() -> Dict[str, Any]:
    """Get sector-based performance data."""
    conn = db_connection.connect()
    
    # Get sectors performance
    query = """
    SELECT 
        s.sector_name,
        s.sector_code,
        COUNT(sp.*) as spot_count,
        ROUND(SUM(sp.gross_rate), 2) as total_revenue,
        ROUND(AVG(sp.gross_rate), 2) as avg_rate
    FROM spots sp
    JOIN customers c ON sp.customer_id = c.customer_id
    JOIN sectors s ON c.sector_id = s.sector_id
    WHERE sp.gross_rate IS NOT NULL
    AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
    GROUP BY s.sector_id, s.sector_name, s.sector_code
    ORDER BY total_revenue DESC
    """
    
    cursor = conn.execute(query)
    sector_data = [dict(row) for row in cursor.fetchall()]
    
    # Get top customers by sector
    customer_query = """
    SELECT 
        s.sector_name,
        c.normalized_name as customer_name,
        COUNT(sp.*) as spot_count,
        ROUND(SUM(sp.gross_rate), 2) as total_revenue
    FROM spots sp
    JOIN customers c ON sp.customer_id = c.customer_id
    JOIN sectors s ON c.sector_id = s.sector_id
    WHERE sp.gross_rate IS NOT NULL
    AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
    GROUP BY s.sector_id, c.customer_id
    ORDER BY s.sector_name, total_revenue DESC
    """
    
    cursor = conn.execute(customer_query)
    customer_data = [dict(row) for row in cursor.fetchall()]
    
    return {
        'sectors': sector_data,
        'top_customers_by_sector': customer_data
    }


@app.route('/')
def index():
    """Landing page with links to all reports."""
    return render_template('index.html')


@app.route('/report1')
def monthly_revenue_report():
    """Monthly Revenue Summary Report."""
    try:
        data = get_monthly_revenue_summary()
        return render_template('report1.html', 
                             title="Monthly Revenue Summary",
                             data=data)
    except Exception as e:
        logger.error(f"Error generating monthly revenue report: {e}")
        return f"Error generating report: {e}", 500


@app.route('/report2')
def expectation_tracking_report():
    """Management Expectation Tracking Report."""
    try:
        # Generate mock expectation data based on quarterly performance
        quarterly_data = get_quarterly_performance_data()
        ae_data = get_ae_performance_data()
        
        # Create expectation tracking structure
        expectation_data = {
            'current_year': quarterly_data['current_year'],
            'quarterly_data': quarterly_data['quarterly_data'],
            'ae_performance': ae_data
        }
        
        return render_template('report2.html',
                             title="Management Expectation Tracking",
                             data=expectation_data)
    except Exception as e:
        logger.error(f"Error generating expectation tracking report: {e}")
        return f"Error generating report: {e}", 500


@app.route('/report3')
def performance_story_report():
    """Quarterly Performance Story Report."""
    try:
        data = get_quarterly_performance_data()
        ae_data = get_ae_performance_data()
        
        performance_data = {
            **data,
            'ae_performance': ae_data
        }
        
        return render_template('report3.html',
                             title="Quarterly Performance Story", 
                             data=performance_data)
    except Exception as e:
        logger.error(f"Error generating performance story report: {e}")
        return f"Error generating report: {e}", 500


@app.route('/report4')
def quarterly_sectors_report():
    """Enhanced Quarterly Performance with Sector Analysis."""
    try:
        quarterly_data = get_quarterly_performance_data()
        sector_data = get_sector_performance_data()
        ae_data = get_ae_performance_data()
        
        combined_data = {
            **quarterly_data,
            **sector_data,
            'ae_performance': ae_data
        }
        
        return render_template('report4.html',
                             title="Quarterly Performance with Sector Analysis",
                             data=combined_data)
    except Exception as e:
        logger.error(f"Error generating quarterly sectors report: {e}")
        return f"Error generating report: {e}", 500


@app.route('/api/data/<report_type>')
def api_data(report_type: str):
    """API endpoint to get report data as JSON."""
    try:
        if report_type == 'monthly':
            data = get_monthly_revenue_summary()
        elif report_type == 'quarterly':
            data = get_quarterly_performance_data()
        elif report_type == 'ae':
            data = get_ae_performance_data()
        elif report_type == 'sectors':
            data = get_sector_performance_data()
        else:
            return jsonify({'error': 'Invalid report type'}), 400
            
        return jsonify(data, cls=DecimalEncoder)
    except Exception as e:
        logger.error(f"Error getting {report_type} data: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 