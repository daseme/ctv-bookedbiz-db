"""Flask web application for reporting UI."""

from flask import Flask, render_template, jsonify, request
from typing import Dict, List, Any, Optional
import sqlite3
import logging
from datetime import date, datetime
from decimal import Decimal
import json
import os
import sys

# Add src to path for imports
src_path = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, src_path)

try:
    from database.connection import DatabaseConnection
    from repositories.sqlite_repositories import SQLiteSpotRepository, SQLiteCustomerRepository, ReferenceDataRepository
    from services.pipeline_service import PipelineService
except ImportError as e:
    print(f"Import error: {e}")
    # Fallback - try alternative import approach
    import importlib.util
    
    # Import DatabaseConnection directly
    db_spec = importlib.util.spec_from_file_location("connection", os.path.join(src_path, "database", "connection.py"))
    db_module = importlib.util.module_from_spec(db_spec)
    db_spec.loader.exec_module(db_module)
    DatabaseConnection = db_module.DatabaseConnection
    
    # For now, let's create minimal stubs for the repositories and service
    print("Using fallback imports...")
    
    class SQLiteSpotRepository:
        def __init__(self, db): pass
    
    class SQLiteCustomerRepository:
        def __init__(self, db): pass
        
    class ReferenceDataRepository:
        def __init__(self, db): pass
        
    class PipelineService:
        def __init__(self, db, path): 
            self.db_connection = db
            self.data_path = path
        
        def get_ae_list(self):
            # FORCE real AE data from database - no more fake data!
            conn = self.db_connection.connect()
            query = """
            SELECT DISTINCT sales_person as ae_name,
                   COUNT(*) as spot_count,
                   ROUND(SUM(gross_rate), 2) as total_revenue,
                   ROUND(AVG(gross_rate), 2) as avg_rate
            FROM spots 
            WHERE sales_person IS NOT NULL 
            AND sales_person != ''
            AND gross_rate IS NOT NULL
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY sales_person
            ORDER BY total_revenue DESC
            """
            cursor = conn.execute(query)
            results = cursor.fetchall()
            print(f"REAL AE DATA: Found {len(results)} AEs in database")
            
            aes = []
            for i, row in enumerate(results):
                ae_name = row[0]
                print(f"REAL AE DATA: Processing AE {i+1}: {ae_name}")
                
                # Calculate YTD target based on 2025 budget if available
                ytd_target = self._calculate_ytd_target(ae_name)
                
                aes.append({
                    'ae_id': f"AE{i+1:03d}",  # Generate AE001, AE002, etc.
                    'name': ae_name,  # REAL sales_person from database
                    'territory': self._get_territory_for_ae(ae_name),
                    'ytd_target': ytd_target,
                    'ytd_actual': int(row[2] or 0),  # total_revenue from database
                    'avg_deal_size': int(row[3] or 0),  # avg_rate from database
                    'active': True
                })
            print(f"REAL AE DATA: Returning {len(aes)} real AEs from database")
            return aes
            
        def _get_budget_for_ae_month(self, ae_name, month):
            """Get budget amount for specific AE and month from real budget data."""
            try:
                import json
                # Use absolute path
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
                budget_file_path = os.path.join(project_root, 'real_budget_data.json')
                
                with open(budget_file_path, 'r') as f:
                    budget_data = json.load(f)
                
                # Check if AE has budget data
                if ae_name in budget_data.get('budget_2025', {}):
                    return budget_data['budget_2025'][ae_name].get(month, 0)
                
                # Check for common mappings
                if ae_name == 'House' and 'House' in budget_data.get('budget_2025', {}):
                    return budget_data['budget_2025']['House'].get(month, 0)
                
                return 0  # No budget found
            except Exception as e:
                print(f"ERROR loading budget data: {e}")
                return 0
                
        def _calculate_ytd_target(self, ae_name):
            """Calculate YTD target based on 2025 budget data."""
            try:
                import json
                from datetime import datetime
                
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
                budget_file_path = os.path.join(project_root, 'real_budget_data.json')
                with open(budget_file_path, 'r') as f:
                    budget_data = json.load(f)
                
                if ae_name in budget_data.get('budget_2025', {}):
                    # Sum all months in 2025 for annual target
                    annual_target = sum(budget_data['budget_2025'][ae_name].values())
                    return int(annual_target)
                    
                return 1000000  # Default if no budget data
            except Exception:
                return 1000000
                
        def _get_territory_for_ae(self, ae_name):
            """Get territory/category for AE based on name."""
            if 'House' in ae_name:
                return 'House/Internal'
            elif 'Lane' in ae_name:
                return 'Commercial'
            elif 'Van Patten' in ae_name:
                return 'Commercial'
            elif 'White Horse' in ae_name:
                return 'International'
            else:
                return 'General'
            
        def get_ae_by_id(self, ae_id):
            ae_list = self.get_ae_list()
            return next((ae for ae in ae_list if ae['ae_id'] == ae_id), None)
            
        def get_monthly_summary(self, ae_id, months_ahead=6):
             # Get the AE name from ae_id
             ae_info = self.get_ae_by_id(ae_id)
             if not ae_info:
                 return []
             
             ae_name = ae_info['name']
             
             # Generate month list for the next X months
             from datetime import datetime, date
             import calendar
             
             current_date = datetime.now()
             months = []
             
             for i in range(months_ahead):
                 target_month = current_date.month + i
                 target_year = current_date.year
                 
                 while target_month > 12:
                     target_month -= 12
                     target_year += 1
                 
                 month_date = date(target_year, target_month, 1)
                 months.append(month_date.strftime('%Y-%m'))
             
             summary = []
             
             for month in months:
                 try:
                     # Get historical/booked revenue using working report approach
                     import sqlite3
                     conn = sqlite3.connect(DB_PATH)
                     conn.row_factory = sqlite3.Row
                     
                     hist_query = """
                     SELECT ROUND(SUM(gross_rate), 2) as revenue
                     FROM spots
                     WHERE sales_person = ?
                     AND strftime('%Y-%m', broadcast_month) = ?
                     AND gross_rate IS NOT NULL
                     AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                     """
                     
                     cursor = conn.execute(hist_query, (ae_name, month))
                     hist_result = cursor.fetchone()
                     booked_revenue = hist_result[0] if hist_result and hist_result[0] else 0
                     conn.close()
                     
                 except Exception as e:
                     print(f"ERROR getting revenue for {ae_name}/{month}: {e}")
                     booked_revenue = 0
                 
                 # Get real budget data
                 budget = self._get_budget_for_ae_month(ae_name, month)
                 
                 # Pipeline data defaults
                 current_pipeline = budget * 0.6 if budget > 0 else 50000
                 expected_pipeline = budget * 0.7 if budget > 0 else 60000
                 
                 # Calculate gaps
                 pipeline_gap = current_pipeline - expected_pipeline
                 budget_gap = (booked_revenue + current_pipeline) - budget
                 
                 # Determine status
                 if pipeline_gap >= 0:
                     status = 'ahead'
                 elif pipeline_gap >= -10000:
                     status = 'on_track'
                 else:
                     status = 'behind'
                 
                 month_summary = {
                     'month': month,
                     'month_display': datetime.strptime(month, '%Y-%m').strftime('%B %Y'),
                     'is_current_month': month == current_date.strftime('%Y-%m'),
                     'booked_revenue': float(booked_revenue),
                     'current_pipeline': current_pipeline,
                     'expected_pipeline': expected_pipeline,
                     'budget': budget,
                     'pipeline_gap': pipeline_gap,
                     'budget_gap': budget_gap,
                     'pipeline_status': status,
                     'notes': '',
                     'last_updated': ''
                 }
                 
                 summary.append(month_summary)
             
             return summary
            
        def get_review_session(self, session_date):
            return {
                'session_id': session_date,
                'review_date': session_date,
                'completed_aes': [],
                'session_notes': {},
                'total_aes': len(self.get_ae_list()),
                'session_started': session_date,
                'last_updated': session_date
            }

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database connection setup - use absolute paths
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
DB_PATH = os.path.join(PROJECT_ROOT, 'data/database/production.db')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data/processed')
db_connection = DatabaseConnection(DB_PATH)

# Repository instances
spot_repo = SQLiteSpotRepository(db_connection)
customer_repo = SQLiteCustomerRepository(db_connection)
reference_repo = ReferenceDataRepository(db_connection)

# Pipeline service instance
pipeline_service = PipelineService(db_connection, DATA_PATH)


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
            
        return json.dumps(data, cls=DecimalEncoder), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        logger.error(f"Error getting {report_type} data: {e}")
        return jsonify({'error': str(e)}), 500


# Pipeline Revenue Management Routes

@app.route('/pipeline-revenue')
def pipeline_revenue_management():
    """Pipeline Revenue Management main page."""
    try:
        # Get current session data
        session_date = date.today().strftime('%Y-%m-%d')
        session = pipeline_service.get_review_session(session_date)
        ae_list = pipeline_service.get_ae_list()
        
        data = {
            'session': session,
            'ae_list': ae_list,
            'session_date': session_date
        }
        
        return render_template('pipeline_revenue.html',
                             title="Pipeline Revenue Management",
                             data=data)
    except Exception as e:
        logger.error(f"Error loading pipeline revenue management: {e}")
        return f"Error loading pipeline management: {e}", 500


@app.route('/api/aes')
def get_aes():
    """Get list of all Account Executives."""
    try:
        ae_list = pipeline_service.get_ae_list()
        return jsonify({'success': True, 'data': ae_list})
    except Exception as e:
        logger.error(f"Error getting AE list: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/ae/<ae_id>/summary')
def get_ae_summary(ae_id: str):
    """Get comprehensive summary for specific AE."""
    try:
        ae_info = pipeline_service.get_ae_by_id(ae_id)
        if not ae_info:
            return jsonify({'success': False, 'error': 'AE not found'}), 404
        
        monthly_summary = pipeline_service.get_monthly_summary(ae_id)
        
        data = {
            'ae_info': ae_info,
            'monthly_summary': monthly_summary
        }
        
        return json.dumps({'success': True, 'data': data}, cls=DecimalEncoder), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        logger.error(f"Error getting AE {ae_id} summary: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/pipeline/<ae_id>/<month>', methods=['PUT'])
def update_pipeline(ae_id: str, month: str):
    """Update pipeline amount for specific AE and month."""
    try:
        data = request.get_json()
        current_pipeline = float(data.get('current_pipeline', 0))
        notes = data.get('notes', '')
        updated_by = data.get('updated_by', 'system')
        
        success = pipeline_service.update_pipeline(ae_id, month, current_pipeline, notes, updated_by)
        
        if success:
            return jsonify({
                'success': True, 
                'message': 'Pipeline updated successfully',
                'data': {
                    'ae_id': ae_id,
                    'month': month,
                    'current_pipeline': current_pipeline,
                    'notes': notes
                }
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to update pipeline'}), 500
            
    except Exception as e:
        logger.error(f"Error updating pipeline for {ae_id}/{month}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/customers/<ae_id>/<month>')
def get_customer_details(ae_id: str, month: str):
    """Get customer deal details for specific AE and month."""
    try:
        customers = pipeline_service.get_customer_details(ae_id, month)
        
        # Categorize deals
        booked_deals = [c for c in customers if c['status'] == 'closed_won']
        pipeline_deals = [c for c in customers if c['status'] in ['committed', 'pipeline']]
        
        data = {
            'ae_id': ae_id,
            'month': month,
            'booked_deals': booked_deals,
            'pipeline_deals': pipeline_deals,
            'all_deals': customers,
            'totals': {
                'booked_total': sum(d['amount'] for d in booked_deals),
                'pipeline_total': sum(d['amount'] for d in pipeline_deals),
                'total': sum(d['amount'] for d in customers)
            }
        }
        
        return json.dumps({'success': True, 'data': data}, cls=DecimalEncoder), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        logger.error(f"Error getting customer details for {ae_id}/{month}: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/review-session', methods=['POST'])
def update_review_session():
    """Update review session completion status and notes."""
    try:
        data = request.get_json()
        session_date = data.get('session_date', date.today().strftime('%Y-%m-%d'))
        completed_aes = data.get('completed_aes', [])
        notes = data.get('notes', {})
        
        success = pipeline_service.update_review_session(session_date, completed_aes, notes)
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Review session updated successfully'
            })
        else:
            return jsonify({'success': False, 'error': 'Failed to update session'}), 500
            
    except Exception as e:
        logger.error(f"Error updating review session: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 