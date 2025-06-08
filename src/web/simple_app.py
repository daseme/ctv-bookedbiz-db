"""Simplified Flask web application for testing."""

from flask import Flask, render_template, jsonify, request
import sqlite3
import os
import json
from datetime import datetime

app = Flask(__name__)

# Database path
DB_PATH = '../../data/database/production.db'

def get_db_connection():
    """Get database connection."""
    db_path = os.path.join(os.path.dirname(__file__), DB_PATH)
    print(f"Database path: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_quarter_status(year, quarter_num):
    """
    Determine if a quarter is CLOSED or OPEN based on actual month closures
    
    Args:
        year: Year (int)
        quarter_num: Quarter number 1-4 (int)
    
    Returns:
        'CLOSED' if all months in quarter are closed, 'OPEN' otherwise
    """
    import sqlite3
    
    # Define quarter months based on database format (Mmm-YY)
    quarter_months = {
        1: [f'Jan-{str(year)[2:]}', f'Feb-{str(year)[2:]}', f'Mar-{str(year)[2:]}'],
        2: [f'Apr-{str(year)[2:]}', f'May-{str(year)[2:]}', f'Jun-{str(year)[2:]}'],
        3: [f'Jul-{str(year)[2:]}', f'Aug-{str(year)[2:]}', f'Sep-{str(year)[2:]}'],
        4: [f'Oct-{str(year)[2:]}', f'Nov-{str(year)[2:]}', f'Dec-{str(year)[2:]}']
    }
    
    months_in_quarter = quarter_months.get(quarter_num, [])
    if not months_in_quarter:
        return 'OPEN'
    
    try:
        # Query database for closed months
        db_path = os.path.join(os.path.dirname(__file__), DB_PATH)
        conn = sqlite3.connect(db_path)
        placeholders = ','.join('?' * len(months_in_quarter))
        cursor = conn.execute(f"""
            SELECT broadcast_month 
            FROM month_closures 
            WHERE broadcast_month IN ({placeholders})
        """, months_in_quarter)
        
        closed_months = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Quarter is CLOSED if ALL months are closed
        return 'CLOSED' if len(closed_months) == len(months_in_quarter) else 'OPEN'
        
    except Exception as e:
        print(f"Error checking quarter status: {e}")
        return 'OPEN'  # Default to OPEN on error

def get_budget_for_ae_month(ae_name, month):
    """Get budget for AE and month from real_budget_data.json"""
    try:
        budget_file = os.path.join(os.path.dirname(__file__), '../../data/processed/real_budget_data.json')
        with open(budget_file, 'r') as f:
            budget_data = json.load(f)
        
        # The budget data structure is: {"budget_2025": {"AE Name": {"2025-01": value}}}
        budget_2025 = budget_data.get('budget_2025', {})
        
        # Get budget for this AE and month
        if ae_name in budget_2025:
            ae_budgets = budget_2025[ae_name]
            budget = ae_budgets.get(month, 0)
            return float(budget)
        
        return 0.0
    except Exception as e:
        print(f"ERROR loading budget for {ae_name}/{month}: {e}")
        return 0.0

def load_ae_config():
    """Load AE configuration from JSON file."""
    try:
        config_file = os.path.join(os.path.dirname(__file__), 'ae_config.json')
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config.get('ae_settings', {})
    except Exception as e:
        print(f"ERROR loading AE config: {e}")
        # Fallback to hardcoded values
        return {
            'Charmaine Lane': {'active': True, 'include_in_review': True, 'territory': 'North'},
            'House': {'active': True, 'include_in_review': True, 'territory': 'Central'},
            'Riley Van Patten': {'active': False, 'include_in_review': False, 'territory': 'South'},
            'White Horse International': {'active': True, 'include_in_review': False, 'territory': 'West'},
            'WorldLink': {'active': True, 'include_in_review': True, 'territory': 'General'}
        }

def get_territory_for_ae(ae_name):
    """Get territory for AE."""
    ae_config = load_ae_config()
    if ae_name in ae_config:
        return ae_config[ae_name].get('territory', 'General')
    else:
        return 'General'

def get_ae_query_condition(ae_name):
    """Get the appropriate SQL condition for querying AE data."""
    ae_config = load_ae_config()
    
    if ae_name in ae_config:
        ae_settings = ae_config[ae_name]
        if ae_settings.get('type') == 'agency':
            # For agency-based AEs, look for spots from that agency
            agency_name = ae_settings.get('agency_name', ae_name)
            return f"(agency_id = '{agency_name}' OR bill_code LIKE '{agency_name}:%')"
        else:
            # For regular AEs, look for spots by sales_person
            return f"sales_person = '{ae_name}'"
    else:
        # Default to sales_person lookup
        return f"sales_person = '{ae_name}'"

def is_ae_active(ae_name):
    """Check if AE is active."""
    ae_config = load_ae_config()
    if ae_name in ae_config:
        return ae_config[ae_name].get('active', True)
    else:
        return True

def is_ae_in_review(ae_name):
    """Check if AE should be included in biweekly reviews."""
    ae_config = load_ae_config()
    if ae_name in ae_config:
        return ae_config[ae_name].get('include_in_review', True)
    else:
        return True

@app.route('/')
def index():
    """Landing page."""
    return render_template('index.html', title="Revenue Reports Dashboard")

@app.route('/test')
def test_db():
    """Test database connection and show all AEs."""
    try:
        conn = get_db_connection()
        
        # Get total spots count
        cursor = conn.execute('SELECT COUNT(*) FROM spots')
        count = cursor.fetchone()[0]
        
        # Get all distinct AEs from database
        ae_query = """
        SELECT DISTINCT sales_person as ae_name,
               COUNT(*) as spot_count,
               ROUND(SUM(gross_rate), 2) as total_revenue
        FROM spots 
        WHERE sales_person IS NOT NULL 
        AND sales_person != ''
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY sales_person
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(ae_query)
        ae_results = cursor.fetchall()
        conn.close()
        
        # Format results
        result = f"Database connected successfully. Total spots: {count}<br><br>"
        result += "<strong>All AEs in Database:</strong><br>"
        for i, row in enumerate(ae_results, 1):
            ae_name = row['ae_name']
            spot_count = row['spot_count']
            total_revenue = row['total_revenue'] or 0
            result += f"{i}. {ae_name} - {spot_count} spots, ${total_revenue:,.2f}<br>"
        
        return result
    except Exception as e:
        return f"Database connection failed: {str(e)}"

@app.route('/report1')
def report1():
    """Monthly Revenue Report with date range filtering."""
    try:
        conn = get_db_connection()
        
        # Get date range parameters
        from_month = request.args.get('from_month', '')
        from_year = request.args.get('from_year', '')
        to_month = request.args.get('to_month', '')
        to_year = request.args.get('to_year', '')
        
        # Build date range conditions
        date_conditions = []
        
        if from_year and from_month:
            from_date = f"{from_year}-{from_month}"
            date_conditions.append(f"strftime('%Y-%m', broadcast_month) >= '{from_date}'")
        elif from_year:
            date_conditions.append(f"strftime('%Y', broadcast_month) >= '{from_year}'")
        elif from_month:
            date_conditions.append(f"strftime('%m', broadcast_month) >= '{from_month}'")
            
        if to_year and to_month:
            to_date = f"{to_year}-{to_month}"
            date_conditions.append(f"strftime('%Y-%m', broadcast_month) <= '{to_date}'")
        elif to_year:
            date_conditions.append(f"strftime('%Y', broadcast_month) <= '{to_year}'")
        elif to_month:
            date_conditions.append(f"strftime('%m', broadcast_month) <= '{to_month}'")
        
        # Add date conditions to WHERE clause
        date_filter = ""
        if date_conditions:
            date_filter = "AND " + " AND ".join(date_conditions)
        
        # Get available years for the form
        years_query = """
        SELECT DISTINCT strftime('%Y', broadcast_month) as year 
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        ORDER BY year DESC
        """
        
        cursor = conn.execute(years_query)
        available_years = [row[0] for row in cursor.fetchall()]
        
        # Modified query to use gross_rate OR spot_value to include 2025 data
        query = """
        SELECT 
            strftime('%Y-%m', broadcast_month) as month,
            CASE strftime('%m', broadcast_month)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
            END || ' ' || strftime('%Y', broadcast_month) as formatted_month,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate,
            ROUND(MIN(gross_rate), 2) as min_rate,
            ROUND(MAX(gross_rate), 2) as max_rate
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        ORDER BY month DESC
        LIMIT 24
        """
        
        cursor = conn.execute(query)
        results = cursor.fetchall()
        
        # Add quarterly data for report1
        quarterly_query = """
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
        ORDER BY year DESC, quarter DESC
        LIMIT 8
        """
        
        cursor = conn.execute(quarterly_query)
        quarterly_data = [dict(row) for row in cursor.fetchall()]
        
        # Add status to quarterly data based on actual month closures
        for quarter_row in quarterly_data:
            year = int(quarter_row['year'])
            quarter = quarter_row['quarter']
            
            # Extract quarter number from quarter string (Q1, Q2, Q3, Q4)
            quarter_num = int(quarter[1:])
            
            # Determine quarter status based on actual month closures
            quarter_row['status'] = get_quarter_status(year, quarter_num)
        
        conn.close()
        
        # Convert to list of dictionaries
        columns = ['month', 'formatted_month', 'spot_count', 'total_revenue', 'avg_rate', 'min_rate', 'max_rate']
        monthly_data = [dict(zip(columns, row)) for row in results]
        
        # Add month closure status to each monthly row
        for month_row in monthly_data:
            year_month = month_row['month']  # Format: 'YYYY-MM'
            if year_month:
                # Convert to database month format (e.g., 'Jan-24')
                year, month = year_month.split('-')
                month_names = {
                    '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
                    '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
                    '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
                }
                month_display = f"{month_names[month]}-{year[2:]}"
                
                # Check if month is closed
                try:
                    conn_check = get_db_connection()
                    cursor = conn_check.execute("""
                        SELECT 1 FROM month_closures 
                        WHERE broadcast_month = ?
                    """, (month_display,))
                    is_closed = cursor.fetchone() is not None
                    conn_check.close()
                    
                    month_row['status'] = 'CLOSED' if is_closed else 'OPEN'
                except Exception as e:
                    print(f"Error checking month closure status: {e}")
                    month_row['status'] = 'UNKNOWN'
            else:
                month_row['status'] = 'UNKNOWN'
        
        # Prepare data for template
        data = {
            'monthly_data': monthly_data,
            'quarterly_data': quarterly_data,
            'available_years': available_years,
            'filters': {
                'from_month': from_month,
                'from_year': from_year,
                'to_month': to_month,
                'to_year': to_year
            }
        }
        
        return render_template('report1.html', title="Monthly Revenue Report", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report2')
def report2():
    """Expectation vs. Reality Tracking Report."""
    try:
        conn = get_db_connection()
        
        # Modified query to use gross_rate OR spot_value to include 2025 data
        expectation_query = """
        SELECT 
            strftime('%Y-%m', broadcast_month) as month,
            CASE strftime('%m', broadcast_month)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
            END || ' ' || strftime('%Y', broadcast_month) as formatted_month,
            ROUND(SUM(gross_rate), 2) as actual_revenue,
            ROUND(SUM(gross_rate) * 1.1, 2) as expected_revenue,
            ROUND((SUM(gross_rate) / (SUM(gross_rate) * 1.1)) * 100, 1) as performance_pct
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        
        UNION ALL
        
        SELECT 
            '9999' as month,
            'TOTAL' as formatted_month,
            ROUND(SUM(gross_rate), 2) as actual_revenue,
            ROUND(SUM(gross_rate) * 1.1, 2) as expected_revenue,
            ROUND((SUM(gross_rate) / (SUM(gross_rate) * 1.1)) * 100, 1) as performance_pct
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        
        ORDER BY month DESC
        LIMIT 25
        """
        
        # Quarterly data for report2 template
        quarterly_query = """
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
        ORDER BY year DESC, quarter DESC
        LIMIT 8
        """
        
        # AE Performance Analysis - Modified to use gross_rate OR spot_value
        ae_query = """
        SELECT 
            sales_person,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        AND sales_person IS NOT NULL 
        AND sales_person != ''
        GROUP BY sales_person
        ORDER BY total_revenue DESC
        LIMIT 5
        """
        
        cursor = conn.execute(expectation_query)
        monthly_results = cursor.fetchall()
        
        cursor = conn.execute(quarterly_query)
        quarterly_data = [dict(row) for row in cursor.fetchall()]
        
        # Get available months to determine quarter completion
        months_query = """
        SELECT DISTINCT strftime('%Y-%m', broadcast_month) as month
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        ORDER BY month
        """
        cursor = conn.execute(months_query)
        available_months = {row[0] for row in cursor.fetchall()}
        
        cursor = conn.execute(ae_query)
        ae_performance = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Function to check if quarter is complete
        def is_quarter_complete(year, quarter):
            quarter_months = {
                'Q1': [f'{year}-01', f'{year}-02', f'{year}-03'],
                'Q2': [f'{year}-04', f'{year}-05', f'{year}-06'],
                'Q3': [f'{year}-07', f'{year}-08', f'{year}-09'],
                'Q4': [f'{year}-10', f'{year}-11', f'{year}-12']
            }
            return all(month in available_months for month in quarter_months[quarter])
        
        # Add status to quarterly data based on actual month closures
        for quarter_row in quarterly_data:
            year = int(quarter_row['year'])
            quarter = quarter_row['quarter']
            
            # Extract quarter number from quarter string (Q1, Q2, Q3, Q4)
            quarter_num = int(quarter[1:])
            
            # Determine quarter status based on actual month closures
            quarter_row['status'] = get_quarter_status(year, quarter_num)
        
        # Convert monthly results to list of dictionaries
        columns = ['month', 'formatted_month', 'actual_revenue', 'expected_revenue', 'performance_pct']
        monthly_data = [dict(zip(columns, row)) for row in monthly_results]
        
        data = {
            'monthly_data': monthly_data,
            'quarterly_data': quarterly_data,
            'ae_performance': ae_performance,
            'current_year': datetime.now().year
        }
        
        return render_template('report2.html', title="Expectation vs. Reality Tracking", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report3')
def report3():
    """Performance Story Report."""
    try:
        conn = get_db_connection()
        
        # Modified query to use only gross_rate and add filtering for gross_rate IS NOT NULL
        query = """
        SELECT 
            strftime('%Y-%m', broadcast_month) as month,
            CASE strftime('%m', broadcast_month)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
            END || ' ' || strftime('%Y', broadcast_month) as formatted_month,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as revenue,
            ROUND(AVG(gross_rate), 2) as avg_spot_value,
            LAG(ROUND(SUM(gross_rate), 2)) OVER (ORDER BY strftime('%Y-%m', broadcast_month)) as prev_month_revenue,
            ROUND(
                (ROUND(SUM(gross_rate), 2) - 
                 LAG(ROUND(SUM(gross_rate), 2)) OVER (ORDER BY strftime('%Y-%m', broadcast_month))) / 
                NULLIF(LAG(ROUND(SUM(gross_rate), 2)) OVER (ORDER BY strftime('%Y-%m', broadcast_month)), 0) * 100, 1
            ) as growth_pct
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        
        UNION ALL
        
        SELECT 
            'TOTAL' as month,
            'Performance Summary' as formatted_month,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as revenue,
            ROUND(AVG(gross_rate), 2) as avg_spot_value,
            NULL as prev_month_revenue,
            NULL as growth_pct
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        
        ORDER BY month DESC
        LIMIT 13
        """
        
        # Quarterly data for report3 template
        quarterly_query = """
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
        ORDER BY year DESC, quarter DESC
        LIMIT 8
        """
        
        # AE performance data
        ae_query = """
        SELECT 
            COALESCE(sales_person, 'Unassigned') as ae_name,
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
        LIMIT 10
        """
        
        cursor = conn.execute(query)
        monthly_results = cursor.fetchall()
        
        cursor = conn.execute(quarterly_query)
        quarterly_data = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(ae_query)
        ae_performance = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Convert monthly results to list of dictionaries
        columns = ['month', 'formatted_month', 'spot_count', 'revenue', 'avg_spot_value', 'prev_month_revenue', 'growth_pct']
        monthly_data = [dict(zip(columns, row)) for row in monthly_results]
        
        data = {
            'monthly_data': monthly_data,
            'quarterly_data': quarterly_data,
            'ae_performance': ae_performance,
            'current_year': datetime.now().year
        }
        
        return render_template('report3.html', title="Performance Story", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report4')
def report4():
    """Enhanced Sector Analysis Report - Two-Tiered Structure (Outreach/Commercial/Political)."""
    try:
        conn = get_db_connection()
        
        # Get selected year from query parameter, default to 2024
        selected_year = request.args.get('year', '2024')
        
        # Get available years
        years_query = """
        SELECT DISTINCT strftime('%Y', broadcast_month) as year 
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        ORDER BY year DESC
        """
        
        # Sector Group Overview - Primary tier (with year filter)
        sector_group_query = """
        SELECT 
            COALESCE(s.sector_group, 'UNASSIGNED') as sector_group,
            COUNT(DISTINCT c.customer_id) as customer_count,
            COUNT(sp.spot_id) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate,
            ROUND(SUM(sp.gross_rate) * 100.0 / (
                SELECT SUM(gross_rate) 
                FROM spots 
                WHERE gross_rate IS NOT NULL 
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND strftime('%Y', broadcast_month) = ?
            ), 2) as revenue_percentage
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        AND strftime('%Y', sp.broadcast_month) = ?
        GROUP BY s.sector_group
        ORDER BY total_revenue DESC
        """
        
        # Detailed sector breakdown - Secondary tier (with year filter)
        detailed_sector_query = """
        SELECT 
            COALESCE(s.sector_group, 'UNASSIGNED') as sector_group,
            COALESCE(s.sector_code, 'UNASSIGNED') as sector_code,
            COALESCE(s.sector_name, 'Unassigned') as sector_name,
            COUNT(DISTINCT c.customer_id) as customer_count,
            COUNT(sp.spot_id) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate,
            ROUND(SUM(sp.gross_rate) * 100.0 / (
                SELECT SUM(gross_rate) 
                FROM spots 
                WHERE gross_rate IS NOT NULL 
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND strftime('%Y', broadcast_month) = ?
            ), 2) as revenue_percentage
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        AND strftime('%Y', sp.broadcast_month) = ?
        GROUP BY s.sector_group, s.sector_id, s.sector_code, s.sector_name
        ORDER BY s.sector_group, total_revenue DESC
        """
        
        # Top customers by sector group (with year filter)
        top_customers_by_group_query = """
        SELECT 
            COALESCE(s.sector_group, 'UNASSIGNED') as sector_group,
            COALESCE(s.sector_name, 'Unassigned') as sector_name,
            c.normalized_name,
            COUNT(sp.spot_id) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        AND strftime('%Y', sp.broadcast_month) = ?
        GROUP BY s.sector_group, s.sector_name, c.customer_id, c.normalized_name
        ORDER BY s.sector_group, total_revenue DESC
        """
        
        # Sector assignment status (with year filter)
        sector_status_query = """
        WITH customer_revenue AS (
            SELECT 
                c.customer_id,
                c.sector_id,
                ROUND(SUM(COALESCE(sp.gross_rate, 0)), 2) as customer_revenue
            FROM customers c
            LEFT JOIN spots sp ON c.customer_id = sp.customer_id
                AND sp.gross_rate IS NOT NULL
                AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
                AND strftime('%Y', sp.broadcast_month) = ?
            GROUP BY c.customer_id, c.sector_id
        )
        SELECT 
            COUNT(CASE WHEN sector_id IS NOT NULL THEN 1 END) as assigned_customers,
            COUNT(*) as total_customers,
            ROUND(COUNT(CASE WHEN sector_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as assignment_percentage,
            ROUND(SUM(CASE WHEN sector_id IS NOT NULL THEN customer_revenue ELSE 0 END), 2) as assigned_revenue,
            ROUND(SUM(customer_revenue), 2) as total_revenue,
            ROUND(SUM(CASE WHEN sector_id IS NOT NULL THEN customer_revenue ELSE 0 END) * 100.0 / 
                  NULLIF(SUM(customer_revenue), 0), 1) as revenue_assignment_percentage
        FROM customer_revenue
        WHERE customer_revenue > 0
        """
        
        # Top unassigned customers (candidates for sector assignment, with year filter)
        unassigned_customers_query = """
        SELECT 
            c.normalized_name,
            COUNT(sp.spot_id) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate,
            MIN(sp.air_date) as first_spot_date,
            MAX(sp.air_date) as last_spot_date
        FROM customers c
        LEFT JOIN spots sp ON c.customer_id = sp.customer_id
            AND sp.gross_rate IS NOT NULL
            AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
            AND strftime('%Y', sp.broadcast_month) = ?
        WHERE c.sector_id IS NULL
        AND sp.spot_id IS NOT NULL
        GROUP BY c.customer_id, c.normalized_name
        ORDER BY total_revenue DESC
        LIMIT 15
        """
        
        # Quarterly data for template (with year filter)
        quarterly_query = """
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
        AND strftime('%Y', broadcast_month) = ?
        GROUP BY quarter, year
        ORDER BY year DESC, quarter DESC
        """
        
        # AE performance data (with year filter)
        ae_query = """
        SELECT 
            COALESCE(sales_person, 'Unassigned') as ae_name,
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
        AND strftime('%Y', broadcast_month) = ?
        GROUP BY sales_person
        ORDER BY total_revenue DESC
        LIMIT 10
        """
        
        # Execute queries
        cursor = conn.execute(years_query)
        available_years = [row[0] for row in cursor.fetchall()]
        
        cursor = conn.execute(sector_group_query, (selected_year, selected_year))
        sector_groups = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(detailed_sector_query, (selected_year, selected_year))
        detailed_sectors = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(top_customers_by_group_query, (selected_year,))
        all_customers_by_group = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(sector_status_query, (selected_year,))
        sector_status = dict(cursor.fetchone())
        
        cursor = conn.execute(unassigned_customers_query, (selected_year,))
        unassigned_customers = [dict(zip(['customer_name', 'spot_count', 'total_revenue', 'avg_rate', 'first_spot_date', 'last_spot_date'], row)) for row in cursor.fetchall()]
        
        cursor = conn.execute(quarterly_query, (selected_year,))
        quarterly_data = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(ae_query, (selected_year,))
        ae_performance = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Organize top customers by sector group (limit top 5 per group for display)
        customers_by_group = {}
        for customer in all_customers_by_group:
            group = customer['sector_group']
            if group not in customers_by_group:
                customers_by_group[group] = []
            if len(customers_by_group[group]) < 5:  # Limit to top 5 per group
                customers_by_group[group].append(customer)
        
        # Add status to quarterly data based on actual month closures
        for quarter_row in quarterly_data:
            year = int(quarter_row['year'])
            quarter = quarter_row['quarter']
            
            # Extract quarter number from quarter string (Q1, Q2, Q3, Q4)
            quarter_num = int(quarter[1:])
            
            # Determine quarter status based on actual month closures
            quarter_row['status'] = get_quarter_status(year, quarter_num)
        
        data = {
            'sector_groups': sector_groups,
            'detailed_sectors': detailed_sectors,
            'customers_by_group': customers_by_group,
            'sector_status': sector_status,
            'unassigned_customers': unassigned_customers,
            'quarterly_data': quarterly_data,
            'ae_performance': ae_performance,
            'available_years': available_years,
            'selected_year': selected_year,
            'current_year': datetime.now().year
        }
        
        return render_template('report4.html', title="Sector Analysis", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report5')
def report5():
    """Monthly Revenue Dashboard with year selection."""
    try:
        conn = get_db_connection()
        
        # Get selected year from query parameter, default to 2024
        selected_year = request.args.get('year', '2024')
        
        # Get available years - Modified to use only gross_rate data
        years_query = """
        SELECT DISTINCT substr(broadcast_month, 1, 4) as year 
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        ORDER BY year DESC
        """
        
        # Get AE list for the selected year - Modified filtering
        ae_query = """
        SELECT DISTINCT sp.sales_person 
        FROM spots sp
        WHERE sp.sales_person IS NOT NULL 
        AND sp.sales_person != ''
        AND sp.gross_rate IS NOT NULL
        AND strftime('%Y', sp.broadcast_month) = ?
        ORDER BY sp.sales_person
        """
        
        # Get revenue types for the selected year - Modified filtering
        revenue_types_query = """
        SELECT DISTINCT revenue_type 
        FROM spots 
        WHERE revenue_type IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND strftime('%Y', broadcast_month) = ?
        ORDER BY revenue_type
        """
        
        # Main dashboard query - Modified to use only gross_rate
        dashboard_query = """
        SELECT 
            c.normalized_name as customer,
            COALESCE(sp.sales_person, 'Unassigned') as ae,
            sp.revenue_type,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '01' THEN sp.gross_rate ELSE 0 END), 2) as month_1,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '02' THEN sp.gross_rate ELSE 0 END), 2) as month_2,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '03' THEN sp.gross_rate ELSE 0 END), 2) as month_3,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '04' THEN sp.gross_rate ELSE 0 END), 2) as month_4,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '05' THEN sp.gross_rate ELSE 0 END), 2) as month_5,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '06' THEN sp.gross_rate ELSE 0 END), 2) as month_6,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '07' THEN sp.gross_rate ELSE 0 END), 2) as month_7,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '08' THEN sp.gross_rate ELSE 0 END), 2) as month_8,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '09' THEN sp.gross_rate ELSE 0 END), 2) as month_9,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '10' THEN sp.gross_rate ELSE 0 END), 2) as month_10,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '11' THEN sp.gross_rate ELSE 0 END), 2) as month_11,
            ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '12' THEN sp.gross_rate ELSE 0 END), 2) as month_12,
            ROUND(SUM(sp.gross_rate), 2) as total
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        WHERE sp.broadcast_month IS NOT NULL 
        AND sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        AND strftime('%Y', sp.broadcast_month) = ?
        GROUP BY c.normalized_name, sp.sales_person, sp.revenue_type
        ORDER BY total DESC
        """
        
        cursor = conn.execute(years_query)
        available_years = [row[0] for row in cursor.fetchall()]
        
        cursor = conn.execute(ae_query, (selected_year,))
        ae_list = [row[0] for row in cursor.fetchall()]
        
        cursor = conn.execute(revenue_types_query, (selected_year,))
        revenue_types = [row[0] for row in cursor.fetchall()]
        
        cursor = conn.execute(dashboard_query, (selected_year,))
        revenue_data = [dict(row) for row in cursor.fetchall()]
        
        # Get month closure status for the selected year
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        year_suffix = str(selected_year)[2:]  # Get last 2 digits
        
        month_status = []
        for i, month_name in enumerate(month_names, 1):
            month_display = f"{month_name}-{year_suffix}"
            try:
                cursor = conn.execute("""
                    SELECT 1 FROM month_closures 
                    WHERE broadcast_month = ?
                """, (month_display,))
                is_closed = cursor.fetchone() is not None
                month_status.append({
                    'month_num': i,
                    'month_name': month_name,
                    'status': 'CLOSED' if is_closed else 'OPEN'
                })
            except Exception as e:
                print(f"Error checking closure status for {month_display}: {e}")
                month_status.append({
                    'month_num': i,
                    'month_name': month_name,
                    'status': 'UNKNOWN'
                })
        
        conn.close()
        
        # Calculate totals for dashboard
        total_customers = len(revenue_data)
        active_customers = len([r for r in revenue_data if r['total'] > 0])
        total_revenue = sum(r['total'] for r in revenue_data)
        avg_monthly_revenue = total_revenue / 12 if total_revenue > 0 else 0
        
        data = {
            'revenue_data': revenue_data,
            'ae_list': ae_list,
            'revenue_types': revenue_types,
            'available_years': available_years,
            'selected_year': selected_year,
            'total_customers': total_customers,
            'active_customers': active_customers,
            'total_revenue': total_revenue,
            'avg_monthly_revenue': avg_monthly_revenue,
            'current_year': datetime.now().year,
            'month_status': month_status
        }
        
        return render_template('report5.html', title="Monthly Revenue Dashboard", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report6')
def report6():
    """Language revenue analysis report."""
    try:
        conn = get_db_connection()
        
        # Get selected year from query parameter, default to 2024
        from flask import request
        selected_year = request.args.get('year', '2024')
        
        # Language revenue analysis - Modified to use COALESCE for revenue
        language_query = """
        SELECT 
            CASE s.language_code
                WHEN 'E' THEN 'English'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Tagalog'
                WHEN 'SA' THEN 'Spanish'
                WHEN 'C' THEN 'Chinese'
                WHEN 'K' THEN 'Korean'
                WHEN 'P' THEN 'Portuguese'
                WHEN 'M/C' THEN 'Mixed Content'
                WHEN 'Hm' THEN 'Hmong'
                WHEN 'J' THEN 'Japanese'
                WHEN 'H' THEN 'Hindi'
                ELSE COALESCE(s.language_code, 'Unknown')
            END as language_name,
            s.language_code,
            COUNT(*) as spot_count,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as total_revenue,
            ROUND(AVG(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as avg_rate,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)) * 100.0 / 
                (SELECT SUM(gross_rate) FROM spots 
                 WHERE (revenue_type != 'Trade' OR revenue_type IS NULL)
                 AND strftime('%Y', broadcast_month) = ?), 2) as market_share_pct
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.language_code IS NOT NULL
        AND strftime('%Y', s.broadcast_month) = ?
        GROUP BY s.language_code
        ORDER BY total_revenue DESC
        """
        
        # Monthly language trends - Modified to use COALESCE for revenue
        monthly_trends_query = """
        SELECT 
            strftime('%m', s.broadcast_month) as month_num,
            CASE strftime('%m', s.broadcast_month)
                WHEN '01' THEN 'Jan' WHEN '02' THEN 'Feb' WHEN '03' THEN 'Mar'
                WHEN '04' THEN 'Apr' WHEN '05' THEN 'May' WHEN '06' THEN 'Jun'
                WHEN '07' THEN 'Jul' WHEN '08' THEN 'Aug' WHEN '09' THEN 'Sep'
                WHEN '10' THEN 'Oct' WHEN '11' THEN 'Nov' WHEN '12' THEN 'Dec'
            END as month_name,
            CASE s.language_code
                WHEN 'E' THEN 'English'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Tagalog'
                WHEN 'SA' THEN 'Spanish'
                WHEN 'C' THEN 'Chinese'
                WHEN 'K' THEN 'Korean'
                ELSE 'Other'
            END as language_name,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as monthly_revenue
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.language_code IN ('E', 'V', 'T', 'SA', 'C', 'K')
        AND strftime('%Y', s.broadcast_month) = ?
        GROUP BY month_num, s.language_code
        ORDER BY month_num, monthly_revenue DESC
        """
        
        # Get available years - Modified to include both revenue sources
        years_query = """
        SELECT DISTINCT substr(broadcast_month, 1, 4) as year 
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        ORDER BY year DESC
        """
        
        cursor = conn.execute(language_query, (selected_year, selected_year))
        language_data = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(monthly_trends_query, (selected_year,))
        monthly_data = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(years_query)
        available_years = [row['year'] for row in cursor.fetchall()]
        
        conn.close()
        
        # Calculate totals and insights
        total_revenue = sum(lang['total_revenue'] for lang in language_data)
        total_spots = sum(lang['spot_count'] for lang in language_data)
        language_count = len(language_data)
        
        # Top language insights
        top_language = language_data[0] if language_data else None
        
        data = {
            'language_data': language_data,
            'monthly_trends': monthly_data,
            'available_years': available_years,
            'selected_year': selected_year,
            'total_revenue': total_revenue,
            'total_spots': total_spots,
            'language_count': language_count,
            'top_language': top_language,
            'current_year': datetime.now().year
        }
        
        return render_template('report6.html', title="Language Revenue Analysis", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/report7')
def report7():
    """Market-Language revenue analysis report."""
    try:
        conn = get_db_connection()
        
        # Get selected year from query parameter, default to 2024
        from flask import request
        selected_year = request.args.get('year', '2024')
        
        # Market-Language revenue analysis - Modified to use COALESCE for revenue
        market_language_query = """
        SELECT 
            COALESCE(s.market_name, 'Unknown Market') as market_name,
            CASE s.language_code
                WHEN 'E' THEN 'English'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Tagalog'
                WHEN 'SA' THEN 'Spanish'
                WHEN 'C' THEN 'Chinese'
                WHEN 'K' THEN 'Korean'
                WHEN 'P' THEN 'Portuguese'
                WHEN 'M/C' THEN 'Mixed Content'
                WHEN 'Hm' THEN 'Hmong'
                WHEN 'J' THEN 'Japanese'
                WHEN 'H' THEN 'Hindi'
                WHEN 'M' THEN 'Mandarin'
                ELSE COALESCE(s.language_code, 'Unknown')
            END as language_name,
            s.language_code,
            COUNT(*) as spot_count,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as total_revenue,
            ROUND(AVG(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as avg_rate,
            -- Market share within language
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)) * 100.0 / 
                (SELECT SUM(gross_rate) FROM spots 
                 WHERE language_code = s.language_code 
                 AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                 AND strftime('%Y', broadcast_month) = ?), 2) as market_share_within_language,
            -- Language share within market
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)) * 100.0 / 
                (SELECT SUM(gross_rate) FROM spots 
                 WHERE market_name = s.market_name 
                 AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                 AND strftime('%Y', broadcast_month) = ?), 2) as language_share_within_market
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.language_code IS NOT NULL
        AND s.market_name IS NOT NULL
        AND strftime('%Y', s.broadcast_month) = ?
        GROUP BY s.market_name, s.language_code
        ORDER BY s.market_name, total_revenue DESC
        """
        
        # Market summary - Modified to use COALESCE for revenue
        market_summary_query = """
        SELECT 
            s.market_name,
            COUNT(*) as total_spots,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as total_revenue,
            COUNT(DISTINCT s.language_code) as language_count,
            ROUND(AVG(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as avg_rate
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.market_name IS NOT NULL
        AND strftime('%Y', s.broadcast_month) = ?
        GROUP BY s.market_name
        ORDER BY total_revenue DESC
        """
        
        # Language summary - Modified to use COALESCE for revenue
        language_summary_query = """
        SELECT 
            CASE s.language_code
                WHEN 'E' THEN 'English'
                WHEN 'V' THEN 'Vietnamese'
                WHEN 'T' THEN 'Tagalog'
                WHEN 'SA' THEN 'Spanish'
                WHEN 'C' THEN 'Chinese'
                WHEN 'K' THEN 'Korean'
                WHEN 'P' THEN 'Portuguese'
                WHEN 'M/C' THEN 'Mixed Content'
                WHEN 'Hm' THEN 'Hmong'
                WHEN 'J' THEN 'Japanese'
                WHEN 'H' THEN 'Hindi'
                WHEN 'M' THEN 'Mandarin'
                ELSE COALESCE(s.language_code, 'Unknown')
            END as language_name,
            s.language_code,
            COUNT(*) as total_spots,
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as total_revenue,
            COUNT(DISTINCT s.market_name) as market_reach,
            ROUND(AVG(COALESCE(s.gross_rate, s.spot_value, 0)), 2) as avg_rate
        FROM spots s
        WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND s.language_code IS NOT NULL
        AND strftime('%Y', s.broadcast_month) = ?
        GROUP BY s.language_code
        ORDER BY total_revenue DESC
        """
        
        # Get available years - Modified to include both revenue sources
        years_query = """
        SELECT DISTINCT substr(broadcast_month, 1, 4) as year 
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        ORDER BY year DESC
        """
        
        cursor = conn.execute(market_language_query, (selected_year, selected_year, selected_year))
        market_language_data = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(market_summary_query, (selected_year,))
        market_summary = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(language_summary_query, (selected_year,))
        language_summary = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(years_query)
        available_years = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        # Calculate insights
        total_markets = len(market_summary)
        total_languages = len(language_summary)
        total_revenue = sum(market['total_revenue'] for market in market_summary)
        total_spots = sum(market['total_spots'] for market in market_summary)
        
        data = {
            'market_language_data': market_language_data,
            'market_summary': market_summary,
            'language_summary': language_summary,
            'available_years': available_years,
            'selected_year': selected_year,
            'total_markets': total_markets,
            'total_languages': total_languages,
            'total_revenue': total_revenue,
            'total_spots': total_spots,
            'current_year': datetime.now().year
        }
        
        return render_template('report7.html', title="Market-Language Revenue Analysis", data=data)
    except Exception as e:
        return f"Error generating report: {str(e)}", 500

@app.route('/api/data/<report_type>')
def api_data(report_type):
    """API endpoint for chart data."""
    try:
        conn = get_db_connection()
        
        if report_type == 'monthly':
            # Modified to use COALESCE for revenue
            query = """
            SELECT 
                strftime('%Y-%m', broadcast_month) as month,
                ROUND(SUM(gross_rate), 2) as revenue
            FROM spots 
            WHERE broadcast_month IS NOT NULL 
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY strftime('%Y-%m', broadcast_month)
            ORDER BY month DESC
            LIMIT 12
            """
            
        elif report_type == 'quarterly':
            # Modified to use COALESCE for revenue
            query = """
            SELECT 
                CASE 
                    WHEN strftime('%m', broadcast_month) IN ('01', '02', '03') THEN 'Q1'
                    WHEN strftime('%m', broadcast_month) IN ('04', '05', '06') THEN 'Q2'
                    WHEN strftime('%m', broadcast_month) IN ('07', '08', '09') THEN 'Q3'
                    WHEN strftime('%m', broadcast_month) IN ('10', '11', '12') THEN 'Q4'
                END as quarter,
                strftime('%Y', broadcast_month) as year,
                ROUND(SUM(gross_rate), 2) as revenue
            FROM spots 
            WHERE broadcast_month IS NOT NULL 
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY quarter, year
            ORDER BY year DESC, quarter DESC
            LIMIT 8
            """
            
        elif report_type == 'revenue_dashboard':
            # Get year parameter with modified filtering
            year = request.args.get('year', '2024')
            query = """
            SELECT 
                c.normalized_name as customer,
                COALESCE(sp.sales_person, 'Unassigned') as ae,
                sp.revenue_type,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '01' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_1,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '02' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_2,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '03' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_3,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '04' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_4,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '05' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_5,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '06' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_6,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '07' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_7,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '08' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_8,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '09' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_9,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '10' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_10,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '11' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_11,
                ROUND(SUM(CASE WHEN strftime('%m', sp.broadcast_month) = '12' THEN COALESCE(sp.gross_rate, sp.spot_value, 0) ELSE 0 END), 2) as month_12,
                ROUND(SUM(sp.gross_rate), 2) as total
            FROM spots sp
            LEFT JOIN customers c ON sp.customer_id = c.customer_id
            WHERE sp.broadcast_month IS NOT NULL 
            AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
            AND strftime('%Y', sp.broadcast_month) = ?
            GROUP BY c.normalized_name, sp.sales_person, sp.revenue_type
            ORDER BY total DESC
            """
            cursor = conn.execute(query, (year,))
        else:
            return jsonify([])
        
        if report_type in ['monthly', 'quarterly']:
            cursor = conn.execute(query)
        
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        column_names = [description[0] for description in cursor.description]
        data = [dict(zip(column_names, row)) for row in results]
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Sector Management Routes
@app.route('/sector-management')
def sector_management():
    """Sector management dashboard."""
    try:
        conn = get_db_connection()
        
        # Get filter parameters
        search_term = request.args.get('search', '').strip()
        sector_filter = request.args.get('sector', '')
        assignment_filter = request.args.get('assignment', '')  # 'assigned', 'unassigned', 'all'
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        
        # Get all sectors for dropdown
        sectors_query = """
        SELECT sector_id, sector_code, sector_name, sector_group
        FROM sectors 
        WHERE is_active = 1 
        ORDER BY sector_group, sector_code
        """
        cursor = conn.execute(sectors_query)
        sectors = [dict(row) for row in cursor.fetchall()]
        
        # Build customer query with filters
        customer_query = """
        SELECT 
            c.customer_id,
            c.normalized_name,
            c.sector_id,
            s.sector_code,
            s.sector_name,
            s.sector_group,
            COUNT(sp.spot_id) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate,
            MIN(sp.broadcast_month) as first_spot,
            MAX(sp.broadcast_month) as last_spot
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        LEFT JOIN spots sp ON c.customer_id = sp.customer_id 
            AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        WHERE c.is_active = 1
        """
        
        params = []
        
        # Apply filters
        if search_term:
            customer_query += " AND c.normalized_name LIKE ?"
            params.append(f"%{search_term}%")
        
        if sector_filter:
            customer_query += " AND s.sector_code = ?"
            params.append(sector_filter)
        
        if assignment_filter == 'assigned':
            customer_query += " AND c.sector_id IS NOT NULL"
        elif assignment_filter == 'unassigned':
            customer_query += " AND c.sector_id IS NULL"
        
        customer_query += """
        GROUP BY c.customer_id, c.normalized_name, c.sector_id, s.sector_code, s.sector_name, s.sector_group
        ORDER BY total_revenue DESC
        """
        
        # Add pagination
        offset = (page - 1) * per_page
        customer_query += f" LIMIT {per_page} OFFSET {offset}"
        
        cursor = conn.execute(customer_query, params)
        customers = [dict(row) for row in cursor.fetchall()]
        
        # Get total count for pagination
        count_query = """
        SELECT COUNT(DISTINCT c.customer_id) as total_count
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        LEFT JOIN spots sp ON c.customer_id = sp.customer_id 
            AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        WHERE c.is_active = 1
        """
        
        count_params = []
        if search_term:
            count_query += " AND c.normalized_name LIKE ?"
            count_params.append(f"%{search_term}%")
        
        if sector_filter:
            count_query += " AND s.sector_code = ?"
            count_params.append(sector_filter)
        
        if assignment_filter == 'assigned':
            count_query += " AND c.sector_id IS NOT NULL"
        elif assignment_filter == 'unassigned':
            count_query += " AND c.sector_id IS NULL"
        
        cursor = conn.execute(count_query, count_params)
        total_count = cursor.fetchone()[0]
        
        # Get summary statistics
        stats_query = """
        SELECT 
            COUNT(*) as total_customers,
            COUNT(sector_id) as assigned_customers,
            COUNT(*) - COUNT(sector_id) as unassigned_customers,
            ROUND(COUNT(sector_id) * 100.0 / COUNT(*), 1) as assignment_percentage
        FROM customers 
        WHERE is_active = 1
        """
        cursor = conn.execute(stats_query)
        stats = dict(cursor.fetchone())
        
        # Sector distribution
        sector_dist_query = """
        SELECT 
            COALESCE(s.sector_code, 'UNASSIGNED') as sector_code,
            COALESCE(s.sector_name, 'Unassigned') as sector_name,
            COALESCE(s.sector_group, 'UNASSIGNED') as sector_group,
            COUNT(c.customer_id) as customer_count,
            ROUND(COUNT(c.customer_id) * 100.0 / (
                SELECT COUNT(*) FROM customers WHERE is_active = 1
            ), 1) as percentage
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE c.is_active = 1
        GROUP BY s.sector_id, s.sector_code, s.sector_name, s.sector_group
        ORDER BY customer_count DESC
        """
        cursor = conn.execute(sector_dist_query)
        sector_distribution = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Calculate pagination info
        total_pages = (total_count + per_page - 1) // per_page
        has_prev = page > 1
        has_next = page < total_pages
        
        data = {
            'customers': customers,
            'sectors': sectors,
            'stats': stats,
            'sector_distribution': sector_distribution,
            'filters': {
                'search': search_term,
                'sector': sector_filter,
                'assignment': assignment_filter
            },
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_count': total_count,
                'total_pages': total_pages,
                'has_prev': has_prev,
                'has_next': has_next
            }
        }
        
        return render_template('sector_management.html', title="Sector Management", data=data)
    except Exception as e:
        return f"Error loading sector management: {str(e)}", 500

@app.route('/api/assign-sector', methods=['POST'])
def assign_sector():
    """API endpoint to assign a customer to a sector."""
    try:
        data = request.get_json()
        customer_id = data.get('customer_id')
        sector_id = data.get('sector_id')
        
        if not customer_id:
            return jsonify({'error': 'Customer ID is required'}), 400
        
        # Allow null sector_id for unassigning
        if sector_id == '':
            sector_id = None
        
        conn = get_db_connection()
        
        # Update customer sector
        cursor = conn.execute("""
            UPDATE customers 
            SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
            WHERE customer_id = ?
        """, (sector_id, customer_id))
        
        if cursor.rowcount == 0:
            conn.close()
            return jsonify({'error': 'Customer not found'}), 404
        
        conn.commit()
        
        # Get updated customer info
        cursor = conn.execute("""
            SELECT 
                c.customer_id,
                c.normalized_name,
                c.sector_id,
                s.sector_code,
                s.sector_name,
                s.sector_group
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE c.customer_id = ?
        """, (customer_id,))
        
        updated_customer = dict(cursor.fetchone())
        conn.close()
        
        return jsonify({
            'success': True,
            'customer': updated_customer,
            'message': f"Customer assigned to {updated_customer.get('sector_name', 'no sector')}"
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ===== PIPELINE REVENUE MANAGEMENT ROUTES =====

@app.route('/pipeline-revenue')
def pipeline_revenue_management():
    """Pipeline Revenue Management page."""
    try:
        # Get list of AEs from database first
        conn = get_db_connection()
        query = """
        SELECT DISTINCT sales_person as ae_name,
               COUNT(*) as spot_count,
               ROUND(SUM(gross_rate), 2) as total_revenue
        FROM spots 
        WHERE sales_person IS NOT NULL 
        AND sales_person != ''
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY sales_person
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(query)
        db_results = cursor.fetchall()
        conn.close()
        
        # Convert DB results to dict for easier lookup
        db_aes = {}
        for row in db_results:
            ae_name = row['ae_name']
            db_aes[ae_name] = {
                'spot_count': row['spot_count'],
                'total_revenue': row['total_revenue'] or 0
            }
        
        # Load AE configuration and create filtered list
        ae_config = load_ae_config()
        ae_list = []
        ae_id_counter = 1
        
        for ae_name, config in ae_config.items():
            # Only include AEs that are active AND should be in review
            if config.get('active', True) and config.get('include_in_review', True):
                ae_list.append({
                    'ae_id': f"AE{ae_id_counter:03d}",
                    'name': ae_name,
                    'territory': config.get('territory', 'General')
                })
                ae_id_counter += 1
        
        # Sort by name for consistent ordering
        ae_list.sort(key=lambda x: x['name'])
        
        # Create data structure expected by template
        # Load or create session data
        session_date = datetime.now().strftime('%Y-%m-%d')
        session_file = os.path.join(os.path.dirname(__file__), '../../data/processed/review_sessions.json')
        
        # Load existing session data if available
        session_data = {
            'session_date': session_date,
            'completed_aes': [],
            'session_notes': {}
        }
        
        if os.path.exists(session_file):
            try:
                with open(session_file, 'r') as f:
                    all_sessions = json.load(f)
                    if session_date in all_sessions:
                        existing_session = all_sessions[session_date]
                        session_data.update({
                            'completed_aes': existing_session.get('completed_aes', []),
                            'session_notes': existing_session.get('notes', {})
                        })
            except:
                pass
        
        data = {
            'session_date': datetime.now().strftime('%B %d, %Y'),
            'ae_list': ae_list,
            'session': session_data
        }
        
        return render_template('pipeline_revenue.html', 
                             title="Pipeline Revenue Management", 
                             data=data)
    except Exception as e:
        print(f"ERROR in pipeline_revenue_management: {e}")
        # Return minimal data structure to prevent template errors
        data = {
            'session_date': datetime.now().strftime('%B %d, %Y'),
            'ae_list': [],
            'session': {
                'session_date': datetime.now().strftime('%Y-%m-%d'),
                'completed_aes': [],
                'session_notes': {}
            }
        }
        return render_template('pipeline_revenue.html', 
                             title="Pipeline Revenue Management", 
                             data=data)

@app.route('/api/aes')
def get_aes():
    """Get list of Account Executives from database and config."""
    try:
        # Get AEs from database first
        conn = get_db_connection()
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
        db_results = cursor.fetchall()
        conn.close()
        
        # Convert DB results to dict for easier lookup
        db_aes = {}
        for row in db_results:
            ae_name = row['ae_name']
            db_aes[ae_name] = {
                'spot_count': row['spot_count'],
                'total_revenue': row['total_revenue'] or 0,
                'avg_rate': row['avg_rate'] or 0
            }
        
        # Load AE configuration and create combined list
        ae_config = load_ae_config()
        aes = []
        ae_id_counter = 1
        
        for ae_name, config in ae_config.items():
            # Only include AEs that are active AND should be in review
            if config.get('active', True) and config.get('include_in_review', True):
                # Get data from database if available
                db_data = db_aes.get(ae_name, {
                    'spot_count': 0,
                    'total_revenue': 0,
                    'avg_rate': 0
                })
                
                aes.append({
                    'ae_id': f"AE{ae_id_counter:03d}",
                    'name': ae_name,
                    'territory': config.get('territory', 'General'),
                    'ytd_actual': int(db_data['total_revenue']),
                    'avg_deal_size': int(db_data['avg_rate']),
                    'active': True
                })
                ae_id_counter += 1
        
        # Sort by total revenue descending
        aes.sort(key=lambda x: x['ytd_actual'], reverse=True)
        
        return jsonify({'success': True, 'data': aes})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ae/<ae_id>/summary')
def get_ae_summary(ae_id: str):
    """Get summary for specific AE."""
    try:
        # Get AE list from config (only active review AEs)
        ae_config = load_ae_config()
        review_aes = []
        for ae_name, config in ae_config.items():
            if config.get('active', True) and config.get('include_in_review', True):
                review_aes.append(ae_name)
        
        # Sort by name for consistent ordering
        review_aes.sort()
        
        # Map AE ID to name (AE001 = first AE, AE002 = second, etc.)
        ae_index = int(ae_id[2:]) - 1  # Convert AE002 to index 1
        if ae_index < 0 or ae_index >= len(review_aes):
            return jsonify({'success': False, 'error': 'AE not found'})
        
        ae_name = review_aes[ae_index]
        
        # Generate all 12 months for the current year
        current_date = datetime.now()
        current_year = current_date.year
        monthly_summary = []
        
        for month_num in range(1, 13):  # January (1) to December (12)
            target_month = month_num
            target_year = current_year
            
            month_str = f"{target_year}-{target_month:02d}"
            
            # Get revenue from database using appropriate query condition
            try:
                conn = get_db_connection()
                ae_condition = get_ae_query_condition(ae_name)
                hist_query = f"""
                SELECT ROUND(SUM(gross_rate), 2) as revenue
                FROM spots
                WHERE {ae_condition}
                AND strftime('%Y-%m', broadcast_month) = ?
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """
                
                cursor = conn.execute(hist_query, (month_str,))
                result = cursor.fetchone()
                booked_revenue = result['revenue'] if result and result['revenue'] else 0
                conn.close()
            except Exception as e:
                print(f"ERROR getting revenue for {ae_name}/{month_str}: {e}")
                booked_revenue = 0
            
            # Get budget
            budget = get_budget_for_ae_month(ae_name, month_str)
            
            # Load saved pipeline data if available
            import json
            import os
            
            pipeline_dir = '../../data/processed'
            pipeline_file = os.path.join(pipeline_dir, 'pipeline_data.json')
            
            saved_pipeline = None
            is_complete = False
            notes = ''
            last_updated = ''
            
            if os.path.exists(pipeline_file):
                try:
                    with open(pipeline_file, 'r') as f:
                        pipeline_data = json.load(f)
                        if ae_name in pipeline_data and month_str in pipeline_data[ae_name]:
                            saved_pipeline = pipeline_data[ae_name][month_str]
                            is_complete = saved_pipeline.get('is_complete', False)
                            notes = saved_pipeline.get('notes', '')
                            last_updated = saved_pipeline.get('last_updated', '')
                except:
                    pass
            
            # Use saved data if available, otherwise calculate defaults
            if saved_pipeline:
                current_pipeline = saved_pipeline.get('current_pipeline', budget * 0.6 if budget > 0 else 50000)
            else:
                current_pipeline = budget * 0.6 if budget > 0 else 50000
            
            # Calculate budget gap (removed pipeline gap since we don't use expected_pipeline)
            budget_gap = (booked_revenue + current_pipeline) - budget
            
            # Status simplified (removed pipeline-based status)
            status = 'on_track'
            
            # Determine if this is current month
            is_current_month = (target_month == current_date.month and target_year == current_date.year)
            is_future_month = (target_year > current_date.year or 
                             (target_year == current_date.year and target_month > current_date.month))
            
            month_summary = {
                'month': month_str,
                'month_display': datetime(target_year, target_month, 1).strftime('%B %Y'),
                'is_current_month': is_current_month,
                'is_future_month': is_future_month,
                'booked_revenue': float(booked_revenue),
                'current_pipeline': current_pipeline if is_current_month or is_future_month else 0,
                'budget': budget,
                'budget_gap': budget_gap,
                'pipeline_status': status,
                'notes': notes,
                'last_updated': last_updated,
                'is_complete': is_complete
            }
            
            monthly_summary.append(month_summary)
        
        # Calculate quarterly summaries
        quarterly_summary = []
        quarters = [
            {'name': 'Q1 2025', 'months': ['2025-01', '2025-02', '2025-03'], 'year': 2025, 'quarter': 1},
            {'name': 'Q2 2025', 'months': ['2025-04', '2025-05', '2025-06'], 'year': 2025, 'quarter': 2},
            {'name': 'Q3 2025', 'months': ['2025-07', '2025-08', '2025-09'], 'year': 2025, 'quarter': 3},
            {'name': 'Q4 2025', 'months': ['2025-10', '2025-11', '2025-12'], 'year': 2025, 'quarter': 4}
        ]
        
        for quarter in quarters:
            quarter_months = [m for m in monthly_summary if m['month'] in quarter['months']]
            if quarter_months:
                quarter_summary = {
                    'quarter_name': quarter['name'],
                    'year': quarter['year'],
                    'quarter_num': quarter['quarter'],
                    'months': quarter['months'],
                    'booked_revenue': sum(m['booked_revenue'] for m in quarter_months),
                    'current_pipeline': sum(m['current_pipeline'] for m in quarter_months if m['is_current_month'] or m['is_future_month']),
                    'budget': sum(m['budget'] for m in quarter_months),
                    'budget_gap': 0,  # Will calculate after
                    'is_complete': all(m.get('is_complete', False) for m in quarter_months if m['is_current_month'] or m['is_future_month']),
                    'month_count': len(quarter_months)
                }
                
                # Calculate quarter budget gap
                quarter_summary['budget_gap'] = (quarter_summary['booked_revenue'] + quarter_summary['current_pipeline']) - quarter_summary['budget']
                
                quarterly_summary.append(quarter_summary)
        
        # Calculate year-to-date totals
        total_booked_revenue = sum(month['booked_revenue'] for month in monthly_summary)
        total_budget = sum(month['budget'] for month in monthly_summary)
        total_pipeline = sum(month['current_pipeline'] for month in monthly_summary if month['is_current_month'] or month['is_future_month'])
        
        # Calculate attainment percentage
        attainment_percentage = (total_booked_revenue / total_budget * 100) if total_budget > 0 else 0
        
        # Calculate average deal size (from actual booked spots)
        try:
            conn = get_db_connection()
            ae_condition = get_ae_query_condition(ae_name)
            avg_deal_query = f"""
            SELECT ROUND(AVG(gross_rate), 0) as avg_deal_size
            FROM spots
            WHERE {ae_condition}
            AND strftime('%Y', broadcast_month) = ?
            AND gross_rate IS NOT NULL
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            cursor = conn.execute(avg_deal_query, (str(current_date.year),))
            result = cursor.fetchone()
            avg_deal_size = result['avg_deal_size'] if result and result['avg_deal_size'] else 0
            conn.close()
        except:
            avg_deal_size = 0

        response_data = {
            'ae_info': {
                'ae_id': ae_id,
                'name': ae_name,
                'territory': get_territory_for_ae(ae_name),
                'active': True,
                'total_revenue': total_booked_revenue,
                'total_budget': total_budget,
                'total_pipeline': total_pipeline,
                'attainment_percentage': attainment_percentage,
                'avg_deal_size': avg_deal_size
            },
            'monthly_summary': monthly_summary,
            'quarterly_summary': quarterly_summary
        }
        
        return jsonify({'success': True, 'data': response_data})
        
    except Exception as e:
        print(f"ERROR in get_ae_summary: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/pipeline/<ae_id>/<month>', methods=['PUT'])
def update_pipeline(ae_id: str, month: str):
    """Update pipeline data."""
    try:
        data = request.get_json()
        
        # Get AE name from AE ID
        conn = get_db_connection()
        query = """
        SELECT DISTINCT sales_person as ae_name
        FROM spots 
        WHERE sales_person IS NOT NULL AND sales_person != ''
        ORDER BY sales_person
        """
        cursor = conn.execute(query)
        all_aes = [row['ae_name'] for row in cursor.fetchall()]
        
        ae_index = int(ae_id[2:]) - 1
        if ae_index < 0 or ae_index >= len(all_aes):
            conn.close()
            return jsonify({'success': False, 'error': 'AE not found'})
        
        ae_name = all_aes[ae_index]
        
        # Save pipeline data to a JSON file
        import json
        import os
        from datetime import datetime
        
        pipeline_dir = '../../data/processed'
        pipeline_file = os.path.join(pipeline_dir, 'pipeline_data.json')
        
        # Create directory if it doesn't exist
        os.makedirs(pipeline_dir, exist_ok=True)
        
        # Load existing pipeline data
        pipeline_data = {}
        if os.path.exists(pipeline_file):
            try:
                with open(pipeline_file, 'r') as f:
                    pipeline_data = json.load(f)
            except:
                pipeline_data = {}
        
        # Create structure if needed
        if ae_name not in pipeline_data:
            pipeline_data[ae_name] = {}
        
        # Update the data (removed expected_pipeline)
        pipeline_data[ae_name][month] = {
            'current_pipeline': data.get('current_pipeline', 0),
            'notes': data.get('notes', ''),
            'last_updated': datetime.now().isoformat(),
            'is_complete': data.get('is_complete', False)
        }
        
        # Save back to file
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_data, f, indent=2)
        
        conn.close()
        return jsonify({'success': True, 'message': 'Pipeline updated successfully'})
        
    except Exception as e:
        print(f"ERROR in update_pipeline: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/customers/<ae_id>/<month>')
def get_customers_for_ae_month(ae_id: str, month: str):
    """Get customers for specific AE and month."""
    try:
        # Get AE list from config (only active review AEs)
        ae_config = load_ae_config()
        review_aes = []
        for ae_name, config in ae_config.items():
            if config.get('active', True) and config.get('include_in_review', True):
                review_aes.append(ae_name)
        
        # Sort by name for consistent ordering
        review_aes.sort()
        
        # Map AE ID to name (AE001 = first AE, AE002 = second, etc.)
        ae_index = int(ae_id[2:]) - 1  # Convert AE002 to index 1
        if ae_index < 0 or ae_index >= len(review_aes):
            return jsonify({'success': False, 'error': 'AE not found'})
        
        ae_name = review_aes[ae_index]
        
        # Get customers for this AE and month - JOIN with customers table using dynamic condition
        conn = get_db_connection()
        ae_condition = get_ae_query_condition(ae_name)
        customer_query = f"""
        SELECT 
            CASE 
                WHEN sp.agency_id IS NOT NULL AND sp.bill_code LIKE '%:%' 
                THEN SUBSTR(sp.bill_code, 1, INSTR(sp.bill_code, ':') - 1) || ':' || COALESCE(c.normalized_name, 'Unknown Customer')
                ELSE COALESCE(c.normalized_name, 'Unknown Customer')
            END as customer_name,
            COUNT(*) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate,
            MIN(sp.broadcast_month) as first_spot,
            MAX(sp.broadcast_month) as last_spot
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        WHERE ({ae_condition.replace('agency_id', 'sp.agency_id').replace('bill_code', 'sp.bill_code').replace('sales_person', 'sp.sales_person')})
        AND strftime('%Y-%m', sp.broadcast_month) = ?
        AND sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        AND c.normalized_name IS NOT NULL
        GROUP BY 
            CASE 
                WHEN sp.agency_id IS NOT NULL AND sp.bill_code LIKE '%:%' 
                THEN SUBSTR(sp.bill_code, 1, INSTR(sp.bill_code, ':') - 1) || ':' || COALESCE(c.normalized_name, 'Unknown Customer')
                ELSE COALESCE(c.normalized_name, 'Unknown Customer')
            END, 
            sp.customer_id
        ORDER BY total_revenue DESC
        """
        
        cursor = conn.execute(customer_query, (month,))
        customers = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return jsonify({
            'success': True, 
            'data': {
                'ae_name': ae_name,
                'month': month,
                'customers': customers,
                'total_customers': len(customers),
                'total_revenue': sum(c['total_revenue'] or 0 for c in customers)
            }
        })
        
    except Exception as e:
        print(f"ERROR in get_customers_for_ae_month: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/pipeline/<ae_id>/<month>/complete', methods=['POST'])
def mark_pipeline_complete(ae_id: str, month: str):
    """Mark a pipeline month as complete."""
    try:
        # Get AE list from config (only active review AEs)
        ae_config = load_ae_config()
        review_aes = []
        for ae_name, config in ae_config.items():
            if config.get('active', True) and config.get('include_in_review', True):
                review_aes.append(ae_name)
        
        # Sort by name for consistent ordering
        review_aes.sort()
        
        ae_index = int(ae_id[2:]) - 1
        if ae_index < 0 or ae_index >= len(review_aes):
            return jsonify({'success': False, 'error': 'AE not found'})
        
        ae_name = review_aes[ae_index]
        
        # Save completion to pipeline data
        import json
        import os
        from datetime import datetime
        
        pipeline_dir = '../../data/processed'
        pipeline_file = os.path.join(pipeline_dir, 'pipeline_data.json')
        
        # Create directory if it doesn't exist
        os.makedirs(pipeline_dir, exist_ok=True)
        
        # Load existing pipeline data
        pipeline_data = {}
        if os.path.exists(pipeline_file):
            try:
                with open(pipeline_file, 'r') as f:
                    pipeline_data = json.load(f)
            except:
                pipeline_data = {}
        
        # Create structure if needed
        if ae_name not in pipeline_data:
            pipeline_data[ae_name] = {}
        if month not in pipeline_data[ae_name]:
            pipeline_data[ae_name][month] = {}
        
        # Mark as complete
        pipeline_data[ae_name][month]['is_complete'] = True
        pipeline_data[ae_name][month]['completed_date'] = datetime.now().isoformat()
        pipeline_data[ae_name][month]['last_updated'] = datetime.now().isoformat()
        
        # Save back to file
        with open(pipeline_file, 'w') as f:
            json.dump(pipeline_data, f, indent=2)
        
        return jsonify({'success': True, 'message': 'Pipeline marked as complete'})
        
    except Exception as e:
        print(f"ERROR in mark_pipeline_complete: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/review-session', methods=['POST'])
def update_review_session():
    """Update review session completion status and notes."""
    try:
        # Parse JSON with detailed error handling
        try:
            data = request.get_json(force=True)
            if data is None:
                data = {}
                print("Received empty JSON, using defaults")
        except Exception as json_error:
            print(f"JSON parsing error: {json_error}")
            print(f"Raw request data: {request.get_data()}")
            return jsonify({
                'success': False, 
                'error': f'Invalid JSON format: {str(json_error)}'
            }), 400
        
        # Extract data with safe defaults
        session_date = data.get('session_date', datetime.now().strftime('%Y-%m-%d'))
        completed_aes = data.get('completed_aes', [])
        notes = data.get('notes', {})
        
        # Validate data types
        if not isinstance(completed_aes, list):
            completed_aes = []
        if not isinstance(notes, dict):
            notes = {}
        
        print(f"✅ Processing review session update:")
        print(f"   Session date: {session_date}")
        print(f"   Completed AEs: {completed_aes}")
        print(f"   Notes: {notes}")
        
        # Save session data to file
        session_file = os.path.join(os.path.dirname(__file__), '../../data/processed/review_sessions.json')
        
        session_data = {
            'session_date': session_date,
            'completed_aes': completed_aes,
            'notes': notes,
            'last_updated': datetime.now().isoformat()
        }
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(session_file), exist_ok=True)
        
        # Load existing sessions
        existing_sessions = {}
        if os.path.exists(session_file):
            try:
                with open(session_file, 'r') as f:
                    existing_sessions = json.load(f)
            except Exception as load_error:
                print(f"⚠️  Could not load existing sessions: {load_error}")
                existing_sessions = {}
        
        # Update session for this date
        existing_sessions[session_date] = session_data
        
        # Save updated sessions
        try:
            with open(session_file, 'w') as f:
                json.dump(existing_sessions, f, indent=2)
            print(f"✅ Successfully saved review session to {session_file}")
        except Exception as save_error:
            print(f"❌ Error saving session file: {save_error}")
            return jsonify({
                'success': False, 
                'error': f'Failed to save session data: {str(save_error)}'
            }), 500
        
        return jsonify({
            'success': True,
            'message': 'Review session updated successfully',
            'data': {
                'session_date': session_date,
                'completed_count': len(completed_aes),
                'notes_count': len(notes)
            }
        })
        
    except Exception as e:
        print(f"❌ Unexpected error in update_review_session: {e}")
        return jsonify({
            'success': False, 
            'error': f'Server error: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 