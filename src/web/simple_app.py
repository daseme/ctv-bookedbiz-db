"""Simplified Flask web application for testing."""

from flask import Flask, render_template, jsonify, request
import sqlite3
import os
from datetime import datetime

app = Flask(__name__)

# Database path
DB_PATH = '../../data/database/test.db'

def get_db_connection():
    """Get database connection."""
    db_path = os.path.join(os.path.dirname(__file__), DB_PATH)
    print(f"Database path: {db_path}")
    print(f"Database exists: {os.path.exists(db_path)}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/')
def index():
    """Landing page."""
    return render_template('index.html', title="Revenue Reports Dashboard")

@app.route('/test')
def test_db():
    """Test database connection."""
    try:
        conn = get_db_connection()
        cursor = conn.execute('SELECT COUNT(*) FROM spots')
        count = cursor.fetchone()[0]
        conn.close()
        return f"Database connected successfully. Total spots: {count}"
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
        AND (gross_rate > 0 OR spot_value > 0)
        ORDER BY year DESC
        """
        
        cursor = conn.execute(years_query)
        available_years = [row[0] for row in cursor.fetchall()]
        
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
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        ORDER BY month DESC
        LIMIT 12
        """
        
        cursor = conn.execute(query)
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        columns = ['month', 'formatted_month', 'spot_count', 'total_revenue', 'avg_rate']
        monthly_data = [dict(zip(columns, row)) for row in results]
        
        # Prepare data for template
        data = {
            'monthly_data': monthly_data,
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
        
        # Modified query to use only gross_rate and add filtering for gross_rate IS NOT NULL
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
        LIMIT 13
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
        
        # AE Performance Analysis - Modified to use only gross_rate
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
        
        # Add status to quarterly data based on completion
        for quarter_row in quarterly_data:
            year = int(quarter_row['year'])
            quarter = quarter_row['quarter']
            current_year = datetime.now().year
            
            if year < current_year:
                # Past years are always closed
                quarter_row['status'] = 'CLOSED'
            elif year == current_year:
                if is_quarter_complete(year, quarter):
                    quarter_row['status'] = 'CLOSED'
                else:
                    quarter_row['status'] = 'OPEN'
            else:
                # Future years are open
                quarter_row['status'] = 'OPEN'
        
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
    """Sector Analysis Report."""
    try:
        conn = get_db_connection()
        
        # Modified query to use only gross_rate and add filtering for gross_rate IS NOT NULL
        query = """
        SELECT 
            CASE 
                WHEN strftime('%m', sp.broadcast_month) IN ('01', '02', '03') THEN 'Q1'
                WHEN strftime('%m', sp.broadcast_month) IN ('04', '05', '06') THEN 'Q2'
                WHEN strftime('%m', sp.broadcast_month) IN ('07', '08', '09') THEN 'Q3'
                WHEN strftime('%m', sp.broadcast_month) IN ('10', '11', '12') THEN 'Q4'
            END as quarter,
            strftime('%Y', sp.broadcast_month) as year,
            COALESCE(sp.billing_type, 'Unknown') as sector,
            COUNT(*) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate
        FROM spots sp
        WHERE sp.broadcast_month IS NOT NULL 
        AND sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        GROUP BY quarter, year, sp.billing_type
        
        UNION ALL
        
        SELECT 
            'TOTAL' as quarter,
            'ALL' as year,
            'All Sectors' as sector,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        
        ORDER BY year DESC, quarter DESC, total_revenue DESC
        LIMIT 21
        """
        
        # Quarterly data for template
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
        
        # Get months present in database to determine quarter completion status
        months_query = """
        SELECT DISTINCT strftime('%Y-%m', broadcast_month) as month
        FROM spots 
        WHERE broadcast_month IS NOT NULL 
        AND gross_rate IS NOT NULL
        ORDER BY month
        """
        
        # Sector summary data using customers as sectors
        sector_query = """
        SELECT 
            COALESCE(c.normalized_name, 'Unknown Customer') as sector_name,
            COUNT(*) as spot_count,
            ROUND(SUM(sp.gross_rate), 2) as total_revenue,
            ROUND(AVG(sp.gross_rate), 2) as avg_rate
        FROM spots sp
        LEFT JOIN customers c ON sp.customer_id = c.customer_id
        WHERE sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        GROUP BY c.customer_id, c.normalized_name
        ORDER BY total_revenue DESC
        LIMIT 10
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
        sector_results = cursor.fetchall()
        
        cursor = conn.execute(quarterly_query)
        quarterly_data = [dict(row) for row in cursor.fetchall()]
        
        # Get available months to determine quarter completion
        cursor = conn.execute(months_query)
        available_months = {row[0] for row in cursor.fetchall()}
        
        cursor = conn.execute(sector_query)
        sectors = [dict(row) for row in cursor.fetchall()]
        
        cursor = conn.execute(ae_query)
        ae_performance = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        
        # Convert sector results to list of dictionaries
        columns = ['quarter', 'year', 'sector', 'spot_count', 'total_revenue', 'avg_rate']
        sector_breakdown = [dict(zip(columns, row)) for row in sector_results]
        
        # Function to check if quarter is complete
        def is_quarter_complete(year, quarter):
            quarter_months = {
                'Q1': [f'{year}-01', f'{year}-02', f'{year}-03'],
                'Q2': [f'{year}-04', f'{year}-05', f'{year}-06'],
                'Q3': [f'{year}-07', f'{year}-08', f'{year}-09'],
                'Q4': [f'{year}-10', f'{year}-11', f'{year}-12']
            }
            return all(month in available_months for month in quarter_months[quarter])
        
        # Add status to quarterly data based on completion
        for quarter_row in quarterly_data:
            year = int(quarter_row['year'])
            quarter = quarter_row['quarter']
            current_year = datetime.now().year
            
            if year < current_year:
                # Past years are always closed
                quarter_row['status'] = 'CLOSED'
            elif year == current_year:
                if is_quarter_complete(year, quarter):
                    quarter_row['status'] = 'CLOSED'
                else:
                    quarter_row['status'] = 'OPEN'
            else:
                # Future years are open
                quarter_row['status'] = 'OPEN'
        
        data = {
            'sector_breakdown': sector_breakdown,
            'quarterly_data': quarterly_data,
            'sectors': sectors,
            'ae_performance': ae_performance,
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
            'current_year': datetime.now().year
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
                (SELECT SUM(COALESCE(gross_rate, spot_value, 0)) FROM spots 
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
                (SELECT SUM(COALESCE(gross_rate, spot_value, 0)) FROM spots 
                 WHERE language_code = s.language_code 
                 AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                 AND strftime('%Y', broadcast_month) = ?), 2) as market_share_within_language,
            -- Language share within market
            ROUND(SUM(COALESCE(s.gross_rate, s.spot_value, 0)) * 100.0 / 
                (SELECT SUM(COALESCE(gross_rate, spot_value, 0)) FROM spots 
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
                ROUND(SUM(COALESCE(gross_rate, spot_value, 0)), 2) as revenue
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
                ROUND(SUM(COALESCE(gross_rate, spot_value, 0)), 2) as revenue
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
                ROUND(SUM(COALESCE(sp.gross_rate, sp.spot_value, 0)), 2) as total
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

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 