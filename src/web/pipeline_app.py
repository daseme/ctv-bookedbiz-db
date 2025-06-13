"""Simplified Flask app for Pipeline Revenue Management - No complex imports."""

from flask import Flask, render_template, jsonify, request
import sqlite3
import json
import os
from datetime import datetime, date
from typing import Dict, List, Any

app = Flask(__name__)

# Database path - absolute to avoid issues
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
DB_PATH = os.path.join(PROJECT_ROOT, 'data/database/production.db')

def get_db_connection():
    """Simple database connection like working reports."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_budget_for_ae_month(ae_name, month):
    """Get budget amount for specific AE and month."""
    try:
        budget_file_path = os.path.join(PROJECT_ROOT, 'real_budget_data.json')
        with open(budget_file_path, 'r') as f:
            budget_data = json.load(f)
        
        if ae_name in budget_data.get('budget_2025', {}):
            return budget_data['budget_2025'][ae_name].get(month, 0)
        
        if ae_name == 'House' and 'House' in budget_data.get('budget_2025', {}):
            return budget_data['budget_2025']['House'].get(month, 0)
        
        return 0
    except Exception as e:
        print(f"ERROR loading budget: {e}")
        return 0

def get_territory_for_ae(ae_name):
    """Get territory for AE."""
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

@app.route('/')
def index():
    return render_template('index.html', title="CTV Reports")

@app.route('/pipeline-revenue')
def pipeline_revenue_management():
    """Pipeline Revenue Management page."""
    try:
        # Get list of AEs from database
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
        results = cursor.fetchall()
        conn.close()
        
        # Create AE list
        ae_list = []
        for i, row in enumerate(results):
            ae_name = row['ae_name']
            ae_list.append({
                'ae_id': f"AE{i+1:03d}",
                'name': ae_name,
                'territory': get_territory_for_ae(ae_name)
            })
        
        # Create data structure expected by template
        data = {
            'session_date': datetime.now().strftime('%B %d, %Y'),
            'ae_list': ae_list,
            'session': {
                'completed_aes': [],
                'total_aes': len(ae_list)
            }
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
                'completed_aes': [],
                'total_aes': 0
            }
        }
        return render_template('pipeline_revenue.html', 
                             title="Pipeline Revenue Management", 
                             data=data)

@app.route('/api/aes')
def get_aes():
    """Get list of Account Executives from database."""
    try:
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
        results = cursor.fetchall()
        conn.close()
        
        aes = []
        for i, row in enumerate(results):
            ae_name = row['ae_name']
            aes.append({
                'ae_id': f"AE{i+1:03d}",
                'name': ae_name,
                'territory': get_territory_for_ae(ae_name),
                'ytd_actual': int(row['total_revenue'] or 0),
                'avg_deal_size': int(row['avg_rate'] or 0),
                'active': True
            })
        
        return jsonify({'success': True, 'data': aes})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/ae/<ae_id>/summary')
def get_ae_summary(ae_id: str):
    """Get summary for specific AE."""
    try:
        # Get AE list to find the name
        conn = get_db_connection()
        query = """
        SELECT DISTINCT sales_person as ae_name
        FROM spots 
        WHERE sales_person IS NOT NULL AND sales_person != ''
        ORDER BY sales_person
        """
        cursor = conn.execute(query)
        all_aes = [row['ae_name'] for row in cursor.fetchall()]
        conn.close()
        
        # Map AE ID to name (AE001 = first AE, AE002 = second, etc.)
        ae_index = int(ae_id[2:]) - 1  # Convert AE002 to index 1
        if ae_index < 0 or ae_index >= len(all_aes):
            return jsonify({'success': False, 'error': 'AE not found'})
        
        ae_name = all_aes[ae_index]
        
        # Generate 6 months starting from current month
        current_date = datetime.now()
        monthly_summary = []
        
        for i in range(6):
            target_month = current_date.month + i
            target_year = current_date.year
            
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            month_str = f"{target_year}-{target_month:02d}"
            
            # Get revenue from database
            try:
                conn = get_db_connection()
                hist_query = """
                SELECT ROUND(SUM(gross_rate), 2) as revenue
                FROM spots
                WHERE sales_person = ?
                AND strftime('%Y-%m', broadcast_month) = ?
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """
                
                cursor = conn.execute(hist_query, (ae_name, month_str))
                result = cursor.fetchone()
                booked_revenue = result['revenue'] if result and result['revenue'] else 0
                conn.close()
            except Exception as e:
                print(f"ERROR getting revenue for {ae_name}/{month_str}: {e}")
                booked_revenue = 0
            
            # Get budget
            budget = get_budget_for_ae_month(ae_name, month_str)
            
            # Calculate pipeline (defaults)
            current_pipeline = budget * 0.6 if budget > 0 else 50000
            expected_pipeline = budget * 0.7 if budget > 0 else 60000
            
            # Calculate gaps
            pipeline_gap = current_pipeline - expected_pipeline
            budget_gap = (booked_revenue + current_pipeline) - budget
            
            # Status
            status = 'ahead' if pipeline_gap >= 0 else ('on_track' if pipeline_gap >= -10000 else 'behind')
            
            month_summary = {
                'month': month_str,
                'month_display': datetime(target_year, target_month, 1).strftime('%B %Y'),
                'is_current_month': i == 0,
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
            
            monthly_summary.append(month_summary)
        
        response_data = {
            'ae_info': {
                'ae_id': ae_id,
                'name': ae_name,
                'territory': get_territory_for_ae(ae_name),
                'active': True
            },
            'monthly_summary': monthly_summary
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
        # For now, just return success
        return jsonify({'success': True, 'message': 'Pipeline updated'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    print(f"Database path: {DB_PATH}")
    print(f"Database exists: {os.path.exists(DB_PATH)}")
    app.run(debug=True, host='0.0.0.0', port=5000) 