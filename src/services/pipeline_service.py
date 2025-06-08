"""Pipeline Revenue Management Service."""

import json
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, date
from decimal import Decimal
import sqlite3
import logging

logger = logging.getLogger(__name__)


class PipelineService:
    """Service class for pipeline revenue management."""
    
    def __init__(self, db_connection, data_path: str):
        """Initialize with database connection and data file path."""
        self.db_connection = db_connection
        self.data_path = data_path
        self.pipeline_file = os.path.join(data_path, 'pipeline_data.json')
        self.budget_file = os.path.join(data_path, 'budget_data.json')
        self.session_file = os.path.join(data_path, 'review_sessions.json')
    
    def _load_json_file(self, filepath: str) -> Dict[str, Any]:
        """Load and parse JSON file."""
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"File not found: {filepath}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {filepath}: {e}")
            return {}
    
    def _save_json_file(self, filepath: str, data: Dict[str, Any]) -> bool:
        """Save data to JSON file."""
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            return True
        except Exception as e:
            logger.error(f"Error saving {filepath}: {e}")
            return False
    
    def get_ae_list(self) -> List[Dict[str, Any]]:
        """Get list of all Account Executives."""
        session_data = self._load_json_file(self.session_file)
        return session_data.get('ae_list', [])
    
    def get_ae_by_id(self, ae_id: str) -> Optional[Dict[str, Any]]:
        """Get specific AE by ID."""
        ae_list = self.get_ae_list()
        return next((ae for ae in ae_list if ae['ae_id'] == ae_id), None)
    
    def get_historical_revenue(self, ae_id: str, start_month: str, end_month: str) -> List[Dict[str, Any]]:
        """Get historical revenue from database by AE and date range."""
        conn = self.db_connection.connect()
        
        query = """
        SELECT 
            strftime('%Y-%m', broadcast_month) as month,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as total_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots
        WHERE sales_person = ?
        AND broadcast_month BETWEEN ? AND ?
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        ORDER BY month
        """
        
        cursor = conn.execute(query, (ae_id, start_month, end_month))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_booked_revenue(self, ae_id: str, start_month: str, end_month: str) -> List[Dict[str, Any]]:
        """Get booked (future confirmed) revenue from database."""
        conn = self.db_connection.connect()
        
        # For now, we'll simulate booked revenue as future spots with contracts
        # This would need to be adjusted based on actual database schema
        query = """
        SELECT 
            strftime('%Y-%m', broadcast_month) as month,
            COUNT(*) as spot_count,
            ROUND(SUM(gross_rate), 2) as booked_revenue,
            ROUND(AVG(gross_rate), 2) as avg_rate
        FROM spots
        WHERE sales_person = ?
        AND broadcast_month BETWEEN ? AND ?
        AND broadcast_month > date('now')
        AND gross_rate IS NOT NULL
        AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        GROUP BY strftime('%Y-%m', broadcast_month)
        ORDER BY month
        """
        
        cursor = conn.execute(query, (ae_id, start_month, end_month))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_customer_details(self, ae_id: str, month: str) -> List[Dict[str, Any]]:
        """Get customer deal details for specific AE and month."""
        conn = self.db_connection.connect()
        
        query = """
        SELECT 
            c.normalized_name as customer_name,
            'Revenue Spot' as deal_description,
            sp.gross_rate as amount,
            sp.air_date as expected_close_date,
            CASE 
                WHEN sp.broadcast_month <= date('now') THEN 'closed_won'
                WHEN sp.broadcast_month > date('now') THEN 'committed'
                ELSE 'pipeline'
            END as status,
            1.0 as probability
        FROM spots sp
        JOIN customers c ON sp.customer_id = c.customer_id
        WHERE sp.sales_person = ?
        AND strftime('%Y-%m', sp.broadcast_month) = ?
        AND sp.gross_rate IS NOT NULL
        AND (sp.revenue_type != 'Trade' OR sp.revenue_type IS NULL)
        ORDER BY sp.gross_rate DESC
        """
        
        cursor = conn.execute(query, (ae_id, month))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_pipeline_data(self, ae_id: str, period: str) -> List[Dict[str, Any]]:
        """Get pipeline data for AE and period from JSON storage."""
        pipeline_data = self._load_json_file(self.pipeline_file)
        data = pipeline_data.get('pipeline_data', [])
        
        if period:
            # Filter by specific month
            return [item for item in data if item.get('ae_id') == ae_id and item.get('month') == period]
        else:
            # Return all data for AE
            return [item for item in data if item.get('ae_id') == ae_id]
    
    def get_budget_data(self, ae_id: str, period: str) -> List[Dict[str, Any]]:
        """Get budget data for AE and period from JSON storage."""
        budget_data = self._load_json_file(self.budget_file)
        data = budget_data.get('budget_data', [])
        
        if period:
            return [item for item in data if item.get('ae_id') == ae_id and item.get('month') == period]
        else:
            return [item for item in data if item.get('ae_id') == ae_id]
    
    def update_pipeline(self, ae_id: str, month: str, current_pipeline: float, notes: str = "", updated_by: str = "system") -> bool:
        """Update pipeline data in JSON storage."""
        pipeline_data = self._load_json_file(self.pipeline_file)
        data = pipeline_data.get('pipeline_data', [])
        
        # Find existing entry or create new one
        updated = False
        for item in data:
            if item.get('ae_id') == ae_id and item.get('month') == month:
                item['current_pipeline'] = current_pipeline
                item['notes'] = notes
                item['last_updated'] = datetime.now().isoformat()
                item['updated_by'] = updated_by
                updated = True
                break
        
        if not updated:
            # Create new entry
            new_entry = {
                'ae_id': ae_id,
                'month': month,
                'current_pipeline': current_pipeline,
                'expected_pipeline': 0,  # Would need to be set separately
                'last_updated': datetime.now().isoformat(),
                'updated_by': updated_by,
                'notes': notes
            }
            data.append(new_entry)
        
        pipeline_data['pipeline_data'] = data
        return self._save_json_file(self.pipeline_file, pipeline_data)
    
    def get_monthly_summary(self, ae_id: str, months_ahead: int = 6) -> List[Dict[str, Any]]:
        """Get comprehensive monthly summary for AE including all revenue types."""
        # Generate month list
        current_date = datetime.now()
        months = []
        
        for i in range(months_ahead):
            # Calculate the target month
            target_month = current_date.month + i
            target_year = current_date.year
            
            # Handle year rollover
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            month_date = date(target_year, target_month, 1)
            months.append(month_date.strftime('%Y-%m'))
        
        summary = []
        for month in months:
            # Get pipeline data
            pipeline = self.get_pipeline_data(ae_id, month)
            pipeline_item = pipeline[0] if pipeline else {}
            
            # Get budget data
            budget = self.get_budget_data(ae_id, month)
            budget_item = budget[0] if budget else {}
            
            # Get booked revenue (from database)
            booked = self.get_booked_revenue(ae_id, month, month)
            booked_amount = booked[0]['booked_revenue'] if booked else 0
            
            # Calculate gaps
            current_pipeline = pipeline_item.get('current_pipeline', 0)
            expected_pipeline = pipeline_item.get('expected_pipeline', 0)
            budget_amount = budget_item.get('budget', 0)
            
            pipeline_gap = current_pipeline - expected_pipeline
            budget_gap = (booked_amount + current_pipeline) - budget_amount
            
            month_summary = {
                'month': month,
                'month_display': datetime.strptime(month, '%Y-%m').strftime('%B %Y'),
                'is_current_month': month == current_date.strftime('%Y-%m'),
                'booked_revenue': booked_amount,
                'current_pipeline': current_pipeline,
                'expected_pipeline': expected_pipeline,
                'budget': budget_amount,
                'pipeline_gap': pipeline_gap,
                'budget_gap': budget_gap,
                'pipeline_status': self._get_pipeline_status(pipeline_gap),
                'notes': pipeline_item.get('notes', ''),
                'last_updated': pipeline_item.get('last_updated', '')
            }
            
            summary.append(month_summary)
        
        return summary
    
    def _get_pipeline_status(self, gap: float) -> str:
        """Determine pipeline status based on gap."""
        if gap >= 0:
            return 'ahead'
        elif gap >= -10000:  # Within 10k
            return 'on_track' 
        else:
            return 'behind'
    
    def get_review_session(self, session_date: str) -> Dict[str, Any]:
        """Get or create review session data."""
        session_data = self._load_json_file(self.session_file)
        sessions = session_data.get('review_sessions', [])
        
        # Find existing session
        session = next((s for s in sessions if s['session_id'] == session_date), None)
        
        if not session:
            # Create new session
            session = {
                'session_id': session_date,
                'review_date': session_date,
                'completed_aes': [],
                'session_notes': {},
                'total_aes': len(session_data.get('ae_list', [])),
                'session_started': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat()
            }
            sessions.append(session)
            session_data['review_sessions'] = sessions
            self._save_json_file(self.session_file, session_data)
        
        return session
    
    def update_review_session(self, session_date: str, completed_aes: List[str], notes: Dict[str, str]) -> bool:
        """Update review session completion status and notes."""
        session_data = self._load_json_file(self.session_file)
        sessions = session_data.get('review_sessions', [])
        
        for session in sessions:
            if session['session_id'] == session_date:
                session['completed_aes'] = completed_aes
                session['session_notes'] = notes
                session['last_updated'] = datetime.now().isoformat()
                break
        
        session_data['review_sessions'] = sessions
        return self._save_json_file(self.session_file, session_data) 