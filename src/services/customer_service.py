"""Customer Service - Handles customer deal operations."""

import sqlite3
from typing import Dict, List, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class CustomerService:
    """Service for customer deal operations."""
    
    def __init__(self, db_path: str):
        """Initialize with database path."""
        self.db_path = db_path
    
    def _get_db_connection(self):
        """Get database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _format_month_for_query(self, year_month: str) -> str:
        """Convert YYYY-MM format to broadcast month date pattern for LIKE query."""
        try:
            year, month = year_month.split('-')
            # The broadcast_month is stored as datetime like '2025-01-15 00:00:00'
            # So we'll search for pattern '2025-01%'
            return f"{year}-{month}%"
        except:
            logger.error(f"Error converting month format: {year_month}")
            return f"{year_month}%"
    
    def _is_historical_month(self, broadcast_month_str: str) -> bool:
        """Determine if broadcast month is historical (past) or future."""
        try:
            # Parse the datetime string like '2024-01-15 00:00:00'
            broadcast_date = datetime.strptime(broadcast_month_str.split(' ')[0], '%Y-%m-%d')
            current_date = datetime.now()
            
            return broadcast_date < current_date
        except:
            logger.error(f"Error parsing broadcast month: {broadcast_month_str}")
            return False
    
    def get_customer_deals(self, ae_name: str, month: str) -> List[Dict[str, Any]]:
        """Get customer deal details for specific AE and month.
        
        Args:
            ae_name: AE name (e.g., 'Charmaine Lane', 'House', 'WorldLink')
            month: Year-month format (e.g., '2025-01')
        """
        conn = self._get_db_connection()
        month_pattern = self._format_month_for_query(month)
        
        try:
            if ae_name == 'WorldLink':
                # WorldLink is an agency - look up by agency relationship
                query = """
                SELECT 
                    c.normalized_name as customer_name,
                    CASE 
                        WHEN s.program IS NOT NULL THEN s.program || ' Spot'
                        ELSE 'Revenue Spot'
                    END as deal_description,
                    s.gross_rate as amount,
                    s.air_date as expected_close_date,
                    s.broadcast_month,
                    s.is_historical,
                    s.spot_type,
                    s.length_seconds,
                    a.agency_name
                FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id  
                JOIN agencies a ON s.agency_id = a.agency_id
                WHERE a.agency_name = 'WorldLink'
                AND s.broadcast_month LIKE ?
                AND s.gross_rate IS NOT NULL
                AND s.gross_rate > 0
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                ORDER BY s.gross_rate DESC
                """
                cursor = conn.execute(query, (month_pattern,))
            else:
                # Regular AE lookup by sales_person field
                query = """
                SELECT 
                    c.normalized_name as customer_name,
                    CASE 
                        WHEN s.program IS NOT NULL THEN s.program || ' Spot'
                        ELSE 'Revenue Spot'
                    END as deal_description,
                    s.gross_rate as amount,
                    s.air_date as expected_close_date,
                    s.broadcast_month,
                    s.is_historical,
                    s.spot_type,
                    s.length_seconds,
                    COALESCE(a.agency_name, 'Direct') as agency_name
                FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id
                LEFT JOIN agencies a ON s.agency_id = a.agency_id
                WHERE s.sales_person = ?
                AND s.broadcast_month LIKE ?
                AND s.gross_rate IS NOT NULL  
                AND s.gross_rate > 0
                AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                ORDER BY s.gross_rate DESC
                """
                cursor = conn.execute(query, (ae_name, month_pattern))
            
            results = []
            for row in cursor.fetchall():
                row_dict = dict(row)
                
                # Determine status based on is_historical flag and broadcast month
                is_past = self._is_historical_month(row_dict['broadcast_month']) or row_dict['is_historical']
                row_dict['status'] = 'closed_won' if is_past else 'pipeline'
                row_dict['probability'] = 1.0 if is_past else 0.8
                
                results.append(row_dict)
            
            logger.info(f"Found {len(results)} deals for {ae_name} in {month} (pattern: {month_pattern})")
            return results
            
        except Exception as e:
            logger.error(f"Error getting customer deals for {ae_name}, {month}: {e}")
            return []
        finally:
            conn.close()
    
    def categorize_deals(self, deals: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Categorize deals into booked vs pipeline and aggregate by customer.
        
        In this revenue system:
        - Booked: Actual aired spots with confirmed revenue
        - Pipeline: Expected revenue gap (Budget - Booked) tracked during reviews
        
        For now, we'll treat all deals as booked since they represent actual spots.
        Pipeline calculation should be done at the budget/target level.
        """
        # All deals from spots table are booked revenue (actual aired spots)
        booked_deals = deals
        pipeline_deals = []  # Pipeline is calculated separately from budget targets
        
        # Aggregate deals by customer for display
        def aggregate_by_customer(deal_list):
            customer_aggregates = {}
            for deal in deal_list:
                customer_name = deal['customer_name']
                if customer_name not in customer_aggregates:
                    customer_aggregates[customer_name] = {
                        'customer_name': customer_name,
                        'spot_count': 0,
                        'total_revenue': 0.0,
                        'first_spot': None,
                        'status': deal['status'],
                        'deals': []
                    }
                
                customer_aggregates[customer_name]['spot_count'] += 1
                customer_aggregates[customer_name]['total_revenue'] += float(deal['amount']) if deal['amount'] else 0
                customer_aggregates[customer_name]['deals'].append(deal)
                
                # Track earliest air date as first spot
                if deal['expected_close_date']:
                    if (customer_aggregates[customer_name]['first_spot'] is None or 
                        deal['expected_close_date'] < customer_aggregates[customer_name]['first_spot']):
                        customer_aggregates[customer_name]['first_spot'] = deal['expected_close_date']
            
            # Convert to list and sort by revenue descending
            result = list(customer_aggregates.values())
            result.sort(key=lambda x: x['total_revenue'], reverse=True)
            return result
        
        booked_customers = aggregate_by_customer(booked_deals)
        pipeline_customers = aggregate_by_customer(pipeline_deals)
        all_customers = aggregate_by_customer(deals)
        
        booked_total = sum(float(d['amount']) for d in booked_deals if d['amount'])
        pipeline_total = sum(float(d['amount']) for d in pipeline_deals if d['amount'])
        
        return {
            'booked_deals': booked_customers,
            'pipeline_deals': pipeline_customers,
            'all_deals': all_customers,
            'totals': {
                'booked_total': round(booked_total, 2),
                'pipeline_total': round(pipeline_total, 2),
                'total': round(booked_total + pipeline_total, 2)
            },
            'counts': {
                'booked_count': len(booked_customers),
                'pipeline_count': len(pipeline_customers),
                'total_count': len(all_customers)
            }
        } 