"""SQLite implementations of repositories."""

import sqlite3
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from decimal import Decimal
import logging
from dataclasses import asdict

from .interfaces import SpotRepository, CustomerRepository
from ..models.entities import Spot, Customer, Agency, Market, Language, Sector
from ..database.connection import DatabaseConnection

logger = logging.getLogger(__name__)

class SQLiteSpotRepository(SpotRepository):
    """SQLite implementation of spot repository."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def save(self, spot: Spot) -> Spot:
        """Save a spot and return it with assigned ID."""
        with self.db.transaction() as conn:
            if spot.spot_id is None:
                # Insert new spot
                spot_dict = self._spot_to_dict(spot)
                columns = ', '.join(spot_dict.keys())
                placeholders = ', '.join(['?' for _ in spot_dict.keys()])
                
                query = f"""
                INSERT INTO spots ({columns})
                VALUES ({placeholders})
                """
                
                cursor = conn.execute(query, list(spot_dict.values()))
                spot.spot_id = cursor.lastrowid
                spot.load_date = datetime.now()
                
                logger.info(f"Created new spot with ID {spot.spot_id}")
            else:
                # Update existing spot
                spot_dict = self._spot_to_dict(spot)
                set_clause = ', '.join([f"{k} = ?" for k in spot_dict.keys() if k != 'spot_id'])
                values = [v for k, v in spot_dict.items() if k != 'spot_id']
                values.append(spot.spot_id)
                
                query = f"""
                UPDATE spots 
                SET {set_clause}
                WHERE spot_id = ?
                """
                
                conn.execute(query, values)
                logger.info(f"Updated spot with ID {spot.spot_id}")
            
            return spot
    
    # ... rest of SQLiteSpotRepository methods from your file ...

class SQLiteCustomerRepository(CustomerRepository):
    """SQLite implementation of customer repository."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    # ... all the SQLiteCustomerRepository methods from your file ...

class ReferenceDataRepository:
    """Repository for reference data (markets, sectors, languages, agencies)."""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    # ... all the ReferenceDataRepository methods from your file ...