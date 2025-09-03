"""SQLite implementations of repositories with sector assignment capabilities."""

import sqlite3
from typing import Optional, List, Dict, Any, Tuple
from datetime import date, datetime
from decimal import Decimal
import logging
from dataclasses import asdict

from .interfaces import SpotRepository, CustomerRepository
from src.models.entities import Spot, Customer, Agency, Market, Language, Sector
from src.models.sector_assignment import (
    CustomerRevenueInfo, AssignmentResult, AssignmentMethod
)
from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)

class SQLiteSpotRepository(SpotRepository, BaseService):
    """SQLite implementation of spot repository."""
    
    def __init__(self, db_connection: DatabaseConnection):
        BaseService.__init__(self, db_connection)
    
    def save(self, spot: Spot) -> Spot:
        """Save a spot and return it with assigned ID."""
        with self.safe_transaction() as conn:
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
    
    def find_by_id(self, spot_id: int) -> Optional[Spot]:
        """Find a spot by its ID."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM spots WHERE spot_id = ?", (spot_id,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_spot(row)
            return None
    
    def find_by_customer_and_date_range(self, customer_id: int, start_date: date, end_date: date) -> List[Spot]:
        """Find spots for a customer within a date range using broadcast_month."""
        with self.safe_connection() as conn:
            query = """
            SELECT * FROM spots 
            WHERE customer_id = ? AND broadcast_month BETWEEN ? AND ?
            ORDER BY broadcast_month
            """
            cursor = conn.execute(query, (customer_id, start_date, end_date))
            
            return [self._row_to_spot(row) for row in cursor.fetchall()]
    
    def get_revenue_by_ae_and_month(self, ae_name: str, year: int, month: int) -> Decimal:
        """Get total revenue for an AE in a specific month using broadcast_month."""
        with self.safe_connection() as conn:
            query = """
            SELECT COALESCE(SUM(gross_rate), 0) as total_revenue
            FROM spots 
            WHERE sales_person = ? 
            AND strftime('%Y', broadcast_month) = ? 
            AND strftime('%m', broadcast_month) = ?
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """
            
            cursor = conn.execute(query, (ae_name, str(year), f"{month:02d}"))
            result = cursor.fetchone()
            
            return Decimal(str(result['total_revenue'])) if result['total_revenue'] else Decimal('0')
    
    def delete_future_data(self, cutoff_date: date) -> int:
        """Delete non-historical data after cutoff date using broadcast_month."""
        with self.safe_transaction() as conn:
            query = """
            DELETE FROM spots 
            WHERE broadcast_month > ? AND is_historical = 0
            """
            
            cursor = conn.execute(query, (cutoff_date,))
            deleted_count = cursor.rowcount
            
            logger.info(f"Deleted {deleted_count} future spots after {cutoff_date}")
            return deleted_count
    
    def get_spots_for_reporting(self, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get spots with all related data for reporting (uses the view)."""
        with self.safe_connection() as conn:
        
            query = "SELECT * FROM spots_reporting"
            params = []
            
            if filters:
                conditions = []
                for key, value in filters.items():
                    if value is not None:
                        conditions.append(f"{key} = ?")
                        params.append(value)
                
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY broadcast_month"
            
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]
    
    def _spot_to_dict(self, spot: Spot) -> Dict[str, Any]:
        """Convert Spot object to dictionary for database operations."""
        spot_dict = asdict(spot)
        # Remove None spot_id for inserts
        if spot_dict.get('spot_id') is None:
            spot_dict.pop('spot_id', None)
        return spot_dict
    
    def _row_to_spot(self, row: sqlite3.Row) -> Spot:
        """Convert database row to Spot object."""
        return Spot(
            spot_id=row['spot_id'],
            bill_code=row['bill_code'],
            air_date=date.fromisoformat(row['air_date']) if row['air_date'] else None,
            end_date=date.fromisoformat(row['end_date']) if row['end_date'] else None,
            day_of_week=row['day_of_week'],
            time_in=row['time_in'],
            time_out=row['time_out'],
            length_seconds=row['length_seconds'],
            media=row['media'],
            comments=row['comments'],  # was program=row['program']
            language_code=row['language_code'],
            format=row['format'],
            sequence_number=row['sequence_number'],
            line_number=row['line_number'],
            spot_type=row['spot_type'],
            estimate=row['estimate'],
            gross_rate=Decimal(str(row['gross_rate'])) if row['gross_rate'] else None,
            make_good=row['make_good'],
            spot_value=Decimal(str(row['spot_value'])) if row['spot_value'] else None,
            broadcast_month=row['broadcast_month'],
            broker_fees=Decimal(str(row['broker_fees'])) if row['broker_fees'] else None,
            priority=row['priority'],
            station_net=Decimal(str(row['station_net'])) if row['station_net'] else None,
            sales_person=row['sales_person'],
            revenue_type=row['revenue_type'],
            billing_type=row['billing_type'],
            agency_flag=row['agency_flag'],
            affidavit_flag=row['affidavit_flag'],
            contract=row['contract'],
            market_name=row['market_name'],
            customer_id=row['customer_id'],
            agency_id=row['agency_id'],
            market_id=row['market_id'],
            language_id=row['language_id'],
            load_date=datetime.fromisoformat(row['load_date']) if row['load_date'] else None,
            source_file=row['source_file'],
            is_historical=bool(row['is_historical']),
            effective_date=date.fromisoformat(row['effective_date']) if row['effective_date'] else None
        )

class SQLiteCustomerRepository(CustomerRepository, BaseService):
    """SQLite implementation of customer repository with sector assignment capabilities."""
    
    def __init__(self, db_connection: DatabaseConnection):
        BaseService.__init__(self, db_connection)
    
    def save(self, customer: Customer) -> Customer:
        """Save a customer and return it with assigned ID."""
        with self.safe_transaction() as conn:
            if customer.customer_id is None:
                # Insert new customer
                query = """
                INSERT INTO customers (normalized_name, sector_id, agency_id, customer_type, notes, is_active)
                VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor = conn.execute(query, (
                    customer.normalized_name,
                    customer.sector_id,
                    customer.agency_id,
                    customer.customer_type,
                    customer.notes,
                    customer.is_active
                ))
                customer.customer_id = cursor.lastrowid
                customer.created_date = datetime.now()
                
                logger.info(f"Created new customer: {customer.normalized_name} (ID: {customer.customer_id})")
            else:
                # Update existing customer
                query = """
                UPDATE customers 
                SET normalized_name = ?, sector_id = ?, agency_id = ?, 
                    customer_type = ?, notes = ?, is_active = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
                """
                conn.execute(query, (
                    customer.normalized_name,
                    customer.sector_id,
                    customer.agency_id,
                    customer.customer_type,
                    customer.notes,
                    customer.is_active,
                    customer.customer_id
                ))
                customer.updated_date = datetime.now()
                
                logger.info(f"Updated customer: {customer.normalized_name} (ID: {customer.customer_id})")
            
            return customer
    
    def find_by_normalized_name(self, normalized_name: str) -> Optional[Customer]:
        """Find a customer by normalized name."""
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE normalized_name = ?"
            cursor = conn.execute(query, (normalized_name,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_customer(row)
            return None
    
    def find_by_id(self, customer_id: int) -> Optional[Customer]:
        """Find a customer by ID."""
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE customer_id = ?"
            cursor = conn.execute(query, (customer_id,))
            row = cursor.fetchone()
            
            if row:
                return self._row_to_customer(row)
            return None
    
    def find_similar_customers(self, name: str, threshold: float = 0.8) -> List[Tuple[Customer, float]]:
        """Find customers with similar names above threshold."""
        # Basic similarity implementation - in production use proper fuzzy matching
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE normalized_name LIKE ? AND is_active = 1"
            # Simple wildcard matching - replace with proper fuzzy matching in production
            cursor = conn.execute(query, (f"%{name}%",))
            
            results = []
            for row in cursor.fetchall():
                customer = self._row_to_customer(row)
                # Simple similarity score - replace with proper algorithm (Levenshtein, etc.)
                similarity = self._calculate_simple_similarity(name, customer.normalized_name)
                if similarity >= threshold:
                    results.append((customer, similarity))
            
            return sorted(results, key=lambda x: x[1], reverse=True)
    
    def get_all_customers(self) -> List[Customer]:
        """Get all active customers."""
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE is_active = 1 ORDER BY normalized_name"
            cursor = conn.execute(query)
            
            return [self._row_to_customer(row) for row in cursor.fetchall()]
    
    def find_customers_by_sector(self, sector_id: int) -> List[Customer]:
        """Find all customers in a specific sector."""
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE sector_id = ? AND is_active = 1 ORDER BY normalized_name"
            cursor = conn.execute(query, (sector_id,))
            
            return [self._row_to_customer(row) for row in cursor.fetchall()]
    
    def find_customers_by_agency(self, agency_id: int) -> List[Customer]:
        """Find all customers associated with a specific agency."""
        with self.safe_connection() as conn:
            query = "SELECT * FROM customers WHERE agency_id = ? AND is_active = 1 ORDER BY normalized_name"
            cursor = conn.execute(query, (agency_id,))
            
            return [self._row_to_customer(row) for row in cursor.fetchall()]
    
    # ===================================================================
    # SECTOR ASSIGNMENT METHODS
    # ===================================================================
    
    def get_unassigned_customers(self, limit: int = 50, offset: int = 0) -> List[Customer]:
        """Get customers without sector assignments."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    c.customer_id,
                    c.normalized_name,
                    c.agency_id,
                    a.agency_name,
                    s.sector_id,
                    s.sector_name,
                    GROUP_CONCAT(DISTINCT sp.bill_code) as sample_bill_codes
                FROM customers c
                LEFT JOIN agencies a ON c.agency_id = a.agency_id
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                LEFT JOIN spots sp ON c.customer_id = sp.customer_id
                WHERE c.sector_id IS NULL
                AND c.is_active = 1
                GROUP BY c.customer_id, c.normalized_name, c.agency_id, a.agency_name
                ORDER BY c.normalized_name
                LIMIT ? OFFSET ?
            """, (limit, offset))
            
            customers = []
            
            for row in cursor.fetchall():
                # Extract agency from bill codes if not in FK
                agency_name = row['agency_name']
                if not agency_name and row['sample_bill_codes']:
                    agency_name = self._extract_agency_from_bill_codes(row['sample_bill_codes'])
                
                customer = self._row_to_customer(row)
                # Add the extracted agency_name if we found one
                if agency_name and not customer.agency_id:
                    customer.notes = f"Agency detected from bill codes: {agency_name}"
                
                customers.append(customer)
            
            return customers
    
    def assign_sector_to_customer(self, customer_id: int, sector_id: int, 
                                method: AssignmentMethod, assigned_by: str) -> AssignmentResult:
        """Assign sector to customer with audit trail."""
        try:
            # Get current sector for audit
            old_sector_id = None
            with self.safe_connection() as conn:
                cursor = conn.execute("SELECT sector_id FROM customers WHERE customer_id = ?", (customer_id,))
                row = cursor.fetchone()
                if row:
                    old_sector_id = row['sector_id']
            
            with self.safe_transaction() as conn:
                # Update customer sector
                cursor = conn.execute("""
                    UPDATE customers 
                    SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                    WHERE customer_id = ? AND is_active = 1
                """, (sector_id, customer_id))
                
                if cursor.rowcount == 0:
                    return AssignmentResult(
                        customer_id=customer_id,
                        sector_id=sector_id,
                        success=False,
                        error_message="Customer not found or inactive",
                        method=method,
                        assigned_by=assigned_by
                    )
                
                # Create audit trail entry
                self._create_audit_entry(conn, customer_id, old_sector_id, sector_id, method, assigned_by)
                
                logger.info(f"Assigned sector {sector_id} to customer {customer_id} via {method.value}")
                
                return AssignmentResult(
                    customer_id=customer_id,
                    sector_id=sector_id,
                    success=True,
                    method=method,
                    assigned_by=assigned_by
                )
                
        except Exception as e:
            logger.error(f"Error assigning sector to customer {customer_id}: {e}")
            return AssignmentResult(
                customer_id=customer_id,
                sector_id=sector_id,
                success=False,
                error_message=str(e),
                method=method,
                assigned_by=assigned_by
            )
    
    def get_customer_revenue_info(self, customer_id: int) -> CustomerRevenueInfo:
        """Get customer revenue context for sector assignment decisions."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_spots,
                    COALESCE(SUM(gross_rate), 0) as total_revenue,
                    COUNT(DISTINCT broadcast_month) as months_active,
                    MIN(air_date) as first_spot,
                    MAX(air_date) as last_spot,
                    MAX(broadcast_month) as last_broadcast_month
                FROM spots 
                WHERE customer_id = ? 
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            """, (customer_id,))
            
            row = cursor.fetchone()
            
            return CustomerRevenueInfo(
                total_spots=row['total_spots'],
                total_revenue=float(row['total_revenue']),
                months_active=row['months_active'],
                first_spot=row['first_spot'],
                last_spot=row['last_spot'],
                last_broadcast_month=row['last_broadcast_month']
            )
    
    def _extract_agency_from_bill_codes(self, bill_codes_string: str) -> Optional[str]:
        """Extract agency from bill code patterns."""
        if not bill_codes_string:
            return None
        
        bill_codes = [bc.strip() for bc in bill_codes_string.split(',')]
        
        for bill_code in bill_codes[:5]:  # Check first few
            if ':' in bill_code:
                parts = bill_code.split(':')
                if len(parts) >= 2:
                    potential_agency = parts[0].strip()
                    if 3 <= len(potential_agency) <= 30:
                        return potential_agency
        
        return None
    
    def _create_audit_entry(self, conn: sqlite3.Connection, customer_id: int, 
                          old_sector_id: Optional[int], new_sector_id: int, 
                          method: AssignmentMethod, assigned_by: str) -> None:
        """Create audit trail entry for sector assignment."""
        # Note: You'll need to create this table if it doesn't exist
        conn.execute("""
            INSERT OR IGNORE INTO sector_assignment_audit 
            (customer_id, old_sector_id, new_sector_id, assignment_method, assigned_by, assigned_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (customer_id, old_sector_id, new_sector_id, method.value, assigned_by, datetime.now().isoformat()))
    
    def _calculate_simple_similarity(self, name1: str, name2: str) -> float:
        """Calculate simple similarity score between two names."""
        # Simple implementation - in production use proper similarity algorithms
        name1_lower = name1.lower().strip()
        name2_lower = name2.lower().strip()
        
        if name1_lower == name2_lower:
            return 1.0
        
        if name1_lower in name2_lower or name2_lower in name1_lower:
            # Substring match - score based on length ratio
            shorter = min(len(name1_lower), len(name2_lower))
            longer = max(len(name1_lower), len(name2_lower))
            return shorter / longer
        
        # No match
        return 0.0
    
    def _row_to_customer(self, row: sqlite3.Row) -> Customer:
        """Convert database row to Customer object."""
        return Customer(
            customer_id=row['customer_id'],
            normalized_name=row['normalized_name'],
            sector_id=row['sector_id'],
            agency_id=row['agency_id'],
            created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
            updated_date=datetime.fromisoformat(row['updated_date']) if row['updated_date'] else None,
            customer_type=row['customer_type'],
            is_active=bool(row['is_active']),
            notes=row['notes']
        )

class SQLiteSectorRepository(BaseService):
    """SQLite implementation of sector repository for sector assignment functionality."""
    
    def __init__(self, db_connection: DatabaseConnection):
        BaseService.__init__(self, db_connection)
    
    def get_all_active_sectors(self) -> List[Sector]:
        """Get all active sectors."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT sector_id, sector_code, sector_name, sector_group 
                FROM sectors 
                WHERE is_active = 1
                ORDER BY sector_group, sector_name
            """)
            
            return [Sector(
                sector_id=row['sector_id'],
                sector_code=row['sector_code'],
                sector_name=row['sector_name'],
                sector_group=row['sector_group'],
                is_active=True,
                created_date=None
            ) for row in cursor.fetchall()]
    
    def get_sector_by_id(self, sector_id: int) -> Optional[Sector]:
        """Get sector by ID."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT sector_id, sector_code, sector_name, sector_group
                FROM sectors 
                WHERE sector_id = ? AND is_active = 1
            """, (sector_id,))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            return Sector(
                sector_id=row['sector_id'],
                sector_code=row['sector_code'],
                sector_name=row['sector_name'],
                sector_group=row['sector_group'],
                is_active=True,
                created_date=None
            )
    
    def get_sector_by_code(self, sector_code: str) -> Optional[Sector]:
        """Get sector by code."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT sector_id, sector_code, sector_name, sector_group
                FROM sectors 
                WHERE sector_code = ? AND is_active = 1
            """, (sector_code.upper(),))
            
            row = cursor.fetchone()
            if not row:
                return None
                
            return Sector(
                sector_id=row['sector_id'],
                sector_code=row['sector_code'],
                sector_name=row['sector_name'],
                sector_group=row['sector_group'],
                is_active=True,
                created_date=None
            )
    
    def get_sectors_in_group(self, group_name: str) -> List[Sector]:
        """Get all sectors in a group."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT sector_id, sector_code, sector_name, sector_group
                FROM sectors 
                WHERE sector_group = ? AND is_active = 1
                ORDER BY sector_name
            """, (group_name,))
            
            return [Sector(
                sector_id=row['sector_id'],
                sector_code=row['sector_code'],
                sector_name=row['sector_name'],
                sector_group=row['sector_group'],
                is_active=True,
                created_date=None
            ) for row in cursor.fetchall()]

class ReferenceDataRepository(BaseService):
    """Repository for reference data (markets, sectors, languages, agencies)."""
    
    def __init__(self, db_connection: DatabaseConnection):
        BaseService.__init__(self, db_connection)
    
    # ===================================================================
    # AGENCY OPERATIONS
    # ===================================================================
    
    def get_or_create_agency(self, agency_name: str) -> Agency:
        """Get existing agency or create new one."""
        with self.safe_transaction() as conn:
            # Try to find existing
            cursor = conn.execute("SELECT * FROM agencies WHERE agency_name = ?", (agency_name,))
            row = cursor.fetchone()
            
            if row:
                return Agency(
                    agency_id=row['agency_id'],
                    agency_name=row['agency_name'],
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
                    updated_date=datetime.fromisoformat(row['updated_date']) if row['updated_date'] else None,
                    is_active=bool(row['is_active']),
                    notes=row['notes']
                )
            
            # Create new agency
            cursor = conn.execute(
                "INSERT INTO agencies (agency_name) VALUES (?)",
                (agency_name,)
            )
            
            agency_id = cursor.lastrowid
            logger.info(f"Created new agency: {agency_name} (ID: {agency_id})")
            
            return Agency(
                agency_id=agency_id,
                agency_name=agency_name,
                created_date=datetime.now(),
                is_active=True
            )
    
    def find_agency_by_name(self, agency_name: str) -> Optional[Agency]:
        """Find agency by name (case-insensitive)."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM agencies WHERE LOWER(agency_name) = LOWER(?)",
                (agency_name,)
            )
            row = cursor.fetchone()
            
            if row:
                return Agency(
                    agency_id=row['agency_id'],
                    agency_name=row['agency_name'],
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
                    updated_date=datetime.fromisoformat(row['updated_date']) if row['updated_date'] else None,
                    is_active=bool(row['is_active']),
                    notes=row['notes']
                )
            return None
    
    def get_all_agencies(self) -> List[Agency]:
        """Get all active agencies."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM agencies WHERE is_active = 1 ORDER BY agency_name")
            
            return [
                Agency(
                    agency_id=row['agency_id'],
                    agency_name=row['agency_name'],
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None,
                    updated_date=datetime.fromisoformat(row['updated_date']) if row['updated_date'] else None,
                    is_active=bool(row['is_active']),
                    notes=row['notes']
                )
                for row in cursor.fetchall()
            ]
    
    # ===================================================================
    # MARKET OPERATIONS
    # ===================================================================
    
    def get_market_by_name(self, market_name: str) -> Optional[Market]:
        """Get market by name (case-insensitive)."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM markets WHERE LOWER(market_name) = LOWER(?)",
                (market_name,)
            )
            row = cursor.fetchone()
            
            if row:
                return Market(
                    market_id=row['market_id'],
                    market_name=row['market_name'],
                    market_code=row['market_code'],
                    region=row['region'],
                    is_active=bool(row['is_active']),
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
            return None
    
    def get_market_by_code(self, market_code: str) -> Optional[Market]:
        """Get market by standardized code."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM markets WHERE market_code = ?", (market_code,))
            row = cursor.fetchone()
            
            if row:
                return Market(
                    market_id=row['market_id'],
                    market_name=row['market_name'],
                    market_code=row['market_code'],
                    region=row['region'],
                    is_active=bool(row['is_active']),
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
            return None
    
    def get_all_markets(self) -> List[Market]:
        """Get all active markets."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM markets WHERE is_active = 1 ORDER BY market_name")
            
            return [
                Market(
                    market_id=row['market_id'],
                    market_name=row['market_name'],
                    market_code=row['market_code'],
                    region=row['region'],
                    is_active=bool(row['is_active']),
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
                for row in cursor.fetchall()
            ]
    
    # ===================================================================
    # SECTOR OPERATIONS
    # ===================================================================
    
    def get_sector_by_code(self, sector_code: str) -> Optional[Sector]:
        """Get sector by code."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM sectors WHERE sector_code = ?", (sector_code,))
            row = cursor.fetchone()
            
            if row:
                return Sector(
                    sector_id=row['sector_id'],
                    sector_code=row['sector_code'],
                    sector_name=row['sector_name'],
                    sector_group=row['sector_group'],
                    is_active=bool(row['is_active']),
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
            return None
    
    def get_all_sectors(self) -> List[Sector]:
        """Get all active sectors."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM sectors WHERE is_active = 1 ORDER BY sector_name")
            
            return [
                Sector(
                    sector_id=row['sector_id'],
                    sector_code=row['sector_code'],
                    sector_name=row['sector_name'],
                    sector_group=row['sector_group'],
                    is_active=bool(row['is_active']),
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
                for row in cursor.fetchall()
            ]
    
    # ===================================================================
    # LANGUAGE OPERATIONS
    # ===================================================================
    
    def get_language_by_code(self, language_code: str) -> Optional[Language]:
        """Get language by code."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM languages WHERE language_code = ?", (language_code,))
            row = cursor.fetchone()
            
            if row:
                return Language(
                    language_id=row['language_id'],
                    language_code=row['language_code'],
                    language_name=row['language_name'],
                    language_group=row['language_group'],
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
            return None
    
    def get_all_languages(self) -> List[Language]:
        """Get all languages."""
        with self.safe_connection() as conn:
            cursor = conn.execute("SELECT * FROM languages ORDER BY language_name")
            
            return [
                Language(
                    language_id=row['language_id'],
                    language_code=row['language_code'],
                    language_name=row['language_name'],
                    language_group=row['language_group'],
                    created_date=datetime.fromisoformat(row['created_date']) if row['created_date'] else None
                )
                for row in cursor.fetchall()
            ]
    
    # ===================================================================
    # UTILITY METHODS
    # ===================================================================
    
    def standardize_market_code(self, market_name: str) -> Optional[str]:
        """Get standardized market code for a market name."""
        market = self.get_market_by_name(market_name)
        return market.market_code if market else None
    
    def get_sector_choices_for_display(self) -> List[Tuple[int, str, str]]:
        """Get sectors formatted for CLI display as (id, code, name) tuples."""
        sectors = self.get_all_sectors()
        return [(s.sector_id, s.sector_code, s.sector_name) for s in sectors]