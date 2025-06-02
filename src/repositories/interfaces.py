"""Abstract repository interfaces."""

from abc import ABC, abstractmethod
from typing import Optional, List
from datetime import date
from decimal import Decimal

# Import your models - adjust path as needed
from ..models.entities import Spot, Customer

class SpotRepository(ABC):
    """Abstract repository for spot data operations."""
    
    @abstractmethod
    def save(self, spot: Spot) -> Spot:
        """Save a spot and return it with assigned ID."""
        pass
    
    @abstractmethod
    def find_by_id(self, spot_id: int) -> Optional[Spot]:
        """Find a spot by its ID."""
        pass
    
    @abstractmethod
    def find_by_customer_and_date_range(self, customer_id: int, start_date: date, end_date: date) -> List[Spot]:
        """Find spots for a customer within a date range."""
        pass
    
    @abstractmethod
    def get_revenue_by_ae_and_month(self, ae_name: str, year: int, month: int) -> Decimal:
        """Get total revenue for an AE in a specific month."""
        pass
    
    @abstractmethod
    def delete_future_data(self, cutoff_date: date) -> int:
        """Delete non-historical data after cutoff date. Returns count deleted."""
        pass

class CustomerRepository(ABC):
    """Abstract repository for customer data operations."""
    
    @abstractmethod
    def save(self, customer: Customer) -> Customer:
        """Save a customer and return it with assigned ID."""
        pass
    
    @abstractmethod
    def find_by_normalized_name(self, normalized_name: str) -> Optional[Customer]:
        """Find a customer by normalized name."""
        pass
    
    @abstractmethod
    def find_similar_customers(self, name: str, threshold: float = 0.8) -> List[tuple[Customer, float]]:
        """Find customers with similar names above threshold."""
        pass