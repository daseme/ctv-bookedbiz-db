"""
Database model definitions using dataclasses.
Matches actual Excel structure and business requirements.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List
import re


@dataclass
class Spot:
    """
    Represents a single spot (commercial) entry from the booked business report.
    
    This is the main transactional record containing all revenue-generating
    commercial spots that have been booked. Includes all Excel columns.
    """
    # Required fields
    bill_code: str  # Customer identifier
    air_date: date
    
    # Date/time fields
    end_date: Optional[date] = None
    day: Optional[str] = None  # Day of week
    time_in: Optional[str] = None  # HH:MM:SS format
    time_out: Optional[str] = None  # HH:MM:SS format
    
    # Spot details
    length: Optional[str] = None  # Length in seconds
    media: Optional[str] = None
    program: Optional[str] = None
    lang: Optional[str] = None  # Language code (E, M, T, etc.)
    format: Optional[str] = None
    number_field: Optional[int] = None  # The '#' column
    line: Optional[int] = None
    type: Optional[str] = None  # COM or BNS
    estimate: Optional[str] = None
    
    # Financial fields
    gross_rate: Optional[Decimal] = None
    make_good: Optional[str] = None
    spot_value: Optional[Decimal] = None
    month: Optional[str] = None  # mmm-yy format
    broker_fees: Optional[Decimal] = None
    priority: Optional[int] = None
    station_net: Optional[Decimal] = None
    
    # Business fields
    sales_person: Optional[str] = None  # AE name
    revenue_type: Optional[str] = None
    billing_type: Optional[str] = None  # Calendar or Broadcast
    agency_flag: Optional[str] = None  # 'Agency?' column
    affidavit_flag: Optional[str] = None  # Y/N
    contract: Optional[str] = None
    market: Optional[str] = None
    
    # Normalized fields
    normalized_customer: Optional[str] = None
    market_code: Optional[str] = None
    
    # Metadata
    spot_id: Optional[int] = None
    load_date: Optional[datetime] = None
    source_file: Optional[str] = None
    is_historical: bool = False
    effective_date: Optional[date] = None  # When forward-looking data was loaded
    
    def validate(self) -> List[str]:
        """Validate the spot data and return a list of validation errors."""
        errors = []
        
        # Required field validation
        if not self.bill_code or not self.bill_code.strip():
            errors.append("Bill code (customer) is required")
            
        if not self.air_date:
            errors.append("Air date is required")
            
        # Date validation
        if self.air_date and self.air_date > date.today() + timedelta(days=730):  # 2 years
            errors.append(f"Air date {self.air_date} is too far in the future")
            
        if self.end_date and self.air_date and self.end_date < self.air_date:
            errors.append("End date cannot be before air date")
            
        # Time format validation
        if self.time_in and not self._validate_time_format(self.time_in):
            errors.append(f"Invalid time_in format: {self.time_in}")
            
        if self.time_out and not self._validate_time_format(self.time_out):
            errors.append(f"Invalid time_out format: {self.time_out}")
            
        # Financial validation
        if self.gross_rate is not None and self.gross_rate < 0:
            errors.append("Gross rate cannot be negative")
            
        if self.station_net is not None and self.station_net < 0:
            errors.append("Station net cannot be negative")
            
        if self.spot_value is not None and self.spot_value < 0:
            errors.append("Spot value cannot be negative")
            
        if self.broker_fees is not None and self.broker_fees < 0:
            errors.append("Broker fees cannot be negative")
            
        # Revenue type validation - exclude Trade
        if self.revenue_type and self.revenue_type.upper() == "TRADE":
            errors.append("Trade revenue type should be excluded")
            
        # Type validation
        if self.type and self.type.upper() not in ["COM", "BNS", ""]:
            errors.append(f"Invalid type: {self.type} (must be COM or BNS)")
            
        # Billing type validation
        if self.billing_type and self.billing_type not in ["Calendar", "Broadcast", ""]:
            errors.append(f"Invalid billing type: {self.billing_type}")
            
        return errors
    
    def _validate_time_format(self, time_str: str) -> bool:
        """Validate HH:MM:SS time format."""
        pattern = r'^\d{1,2}:\d{2}:\d{2}$'
        return bool(re.match(pattern, time_str))
    
    def is_valid(self) -> bool:
        """Check if the spot data is valid."""
        return len(self.validate()) == 0


@dataclass
class Customer:
    """
    Represents a normalized customer entity.
    
    Each customer has a unique normalized name that multiple
    original customer names can map to. Customers are categorized
    by sector for performance analysis.
    """
    normalized_name: str
    sector: Optional[str] = None  # AUTO, CPG, INS, OUTR, etc.
    customer_id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    customer_type: Optional[str] = None
    notes: Optional[str] = None
    
    def validate(self) -> List[str]:
        """Validate the customer data."""
        errors = []
        
        if not self.normalized_name or not self.normalized_name.strip():
            errors.append("Normalized name is required")
            
        # Ensure normalized name doesn't have trailing spaces
        if self.normalized_name and self.normalized_name != self.normalized_name.strip():
            errors.append("Normalized name has trailing whitespace")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the customer data is valid."""
        return len(self.validate()) == 0


@dataclass
class CustomerMapping:
    """
    Maps original customer names to normalized versions.
    
    This allows the system to handle variations in customer names
    while maintaining a single normalized version for reporting.
    """
    original_name: str
    normalized_name: str
    mapping_id: Optional[int] = None
    created_date: Optional[datetime] = None
    created_by: str = "system"
    confidence_score: Optional[float] = None
    
    def validate(self) -> List[str]:
        """Validate the mapping data."""
        errors = []
        
        if not self.original_name or not self.original_name.strip():
            errors.append("Original name is required")
            
        if not self.normalized_name or not self.normalized_name.strip():
            errors.append("Normalized name is required")
            
        if self.confidence_score is not None:
            if self.confidence_score < 0 or self.confidence_score > 1:
                errors.append("Confidence score must be between 0 and 1")
                
        return errors
    
    def is_valid(self) -> bool:
        """Check if the mapping data is valid."""
        return len(self.validate()) == 0


@dataclass
class Budget:
    """
    Represents monthly budget targets for Account Executives.
    
    Budgets are set at the AE level by month and used for
    performance tracking and variance analysis.
    """
    ae_name: str
    year: int
    month: int
    budget_amount: Decimal
    budget_id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    source: Optional[str] = None
    
    def validate(self) -> List[str]:
        """Validate the budget data."""
        errors = []
        
        if not self.ae_name or not self.ae_name.strip():
            errors.append("AE name is required")
            
        if self.year < 2000 or self.year > 2100:
            errors.append(f"Year {self.year} is out of valid range")
            
        if self.month < 1 or self.month > 12:
            errors.append(f"Month {self.month} must be between 1 and 12")
            
        if self.budget_amount < 0:
            errors.append("Budget amount cannot be negative")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the budget data is valid."""
        return len(self.validate()) == 0


@dataclass
class Pipeline:
    """
    Represents management's revenue expectations by AE and month.
    
    Pipeline data is updated bi-weekly and tracks management's
    current expectations for future revenue (not customer-specific).
    This is the "budget balancing mechanism" that plugs the gap
    between booked revenue and budgeted targets.
    """
    ae_name: str
    year: int
    month: int
    pipeline_amount: Decimal
    update_date: date
    is_current: bool = True
    pipeline_id: Optional[int] = None
    created_date: Optional[datetime] = None
    created_by: Optional[str] = None
    notes: Optional[str] = None
    
    def validate(self) -> List[str]:
        """Validate the pipeline data."""
        errors = []
        
        if not self.ae_name or not self.ae_name.strip():
            errors.append("AE name is required")
            
        if self.year < 2000 or self.year > 2100:
            errors.append(f"Year {self.year} is out of valid range")
            
        if self.month < 1 or self.month > 12:
            errors.append(f"Month {self.month} must be between 1 and 12")
            
        if self.pipeline_amount < 0:
            errors.append("Pipeline amount cannot be negative")
            
        if not self.update_date:
            errors.append("Update date is required")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the pipeline data is valid."""
        return len(self.validate()) == 0


@dataclass
class Market:
    """
    Represents a geographic market with its standardized code.
    
    Markets are mapped from various spellings/formats in the source
    data to consistent codes for reporting.
    """
    market_name: str
    market_code: str
    market_id: Optional[int] = None
    region: Optional[str] = None
    is_active: bool = True
    created_date: Optional[datetime] = None
    
    def validate(self) -> List[str]:
        """Validate the market data."""
        errors = []
        
        if not self.market_name or not self.market_name.strip():
            errors.append("Market name is required")
            
        if not self.market_code or not self.market_code.strip():
            errors.append("Market code is required")
            
        # Market code should be uppercase and alphanumeric
        if self.market_code and not re.match(r'^[A-Z0-9]+$', self.market_code):
            errors.append("Market code must be uppercase alphanumeric")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the market data is valid."""
        return len(self.validate()) == 0


@dataclass
class Language:
    """
    Represents a language mapping for future reporting.
    
    Maps language codes from Excel (E, M, T, etc.) to full names
    and language groups for analytics.
    """
    language_code: str
    language_name: str
    language_group: Optional[str] = None
    language_id: Optional[int] = None
    created_date: Optional[datetime] = None
    
    def validate(self) -> List[str]:
        """Validate the language data."""
        errors = []
        
        if not self.language_code or not self.language_code.strip():
            errors.append("Language code is required")
            
        if not self.language_name or not self.language_name.strip():
            errors.append("Language name is required")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the language data is valid."""
        return len(self.validate()) == 0


@dataclass
class Sector:
    """
    Represents a business sector for customer categorization.
    
    Sectors are used to group customers for performance analysis
    and reporting (e.g., Automotive, CPG, Insurance, Outreach).
    """
    sector_code: str  # AUTO, CPG, INS, OUTR, etc.
    sector_name: str  # Full descriptive name
    sector_group: Optional[str] = None  # Higher level grouping if needed
    sector_id: Optional[int] = None
    is_active: bool = True
    created_date: Optional[datetime] = None
    
    def validate(self) -> List[str]:
        """Validate the sector data."""
        errors = []
        
        if not self.sector_code or not self.sector_code.strip():
            errors.append("Sector code is required")
            
        if not self.sector_name or not self.sector_name.strip():
            errors.append("Sector name is required")
            
        # Sector code should be uppercase and alphanumeric
        if self.sector_code and not re.match(r'^[A-Z0-9]+, self.sector_code):
            errors.append("Sector code must be uppercase alphanumeric")
            
        return errors
    
    def is_valid(self) -> bool:
        """Check if the sector data is valid."""
        return len(self.validate()) == 0