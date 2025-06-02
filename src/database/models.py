"""
Fixed data models with proper separation of concerns and business rule validation.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Protocol
from abc import ABC, abstractmethod
from enum import Enum
import re


# ===================================================================
# VALIDATION INFRASTRUCTURE
# ===================================================================

@dataclass
class ValidationError:
    field: str
    message: str
    code: str
    severity: str = "error"  # error, warning, info


@dataclass 
class ValidationResult:
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    
    def is_valid(self) -> bool:
        return len(self.errors) == 0
    
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0
    
    def add_error(self, field: str, message: str, code: str = "VALIDATION_ERROR"):
        self.errors.append(ValidationError(field, message, code, "error"))
    
    def add_warning(self, field: str, message: str, code: str = "VALIDATION_WARNING"):
        self.warnings.append(ValidationError(field, message, code, "warning"))


class Validator(Protocol):
    """Protocol for validation classes."""
    def validate(self, obj) -> ValidationResult:
        ...


# ===================================================================
# ENUMS FOR TYPE SAFETY
# ===================================================================

class SpotType(Enum):
    COMMERCIAL = "COM"
    BONUS = "BNS"


class BillingType(Enum):
    CALENDAR = "Calendar"
    BROADCAST = "Broadcast"


class AffidavitFlag(Enum):
    YES = "Y"
    NO = "N"


# ===================================================================
# PURE DATA MODELS (No business logic)
# ===================================================================

@dataclass
class Agency:
    """Represents an advertising agency."""
    agency_name: str
    agency_id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    is_active: bool = True
    notes: Optional[str] = None


@dataclass
class Sector:
    """Represents a business sector for customer categorization."""
    sector_code: str  # AUTO, CPG, INS, OUTR, etc.
    sector_name: str  # Full descriptive name
    sector_id: Optional[int] = None
    sector_group: Optional[str] = None
    is_active: bool = True
    created_date: Optional[datetime] = None


@dataclass
class Market:
    """Represents a geographic market with standardized code."""
    market_name: str
    market_code: str
    market_id: Optional[int] = None
    region: Optional[str] = None
    is_active: bool = True
    created_date: Optional[datetime] = None


@dataclass
class Language:
    """Represents a language mapping."""
    language_code: str
    language_name: str
    language_id: Optional[int] = None
    language_group: Optional[str] = None
    created_date: Optional[datetime] = None


@dataclass
class Customer:
    """Represents a normalized customer entity."""
    normalized_name: str
    customer_id: Optional[int] = None
    sector_id: Optional[int] = None
    agency_id: Optional[int] = None  # If customer comes through agency
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    customer_type: Optional[str] = None
    is_active: bool = True
    notes: Optional[str] = None


@dataclass
class CustomerMapping:
    """Maps original customer names to normalized versions."""
    original_name: str
    customer_id: int
    mapping_id: Optional[int] = None
    created_date: Optional[datetime] = None
    created_by: str = "system"
    confidence_score: Optional[float] = None


@dataclass
class Spot:
    """
    Core transactional record - a single commercial spot booking.
    Pure data structure with no business logic.
    """
    # Required fields
    bill_code: str
    air_date: date
    
    # Date/time fields
    end_date: Optional[date] = None
    day_of_week: Optional[str] = None
    time_in: Optional[str] = None
    time_out: Optional[str] = None
    
    # Spot details
    length_seconds: Optional[str] = None
    media: Optional[str] = None
    program: Optional[str] = None
    language_code: Optional[str] = None
    format: Optional[str] = None
    sequence_number: Optional[int] = None
    line_number: Optional[int] = None
    spot_type: Optional[str] = None  # COM or BNS
    estimate: Optional[str] = None
    
    # Financial fields
    gross_rate: Optional[Decimal] = None
    make_good: Optional[str] = None
    spot_value: Optional[Decimal] = None
    broadcast_month: Optional[str] = None  # mmm-yy format
    broker_fees: Optional[Decimal] = None
    priority: Optional[int] = None
    station_net: Optional[Decimal] = None
    
    # Business fields
    sales_person: Optional[str] = None
    revenue_type: Optional[str] = None
    billing_type: Optional[str] = None
    agency_flag: Optional[str] = None
    affidavit_flag: Optional[str] = None
    contract: Optional[str] = None
    market_name: Optional[str] = None  # Original from Excel
    
    # Foreign key relationships
    customer_id: Optional[int] = None
    agency_id: Optional[int] = None
    market_id: Optional[int] = None
    language_id: Optional[int] = None
    
    # Metadata
    spot_id: Optional[int] = None
    load_date: Optional[datetime] = None
    source_file: Optional[str] = None
    is_historical: bool = False
    effective_date: Optional[date] = None


@dataclass
class Budget:
    """Monthly budget targets for Account Executives."""
    ae_name: str
    year: int
    month: int
    budget_amount: Decimal
    budget_id: Optional[int] = None
    created_date: Optional[datetime] = None
    updated_date: Optional[datetime] = None
    source: Optional[str] = None


@dataclass
class Pipeline:
    """Management's revenue expectations by AE and month."""
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


# ===================================================================
# BUSINESS RULE VALIDATORS (Separated from data models)
# ===================================================================

class SpotValidator:
    """Validates spot data according to business rules."""
    
    def validate(self, spot: Spot) -> ValidationResult:
        result = ValidationResult()
        
        # Required field validation
        self._validate_required_fields(spot, result)
        
        # Business rule validation
        self._validate_business_rules(spot, result)
        
        # Data format validation
        self._validate_data_formats(spot, result)
        
        # Financial validation
        self._validate_financial_fields(spot, result)
        
        return result
    
    def _validate_required_fields(self, spot: Spot, result: ValidationResult):
        """Validate required fields."""
        if not spot.bill_code or not spot.bill_code.strip():
            result.add_error("bill_code", "Bill code is required", "REQUIRED_FIELD")
            
        if not spot.air_date:
            result.add_error("air_date", "Air date is required", "REQUIRED_FIELD")
    
    def _validate_business_rules(self, spot: Spot, result: ValidationResult):
        """Validate business-specific rules."""
        # Critical business rule: exclude Trade revenue type
        if spot.revenue_type and spot.revenue_type.upper() == "TRADE":
            result.add_error("revenue_type", 
                           "Trade revenue type must be excluded per business rules", 
                           "BUSINESS_RULE_VIOLATION")
        
        # Date range validation
        if spot.air_date:
            # Historical data shouldn't be too old (data quality check)
            if spot.air_date < date(2020, 1, 1):
                result.add_warning("air_date", 
                                 "Air date is very old - verify data quality", 
                                 "DATA_QUALITY")
            
            # Future dates shouldn't be too far out
            if spot.air_date > date.today() + timedelta(days=730):
                result.add_error("air_date", 
                               "Air date is too far in the future", 
                               "INVALID_DATE_RANGE")
        
        # End date validation
        if spot.end_date and spot.air_date and spot.end_date < spot.air_date:
            result.add_error("end_date", 
                           "End date cannot be before air date", 
                           "INVALID_DATE_RANGE")
    
    def _validate_data_formats(self, spot: Spot, result: ValidationResult):
        """Validate data formats."""
        # Time format validation
        if spot.time_in and not self._is_valid_time_format(spot.time_in):
            result.add_error("time_in", 
                           f"Invalid time format: {spot.time_in} (expected HH:MM:SS)", 
                           "INVALID_FORMAT")
        
        if spot.time_out and not self._is_valid_time_format(spot.time_out):
            result.add_error("time_out", 
                           f"Invalid time format: {spot.time_out} (expected HH:MM:SS)", 
                           "INVALID_FORMAT")
        
        # Spot type validation
        if spot.spot_type and spot.spot_type not in ["COM", "BNS", ""]:
            result.add_error("spot_type", 
                           f"Invalid spot type: {spot.spot_type} (must be COM or BNS)", 
                           "INVALID_ENUM_VALUE")
        
        # Billing type validation
        if spot.billing_type and spot.billing_type not in ["Calendar", "Broadcast", ""]:
            result.add_error("billing_type", 
                           f"Invalid billing type: {spot.billing_type}", 
                           "INVALID_ENUM_VALUE")
        
        # Affidavit flag validation
        if spot.affidavit_flag and spot.affidavit_flag not in ["Y", "N", ""]:
            result.add_error("affidavit_flag", 
                           f"Invalid affidavit flag: {spot.affidavit_flag} (must be Y or N)", 
                           "INVALID_ENUM_VALUE")
    
    def _validate_financial_fields(self, spot: Spot, result: ValidationResult):
        """Validate financial fields."""
        financial_fields = [
            ("gross_rate", spot.gross_rate),
            ("station_net", spot.station_net),
            ("spot_value", spot.spot_value),
            ("broker_fees", spot.broker_fees)
        ]
        
        for field_name, value in financial_fields:
            if value is not None and value < 0:
                result.add_error(field_name, 
                               f"{field_name.replace('_', ' ').title()} cannot be negative", 
                               "INVALID_FINANCIAL_VALUE")
    
    def _is_valid_time_format(self, time_str: str) -> bool:
        """Validate HH:MM:SS time format."""
        pattern = r'^\d{1,2}:\d{2}:\d{2}$'
        if not re.match(pattern, time_str):
            return False
        
        # Additional validation - ensure valid time components
        try:
            parts = time_str.split(':')
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
            return 0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59
        except (ValueError, IndexError):
            return False


class CustomerValidator:
    """Validates customer data."""
    
    def validate(self, customer: Customer) -> ValidationResult:
        result = ValidationResult()
        
        if not customer.normalized_name or not customer.normalized_name.strip():
            result.add_error("normalized_name", "Normalized name is required", "REQUIRED_FIELD")
        
        # Check for whitespace issues
        if customer.normalized_name and customer.normalized_name != customer.normalized_name.strip():
            result.add_error("normalized_name", 
                           "Normalized name has leading/trailing whitespace", 
                           "DATA_QUALITY")
        
        return result


class BudgetValidator:
    """Validates budget data."""
    
    def validate(self, budget: Budget) -> ValidationResult:
        result = ValidationResult()
        
        if not budget.ae_name or not budget.ae_name.strip():
            result.add_error("ae_name", "AE name is required", "REQUIRED_FIELD")
        
        if budget.year < 2000 or budget.year > 2100:
            result.add_error("year", f"Year {budget.year} is out of valid range", "INVALID_RANGE")
        
        if budget.month < 1 or budget.month > 12:
            result.add_error("month", f"Month {budget.month} must be between 1 and 12", "INVALID_RANGE")
        
        if budget.budget_amount < 0:
            result.add_error("budget_amount", "Budget amount cannot be negative", "INVALID_FINANCIAL_VALUE")
        
        return result


class PipelineValidator:
    """Validates pipeline data."""
    
    def validate(self, pipeline: Pipeline) -> ValidationResult:
        result = ValidationResult()
        
        if not pipeline.ae_name or not pipeline.ae_name.strip():
            result.add_error("ae_name", "AE name is required", "REQUIRED_FIELD")
        
        if pipeline.year < 2000 or pipeline.year > 2100:
            result.add_error("year", f"Year {pipeline.year} is out of valid range", "INVALID_RANGE")
        
        if pipeline.month < 1 or pipeline.month > 12:
            result.add_error("month", f"Month {pipeline.month} must be between 1 and 12", "INVALID_RANGE")
        
        if pipeline.pipeline_amount < 0:
            result.add_error("pipeline_amount", "Pipeline amount cannot be negative", "INVALID_FINANCIAL_VALUE")
        
        if not pipeline.update_date:
            result.add_error("update_date", "Update date is required", "REQUIRED_FIELD")
        
        return result


# ===================================================================
# REPOSITORY INTERFACES (Abstract base)
# ===================================================================

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
        """Find customers with similar names above threshold. Returns (customer, similarity_score) tuples."""
        pass


# ===================================================================
# DOMAIN SERVICES (Business logic)
# ===================================================================

class BillCodeParser:
    """Parses bill codes to extract agency and customer information."""
    
    def parse(self, bill_code: str) -> tuple[Optional[str], str]:
        """
        Parse bill_code into (agency_name, customer_name).
        
        Examples:
        - "IW Group:CMS" -> ("IW Group", "CMS")
        - "CMS" -> (None, "CMS")
        """
        if not bill_code or not bill_code.strip():
            raise ValueError("Bill code cannot be empty")
        
        bill_code = bill_code.strip()
        
        if ':' in bill_code:
            parts = bill_code.split(':', 1)  # Split on first colon only
            agency_name = parts[0].strip()
            customer_name = parts[1].strip()
            
            if not agency_name or not customer_name:
                raise ValueError(f"Invalid bill code format: {bill_code}")
            
            return (agency_name, customer_name)
        else:
            return (None, bill_code)


# ===================================================================
# EXAMPLE USAGE
# ===================================================================

if __name__ == "__main__":
    # Example of how to use the fixed models and validators
    
    # Create a spot with data
    spot = Spot(
        bill_code="IW Group:CMS Production",
        air_date=date(2025, 6, 15),
        gross_rate=Decimal("1500.00"),
        revenue_type="Digital",  # Not Trade, so valid
        spot_type="COM"
    )
    
    # Validate the spot
    validator = SpotValidator()
    result = validator.validate(spot)
    
    if result.is_valid():
        print("Spot is valid!")
    else:
        print("Validation errors:")
        for error in result.errors:
            print(f"  {error.field}: {error.message}")
    
    # Parse bill code
    parser = BillCodeParser()
    agency, customer = parser.parse(spot.bill_code)
    print(f"Agency: {agency}, Customer: {customer}")