"""
Pure data models (entities) for the sales database tool.
No business logic - just data structures with type hints.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Protocol


# ===================================================================
# VALIDATION INFRASTRUCTURE
# ===================================================================

@dataclass
class ValidationError:
    """Represents a validation error with context."""
    field: str
    message: str
    code: str
    severity: str = "error"  # error, warning, info


@dataclass 
class ValidationResult:
    """Container for validation results with errors and warnings."""
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    
    def is_valid(self) -> bool:
        """Check if validation passed (no errors)."""
        return len(self.errors) == 0
    
    def has_warnings(self) -> bool:
        """Check if there are any warnings."""
        return len(self.warnings) > 0
    
    def add_error(self, field: str, message: str, code: str = "VALIDATION_ERROR"):
        """Add an error to the validation result."""
        self.errors.append(ValidationError(field, message, code, "error"))
    
    def add_warning(self, field: str, message: str, code: str = "VALIDATION_WARNING"):
        """Add a warning to the validation result."""
        self.warnings.append(ValidationError(field, message, code, "warning"))


class Validator(Protocol):
    """Protocol for validation classes."""
    def validate(self, obj) -> ValidationResult:
        """Validate an object and return validation result."""
        ...


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
    spot_type: Optional[str] = None  # AV, BB, BNS, COM, CRD, PKG, PRD, PRG, SVC
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