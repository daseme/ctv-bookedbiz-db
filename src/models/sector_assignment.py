"""
Domain models for sector assignment system
Clean Architecture - Domain Layer
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Protocol
from datetime import datetime
from enum import Enum


class AssignmentMethod(Enum):
    """Methods for sector assignment"""

    AUTO_HIGH_CONFIDENCE = "auto_high_confidence"
    AUTO_PATTERN_MATCH = "auto_pattern_match"
    MANUAL_DIRECT = "manual_direct"
    MANUAL_OVERRIDE = "manual_override"
    BATCH_IMPORT = "batch_import"


class AssignmentStatus(Enum):
    """Status of assignment operations"""

    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    REQUIRES_REVIEW = "requires_review"


@dataclass
class Customer:
    """Customer domain entity for sector assignment"""

    customer_id: int
    normalized_name: str
    current_sector_id: Optional[int] = None
    current_sector_name: Optional[str] = None
    agency_id: Optional[int] = None
    agency_name: Optional[str] = None

    @property
    def has_agency(self) -> bool:
        """Check if customer has agency relationship"""
        return self.agency_name is not None or self.agency_id is not None

    @property
    def display_name(self) -> str:
        """Get display-friendly customer name"""
        if len(self.normalized_name) > 50:
            return self.normalized_name[:47] + "..."
        return self.normalized_name


@dataclass
class Sector:
    """Sector domain entity with hierarchy support"""

    sector_id: int
    sector_code: str
    sector_name: str
    sector_group: str

    @property
    def full_display_name(self) -> str:
        """Get full sector display with code and name"""
        return f"{self.sector_code}: {self.sector_name}"


@dataclass
class SectorGroup:
    """Sector group aggregate for hierarchy management"""

    group_name: str
    sectors: List[Sector]

    @property
    def sector_codes(self) -> List[str]:
        """Get all sector codes in this group"""
        return [sector.sector_code for sector in self.sectors]

    def get_sector_by_code(self, code: str) -> Optional[Sector]:
        """Find sector in this group by code"""
        for sector in self.sectors:
            if sector.sector_code == code.upper():
                return sector
        return None


@dataclass
class SectorSuggestion:
    """Value object for sector suggestions with confidence scoring"""

    sector: Sector
    confidence: float
    reason: str
    method: AssignmentMethod = AssignmentMethod.AUTO_PATTERN_MATCH

    @property
    def is_high_confidence(self) -> bool:
        """Check if suggestion meets high confidence threshold"""
        return self.confidence >= 0.90

    @property
    def confidence_percentage(self) -> str:
        """Get formatted confidence percentage"""
        return f"{self.confidence:.0%}"


@dataclass
class CustomerRevenueInfo:
    """Value object for customer financial context"""

    total_spots: int
    total_revenue: float
    months_active: int
    first_spot: Optional[str]
    last_spot: Optional[str]
    last_broadcast_month: Optional[str] = None

    @property
    def revenue_per_spot(self) -> float:
        """Calculate average revenue per spot"""
        return self.total_revenue / self.total_spots if self.total_spots > 0 else 0.0

    @property
    def is_active_customer(self) -> bool:
        """Check if customer has recent activity"""
        return self.total_spots > 0 and self.total_revenue > 0

    @property
    def revenue_tier(self) -> str:
        """Categorize customer by revenue"""
        if self.total_revenue >= 100000:
            return "Enterprise"
        elif self.total_revenue >= 25000:
            return "Major"
        elif self.total_revenue >= 5000:
            return "Standard"
        else:
            return "Small"


@dataclass
class AssignmentResult:
    """Result object for assignment operations"""

    customer_id: int
    sector_id: int
    success: bool
    method: AssignmentMethod = AssignmentMethod.MANUAL_DIRECT
    error_message: Optional[str] = None
    assigned_at: datetime = field(default_factory=datetime.now)
    assigned_by: str = "system"

    @property
    def is_successful(self) -> bool:
        """Check if assignment was successful"""
        return self.success and self.error_message is None


@dataclass
class BatchAssignmentResult:
    """Aggregate result for batch operations"""

    attempted: int
    successful: int
    errors: List[Dict[str, Any]] = field(default_factory=list)
    total_revenue_assigned: float = 0.0
    total_spots_assigned: int = 0
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        return (self.successful / self.attempted * 100) if self.attempted > 0 else 0.0

    @property
    def has_errors(self) -> bool:
        """Check if batch had any errors"""
        return len(self.errors) > 0

    def add_error(self, customer_name: str, customer_id: int, error: str) -> None:
        """Add error to batch result"""
        self.errors.append(
            {
                "customer_name": customer_name,
                "customer_id": customer_id,
                "error": error,
                "timestamp": datetime.now().isoformat(),
            }
        )


@dataclass
class AssignmentAudit:
    """Audit trail for sector assignments"""

    audit_id: Optional[int] = None
    customer_id: int = 0
    old_sector_id: Optional[int] = None
    new_sector_id: Optional[int] = None
    assignment_method: AssignmentMethod = AssignmentMethod.MANUAL_DIRECT
    confidence_score: Optional[float] = None
    assigned_by: str = "system"
    assigned_at: datetime = field(default_factory=datetime.now)
    notes: Optional[str] = None


@dataclass
class UnassignedCustomerSummary:
    """Summary statistics for unassigned customers"""

    total_count: int
    total_revenue: float
    total_spots: int
    months_represented: int
    revenue_percentage: float

    @property
    def average_revenue_per_customer(self) -> float:
        """Average revenue per unassigned customer"""
        return self.total_revenue / self.total_count if self.total_count > 0 else 0.0


@dataclass
class SectorAssignmentReport:
    """Comprehensive report for sector assignment status"""

    generated_at: datetime = field(default_factory=datetime.now)
    assignment_summary: Optional[Dict[str, int]] = None
    revenue_impact: Optional[Dict[str, Any]] = None
    auto_candidates: List[Dict[str, Any]] = field(default_factory=list)
    problem_customers: List[Dict[str, Any]] = field(default_factory=list)
    sector_distribution: List[Dict[str, Any]] = field(default_factory=list)
    recommendations: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def completion_percentage(self) -> float:
        """Get assignment completion percentage"""
        if not self.assignment_summary:
            return 0.0
        total = self.assignment_summary.get("total_customers", 0)
        assigned = self.assignment_summary.get("assigned_customers", 0)
        return (assigned / total * 100) if total > 0 else 0.0


# Protocols for dependency injection and testing


class SectorRepository(Protocol):
    """Repository interface for sector data access"""

    def get_all_active_sectors(self) -> List[Sector]:
        """Get all active sectors"""
        ...

    def get_sector_by_id(self, sector_id: int) -> Optional[Sector]:
        """Get sector by ID"""
        ...

    def get_sector_by_code(self, sector_code: str) -> Optional[Sector]:
        """Get sector by code"""
        ...

    def get_sectors_in_group(self, group_name: str) -> List[Sector]:
        """Get all sectors in a group"""
        ...


class CustomerRepository(Protocol):
    """Repository interface for customer data access"""

    def get_unassigned_customers(
        self, limit: int = 50, offset: int = 0
    ) -> List[Customer]:
        """Get customers without sector assignments"""
        ...

    def get_customer_by_id(self, customer_id: int) -> Optional[Customer]:
        """Get customer by ID"""
        ...

    def assign_sector_to_customer(
        self,
        customer_id: int,
        sector_id: int,
        method: AssignmentMethod,
        assigned_by: str,
    ) -> AssignmentResult:
        """Assign sector to customer"""
        ...

    def get_customer_revenue_info(self, customer_id: int) -> CustomerRevenueInfo:
        """Get customer revenue context"""
        ...


class PatternMatcher(Protocol):
    """Interface for sector pattern matching logic"""

    def suggest_sectors(
        self, customer_name: str, agency_name: Optional[str] = None
    ) -> List[SectorSuggestion]:
        """Get sector suggestions for customer"""
        ...

    def get_high_confidence_matches(
        self, customer_names: List[str], min_confidence: float = 0.90
    ) -> List[tuple[str, SectorSuggestion]]:
        """Get customers suitable for auto-assignment"""
        ...


@dataclass
class AssignmentConfiguration:
    """Configuration for assignment behavior"""

    min_auto_confidence: float = 0.90
    batch_size: int = 100
    enable_audit_trail: bool = True
    require_confirmation_above_revenue: float = 50000.0
    default_assignment_method: AssignmentMethod = AssignmentMethod.MANUAL_DIRECT

    def __post_init__(self):
        """Validate configuration values"""
        if not 0.0 <= self.min_auto_confidence <= 1.0:
            raise ValueError("min_auto_confidence must be between 0.0 and 1.0")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.require_confirmation_above_revenue < 0:
            raise ValueError("require_confirmation_above_revenue must be non-negative")
