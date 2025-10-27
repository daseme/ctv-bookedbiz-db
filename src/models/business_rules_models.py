"""
Business Rules Data Models (Updated with Stakeholder Language)
==============================================================

Data models for the business rules system used in language block assignment.
Updated to use stakeholder-friendly terminology and fixed for direct imports.
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Dict, Any
from datetime import datetime


class BusinessRuleType(Enum):
    """Business rule types with stakeholder-friendly names"""

    DIRECT_RESPONSE_SALES = "direct_response_sales"
    GOVERNMENT_PUBLIC_SERVICE = "government_public_service"
    POLITICAL_CAMPAIGNS = "political_campaigns"
    NONPROFIT_AWARENESS = "nonprofit_awareness"
    EXTENDED_CONTENT_BLOCKS = "extended_content_blocks"


class CustomerIntent(Enum):
    """Customer intent classification for spot placement"""

    LANGUAGE_SPECIFIC = "language_specific"  # Single block, language-targeted
    TIME_SPECIFIC = "time_specific"  # Multi-block, same day-part
    INDIFFERENT = "indifferent"  # Multi-block, customer flexible
    NO_GRID_COVERAGE = "no_grid_coverage"  # Market has no programming grid


class AssignmentMethod(Enum):
    """Method used for spot assignment"""

    AUTO_COMPUTED = "auto_computed"
    MANUAL_OVERRIDE = "manual_override"
    BUSINESS_RULE_AUTO_RESOLVED = "business_rule_auto_resolved"
    BUSINESS_RULE_FLAGGED = "business_rule_flagged"
    NO_GRID_AVAILABLE = "no_grid_available"


@dataclass
class BusinessRule:
    """Definition of a business rule for language block assignment"""

    rule_type: BusinessRuleType
    name: str
    description: str
    sector_codes: List[str]
    min_duration_minutes: Optional[int] = None
    max_duration_minutes: Optional[int] = None
    customer_intent: CustomerIntent = CustomerIntent.INDIFFERENT
    auto_resolve: bool = True
    priority: int = 1  # Higher priority rules are checked first

    def __post_init__(self):
        """Validate rule configuration"""
        if self.min_duration_minutes and self.max_duration_minutes:
            if self.min_duration_minutes >= self.max_duration_minutes:
                raise ValueError(
                    "min_duration_minutes must be less than max_duration_minutes"
                )

        if self.priority < 1:
            raise ValueError("priority must be >= 1")


@dataclass
class SpotData:
    """Spot data for business rule evaluation"""

    spot_id: int
    customer_id: int
    sector_code: Optional[str]
    sector_name: Optional[str]
    bill_code: str
    duration_minutes: int
    gross_rate: float
    customer_name: str
    time_in: str
    time_out: str
    air_date: str
    market_id: int

    def __post_init__(self):
        """Validate spot data"""
        if self.duration_minutes < 0:
            raise ValueError("duration_minutes cannot be negative")

        if not self.bill_code:
            raise ValueError("bill_code is required")


@dataclass
class BusinessRuleResult:
    """Result of business rule evaluation"""

    spot_id: int
    rule_applied: Optional[BusinessRule] = None
    customer_intent: Optional[CustomerIntent] = None
    auto_resolved: bool = False
    requires_attention: bool = False
    alert_reason: Optional[str] = None
    confidence: float = 1.0
    notes: Optional[str] = None
    evaluation_timestamp: datetime = None

    def __post_init__(self):
        """Set default timestamp if not provided"""
        if self.evaluation_timestamp is None:
            self.evaluation_timestamp = datetime.now()

        if self.confidence < 0 or self.confidence > 1:
            raise ValueError("confidence must be between 0 and 1")


@dataclass
class AssignmentResult:
    """Result of spot assignment to language blocks"""

    spot_id: int
    success: bool
    schedule_id: Optional[int] = None
    block_id: Optional[int] = None
    customer_intent: Optional[CustomerIntent] = None
    spans_multiple_blocks: bool = False
    blocks_spanned: Optional[List[int]] = None
    primary_block_id: Optional[int] = None
    requires_attention: bool = False
    alert_reason: Optional[str] = None
    assignment_method: AssignmentMethod = AssignmentMethod.AUTO_COMPUTED
    business_rule_applied: Optional[str] = None
    confidence: float = 1.0
    notes: Optional[str] = None
    error_message: Optional[str] = None
    assigned_date: datetime = None

    def __post_init__(self):
        """Set default timestamp if not provided"""
        if self.assigned_date is None:
            self.assigned_date = datetime.now()


@dataclass
class BusinessRuleStats:
    """Statistics for business rule performance"""

    total_evaluated: int = 0
    auto_resolved: int = 0
    flagged_for_review: int = 0
    rules_applied: Dict[str, int] = None
    auto_resolve_rate: float = 0.0
    evaluation_start_time: datetime = None
    evaluation_end_time: datetime = None

    def __post_init__(self):
        """Initialize default values"""
        if self.rules_applied is None:
            self.rules_applied = {}

        if self.evaluation_start_time is None:
            self.evaluation_start_time = datetime.now()

        # Calculate auto resolve rate
        if self.total_evaluated > 0:
            self.auto_resolve_rate = self.auto_resolved / self.total_evaluated

    def add_rule_application(self, rule_type: str, auto_resolved: bool = True):
        """Add a rule application to the stats"""
        self.total_evaluated += 1
        self.rules_applied[rule_type] = self.rules_applied.get(rule_type, 0) + 1

        if auto_resolved:
            self.auto_resolved += 1
        else:
            self.flagged_for_review += 1

        # Recalculate rate
        self.auto_resolve_rate = self.auto_resolved / self.total_evaluated

    def finalize(self):
        """Mark the evaluation as complete"""
        self.evaluation_end_time = datetime.now()

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the statistics"""
        return {
            "total_evaluated": self.total_evaluated,
            "auto_resolved": self.auto_resolved,
            "flagged_for_review": self.flagged_for_review,
            "auto_resolve_rate": self.auto_resolve_rate,
            "rules_applied": self.rules_applied,
            "evaluation_duration": (
                self.evaluation_end_time - self.evaluation_start_time
            ).total_seconds()
            if self.evaluation_end_time
            else None,
        }


# Constants for business rule configuration
DEFAULT_BUSINESS_RULES_CONFIG = {
    "media_infomercial_min_duration": 480,  # 8 hours (not used in updated rules)
    "nonprofit_campaign_min_duration": 300,  # 5 hours
    "long_duration_content_min_duration": 720,  # 12 hours
    "default_confidence_threshold": 0.8,
    "max_rule_priority": 10,
}

# Sector codes used in business rules
BUSINESS_RULE_SECTORS = {
    "MEDIA": "Media & Entertainment (Direct Response Sales)",
    "GOV": "Government (Public Service)",
    "POLITICAL": "Political (Campaign Advertising)",
    "NPO": "Non-Profit (Awareness Campaigns)",
    "AUTO": "Automotive",
    "HEALTH": "Healthcare",
    "RETAIL": "Retail",
}

# Alert reasons for business rule applications with stakeholder-friendly language
BUSINESS_RULE_ALERT_REASONS = {
    "AUTO_RESOLVED_DIRECT_RESPONSE_SALES": "Auto-resolved: Direct response sales campaign (infomercial) - designed for broad audience reach",
    "AUTO_RESOLVED_GOVERNMENT_PUBLIC_SERVICE": "Auto-resolved: Government public service campaign - community-wide messaging",
    "AUTO_RESOLVED_POLITICAL_CAMPAIGNS": "Auto-resolved: Political campaign advertising - broad demographic reach required",
    "AUTO_RESOLVED_NONPROFIT_AWARENESS": "Auto-resolved: Nonprofit awareness campaign (5+ hours) - extended community outreach",
    "AUTO_RESOLVED_EXTENDED_CONTENT_BLOCKS": "Auto-resolved: Extended content block (12+ hours) - inherently spans multiple blocks",
    "FLAGGED_FOR_REVIEW": "Flagged for manual review by business rule",
}

# Stakeholder communication templates
STAKEHOLDER_COMMUNICATION_TEMPLATES = {
    "DIRECT_RESPONSE_SALES": {
        "short_description": "Infomercial/Direct Response",
        "stakeholder_explanation": "These are direct response sales campaigns (infomercials) that are intentionally designed for broad audience reach across all language blocks to maximize response rates.",
        "business_justification": "Auto-resolving these reduces manual review workload while ensuring appropriate broad-reach assignment.",
    },
    "GOVERNMENT_PUBLIC_SERVICE": {
        "short_description": "Government PSA",
        "stakeholder_explanation": "Government public service announcements are designed for community-wide reach across all demographics.",
        "business_justification": "These campaigns intentionally target all language communities for maximum public awareness.",
    },
    "POLITICAL_CAMPAIGNS": {
        "short_description": "Political Advertising",
        "stakeholder_explanation": "Political campaigns require broad demographic reach to maximize voter engagement across all language communities.",
        "business_justification": "Political advertisers specifically purchase broad reach to ensure maximum voter contact.",
    },
    "NONPROFIT_AWARENESS": {
        "short_description": "Nonprofit Awareness",
        "stakeholder_explanation": "Extended nonprofit awareness campaigns (5+ hours) are conducting broad outreach for maximum community impact.",
        "business_justification": "Long-duration nonprofit campaigns are designed for sustained awareness across all communities.",
    },
    "EXTENDED_CONTENT_BLOCKS": {
        "short_description": "Extended Programming",
        "stakeholder_explanation": "Content running 12+ hours inherently spans multiple language blocks regardless of customer intent.",
        "business_justification": "Extended duration content naturally crosses multiple programming blocks by design.",
    },
}
