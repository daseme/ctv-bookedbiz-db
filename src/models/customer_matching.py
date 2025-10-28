# src/models/customer_matching.py
"""Domain models for customer matching and normalization integration."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class MatchMethod(Enum):
    """Business rules for how customer matches were identified."""

    EXACT_MATCH = ("exact", "Direct match to customers table", 1.0)
    ALIAS_MATCH = ("alias", "Matched via entity_aliases", 0.98)
    FUZZY_HIGH = ("fuzzy_high", "High confidence fuzzy match", 0.92)
    FUZZY_REVIEW = ("fuzzy_review", "Requires manual review", 0.80)
    FUZZY_LOW = ("fuzzy_low", "Low confidence match", 0.60)
    NO_MATCH = ("no_match", "No viable match found", 0.0)

    def __init__(self, code: str, description: str, min_confidence: float):
        self.code = code
        self.description = description
        self.min_confidence = min_confidence

    @property
    def is_high_confidence(self) -> bool:
        return self.min_confidence >= 0.92

    @property
    def requires_review(self) -> bool:
        return 0.70 <= self.min_confidence < 0.92


class CustomerMatchStatus(Enum):
    """Status tracking for customer matching workflow."""

    UNMATCHED = ("unmatched", "Not yet analyzed")
    HIGH_CONFIDENCE = ("high_confidence", "Auto-approvable match found")
    PENDING_REVIEW = ("pending_review", "In review queue")
    APPROVED = ("approved", "Match approved and alias created")
    REJECTED = ("rejected", "Match rejected, needs manual setup")
    NEEDS_MANUAL_SETUP = ("manual_setup", "No match found, needs new customer")

    def __init__(self, code: str, description: str):
        self.code = code
        self.description = description


@dataclass
class MatchSuggestion:
    """Individual match suggestion for a customer."""

    customer_id: int
    customer_name: str
    confidence_score: float
    match_reasons: List[str] = field(default_factory=list)

    @property
    def is_high_confidence(self) -> bool:
        return self.confidence_score >= 0.92

    @property
    def requires_review(self) -> bool:
        return 0.70 <= self.confidence_score < 0.92


@dataclass
class CustomerMatchCandidate:
    """Value object representing a customer that needs matching analysis."""

    bill_code_raw: str
    normalized_name: str
    revenue: float
    spot_count: int
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    months_active: List[str] = field(default_factory=list)

    # Match analysis results
    match_status: CustomerMatchStatus = CustomerMatchStatus.UNMATCHED
    best_match: Optional[MatchSuggestion] = None
    all_suggestions: List[MatchSuggestion] = field(default_factory=list)
    match_method: Optional[MatchMethod] = None

    # Business metadata
    revenue_types: List[str] = field(default_factory=list)
    analyzed_at: Optional[datetime] = None
    notes: str = ""

    @property
    def is_high_value(self) -> bool:
        """High-value customers get priority attention."""
        return self.revenue >= 2000 or self.spot_count >= 10

    @property
    def is_recent(self) -> bool:
        """Recent activity suggests active customer."""
        if not self.last_seen:
            return False
        # Simple check - could be enhanced with actual date parsing
        return any(month in self.last_seen for month in ["Nov-24", "Dec-24", "Jan-25"])

    @property
    def priority_score(self) -> float:
        """Priority for review queue ordering."""
        base_score = min(self.revenue / 10000, 1.0)  # Revenue component
        if self.is_recent:
            base_score *= 1.5
        if self.best_match and self.best_match.is_high_confidence:
            base_score *= 1.3
        return base_score


@dataclass
class CustomerMatchFilters:
    """Filters for customer matching queries."""

    min_revenue: float = 0
    min_spots: int = 0
    revenue_types: List[str] = field(default_factory=list)
    status_filter: Optional[CustomerMatchStatus] = None
    include_low_value: bool = False
    months_active: List[str] = field(default_factory=list)
    search_text: str = ""

    @classmethod
    def from_request_args(cls, args: Dict[str, Any]) -> CustomerMatchFilters:
        """Create filters from Flask request args."""
        return cls(
            min_revenue=float(args.get("min_revenue", 0)),
            min_spots=int(args.get("min_spots", 0)),
            revenue_types=args.get("revenue_types", "").split(",")
            if args.get("revenue_types")
            else [],
            status_filter=CustomerMatchStatus(args.get("status"))
            if args.get("status")
            else None,
            include_low_value=args.get("include_low_value", "").lower() == "true",
            search_text=args.get("q", "").strip(),
        )


@dataclass
class CustomerMatchingResult:
    """Result of customer matching analysis."""

    total_analyzed: int
    high_confidence_matches: int
    pending_review: int
    no_matches: int
    candidates: List[CustomerMatchCandidate]
    analysis_timestamp: datetime = field(default_factory=datetime.now)

    @property
    def auto_approvable_count(self) -> int:
        return len(
            [
                c
                for c in self.candidates
                if c.match_status == CustomerMatchStatus.HIGH_CONFIDENCE
            ]
        )

    @property
    def needs_attention_count(self) -> int:
        return len(
            [
                c
                for c in self.candidates
                if c.match_status
                in [
                    CustomerMatchStatus.PENDING_REVIEW,
                    CustomerMatchStatus.NEEDS_MANUAL_SETUP,
                ]
            ]
        )


@dataclass
class ReviewActionRequest:
    """Request to approve/reject a customer match."""

    candidate_bill_code: str
    action: str  # 'approve', 'reject', 'defer'
    target_customer_id: Optional[int] = None
    notes: str = ""
    approved_by: str = "web_user"

    def validate(self) -> List[str]:
        """Validate the action request."""
        errors = []
        if not self.candidate_bill_code.strip():
            errors.append("Bill code is required")
        if self.action not in ["approve", "reject", "defer"]:
            errors.append("Action must be approve, reject, or defer")
        if self.action == "approve" and not self.target_customer_id:
            errors.append("Target customer ID required for approval")
        return errors
