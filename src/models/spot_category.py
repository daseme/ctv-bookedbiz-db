from enum import Enum
from typing import Optional


class SpotCategory(Enum):
    """Spot processing categories based on business rules"""

    LANGUAGE_ASSIGNMENT_REQUIRED = "language_assignment_required"
    REVIEW_CATEGORY = "review_category"
    DEFAULT_ENGLISH = "default_english"


def categorize_spot(
    revenue_type: Optional[str], spot_type: Optional[str]
) -> SpotCategory:
    """
    Pure business logic - categorize spots based on revenue_type + spot_type

    Args:
        revenue_type: Revenue type from spots table
        spot_type: Spot type from spots table

    Returns:
        SpotCategory enum indicating how spot should be processed
    """

    # Handle None values
    revenue_type = revenue_type or ""
    spot_type = spot_type or ""

    # Category 1: Language Assignment Required (288,074 spots)
    if revenue_type == "Internal Ad Sales" and spot_type in ["COM", "BNS"]:
        return SpotCategory.LANGUAGE_ASSIGNMENT_REQUIRED

    if revenue_type == "Local":  # Treat as Internal Ad Sales
        return SpotCategory.LANGUAGE_ASSIGNMENT_REQUIRED

    # Category 2: Review Category (5,872 spots)
    if revenue_type == "Internal Ad Sales" and spot_type in ["PKG", "CRD", "AV"]:
        return SpotCategory.REVIEW_CATEGORY

    if revenue_type == "Other" and spot_type in ["COM", "BNS", ""]:
        return SpotCategory.REVIEW_CATEGORY

    # Category 3: Default English (826,609+ spots)
    if revenue_type in ["Direct Response Sales", "Paid Programming", "Branded Content"]:
        return SpotCategory.DEFAULT_ENGLISH

    # Edge cases - Other + SVC/PRD → English
    if revenue_type == "Other" and spot_type in ["SVC", "PRD"]:
        return SpotCategory.DEFAULT_ENGLISH

    # Fallback for anything we missed → Review
    return SpotCategory.REVIEW_CATEGORY
