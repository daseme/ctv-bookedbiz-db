from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from enum import Enum


class LanguageStatus(Enum):
    """Language determination status"""

    DETERMINED = "determined"
    UNDETERMINED = "undetermined"
    DEFAULT = "default"
    INVALID = "invalid"


@dataclass
class LanguageAssignment:
    """Language assignment result with undetermined language handling"""

    spot_id: int
    language_code: str
    language_status: LanguageStatus
    confidence: float = 1.0
    assignment_method: str = "direct_mapping"
    assigned_date: datetime = datetime.now()
    requires_review: bool = False
    notes: Optional[str] = None


@dataclass
class SpotLanguageData:
    """Basic spot language data"""

    spot_id: int
    language_code: Optional[str]
    revenue_type: Optional[str]
    market_id: Optional[int]
    gross_rate: Optional[float]
    bill_code: Optional[str]
