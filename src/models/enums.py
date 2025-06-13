"""
Enums for type safety in the sales database tool.
"""

from enum import Enum


class SpotType(Enum):
    """Commercial spot types."""
    AV = "AV"
    BB = "BB"
    BONUS = "BNS"
    COMMERCIAL = "COM"
    CRD = "CRD"
    PKG = "PKG"
    PRD = "PRD"
    PRG = "PRG"
    SVC = "SVC"


class BillingType(Enum):
    """Billing type options."""
    CALENDAR = "Calendar"
    BROADCAST = "Broadcast"


class AffidavitFlag(Enum):
    """Affidavit flag values."""
    YES = "Y"
    NO = "N"