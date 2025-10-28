#!/usr/bin/env python3
"""
Administrative entry filter for customer matching.

This module provides functions to identify and filter out administrative/internal
entries that shouldn't be included in customer matching algorithms.

Usage:
    from admin_filter import is_admin_entry, should_exclude_from_matching

    if not should_exclude_from_matching(bill_code):
        # Process for customer matching
        pass
"""

from __future__ import annotations

import re
from typing import Set

# Known administrative patterns
ADMIN_PATTERNS = [
    r"DO NOT INVOIC",
    r"DO NOT BILL",
    r"BROKER FEES?",
    r"CREDIT\b",
    r"CREDIT MEMO",
    r"ADJUSTMENT",
    r"REFUND",
    r"CONTRA",
]

# Known administrative entries (exact matches, case-insensitive)
KNOWN_ADMIN_ENTRIES: Set[str] = {
    "WorldLink Broker Fees (DO NOT INVOICE)",
    "Worldlink:Credit Associates (MA)",
    "Martin Retail Group:Tristate Cadillac (Broker Fee",
    "Martin Retail Group:Chevy North Central (Broker F",
    "Martin Retail Group:Chevy West (Broker Fees  - DO",
    "Admerasia:McDonald's CREDIT",
    "Admerasia:McDonald's CREDIT MEMO",
    # Suspicious entries that are likely placeholders/adjustments
    "AVS",  # $0 revenue with 371 spots - likely placeholder
    "NAM",  # -$5,000 revenue - likely adjustment/credit
    # Add more as you discover them
}

# Suspicious patterns that might be admin (for manual review)
# Note: Being very conservative here - government agencies often use short abbreviations
SUSPICIOUS_PATTERNS = [
    r"^(AVS|NAM|TBD|TBA|TEST|TEMP)$",  # Known placeholder-like entries
    # Removed general short abbreviation pattern - too many false positives
]

# Compile regex patterns for efficiency
_ADMIN_REGEX = re.compile(r"\b(?:" + "|".join(ADMIN_PATTERNS) + r")\b", re.IGNORECASE)

_SUSPICIOUS_REGEXES = [
    re.compile(pattern, re.IGNORECASE) for pattern in SUSPICIOUS_PATTERNS
]


def is_admin_entry(bill_code: str) -> bool:
    """
    Check if a bill_code is a known administrative entry.

    Returns True for entries that should be excluded from customer matching.
    """
    if not bill_code or not bill_code.strip():
        return True

    bill_code = bill_code.strip()

    # Check exact matches (case-insensitive)
    if bill_code.lower() in {entry.lower() for entry in KNOWN_ADMIN_ENTRIES}:
        return True

    # Check regex patterns
    if _ADMIN_REGEX.search(bill_code):
        return True

    return False


def is_suspicious_entry(
    bill_code: str, revenue: float = None, spot_count: int = None
) -> bool:
    """
    Check if a bill_code has suspicious patterns that might indicate admin entry.

    These should be manually reviewed but not automatically excluded.
    Now includes financial indicators for better accuracy.
    """
    if not bill_code or not bill_code.strip():
        return True

    bill_code = bill_code.strip()

    # Check suspicious patterns (now very conservative)
    for regex in _SUSPICIOUS_REGEXES:
        if regex.search(bill_code):
            return True

    # Financial red flags (if revenue/spot data provided)
    if revenue is not None and spot_count is not None:
        # Zero revenue with many spots (possible placeholder)
        if revenue == 0 and spot_count > 100:
            return True

        # Small negative revenue with few spots (possible adjustment/credit)
        if revenue < 0 and spot_count < 10 and revenue > -10000:
            return True

    return False


def should_exclude_from_matching(bill_code: str) -> bool:
    """
    Main function to determine if a bill_code should be excluded from customer matching.

    Returns True if the entry should be excluded.
    """
    return is_admin_entry(bill_code)


def categorize_bill_code(
    bill_code: str, revenue: float = None, spot_count: int = None
) -> str:
    """
    Categorize a bill_code into: 'admin', 'suspicious', or 'clean'.

    Useful for analysis and reporting.
    Now includes financial data for better accuracy.
    """
    if is_admin_entry(bill_code):
        return "admin"
    elif is_suspicious_entry(bill_code, revenue, spot_count):
        return "suspicious"
    else:
        return "clean"


def get_admin_filter_stats(
    bill_codes: list[str], revenues: list[float] = None, spot_counts: list[int] = None
) -> dict:
    """
    Get statistics on how many entries would be filtered by category.
    Now supports financial data for better accuracy.
    """
    categories = {"admin": 0, "suspicious": 0, "clean": 0}

    for i, bill_code in enumerate(bill_codes):
        revenue = revenues[i] if revenues and i < len(revenues) else None
        spot_count = spot_counts[i] if spot_counts and i < len(spot_counts) else None

        category = categorize_bill_code(bill_code, revenue, spot_count)
        categories[category] += 1

    total = len(bill_codes)
    return {
        "total_entries": total,
        "admin_count": categories["admin"],
        "suspicious_count": categories["suspicious"],
        "clean_count": categories["clean"],
        "admin_pct": categories["admin"] / total * 100 if total > 0 else 0,
        "suspicious_pct": categories["suspicious"] / total * 100 if total > 0 else 0,
        "clean_pct": categories["clean"] / total * 100 if total > 0 else 0,
    }


# For easy import
__all__ = [
    "is_admin_entry",
    "is_suspicious_entry",
    "should_exclude_from_matching",
    "categorize_bill_code",
    "get_admin_filter_stats",
    "KNOWN_ADMIN_ENTRIES",
    "ADMIN_PATTERNS",
]
