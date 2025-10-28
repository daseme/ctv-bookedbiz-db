#!/usr/bin/env python3
"""
Normalization + parsing utilities for customer name matching.

Pure, reusable, and testable. Keep this the single source of truth for
how names are normalized across ETL, DB backfills, analyzers, and UIs.

New: extract_billcode_parts(...) returns (agency_raw, customer_raw).
Matching should use the customer segment; reporting can also use agency.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Tuple

# --- Optional dependency with safe fallback ---
try:
    from unidecode import unidecode  # type: ignore
except ImportError:  # pragma: no cover

    def unidecode(s: str) -> str:  # minimal fallback
        return s

# ---------------------
# Configurable patterns
# ---------------------

# Business suffixes commonly appended to legal names.
BUSINESS_SUFFIXES: Iterable[str] = (
    "incorporated",
    "inc",
    "l.l.c",
    "llc",
    "co",
    "co.",
    "corp",
    "corp.",
    "ltd",
    "ltd.",
    "company",
    "companies",
    "llp",
    "lp",
    "plc",
    "gmbh",
    "s.a.",
    "s.a",
    "s.p.a",
    "sa",
    "bv",
    "pty",
    "pte",
    "kk",
    "kabushiki",
    "有限会社",
    "株式会社",
)

# Leading articles to remove.
ARTICLES: Iterable[str] = ("the",)

# Tokens frequently present after/around client names inside bill codes.
NOISE_TOKENS: Iterable[str] = (
    r"\b(q\d{1}|fy\d{2,4}|h\d|s\d|w\d)\b",  # Q4, FY24, H1, S2, W3
    r"\b(holiday|promo|flight|brand|test|usa|us|na|intl|global)\b",
    r"\b(sfo|sf|la|ny|nyc|chi|dal|sea|min|cv)\b",  # market/geo shorthands
    r"\b(summer|spring|fall|winter)\b",
)

# Bill code separators (agency:client, network-client, etc.)
SEPARATORS = r"[:\|\-/–—]"

# -----------------
# Compiled regexes
# -----------------
_SUFFIXES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(s) for s in BUSINESS_SUFFIXES) + r")\b\.?",
    re.IGNORECASE,
)
_ARTICLES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(a) for a in ARTICLES) + r")\b", re.IGNORECASE
)
_NON_WORD_RE = re.compile(r"[^\w\s&]")
_MULTI_WS_RE = re.compile(r"\s+")
_YEAR_CODE_RE = re.compile(
    r"\b\d{2,4}\b"
)  # drop isolated years/codes commonly appended

_NOISE_RES = [re.compile(pat, re.IGNORECASE) for pat in NOISE_TOKENS]
_SEPARATORS_RE = re.compile(SEPARATORS)


# -----------------
# Public functions
# -----------------


def normalize_business_name(s: str) -> str:
    """
    Deterministically normalize a business name.

    Steps (in order):
      1) Unicode -> ASCII via unidecode
      2) casefold()
      3) & -> ' and '
      4) strip punctuation (keep word chars and spaces)
      5) remove articles
      6) remove common business suffixes
      7) drop isolated numeric year/codes
      8) collapse whitespace
    """
    x = unidecode((s or "").strip())
    x = x.casefold()
    x = x.replace("&", " and ")
    x = _NON_WORD_RE.sub(" ", x)
    x = _ARTICLES_RE.sub(" ", x)
    x = _SUFFIXES_RE.sub(" ", x)
    x = _YEAR_CODE_RE.sub(" ", x)
    x = _MULTI_WS_RE.sub(" ", x).strip()
    return x


def normalize_tokens(s: str) -> List[str]:
    """Tokenize a name after normalization (unique, sorted, no empties)."""
    n = normalize_business_name(s)
    toks = [t for t in n.split(" ") if t]
    return sorted(set(toks))


def token_signature(norm_name: str) -> str:
    """
    Build a token signature from an already-normalized name.
    Useful for blocking and quick equality checks.
    """
    toks = [t for t in norm_name.split() if t]
    return " ".join(sorted(set(toks)))


def _strip_noise_tokens(raw: str) -> str:
    """Remove common campaign/time/geo noise from a raw candidate segment."""
    cc = raw
    for rx in _NOISE_RES:
        cc = rx.sub(" ", cc)
    cc = _MULTI_WS_RE.sub(" ", cc).strip()
    return cc


def extract_billcode_parts(bill_code: str) -> Tuple[str, str]:
    """
    Return (agency_raw, customer_raw) from a bill_code.

    - Splits on common separators (':', '|', '-', '/')
    - Customer = **last** segment (after noise removal)
    - Agency   = everything **before last**, joined with a single space (raw trimmed; no noise removal)
      Rationale: preserve agency string; some tokens may be meaningful (e.g., specific agency name).
    - If only one segment, agency is '' and customer is that segment (after noise removal).
    """
    if not bill_code:
        return "", ""

    parts = [p.strip() for p in _SEPARATORS_RE.split(bill_code) if p and p.strip()]
    if not parts:
        # nothing to split; treat whole as customer
        return "", _strip_noise_tokens(bill_code.strip())

    if len(parts) == 1:
        return "", _strip_noise_tokens(parts[0])

    agency_raw = " ".join(parts[:-1]).strip()
    customer_raw = _strip_noise_tokens(parts[-1])
    return agency_raw, customer_raw


def extract_customer_from_bill_code(bill_code: str) -> str:
    """
    Backward-compatible helper that returns **only the customer** portion.
    Internally uses extract_billcode_parts(...).
    """
    _, customer_raw = extract_billcode_parts(bill_code)
    return customer_raw


__all__ = [
    "normalize_business_name",
    "normalize_tokens",
    "token_signature",
    "extract_billcode_parts",
    "extract_customer_from_bill_code",
    "BUSINESS_SUFFIXES",
    "ARTICLES",
    "NOISE_TOKENS",
    "SEPARATORS",
]
