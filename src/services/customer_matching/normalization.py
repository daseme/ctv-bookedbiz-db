    #!/usr/bin/env python3
"""
Normalization + parsing utilities for customer name matching.

Pure, reusable, and testable. Keep this the single source of truth for
how names are normalized across ETL, DB backfills, analyzers, and UIs.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional

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
    "incorporated", "inc", "l.l.c", "llc", "co", "co.", "corp", "corp.", "ltd", "ltd.",
    "company", "companies", "llp", "lp", "plc", "gmbh", "s.a.", "s.a", "s.p.a", "sa", "bv",
    "pty", "pte", "kk", "kabushiki", "有限会社", "株式会社"
)

# Leading articles to remove.
ARTICLES: Iterable[str] = ("the",)

# Tokens frequently present after/around client names inside bill codes.
NOISE_TOKENS: Iterable[str] = (
    r"\b(q\d{1}|fy\d{2,4}|h\d|s\d|w\d)\b",          # Q4, FY24, H1, S2, W3
    r"\b(holiday|promo|flight|brand|test|usa|us|na|intl|global)\b",
    r"\b(sfo|sf|la|ny|nyc|chi|dal|sea|min|cv)\b",    # market/geo shorthands
    r"\b(summer|spring|fall|winter)\b",
)

# Bill code separators (agency:client, network-client, etc.)
SEPARATORS = r"[:\|\-/–—]"

# -----------------
# Compiled regexes
# -----------------
_SUFFIXES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(s) for s in BUSINESS_SUFFIXES) + r")\b\.?", re.IGNORECASE
)
_ARTICLES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(a) for a in ARTICLES) + r")\b", re.IGNORECASE
)
_NON_WORD_RE = re.compile(r"[^\w\s&]")
_MULTI_WS_RE = re.compile(r"\s+")
_YEAR_CODE_RE = re.compile(r"\b\d{2,4}\b")  # drop isolated years/codes commonly appended

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
      3) & -> ' and ' (so token-based matchers behave)
      4) strip punctuation (keep word chars and spaces)
      5) remove articles
      6) remove common business suffixes
      7) drop isolated numeric year/codes
      8) collapse whitespace

    Returns a lowercased, space-delimited canonical string.

    Examples
    --------
    >>> normalize_business_name("The Acme, Inc.")
    'acme'
    >>> normalize_business_name("B&O GmbH")
    'b and o'
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
    """
    Tokenize a name after normalization (unique, sorted, no empties).
    """
    n = normalize_business_name(s)
    toks = [t for t in n.split(" ") if t]
    # unique + stable sort for determinism
    return sorted(set(toks))


def token_signature(norm_name: str) -> str:
    """
    Build a token signature from an already-normalized name.
    Useful for blocking and quick equality checks.

    Examples
    --------
    >>> token_signature("acme and partners")
    'acme and partners'
    >>> token_signature("partners acme and")
    'acme and partners'
    """
    toks = [t for t in norm_name.split() if t]
    return " ".join(sorted(set(toks)))


def extract_customer_from_bill_code(bill_code: str) -> str:
    """
    Heuristic extraction of the customer (client) name from a bill_code.
    - Splits on common separators (':', '|', '-', '/')
    - Prefers the last 1–2 segments (agency:client, network:client patterns)
    - Removes common 'noise' tokens (quarters, FY, generic campaign words, simple geos)
    - Picks the candidate with the most alphabetic chars

    Returns the *raw* candidate (not normalized). Call normalize_business_name()
    afterwards for matching.

    Examples
    --------
    >>> extract_customer_from_bill_code("OMD: ACME INC Q4 2024")
    'ACME INC'
    >>> extract_customer_from_bill_code("WBD/ACME-TEST SFO")
    'ACME TEST'
    """
    if not bill_code:
        return ""

    parts = [p.strip() for p in _SEPARATORS_RE.split(bill_code) if p and p.strip()]
    if not parts:
        return bill_code.strip()

    # Candidate generation: prefer last segment, and last-two-joined
    candidates: List[str] = []
    candidates.append(parts[-1])
    if len(parts) >= 2:
        candidates.append(f"{parts[-2]} {parts[-1]}")

    cleaned: List[str] = []
    for cand in candidates:
        cc = cand
        for rx in _NOISE_RES:
            cc = rx.sub(" ", cc)
        cc = _MULTI_WS_RE.sub(" ", cc).strip()
        if cc:
            cleaned.append(cc)

    if not cleaned:
        return bill_code.strip()

    # Winner: most alphabetic characters (simple, effective tie-breaker)
    return max(cleaned, key=lambda t: sum(int(ch.isalpha()) for ch in t))


__all__ = [
    "normalize_business_name",
    "normalize_tokens",
    "token_signature",
    "extract_customer_from_bill_code",
    "BUSINESS_SUFFIXES",
    "ARTICLES",
    "NOISE_TOKENS",
    "SEPARATORS",
]
