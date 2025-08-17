#!/usr/bin/env python3
"""
Customer Name Matching (Blocking + Robust Normalization)

Goals
- Deterministic normalization shared across all steps
- Candidate generation via blocking (prefix + metaphone + token signature)
- RapidFuzz token-based scoring (far better than difflib)
- Read-only SQLite (avoids locks)
- Safe CSV export; safe alias SQL templates
- Human-in-the-loop friendly classification

Usage:
  python customer_name_matcher_blocking.py --db-path production.db --export-unmatched --suggest-aliases
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

# Local (same package)
from .normalization import (
    normalize_business_name,
    extract_customer_from_bill_code,
    token_signature,  # if you keep it in normalization; else remove here
)

# Optional deps (guarded)
try:
    from rapidfuzz import fuzz
    HAVE_RAPIDFUZZ = True
except ImportError:
    HAVE_RAPIDFUZZ = False

try:
    from metaphone import doublemetaphone
except ImportError:
    doublemetaphone = None  # type: ignore

def _metaphone_primary(s: str) -> str:
    if not doublemetaphone:
        return ""
    a, b = doublemetaphone(s)
    return a or ""

try:
    from unidecode import unidecode
except ImportError:
    def unidecode(s: str) -> str:  # minimal fallback
        return s


# ---------- Configuration (edit without touching code) ----------
BUSINESS_SUFFIXES = r"(incorporated|inc|l\.l\.c|llc|co|co\.|corp|corp\.|ltd|ltd\.|company|companies)"
ARTICLES = r"(the)"

NORMALIZATION_CONFIG = {
    "collapse_whitespace": True,
    "strip_articles": True,
    "strip_business_suffixes": True,
    "ampersand_to_and": True,
    "remove_punct": True,
    "casefold": True,
}

# Tokens you often see after the client name inside bill codes
NOISE_TOKENS = [
    r"\b(q\d{1}|fy\d{2,4}|h\d|s\d|w\d)\b",     # Q4, FY24, H1, S2, W3
    r"\b(holiday|promo|flight|brand|test|usa|us|na|intl|global)\b",
    r"\b(sfo|sf|la|ny|nyc|chi|dal|sea|min|cv)\b",  # market/geo shorthands
    r"\b(summer|spring|fall|winter)\b",
]

SEPARATORS = r"[:\|\-/–—]"

# Classification thresholds
HIGH_CONF = 0.92
REVIEW_MIN = 0.80

# ---------- Data classes ----------
@dataclass
class Candidate:
    customer_id: int
    name: str           # normalized name
    raw_name: str       # the DB stored normalized_name (pre-normalized upstream)
    score: float = 0.0

@dataclass
class CustomerMatch:
    bill_code_name_raw: str
    norm_name: str
    spot_count: int
    revenue: float
    first_seen: str
    last_seen: str
    months: Set[str] = field(default_factory=set)

    # Matching outcome
    status: str = "unknown"   # exact|alias|high_confidence|review|unknown
    matched_customer_id: Optional[int] = None
    matched_customer_name: Optional[str] = None
    best_score: float = 0.0
    suggestions: List[Tuple[str, float]] = field(default_factory=list)

# ---------- Pure helpers (top-down) ----------
def analyze_customer_names(db_path: str, cfg: dict) -> List[CustomerMatch]:
    """
    Orchestrates:
      1) Aggregate bill_code -> (customer name, metrics)
      2) Build customer index with blocking keys
      3) Classify each name via candidate generation + scoring
    """
    customers, aliases = load_customer_maps(db_path)
    index = build_block_index(customers)
    bill_rows = aggregate_billcode_customers(db_path)

    results: List[CustomerMatch] = []
    for row in bill_rows:
        raw_candidate = extract_customer_from_bill_code(row["bill_code"])
        norm_candidate = normalize_business_name(raw_candidate)

        match = CustomerMatch(
            bill_code_name_raw=raw_candidate,
            norm_name=norm_candidate,
            spot_count=row["spot_count"],
            revenue=row["total_revenue"] or 0.0,
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            months=set((row["broadcast_months"] or "").split(",")) if row["broadcast_months"] else set(),
        )

        # 1) exact
        if norm_candidate in customers:
            cid, raw_name = customers[norm_candidate]
            match.status = "exact"
            match.matched_customer_id = cid
            match.matched_customer_name = raw_name
            match.best_score = 1.0

        # 2) alias direct hit (alias table stores raw alias_name; normalize before lookup)
        elif norm_candidate in aliases:
            cid, raw_name = aliases[norm_candidate]
            match.status = "alias"
            match.matched_customer_id = cid
            match.matched_customer_name = raw_name
            match.best_score = 1.0

        else:
            # 3) candidates via blocking + score
            cands = generate_candidates(norm_candidate, index)
            if cands:
                # score + sort
                for cand in cands:
                    cand.score = score_name(norm_candidate, cand.name)
                cands.sort(key=lambda c: c.score, reverse=True)

                # record suggestions
                match.suggestions = [(c.raw_name, c.score) for c in cands[:5]]
                best = cands[0]
                match.best_score = best.score

                # classify
                if best.score >= HIGH_CONF and match.revenue >= 2000:
                    match.status = "high_confidence"
                    match.matched_customer_id = best.customer_id
                    match.matched_customer_name = best.raw_name
                elif best.score >= REVIEW_MIN:
                    match.status = "review"
                    match.matched_customer_id = best.customer_id
                    match.matched_customer_name = best.raw_name
                else:
                    match.status = "unknown"
            else:
                match.status = "unknown"

        results.append(match)

    # revenue-desc ordering helps triage
    results.sort(key=lambda m: m.revenue, reverse=True)
    return results


# ---------- Normalization and parsing ----------
def normalize_business_name(s: str) -> str:
    x = unidecode((s or "").strip())
    if NORMALIZATION_CONFIG.get("casefold", True):
        x = x.casefold()
    if NORMALIZATION_CONFIG.get("ampersand_to_and", True):
        x = x.replace("&", " and ")
    if NORMALIZATION_CONFIG.get("remove_punct", True):
        x = re.sub(r"[^\w\s]", " ", x)
    if NORMALIZATION_CONFIG.get("strip_articles", True):
        x = re.sub(rf"\b{ARTICLES}\b", " ", x)
    if NORMALIZATION_CONFIG.get("strip_business_suffixes", True):
        x = re.sub(rf"\b{BUSINESS_SUFFIXES}\b\.?", " ", x)
    x = re.sub(r"\b\d{2,4}\b", " ", x)  # drop isolated years/codes frequently appended
    if NORMALIZATION_CONFIG.get("collapse_whitespace", True):
        x = re.sub(r"\s+", " ", x)
    return x.strip()


def extract_customer_from_bill_code(bill_code: str) -> str:
    """
    More robust than 'split on last colon':
      - Split on common separators
      - Remove obvious noise tokens
      - Choose the longest plausible token sequence
    """
    if not bill_code:
        return ""
    parts = re.split(SEPARATORS, bill_code)
    parts = [p.strip() for p in parts if p.strip()]

    # Heuristic: prefer the last 1–2 segments (agency:client or network:client)
    candidates: List[str] = []
    if parts:
        candidates.append(parts[-1])
        if len(parts) >= 2:
            candidates.append(parts[-2] + " " + parts[-1])

    # Remove noise tokens
    cleaned: List[str] = []
    for c in candidates:
        cc = c
        for pat in NOISE_TOKENS:
            cc = re.sub(pat, " ", cc, flags=re.IGNORECASE)
        cc = re.sub(r"\s+", " ", cc).strip()
        if cc:
            cleaned.append(cc)

    if cleaned:
        # Pick the one with more alpha chars (likely the actual name)
        return max(cleaned, key=lambda t: sum(ch.isalpha() for ch in t))
    return bill_code.strip()


# ---------- Blocking & scoring ----------
def token_signature(norm_name: str) -> str:
    toks = [t for t in norm_name.split() if t]
    return " ".join(sorted(set(toks)))


def build_block_index(customers: Dict[str, Tuple[int, str]]):
    """
    customers: dict[norm_name] -> (customer_id, raw_db_name)
    Returns an object with:
      - by_prefix: first 6 chars -> [Candidate]
      - by_meta:   metaphone key -> [Candidate]
      - by_sig:    token signature -> [Candidate]
    """
    by_prefix: Dict[str, List[Candidate]] = {}
    by_meta: Dict[str, List[Candidate]] = {}
    by_sig: Dict[str, List[Candidate]] = {}

    for nname, (cid, raw) in customers.items():
        pref = nname[:6]
        meta = _metaphone_primary(nname)
        sig = token_signature(nname)
        cand = Candidate(customer_id=cid, name=nname, raw_name=raw)

        by_prefix.setdefault(pref, []).append(cand)
        if meta:
            by_meta.setdefault(meta, []).append(cand)
        by_sig.setdefault(sig, []).append(cand)

    return {"by_prefix": by_prefix, "by_meta": by_meta, "by_sig": by_sig}


def generate_candidates(norm_query: str, index) -> List[Candidate]:
    """
    Union of candidates sharing any block with the query.
    """
    seen: Dict[int, Candidate] = {}
    pref = norm_query[:6]
    sig = token_signature(norm_query)
    meta = _metaphone_primary(norm_query)

    for c in index["by_prefix"].get(pref, ()):
        seen.setdefault(c.customer_id, c)
    for c in index["by_sig"].get(sig, ()):
        seen.setdefault(c.customer_id, c)
    if meta:
        for c in index["by_meta"].get(meta, ()):
            seen.setdefault(c.customer_id, c)

    return list(seen.values())


def score_name(q: str, n: str) -> float:
    """
    Prefer token-based RapidFuzz; fallback to a light-weight ratio.
    """
    if HAVE_RAPIDFUZZ:
        return max(fuzz.WRatio(q, n), fuzz.token_set_ratio(q, n)) / 100.0
    # Light fallback: Jaccard on tokens
    qt = set(q.split())
    nt = set(n.split())
    if not qt or not nt:
        return 0.0
    return len(qt & nt) / len(qt | nt)


# ---------- SQLite IO ----------
def connect_ro(db_path: str) -> sqlite3.Connection:
    uri = f"file:{Path(db_path).as_posix()}?mode=ro&cache=shared"
    con = sqlite3.connect(uri, uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON;")
    return con


def load_customer_maps(db_path: str) -> Tuple[Dict[str, Tuple[int, str]], Dict[str, Tuple[int, str]]]:
    """
    Returns:
      customers: normalized_name(normalized) -> (customer_id, raw_db_name)
      aliases:   normalized(alias_name) -> (customer_id, raw_db_name)
    """
    con = connect_ro(db_path)
    try:
        # active customers
        cur = con.execute("""
            SELECT customer_id, normalized_name
            FROM customers
            WHERE is_active = 1
        """)
        customers: Dict[str, Tuple[int, str]] = {}
        for r in cur.fetchall():
            # Important: normalize again to ensure code+DB share exact rules
            n = normalize_business_name(r["normalized_name"])
            customers[n] = (r["customer_id"], r["normalized_name"])

        # active aliases
        cur = con.execute("""
            SELECT ea.alias_name, ea.target_entity_id, c.normalized_name
            FROM entity_aliases ea
            JOIN customers c ON c.customer_id = ea.target_entity_id
            WHERE ea.entity_type = 'customer' AND ea.is_active = 1
        """)
        aliases: Dict[str, Tuple[int, str]] = {}
        for r in cur.fetchall():
            n_alias = normalize_business_name(r["alias_name"])
            aliases[n_alias] = (r["target_entity_id"], r["normalized_name"])
        return customers, aliases
    finally:
        con.close()


def aggregate_billcode_customers(db_path: str) -> Iterable[sqlite3.Row]:
    con = connect_ro(db_path)
    try:
        cur = con.execute("""
            SELECT 
              bill_code,
              COUNT(*) AS spot_count,
              COALESCE(SUM(station_net), 0) AS total_revenue,
              MIN(air_date) AS first_seen,
              MAX(air_date) AS last_seen,
              GROUP_CONCAT(DISTINCT broadcast_month) AS broadcast_months
            FROM spots
            WHERE bill_code IS NOT NULL
              AND bill_code != ''
              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
            GROUP BY bill_code
        """)
        return list(cur.fetchall())
    finally:
        con.close()


# ---------- Reporting / Export ----------
def summarize(matches: List[CustomerMatch]) -> Dict[str, float]:
    total = len(matches)
    s = {
        "total": total,
        "exact": sum(m.status == "exact" for m in matches),
        "alias": sum(m.status == "alias" for m in matches),
        "high_confidence": sum(m.status == "high_confidence" for m in matches),
        "review": sum(m.status == "review" for m in matches),
        "unknown": sum(m.status == "unknown" for m in matches),
        "total_revenue": sum(m.revenue for m in matches),
        "unknown_revenue": sum(m.revenue for m in matches if m.status == "unknown"),
        "review_revenue": sum(m.revenue for m in matches if m.status == "review"),
        "spots": sum(m.spot_count for m in matches),
        "unknown_spots": sum(m.spot_count for m in matches if m.status == "unknown"),
    }
    return s


def print_summary(s: Dict[str, float]) -> None:
    pct = lambda n: (n / s["total"] * 100) if s["total"] else 0.0
    print("\n" + "="*72)
    print("CUSTOMER NAME MATCHING — SUMMARY")
    print("="*72)
    print(f"Total unique:       {s['total']:,}")
    print(f"Exact:              {s['exact']:,} ({pct(s['exact']):.1f}%)")
    print(f"Alias:              {s['alias']:,} ({pct(s['alias']):.1f}%)")
    print(f"High confidence:    {s['high_confidence']:,} ({pct(s['high_confidence']):.1f}%)")
    print(f"Review:             {s['review']:,} ({pct(s['review']):.1f}%)")
    print(f"Unknown:            {s['unknown']:,} ({pct(s['unknown']):.1f}%)")
    print(f"Total revenue:      ${s['total_revenue']:,.2f}")
    print(f"Unknown revenue:    ${s['unknown_revenue']:,.2f}")
    print(f"Review revenue:     ${s['review_revenue']:,.2f}")
    print(f"Total spots:        {int(s['spots']):,}")
    print(f"Unknown spots:      {int(s['unknown_spots']):,}")


def print_detailed(matches: List[CustomerMatch], limit: int = 50) -> None:
    # Review first, Unknown next (both by revenue)
    review = [m for m in matches if m.status == "review"][:limit]
    unknown = [m for m in matches if m.status == "unknown"][:limit]

    if review:
        print("\nREVIEW (top by revenue)")
        print("-"*120)
        print(f"{'BillCode Name':<34} | {'Revenue':>12} | {'Best Match':<36} | {'Score':>5}")
        print("-"*120)
        for m in review:
            best = m.suggestions[0] if m.suggestions else ("", 0.0)
            print(f"{m.bill_code_name_raw[:34]:<34} | ${m.revenue:>10,.0f} | {best[0][:36]:<36} | {best[1]:.3f}")

    if unknown:
        print("\nUNKNOWN (top by revenue)")
        print("-"*120)
        print(f"{'BillCode Name':<50} | {'Revenue':>12} | {'First–Last Seen'}")
        print("-"*120)
        for m in unknown:
            rng = m.first_seen if m.first_seen == m.last_seen else f"{m.first_seen} to {m.last_seen}"
            print(f"{m.bill_code_name_raw[:50]:<50} | ${m.revenue:>10,.0f} | {rng}")


def export_unmatched_csv(matches: List[CustomerMatch], filename: Optional[str]) -> Optional[str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = filename or f"unmatched_customers_{ts}.csv"
    rows = [m for m in matches if m.status in ("review", "unknown")]
    if not rows:
        print("No review/unknown rows to export.")
        return None
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["customer_name_raw","norm_name","status","spot_count","revenue","first_seen","last_seen","months","suggestions_json"])
        for m in rows:
            w.writerow([
                m.bill_code_name_raw,
                m.norm_name,
                m.status,
                m.spot_count,
                f"{m.revenue:.2f}",
                m.first_seen,
                m.last_seen,
                "|".join(sorted(m.months)),
                json.dumps([{"name": n, "score": round(s,3)} for n,s in m.suggestions], ensure_ascii=False),
            ])
    print(f"Exported: {out}")
    return out


def suggest_alias_sql(matches: List[CustomerMatch],
                      min_revenue: float = 1000.0,
                      min_score: float = 0.85) -> None:
    cands = []
    for m in matches:
        if m.status in ("high_confidence", "review") and m.matched_customer_id and m.best_score >= min_score and m.revenue >= min_revenue:
            cands.append(m)
    if not cands:
        print("\nNo alias suggestions meeting thresholds.")
        return

    print("\nSUGGESTED ALIASES (review then run parameterized in your admin tool)")
    print("-"*110)
    print(f"{'Alias Name (from bill_code)':<40} | {'Target Customer':<36} | {'Score':>5} | {'Revenue':>10}")
    print("-"*110)
    for m in cands[:100]:
        print(f"{m.bill_code_name_raw[:40]:<40} | {m.matched_customer_name[:36]:<36} | {m.best_score:.3f} | ${m.revenue:>10,.0f}")

    # Parameterized template (safer)
    print("\n-- Parameterized SQL template (example):")
    print("""-- For each alias you approve, substitute :alias_name and :target_customer_id
INSERT INTO entity_aliases (alias_name, entity_type, target_entity_id, confidence_score, created_by, notes, is_active)
VALUES (:alias_name, 'customer', :target_customer_id, :confidence_score, :created_by, :notes, 1);
""")


# ---------- Main ----------
def main():
    p = argparse.ArgumentParser(description="Customer name matching with blocking + robust normalization")
    p.add_argument("--db-path", required=True)
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--export-unmatched", action="store_true")
    p.add_argument("--export-filename")
    p.add_argument("--suggest-aliases", action="store_true")
    p.add_argument("--alias-min-revenue", type=float, default=1000.0)
    p.add_argument("--alias-min-score", type=float, default=0.85)
    args = p.parse_args()

    if not Path(args.db_path).exists():
        print(f"Database not found: {args.db_path}")
        sys.exit(1)

    if not HAVE_RAPIDFUZZ:
        print("Warning: rapidfuzz not installed; falling back to a basic token Jaccard. Install with: pip install rapidfuzz")

    matches = analyze_customer_names(args.db_path, NORMALIZATION_CONFIG)
    s = summarize(matches)
    print_summary(s)
    print_detailed(matches, limit=args.limit)

    if args.suggest_aliases:
        suggest_alias_sql(matches, args.alias_min_revenue, args.alias_min_score)

    if args.export_unmatched:
        export_unmatched_csv(matches, args.export_filename)

    print("\nDone.")


if __name__ == "__main__":
    main()
