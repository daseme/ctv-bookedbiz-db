#!/usr/bin/env python3
"""
Unsplit Colon-Name Customers Tool

Discovers customers whose normalized_name contains "Agency:Customer" and
processes two scenarios:

Scenario 1: Both the agency and standalone customer already exist.
  Reassigns spots, aliases, sectors, and AE assignments from the colon-name
  source to the standalone target, then soft-deletes the source.

Scenario 2: Agency exists but NO active standalone customer does.
  Renames the colon-name customer to just the customer part, assigns the
  agency, sets agency_id on spots, and creates a preserving alias.
  Handles inactive collision records that would block the UNIQUE constraint.

Usage:
    python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 1                    # dry-run S1
    python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2                    # dry-run S2
    python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --customer-id 268  # one case
    python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --limit 5          # first 5
    python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --execute          # apply S2
"""

import argparse
import sqlite3
import sys
from dataclasses import dataclass, field
from typing import List, Optional


# ── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class ColonCase:
    """Discovery data for a single colon-name customer."""
    source_id: int
    source_name: str
    agency_part: str
    customer_part: str
    target_id: int
    target_name: str
    agency_id: int
    agency_name: str


@dataclass
class AliasInfo:
    alias_id: int
    alias_name: str
    target_entity_id: int
    is_active: int


@dataclass
class Scenario2Case:
    """Discovery data for a Scenario 2 colon-name customer (rename + assign agency)."""
    source_id: int          # The colon-name customer (will be renamed)
    source_name: str        # e.g., "Misfit:CA Community Colleges"
    agency_part: str        # e.g., "Misfit"
    customer_part: str      # e.g., "CA Community Colleges"
    agency_id: int
    agency_name: str
    collision_id: Optional[int] = None   # Inactive customer with same name (if any)
    collision_name: Optional[str] = None


@dataclass
class Scenario2Analysis:
    """Full impact analysis for a Scenario 2 unsplit (rename + assign agency)."""
    case: Scenario2Case

    # Source details
    source_active: bool = True
    source_created: str = ""
    source_sector: str = ""
    source_ae: str = ""

    # Spots impact
    spots_total: int = 0
    spots_revenue: float = 0.0
    spots_date_min: str = ""
    spots_date_max: str = ""
    spots_agency_null: int = 0      # Spots where agency_id IS NULL -> will be set
    spots_agency_match: int = 0     # Already has correct agency_id
    spots_agency_other: int = 0     # Has different agency_id (leave alone)

    # Alias
    existing_alias: Optional[AliasInfo] = None  # If colon-name alias already exists

    # Collision details
    collision_spots: int = 0        # Should be 0
    collision_aliases: int = 0      # Should be 0

    # Warnings
    warnings: List[str] = field(default_factory=list)


@dataclass
class CaseAnalysis:
    """Full impact analysis for a single unsplit operation."""
    case: ColonCase

    # Source details
    source_active: bool = True
    source_created: str = ""
    source_sector: str = ""
    source_sector_id: Optional[int] = None
    source_ae: str = ""

    # Target details
    target_active: bool = True
    target_created: str = ""
    target_sector: str = ""
    target_sector_id: Optional[int] = None
    target_ae: str = ""
    target_current_spots: int = 0
    target_current_revenue: float = 0.0

    # Agency details
    agency_active: bool = True

    # Spots impact
    spots_total: int = 0
    spots_revenue: float = 0.0
    spots_date_min: str = ""
    spots_date_max: str = ""
    spots_agency_null: int = 0
    spots_agency_match: int = 0
    spots_agency_other: int = 0
    overlap: bool = False

    # Aliases impact
    aliases_to_redirect: List[AliasInfo] = field(default_factory=list)
    existing_preserving_alias: Optional[AliasInfo] = None
    alias_conflicts: List[str] = field(default_factory=list)

    # Sectors impact
    source_sectors: list = field(default_factory=list)
    target_sectors: list = field(default_factory=list)
    sectors_to_add: list = field(default_factory=list)

    # AE assignments impact
    source_assignments: list = field(default_factory=list)
    target_assignments: list = field(default_factory=list)
    assignments_to_move: list = field(default_factory=list)
    assignments_to_skip: list = field(default_factory=list)

    # Warnings
    warnings: List[str] = field(default_factory=list)

    @property
    def after_spots(self):
        return self.target_current_spots + self.spots_total

    @property
    def after_revenue(self):
        return self.target_current_revenue + self.spots_revenue


# ── Database ─────────────────────────────────────────────────────────────────

def open_db(path: str, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
    else:
        conn = sqlite3.connect(path, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ── Discovery ────────────────────────────────────────────────────────────────

def discover_cases(
    conn: sqlite3.Connection,
    customer_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[ColonCase]:
    """Find colon-name customers where both agency and standalone customer exist."""
    base_query = """
        SELECT
            src.customer_id AS source_id,
            src.normalized_name AS source_name,
            TRIM(SUBSTR(src.normalized_name, 1, INSTR(src.normalized_name, ':') - 1)) AS agency_part,
            TRIM(SUBSTR(src.normalized_name, INSTR(src.normalized_name, ':') + 1)) AS customer_part,
            tgt.customer_id AS target_id,
            tgt.normalized_name AS target_name,
            ag.agency_id,
            ag.agency_name
        FROM customers src
        JOIN agencies ag
            ON ag.agency_name = TRIM(SUBSTR(src.normalized_name, 1, INSTR(src.normalized_name, ':') - 1)) COLLATE NOCASE
        JOIN customers tgt
            ON tgt.normalized_name = TRIM(SUBSTR(src.normalized_name, INSTR(src.normalized_name, ':') + 1)) COLLATE NOCASE
            AND tgt.customer_id != src.customer_id
        WHERE src.normalized_name LIKE '%:%'
          AND src.normalized_name NOT LIKE '%:%:%'
          AND tgt.is_active = 1
    """
    params = []

    if customer_id is not None:
        base_query += " AND src.customer_id = ?"
        params.append(customer_id)

    base_query += " ORDER BY src.customer_id"

    if limit is not None:
        base_query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(base_query, params).fetchall()

    # Deduplicate by source_id (case-insensitive agency joins can
    # produce multiple rows when e.g. "WorldLink" and "Worldlink" both exist).
    # Keep the first match (lowest agency_id) for each source.
    seen_source_ids = set()
    results = []
    for r in rows:
        if r["source_id"] in seen_source_ids:
            continue
        seen_source_ids.add(r["source_id"])
        results.append(ColonCase(
            source_id=r["source_id"],
            source_name=r["source_name"],
            agency_part=r["agency_part"],
            customer_part=r["customer_part"],
            target_id=r["target_id"],
            target_name=r["target_name"],
            agency_id=r["agency_id"],
            agency_name=r["agency_name"],
        ))
    return results


def discover_scenario2_cases(
    conn: sqlite3.Connection,
    customer_id: Optional[int] = None,
    limit: Optional[int] = None,
) -> List[Scenario2Case]:
    """Find colon-name customers where agency exists but NO active standalone customer does.

    These need renaming to just the customer part + agency assignment.
    Also detects inactive collision records that would block the UNIQUE constraint.
    """
    base_query = """
        SELECT
            src.customer_id AS source_id,
            src.normalized_name AS source_name,
            TRIM(SUBSTR(src.normalized_name, 1, INSTR(src.normalized_name, ':') - 1)) AS agency_part,
            TRIM(SUBSTR(src.normalized_name, INSTR(src.normalized_name, ':') + 1)) AS customer_part,
            ag.agency_id,
            ag.agency_name,
            inactive.customer_id AS collision_id,
            inactive.normalized_name AS collision_name
        FROM customers src
        JOIN agencies ag
            ON ag.agency_name = TRIM(SUBSTR(src.normalized_name, 1, INSTR(src.normalized_name, ':') - 1)) COLLATE NOCASE
        LEFT JOIN customers tgt
            ON tgt.normalized_name = TRIM(SUBSTR(src.normalized_name, INSTR(src.normalized_name, ':') + 1)) COLLATE NOCASE
            AND tgt.customer_id != src.customer_id
            AND tgt.is_active = 1
        LEFT JOIN customers inactive
            ON inactive.normalized_name = TRIM(SUBSTR(src.normalized_name, INSTR(src.normalized_name, ':') + 1)) COLLATE NOCASE
            AND inactive.customer_id != src.customer_id
            AND inactive.is_active = 0
        WHERE src.normalized_name LIKE '%:%'
          AND src.normalized_name NOT LIKE '%:%:%'
          AND src.is_active = 1
          AND tgt.customer_id IS NULL
    """
    params = []

    if customer_id is not None:
        base_query += " AND src.customer_id = ?"
        params.append(customer_id)

    base_query += " ORDER BY src.customer_id"

    if limit is not None:
        base_query += " LIMIT ?"
        params.append(limit)

    rows = conn.execute(base_query, params).fetchall()

    # Deduplicate by source_id (case-insensitive joins can produce multiple rows)
    # AND by customer_part (two colon-names can share the same advertiser part;
    # only process one per run — the second becomes Scenario 1 on re-run)
    seen_source_ids = set()
    seen_customer_parts = set()
    results = []
    for r in rows:
        if r["source_id"] in seen_source_ids:
            continue
        cpart_lower = r["customer_part"].lower()
        if cpart_lower in seen_customer_parts:
            continue
        seen_source_ids.add(r["source_id"])
        seen_customer_parts.add(cpart_lower)
        results.append(Scenario2Case(
            source_id=r["source_id"],
            source_name=r["source_name"],
            agency_part=r["agency_part"],
            customer_part=r["customer_part"],
            agency_id=r["agency_id"],
            agency_name=r["agency_name"],
            collision_id=r["collision_id"],
            collision_name=r["collision_name"],
        ))
    return results


# ── Analysis ─────────────────────────────────────────────────────────────────

def analyze_case(conn: sqlite3.Connection, case: ColonCase) -> CaseAnalysis:
    """Build full impact analysis for a single colon-name unsplit."""
    a = CaseAnalysis(case=case)

    # ── Source details ────────────────────────────────────────────────────
    src = conn.execute("""
        SELECT is_active, created_date, sector_id, assigned_ae
        FROM customers WHERE customer_id = ?
    """, [case.source_id]).fetchone()
    if src:
        a.source_active = bool(src["is_active"])
        a.source_created = src["created_date"] or ""
        a.source_sector_id = src["sector_id"]
        a.source_ae = src["assigned_ae"] or ""
        if src["sector_id"]:
            sec = conn.execute("SELECT sector_name FROM sectors WHERE sector_id = ?",
                               [src["sector_id"]]).fetchone()
            a.source_sector = sec["sector_name"] if sec else ""

    # ── Target details ───────────────────────────────────────────────────
    tgt = conn.execute("""
        SELECT is_active, created_date, sector_id, assigned_ae
        FROM customers WHERE customer_id = ?
    """, [case.target_id]).fetchone()
    if tgt:
        a.target_active = bool(tgt["is_active"])
        a.target_created = tgt["created_date"] or ""
        a.target_sector_id = tgt["sector_id"]
        a.target_ae = tgt["assigned_ae"] or ""
        if tgt["sector_id"]:
            sec = conn.execute("SELECT sector_name FROM sectors WHERE sector_id = ?",
                               [tgt["sector_id"]]).fetchone()
            a.target_sector = sec["sector_name"] if sec else ""

    # Target current spots/revenue
    tgt_stats = conn.execute("""
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                            THEN gross_rate ELSE 0 END), 0) AS rev
        FROM spots WHERE customer_id = ?
    """, [case.target_id]).fetchone()
    a.target_current_spots = tgt_stats["cnt"]
    a.target_current_revenue = tgt_stats["rev"]

    # ── Agency details ───────────────────────────────────────────────────
    ag = conn.execute("SELECT is_active FROM agencies WHERE agency_id = ?",
                      [case.agency_id]).fetchone()
    if ag:
        a.agency_active = bool(ag["is_active"])

    # ── Spots impact ─────────────────────────────────────────────────────
    spot_stats = conn.execute("""
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                            THEN gross_rate ELSE 0 END), 0) AS rev,
               MIN(air_date) AS d_min,
               MAX(air_date) AS d_max
        FROM spots WHERE customer_id = ?
    """, [case.source_id]).fetchone()
    a.spots_total = spot_stats["cnt"]
    a.spots_revenue = spot_stats["rev"]
    a.spots_date_min = spot_stats["d_min"] or ""
    a.spots_date_max = spot_stats["d_max"] or ""

    # Agency ID breakdown on source spots
    agency_breakdown = conn.execute("""
        SELECT
            SUM(CASE WHEN agency_id IS NULL THEN 1 ELSE 0 END) AS null_count,
            SUM(CASE WHEN agency_id = ? THEN 1 ELSE 0 END) AS match_count,
            SUM(CASE WHEN agency_id IS NOT NULL AND agency_id != ? THEN 1 ELSE 0 END) AS other_count
        FROM spots WHERE customer_id = ?
    """, [case.agency_id, case.agency_id, case.source_id]).fetchone()
    a.spots_agency_null = agency_breakdown["null_count"] or 0
    a.spots_agency_match = agency_breakdown["match_count"] or 0
    a.spots_agency_other = agency_breakdown["other_count"] or 0

    # Check overlap (target already has spots from same agency)
    if a.target_current_spots > 0:
        overlap_check = conn.execute("""
            SELECT COUNT(*) AS cnt FROM spots
            WHERE customer_id = ? AND agency_id = ?
        """, [case.target_id, case.agency_id]).fetchone()
        if overlap_check["cnt"] > 0:
            a.overlap = True
            a.warnings.append(
                f"[OVERLAP] Target already has {overlap_check['cnt']} spots from same agency"
            )

    # ── Aliases impact ───────────────────────────────────────────────────
    # Active aliases pointing at source
    source_aliases = conn.execute("""
        SELECT alias_id, alias_name, target_entity_id, is_active
        FROM entity_aliases
        WHERE target_entity_id = ? AND entity_type = 'customer' AND is_active = 1
    """, [case.source_id]).fetchall()
    a.aliases_to_redirect = [
        AliasInfo(r["alias_id"], r["alias_name"], r["target_entity_id"], r["is_active"])
        for r in source_aliases
    ]

    # Check if preserving alias already exists
    existing = conn.execute("""
        SELECT alias_id, alias_name, target_entity_id, is_active
        FROM entity_aliases
        WHERE alias_name = ? AND entity_type = 'customer'
    """, [case.source_name]).fetchone()
    if existing:
        a.existing_preserving_alias = AliasInfo(
            existing["alias_id"], existing["alias_name"],
            existing["target_entity_id"], existing["is_active"]
        )
        if existing["target_entity_id"] == case.target_id:
            pass  # Already correct
        elif existing["target_entity_id"] == case.source_id:
            pass  # Will be redirected in the alias move step
        else:
            a.alias_conflicts.append(
                f"Alias '{case.source_name}' points to customer #{existing['target_entity_id']}, "
                f"not source #{case.source_id} or target #{case.target_id}"
            )
            a.warnings.append(f"[ALIAS CONFLICT] {a.alias_conflicts[-1]}")

    # ── Sectors impact ───────────────────────────────────────────────────
    a.source_sectors = conn.execute("""
        SELECT cs.sector_id, s.sector_name, cs.is_primary
        FROM customer_sectors cs
        JOIN sectors s ON s.sector_id = cs.sector_id
        WHERE cs.customer_id = ?
    """, [case.source_id]).fetchall()

    a.target_sectors = conn.execute("""
        SELECT cs.sector_id, s.sector_name, cs.is_primary
        FROM customer_sectors cs
        JOIN sectors s ON s.sector_id = cs.sector_id
        WHERE cs.customer_id = ?
    """, [case.target_id]).fetchall()

    target_sector_ids = {r["sector_id"] for r in a.target_sectors}
    a.sectors_to_add = [r for r in a.source_sectors if r["sector_id"] not in target_sector_ids]

    # ── AE assignments impact ────────────────────────────────────────────
    a.source_assignments = conn.execute("""
        SELECT assignment_id, ae_name, assigned_date, ended_date
        FROM ae_assignments
        WHERE entity_type = 'customer' AND entity_id = ?
    """, [case.source_id]).fetchall()

    a.target_assignments = conn.execute("""
        SELECT assignment_id, ae_name, assigned_date, ended_date
        FROM ae_assignments
        WHERE entity_type = 'customer' AND entity_id = ?
    """, [case.target_id]).fetchall()

    # Determine which assignments to move vs skip
    target_active_aes = {
        r["ae_name"] for r in a.target_assignments if r["ended_date"] is None
    }
    for sa in a.source_assignments:
        if sa["ended_date"] is None and sa["ae_name"] in target_active_aes:
            a.assignments_to_skip.append(sa)
        else:
            a.assignments_to_move.append(sa)

    if not a.source_active:
        a.warnings.append("[INACTIVE SOURCE] Source already deactivated — will still process")

    if a.spots_total == 0:
        a.warnings.append("[ZERO SPOTS] Source has no spots — will still move aliases/sectors")

    return a


def analyze_scenario2(conn: sqlite3.Connection, case: Scenario2Case) -> Scenario2Analysis:
    """Build full impact analysis for a Scenario 2 unsplit (rename + assign agency)."""
    a = Scenario2Analysis(case=case)

    # ── Source details ────────────────────────────────────────────────────
    src = conn.execute("""
        SELECT is_active, created_date, sector_id, assigned_ae
        FROM customers WHERE customer_id = ?
    """, [case.source_id]).fetchone()
    if src:
        a.source_active = bool(src["is_active"])
        a.source_created = src["created_date"] or ""
        a.source_ae = src["assigned_ae"] or ""
        if src["sector_id"]:
            sec = conn.execute("SELECT sector_name FROM sectors WHERE sector_id = ?",
                               [src["sector_id"]]).fetchone()
            a.source_sector = sec["sector_name"] if sec else ""

    # ── Spots impact ─────────────────────────────────────────────────────
    spot_stats = conn.execute("""
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL
                            THEN gross_rate ELSE 0 END), 0) AS rev,
               MIN(air_date) AS d_min,
               MAX(air_date) AS d_max
        FROM spots WHERE customer_id = ?
    """, [case.source_id]).fetchone()
    a.spots_total = spot_stats["cnt"]
    a.spots_revenue = spot_stats["rev"]
    a.spots_date_min = spot_stats["d_min"] or ""
    a.spots_date_max = spot_stats["d_max"] or ""

    # Agency ID breakdown on source spots
    agency_breakdown = conn.execute("""
        SELECT
            SUM(CASE WHEN agency_id IS NULL THEN 1 ELSE 0 END) AS null_count,
            SUM(CASE WHEN agency_id = ? THEN 1 ELSE 0 END) AS match_count,
            SUM(CASE WHEN agency_id IS NOT NULL AND agency_id != ? THEN 1 ELSE 0 END) AS other_count
        FROM spots WHERE customer_id = ?
    """, [case.agency_id, case.agency_id, case.source_id]).fetchone()
    a.spots_agency_null = agency_breakdown["null_count"] or 0
    a.spots_agency_match = agency_breakdown["match_count"] or 0
    a.spots_agency_other = agency_breakdown["other_count"] or 0

    # ── Existing alias check ─────────────────────────────────────────────
    existing = conn.execute("""
        SELECT alias_id, alias_name, target_entity_id, is_active
        FROM entity_aliases
        WHERE alias_name = ? AND entity_type = 'customer'
    """, [case.source_name]).fetchone()
    if existing:
        a.existing_alias = AliasInfo(
            existing["alias_id"], existing["alias_name"],
            existing["target_entity_id"], existing["is_active"]
        )

    # ── Collision details ────────────────────────────────────────────────
    if case.collision_id:
        col_spots = conn.execute(
            "SELECT COUNT(*) AS cnt FROM spots WHERE customer_id = ?",
            [case.collision_id]
        ).fetchone()
        a.collision_spots = col_spots["cnt"]

        col_aliases = conn.execute("""
            SELECT COUNT(*) AS cnt FROM entity_aliases
            WHERE target_entity_id = ? AND entity_type = 'customer' AND is_active = 1
        """, [case.collision_id]).fetchone()
        a.collision_aliases = col_aliases["cnt"]

        if a.collision_spots > 0:
            a.warnings.append(
                f"[COLLISION HAS SPOTS] Inactive customer #{case.collision_id} has {a.collision_spots} spots"
            )
        if a.collision_aliases > 0:
            a.warnings.append(
                f"[COLLISION HAS ALIASES] Inactive customer #{case.collision_id} has {a.collision_aliases} active aliases"
            )

    # ── General warnings ─────────────────────────────────────────────────
    if not a.source_active:
        a.warnings.append("[INACTIVE SOURCE] Source already deactivated")

    if a.spots_total == 0:
        a.warnings.append("[ZERO SPOTS] Source has no spots — will still rename and assign agency")

    return a


# ── Display ──────────────────────────────────────────────────────────────────

def fmt_money(val: float) -> str:
    return f"${val:,.2f}"


def print_case(index: int, total: int, a: CaseAnalysis):
    c = a.case
    w = 70

    print(f"\n{'═' * w}")
    label = f"CASE {index} of {total}: {c.source_name}"
    if a.warnings:
        tags = " ".join(w.split("]")[0] + "]" for w in a.warnings if w.startswith("["))
        label += f"  {tags}"
    print(label)
    print(f"{'═' * w}")

    # Source
    print(f"\n  SOURCE (colon-name customer to retire):")
    print(f"    customer_id:   {c.source_id}")
    print(f"    Name:          {c.source_name}")
    print(f"    Active:        {'Yes' if a.source_active else 'No'}")
    print(f"    Created:       {a.source_created or '(unknown)'}")
    sector_str = f"{a.source_sector} (id={a.source_sector_id})" if a.source_sector else "(none)"
    print(f"    Sector:        {sector_str}")
    print(f"    Assigned AE:   {a.source_ae or '(none)'}")

    # Target
    print(f"\n  TARGET (standalone customer to receive spots):")
    print(f"    customer_id:   {c.target_id}")
    print(f"    Name:          {c.target_name}")
    print(f"    Active:        {'Yes' if a.target_active else 'No'}")
    print(f"    Created:       {a.target_created or '(unknown)'}")
    sector_str = f"{a.target_sector} (id={a.target_sector_id})" if a.target_sector else "(none)"
    print(f"    Sector:        {sector_str}")
    print(f"    Assigned AE:   {a.target_ae or '(none)'}")
    print(f"    Current spots: {a.target_current_spots:,}")
    print(f"    After merge:   {a.after_spots:,} spots / {fmt_money(a.after_revenue)} revenue")

    # Agency
    print(f"\n  AGENCY:")
    print(f"    agency_id:     {c.agency_id}")
    print(f"    Name:          {c.agency_name}")
    print(f"    Active:        {'Yes' if a.agency_active else 'No'}")

    # Spots
    print(f"\n  SPOTS IMPACT:")
    print(f"    Moving {a.spots_total:,} spots ({fmt_money(a.spots_revenue)} revenue)")
    if a.spots_total > 0:
        print(f"    Date range: {a.spots_date_min} to {a.spots_date_max}")
        print(f"    agency_id on source spots:")
        print(f"      NULL -> will set to {c.agency_id}:{' ' * max(1, 8 - len(str(a.spots_agency_null)))}{a.spots_agency_null:,}")
        print(f"      Already agency_id={c.agency_id}:{' ' * max(1, 5 - len(str(a.spots_agency_match)))}{a.spots_agency_match:,}")
        print(f"      Different agency_id (keep):{' ' * max(1, 4 - len(str(a.spots_agency_other)))}{a.spots_agency_other:,}")

    # Aliases
    print(f"\n  ALIASES IMPACT:")
    print(f"    Redirecting {len(a.aliases_to_redirect)} alias(es) to target")
    for al in a.aliases_to_redirect:
        print(f"      - \"{al.alias_name}\" (alias #{al.alias_id})")
    if a.existing_preserving_alias:
        ea = a.existing_preserving_alias
        if ea.target_entity_id == c.target_id:
            print(f"    Preserving alias already exists: \"{ea.alias_name}\" -> #{ea.target_entity_id}")
        elif ea.target_entity_id == c.source_id:
            print(f"    Preserving alias will be redirected (currently -> #{ea.target_entity_id})")
        else:
            print(f"    [CONFLICT] Preserving alias points elsewhere: -> #{ea.target_entity_id}")
    else:
        print(f"    New preserving alias: \"{c.source_name}\" -> #{c.target_id}")
    if a.alias_conflicts:
        print(f"    Conflicts:")
        for conflict in a.alias_conflicts:
            print(f"      - {conflict}")
    elif not a.alias_conflicts:
        print(f"    Conflicts: none")

    # Sectors
    print(f"\n  SECTORS IMPACT:")
    src_sectors_str = ", ".join(
        f"{r['sector_name']}{'(primary)' if r['is_primary'] else ''}"
        for r in a.source_sectors
    ) or "(none)"
    tgt_sectors_str = ", ".join(
        f"{r['sector_name']}{'(primary)' if r['is_primary'] else ''}"
        for r in a.target_sectors
    ) or "(none)"
    print(f"    Source: {src_sectors_str}")
    print(f"    Target: {tgt_sectors_str}")
    if a.sectors_to_add:
        names = ", ".join(r["sector_name"] for r in a.sectors_to_add)
        print(f"    Action: adding {len(a.sectors_to_add)} sector(s) to target: {names}")
    else:
        print(f"    Action: no changes (already aligned)")

    # AE assignments
    print(f"\n  AE ASSIGNMENTS:")
    src_ae_str = ", ".join(
        f"{r['ae_name']}{'(active)' if r['ended_date'] is None else '(ended)'}"
        for r in a.source_assignments
    ) or "(none)"
    tgt_ae_str = ", ".join(
        f"{r['ae_name']}{'(active)' if r['ended_date'] is None else '(ended)'}"
        for r in a.target_assignments
    ) or "(none)"
    print(f"    Source: {src_ae_str}")
    print(f"    Target: {tgt_ae_str}")
    if a.assignments_to_move:
        print(f"    Moving {len(a.assignments_to_move)} assignment(s)")
    if a.assignments_to_skip:
        names = ", ".join(r["ae_name"] for r in a.assignments_to_skip)
        print(f"    Skipping {len(a.assignments_to_skip)} (duplicate active): {names}")
    if not a.assignments_to_move and not a.assignments_to_skip:
        print(f"    Action: no changes")

    # Warnings
    if a.warnings:
        print(f"\n  WARNINGS:")
        for warn in a.warnings:
            print(f"    {warn}")

    print(f"\n{'─' * w}")


def print_summary(results: List[CaseAnalysis]):
    w = 70
    print(f"\n{'═' * w}")
    print(f"SUMMARY: {len(results)} cases analyzed")
    print(f"{'═' * w}")

    total_spots = sum(a.spots_total for a in results)
    total_revenue = sum(a.spots_revenue for a in results)
    total_aliases = sum(len(a.aliases_to_redirect) for a in results)
    total_sectors = sum(len(a.sectors_to_add) for a in results)
    total_ae_moves = sum(len(a.assignments_to_move) for a in results)
    overlap_count = sum(1 for a in results if a.overlap)
    zero_spots = sum(1 for a in results if a.spots_total == 0)
    inactive_sources = sum(1 for a in results if not a.source_active)
    conflict_cases = [a for a in results if a.alias_conflicts]

    print(f"\n  Totals:")
    print(f"    Cases:           {len(results):,}")
    print(f"    Spots to move:   {total_spots:,}")
    print(f"    Revenue:         {fmt_money(total_revenue)}")
    print(f"    Aliases to move: {total_aliases:,}")
    print(f"    Sectors to add:  {total_sectors:,}")
    print(f"    AE to move:      {total_ae_moves:,}")

    if overlap_count or zero_spots or inactive_sources or conflict_cases:
        print(f"\n  Flags:")
        if overlap_count:
            print(f"    [OVERLAP] cases:        {overlap_count}")
        if zero_spots:
            print(f"    [ZERO SPOTS] cases:     {zero_spots}")
        if inactive_sources:
            print(f"    [INACTIVE SOURCE] cases: {inactive_sources}")
        if conflict_cases:
            print(f"    [ALIAS CONFLICT] cases:  {len(conflict_cases)}")
            for a in conflict_cases:
                for c in a.alias_conflicts:
                    print(f"      - Case #{a.case.source_id}: {c}")

    print(f"\n{'─' * w}")


# ── Execution ────────────────────────────────────────────────────────────────

def execute_case(conn: sqlite3.Connection, a: CaseAnalysis) -> dict:
    """Apply a single unsplit in its own transaction. Returns stats."""
    c = a.case
    stats = {"source_id": c.source_id, "target_id": c.target_id, "steps": []}

    conn.execute("BEGIN IMMEDIATE")
    try:
        # Step 1: Set agency_id on source spots where NULL
        if a.spots_agency_null > 0:
            rc = conn.execute("""
                UPDATE spots SET agency_id = ?
                WHERE customer_id = ? AND agency_id IS NULL
            """, [c.agency_id, c.source_id]).rowcount
            stats["steps"].append(f"Set agency_id on {rc} spots")

        # Step 2: Move spots from source -> target
        if a.spots_total > 0:
            rc = conn.execute("""
                UPDATE spots SET customer_id = ?
                WHERE customer_id = ?
            """, [c.target_id, c.source_id]).rowcount
            stats["steps"].append(f"Moved {rc} spots")
            stats["spots_moved"] = rc

        # Step 3: Redirect active aliases from source -> target
        if a.aliases_to_redirect:
            for al in a.aliases_to_redirect:
                # Check if target already has an alias with this name
                conflict = conn.execute("""
                    SELECT alias_id, target_entity_id FROM entity_aliases
                    WHERE alias_name = ? AND entity_type = 'customer' AND alias_id != ?
                """, [al.alias_name, al.alias_id]).fetchone()
                if conflict and conflict["target_entity_id"] == c.target_id:
                    # Duplicate — deactivate the source's copy
                    conn.execute("""
                        UPDATE entity_aliases
                        SET is_active = 0, updated_date = CURRENT_TIMESTAMP,
                            notes = COALESCE(notes, '') || ' | Deactivated: duplicate after unsplit at ' || datetime('now')
                        WHERE alias_id = ?
                    """, [al.alias_id])
                    stats["steps"].append(f"Deactivated duplicate alias #{al.alias_id} '{al.alias_name}'")
                elif conflict:
                    # Points elsewhere — skip with warning
                    stats["steps"].append(f"Skipped alias #{al.alias_id} '{al.alias_name}' (conflict with #{conflict['alias_id']})")
                else:
                    conn.execute("""
                        UPDATE entity_aliases
                        SET target_entity_id = ?, updated_date = CURRENT_TIMESTAMP,
                            notes = COALESCE(notes, '') || ' | Redirected from customer ' || ? || ' by unsplit at ' || datetime('now')
                        WHERE alias_id = ?
                    """, [c.target_id, c.source_id, al.alias_id])
                    stats["steps"].append(f"Redirected alias #{al.alias_id} '{al.alias_name}'")

        # Step 4: Create preserving alias (colon_name -> target)
        # Must happen BEFORE soft-delete (trigger checks target is_active)
        if a.existing_preserving_alias:
            ea = a.existing_preserving_alias
            if ea.target_entity_id == c.target_id:
                stats["steps"].append("Preserving alias already exists")
            elif ea.target_entity_id == c.source_id:
                # Was redirected in step 3 (or needs explicit redirect)
                conn.execute("""
                    UPDATE entity_aliases
                    SET target_entity_id = ?, updated_date = CURRENT_TIMESTAMP,
                        notes = COALESCE(notes, '') || ' | Redirected to target by unsplit at ' || datetime('now')
                    WHERE alias_id = ?
                """, [c.target_id, ea.alias_id])
                stats["steps"].append(f"Redirected preserving alias #{ea.alias_id}")
            else:
                stats["steps"].append(f"Skipped preserving alias (conflict: points to #{ea.target_entity_id})")
        else:
            # Re-check at execution time (another case may have created it)
            live_check = conn.execute("""
                SELECT alias_id, target_entity_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'customer'
            """, [c.source_name]).fetchone()
            if live_check:
                stats["steps"].append(
                    f"Preserving alias already exists at execution time (alias #{live_check['alias_id']} -> #{live_check['target_entity_id']})"
                )
            else:
                conn.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                     created_by, notes, is_active)
                    VALUES (?, 'customer', ?, 100, 'unsplit_tool',
                            'Preserving alias from unsplit of customer ' || ?, 1)
                """, [c.source_name, c.target_id, c.source_id])
                stats["steps"].append(f"Created preserving alias '{c.source_name}' -> #{c.target_id}")

        # Step 5: Copy missing sectors to target
        if a.sectors_to_add:
            for sec_row in a.sectors_to_add:
                conn.execute("""
                    INSERT OR IGNORE INTO customer_sectors
                    (customer_id, sector_id, is_primary, assigned_by)
                    VALUES (?, ?, 0, 'unsplit_tool')
                """, [c.target_id, sec_row["sector_id"]])
            stats["steps"].append(f"Added {len(a.sectors_to_add)} sector(s) to target")

        # Step 6: Move non-duplicate AE assignments
        for assign in a.assignments_to_move:
            conn.execute("""
                UPDATE ae_assignments
                SET entity_id = ?,
                    notes = COALESCE(notes, '') || ' | Moved from customer ' || ? || ' by unsplit at ' || datetime('now')
                WHERE assignment_id = ?
            """, [c.target_id, c.source_id, assign["assignment_id"]])
            stats["steps"].append(f"Moved AE assignment #{assign['assignment_id']} ({assign['ae_name']})")
        for assign in a.assignments_to_skip:
            # End the duplicate source assignment
            conn.execute("""
                UPDATE ae_assignments
                SET ended_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Ended: duplicate active on target by unsplit at ' || datetime('now')
                WHERE assignment_id = ?
            """, [assign["assignment_id"]])
            stats["steps"].append(f"Ended duplicate AE assignment #{assign['assignment_id']} ({assign['ae_name']})")

        # Step 7: Soft-delete source customer
        conn.execute("""
            UPDATE customers
            SET is_active = 0,
                updated_date = CURRENT_TIMESTAMP,
                notes = COALESCE(notes, '') || ' | Unsplit into customer ' || ? || ' (agency ' || ? || ') by unsplit_tool at ' || datetime('now')
            WHERE customer_id = ?
        """, [c.target_id, c.agency_id, c.source_id])
        stats["steps"].append(f"Soft-deleted source customer #{c.source_id}")

        # Step 8: Audit record
        conn.execute("""
            INSERT INTO canon_audit (actor, action, key, value, extra)
            VALUES ('unsplit_tool', 'UNSPLIT', ?, ?, ?)
        """, [
            f"customer:{c.source_id}",
            f"customer:{c.target_id}",
            f"source_name={c.source_name}|target_name={c.target_name}|agency_id={c.agency_id}|spots={a.spots_total}|revenue={a.spots_revenue:.2f}"
        ])
        stats["steps"].append("Audit record inserted")

        conn.execute("COMMIT")
        stats["success"] = True

    except Exception as e:
        conn.execute("ROLLBACK")
        stats["success"] = False
        stats["error"] = str(e)

    return stats


def execute_scenario2_case(conn: sqlite3.Connection, a: Scenario2Analysis) -> dict:
    """Apply a single Scenario 2 unsplit (rename + assign agency). Returns stats."""
    c = a.case
    stats = {"source_id": c.source_id, "steps": []}

    conn.execute("BEGIN IMMEDIATE")
    try:
        # Step 1: Handle collision (rename inactive customer out of the way)
        if c.collision_id:
            retired_name = f"{c.collision_name} #retired-{c.collision_id}"
            conn.execute("""
                UPDATE customers
                SET normalized_name = ?,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Retired by unsplit_tool Scenario 2 at ' || datetime('now')
                WHERE customer_id = ?
            """, [retired_name, c.collision_id])
            stats["steps"].append(f"Renamed collision #{c.collision_id} to '{retired_name}'")

        # Step 2: Rename source customer + set agency_id
        conn.execute("""
            UPDATE customers
            SET normalized_name = ?,
                agency_id = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE customer_id = ?
        """, [c.customer_part, c.agency_id, c.source_id])
        stats["steps"].append(f"Renamed #{c.source_id} to '{c.customer_part}', agency_id={c.agency_id}")

        # Step 3: Set agency_id on spots where NULL
        if a.spots_agency_null > 0:
            rc = conn.execute("""
                UPDATE spots SET agency_id = ?
                WHERE customer_id = ? AND agency_id IS NULL
            """, [c.agency_id, c.source_id]).rowcount
            stats["steps"].append(f"Set agency_id on {rc} spots")

        # Step 4: Create preserving alias (old colon name -> same customer_id)
        if a.existing_alias:
            ea = a.existing_alias
            if ea.target_entity_id == c.source_id and ea.is_active:
                stats["steps"].append(f"Preserving alias already exists (alias #{ea.alias_id})")
            elif ea.target_entity_id == c.source_id and not ea.is_active:
                # Reactivate it
                conn.execute("""
                    UPDATE entity_aliases
                    SET is_active = 1, updated_date = CURRENT_TIMESTAMP,
                        notes = COALESCE(notes, '') || ' | Reactivated by unsplit_tool S2 at ' || datetime('now')
                    WHERE alias_id = ?
                """, [ea.alias_id])
                stats["steps"].append(f"Reactivated preserving alias #{ea.alias_id}")
            else:
                stats["steps"].append(
                    f"Preserving alias exists but points to #{ea.target_entity_id} (skipped)"
                )
        else:
            # Re-check at execution time (another case may have created it)
            live_check = conn.execute("""
                SELECT alias_id, target_entity_id FROM entity_aliases
                WHERE alias_name = ? AND entity_type = 'customer'
            """, [c.source_name]).fetchone()
            if live_check:
                stats["steps"].append(
                    f"Preserving alias already exists at execution time (alias #{live_check['alias_id']})"
                )
            else:
                conn.execute("""
                    INSERT INTO entity_aliases
                    (alias_name, entity_type, target_entity_id, confidence_score,
                     created_by, notes, is_active)
                    VALUES (?, 'customer', ?, 100, 'unsplit_tool',
                            'Preserving alias from S2 unsplit of customer ' || ?, 1)
                """, [c.source_name, c.source_id, c.source_id])
                stats["steps"].append(f"Created preserving alias '{c.source_name}' -> #{c.source_id}")

        # Step 5: Audit record
        extra_parts = [
            f"source_name={c.source_name}",
            f"new_name={c.customer_part}",
            f"agency_id={c.agency_id}",
            f"spots={a.spots_total}",
            f"revenue={a.spots_revenue:.2f}",
        ]
        if c.collision_id:
            extra_parts.append(f"collision_id={c.collision_id}")
        conn.execute("""
            INSERT INTO canon_audit (actor, action, key, value, extra)
            VALUES ('unsplit_tool', 'UNSPLIT_S2', ?, ?, ?)
        """, [
            f"customer:{c.source_id}",
            f"renamed_to:{c.customer_part}",
            "|".join(extra_parts),
        ])
        stats["steps"].append("Audit record inserted")

        conn.execute("COMMIT")
        stats["success"] = True

    except Exception as e:
        conn.execute("ROLLBACK")
        stats["success"] = False
        stats["error"] = str(e)

    return stats


def rebuild_metrics(conn: sqlite3.Connection, customer_ids: set, agency_ids: set):
    """Targeted rebuild of entity_metrics for affected entities."""
    print(f"\nRebuilding entity_metrics for {len(customer_ids)} customers and {len(agency_ids)} agencies...")

    # Delete stale rows for affected entities
    for cid in customer_ids:
        conn.execute("DELETE FROM entity_metrics WHERE entity_type = 'customer' AND entity_id = ?", [cid])
    for aid in agency_ids:
        conn.execute("DELETE FROM entity_metrics WHERE entity_type = 'agency' AND entity_id = ?", [aid])

    # Rebuild customer metrics
    if customer_ids:
        placeholders = ",".join("?" * len(customer_ids))
        conn.execute(f"""
            INSERT OR REPLACE INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count, agency_spot_count)
            SELECT
                'customer', customer_id,
                GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
                MAX(air_date),
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
                COUNT(*),
                COUNT(agency_id)
            FROM spots
            WHERE customer_id IN ({placeholders})
            GROUP BY customer_id
        """, list(customer_ids))

    # Rebuild agency metrics
    if agency_ids:
        placeholders = ",".join("?" * len(agency_ids))
        conn.execute(f"""
            INSERT OR REPLACE INTO entity_metrics (entity_type, entity_id, markets, last_active, total_revenue, spot_count)
            SELECT
                'agency', agency_id,
                GROUP_CONCAT(DISTINCT CASE WHEN market_name != '' THEN market_name END),
                MAX(air_date),
                SUM(CASE WHEN revenue_type != 'Trade' OR revenue_type IS NULL THEN gross_rate ELSE 0 END),
                COUNT(*)
            FROM spots
            WHERE agency_id IN ({placeholders})
            GROUP BY agency_id
        """, list(agency_ids))

    conn.commit()
    print("  entity_metrics rebuilt.")

    # Delete stale entity_signals for affected entities (rebuilt at next import)
    signals_deleted = 0
    for cid in customer_ids:
        signals_deleted += conn.execute(
            "DELETE FROM entity_signals WHERE entity_type = 'customer' AND entity_id = ?", [cid]
        ).rowcount
    for aid in agency_ids:
        signals_deleted += conn.execute(
            "DELETE FROM entity_signals WHERE entity_type = 'agency' AND entity_id = ?", [aid]
        ).rowcount
    conn.commit()
    print(f"  Deleted {signals_deleted} stale entity_signals rows (rebuilt at next import).")


def verify_execution(conn: sqlite3.Connection, results: List[CaseAnalysis]):
    """Post-execution verification queries."""
    w = 70
    print(f"\n{'═' * w}")
    print("POST-EXECUTION VERIFICATION")
    print(f"{'═' * w}")

    source_ids = [a.case.source_id for a in results]
    placeholders = ",".join("?" * len(source_ids))

    # 1. Orphaned spots still on deactivated sources
    orphaned = conn.execute(f"""
        SELECT s.customer_id, COUNT(*) AS cnt
        FROM spots s
        JOIN customers c ON c.customer_id = s.customer_id
        WHERE s.customer_id IN ({placeholders}) AND c.is_active = 0
        GROUP BY s.customer_id
    """, source_ids).fetchall()
    if orphaned:
        print(f"\n  [FAIL] Orphaned spots on deactivated sources:")
        for r in orphaned:
            print(f"    customer_id={r['customer_id']}: {r['cnt']} spots")
    else:
        print(f"\n  [OK] No orphaned spots on deactivated sources")

    # 2. Preserving aliases exist
    missing_aliases = []
    for a in results:
        exists = conn.execute("""
            SELECT 1 FROM entity_aliases
            WHERE alias_name = ? AND entity_type = 'customer' AND is_active = 1
        """, [a.case.source_name]).fetchone()
        if not exists:
            missing_aliases.append(a.case.source_name)
    if missing_aliases:
        print(f"\n  [WARN] Missing preserving aliases ({len(missing_aliases)}):")
        for name in missing_aliases[:10]:
            print(f"    - {name}")
        if len(missing_aliases) > 10:
            print(f"    ... and {len(missing_aliases) - 10} more")
    else:
        print(f"  [OK] All {len(results)} preserving aliases exist")

    # 3. Sources are deactivated
    still_active = conn.execute(f"""
        SELECT customer_id, normalized_name FROM customers
        WHERE customer_id IN ({placeholders}) AND is_active = 1
    """, source_ids).fetchall()
    if still_active:
        print(f"\n  [FAIL] Still-active sources ({len(still_active)}):")
        for r in still_active:
            print(f"    #{r['customer_id']}: {r['normalized_name']}")
    else:
        print(f"  [OK] All {len(results)} sources deactivated")

    # 4. Total spot count (sanity)
    total = conn.execute("SELECT COUNT(*) AS cnt FROM spots").fetchone()["cnt"]
    print(f"  [INFO] Total spot count: {total:,}")

    print(f"\n{'─' * w}")


def execute_all(conn: sqlite3.Connection, results: List[CaseAnalysis]):
    """Execute all cases with user confirmation."""
    valid = [a for a in results if not a.alias_conflicts]
    skipped = [a for a in results if a.alias_conflicts]

    if skipped:
        print(f"\nSkipping {len(skipped)} case(s) with alias conflicts:")
        for a in skipped:
            print(f"  - #{a.case.source_id}: {a.case.source_name}")
            for c in a.alias_conflicts:
                print(f"    {c}")

    if not valid:
        print("\nNo valid cases to execute.")
        return

    answer = input(f"\nApply changes to {len(valid)} cases? [yes/NO]: ").strip().lower()
    if answer != "yes":
        print("Aborted.")
        return

    affected_customers = set()
    affected_agencies = set()
    succeeded = 0
    failed = 0

    for i, a in enumerate(valid, 1):
        stats = execute_case(conn, a)
        if stats["success"]:
            succeeded += 1
            affected_customers.add(a.case.source_id)
            affected_customers.add(a.case.target_id)
            affected_agencies.add(a.case.agency_id)
            print(f"  [{i}/{len(valid)}] OK: #{a.case.source_id} -> #{a.case.target_id} ({len(stats['steps'])} steps)")
        else:
            failed += 1
            print(f"  [{i}/{len(valid)}] FAILED: #{a.case.source_id} -> {stats['error']}")
            print(f"\nStopping execution after failure. {succeeded} succeeded, {failed} failed, {len(valid) - i} remaining.")
            break

    print(f"\nExecution complete: {succeeded} succeeded, {failed} failed")

    if succeeded > 0:
        rebuild_metrics(conn, affected_customers, affected_agencies)
        verify_execution(conn, [a for a in valid[:succeeded]])


# ── Scenario 2 Display ────────────────────────────────────────────────────────

def print_scenario2_case(index: int, total: int, a: Scenario2Analysis):
    c = a.case
    w = 70

    print(f"\n{'=' * w}")
    label = f"CASE {index} of {total}: {c.source_name}"
    if a.warnings:
        tags = " ".join(w_str.split("]")[0] + "]" for w_str in a.warnings if w_str.startswith("["))
        label += f"  {tags}"
    print(label)
    print(f"{'=' * w}")

    # Source (will be renamed)
    print(f"\n  SOURCE (colon-name customer to rename):")
    print(f"    customer_id:   {c.source_id}")
    print(f"    Current name:  {c.source_name}")
    print(f"    New name:      {c.customer_part}")
    print(f"    Active:        {'Yes' if a.source_active else 'No'}")
    print(f"    Sector:        {a.source_sector or '(none)'}")
    print(f"    Assigned AE:   {a.source_ae or '(none)'}")

    # Agency
    print(f"\n  AGENCY (will be assigned):")
    print(f"    agency_id:     {c.agency_id}")
    print(f"    Name:          {c.agency_name}")

    # Collision
    if c.collision_id:
        print(f"\n  COLLISION (inactive customer blocking rename):")
        print(f"    customer_id:   {c.collision_id}")
        print(f"    Name:          {c.collision_name}")
        print(f"    Spots:         {a.collision_spots}")
        print(f"    Active aliases: {a.collision_aliases}")
        print(f"    Action:        Rename to '{c.collision_name} #retired-{c.collision_id}'")
    else:
        print(f"\n  COLLISION: none (clean rename)")

    # Spots
    print(f"\n  SPOTS IMPACT:")
    print(f"    Total spots:   {a.spots_total:,} ({fmt_money(a.spots_revenue)} revenue)")
    if a.spots_total > 0:
        print(f"    Date range:    {a.spots_date_min} to {a.spots_date_max}")
        print(f"    agency_id breakdown:")
        print(f"      NULL -> will set to {c.agency_id}: {a.spots_agency_null:,}")
        print(f"      Already {c.agency_id}: {a.spots_agency_match:,}")
        print(f"      Different (keep): {a.spots_agency_other:,}")

    # Alias
    print(f"\n  PRESERVING ALIAS:")
    if a.existing_alias:
        ea = a.existing_alias
        if ea.target_entity_id == c.source_id and ea.is_active:
            print(f"    Already exists: alias #{ea.alias_id} -> #{ea.target_entity_id} (active)")
        elif ea.target_entity_id == c.source_id and not ea.is_active:
            print(f"    Exists but inactive: alias #{ea.alias_id} -> will reactivate")
        else:
            print(f"    Exists but points to #{ea.target_entity_id} (will skip)")
    else:
        print(f"    Will create: '{c.source_name}' -> #{c.source_id}")

    # Warnings
    if a.warnings:
        print(f"\n  WARNINGS:")
        for warn in a.warnings:
            print(f"    {warn}")

    print(f"\n{'-' * w}")


def print_scenario2_summary(results: List[Scenario2Analysis]):
    w = 70
    print(f"\n{'=' * w}")
    print(f"SCENARIO 2 SUMMARY: {len(results)} cases analyzed")
    print(f"{'=' * w}")

    total_spots = sum(a.spots_total for a in results)
    total_revenue = sum(a.spots_revenue for a in results)
    total_agency_null = sum(a.spots_agency_null for a in results)
    collisions = sum(1 for a in results if a.case.collision_id)
    clean_renames = sum(1 for a in results if not a.case.collision_id)
    aliases_existing = sum(1 for a in results if a.existing_alias)
    aliases_to_create = sum(1 for a in results if not a.existing_alias)
    zero_spots = sum(1 for a in results if a.spots_total == 0)
    warning_cases = [a for a in results if a.warnings]

    print(f"\n  Totals:")
    print(f"    Cases:              {len(results):,}")
    print(f"    Total spots:        {total_spots:,}")
    print(f"    Total revenue:      {fmt_money(total_revenue)}")
    print(f"    Spots needing agency_id: {total_agency_null:,}")

    print(f"\n  Collisions:")
    print(f"    Clean renames:      {clean_renames:,}")
    print(f"    With collision:     {collisions:,}")

    print(f"\n  Aliases:")
    print(f"    Already exist:      {aliases_existing:,}")
    print(f"    To create:          {aliases_to_create:,}")

    if zero_spots or warning_cases:
        print(f"\n  Flags:")
        if zero_spots:
            print(f"    [ZERO SPOTS] cases:          {zero_spots}")
        collision_with_spots = [a for a in results if a.collision_spots > 0]
        if collision_with_spots:
            print(f"    [COLLISION HAS SPOTS] cases:  {len(collision_with_spots)}")
            for a in collision_with_spots:
                print(f"      - #{a.case.collision_id}: {a.collision_spots} spots")

    print(f"\n{'-' * w}")


def verify_scenario2(conn: sqlite3.Connection, results: List[Scenario2Analysis]):
    """Post-execution verification for Scenario 2."""
    w = 70
    print(f"\n{'=' * w}")
    print("SCENARIO 2 POST-EXECUTION VERIFICATION")
    print(f"{'=' * w}")

    # 1. All sources renamed (no more colon in normalized_name)
    source_ids = [a.case.source_id for a in results]
    placeholders = ",".join("?" * len(source_ids))
    still_colon = conn.execute(f"""
        SELECT customer_id, normalized_name FROM customers
        WHERE customer_id IN ({placeholders}) AND normalized_name LIKE '%:%'
    """, source_ids).fetchall()
    if still_colon:
        print(f"\n  [FAIL] Still have colon in name ({len(still_colon)}):")
        for r in still_colon:
            print(f"    #{r['customer_id']}: {r['normalized_name']}")
    else:
        print(f"\n  [OK] All {len(results)} sources renamed (no colons)")

    # 2. All sources have agency_id set
    no_agency = conn.execute(f"""
        SELECT customer_id, normalized_name FROM customers
        WHERE customer_id IN ({placeholders}) AND agency_id IS NULL
    """, source_ids).fetchall()
    if no_agency:
        print(f"\n  [FAIL] Missing agency_id ({len(no_agency)}):")
        for r in no_agency:
            print(f"    #{r['customer_id']}: {r['normalized_name']}")
    else:
        print(f"  [OK] All {len(results)} sources have agency_id set")

    # 3. Preserving aliases exist for all old colon names
    missing_aliases = []
    for a in results:
        exists = conn.execute("""
            SELECT 1 FROM entity_aliases
            WHERE alias_name = ? AND entity_type = 'customer' AND is_active = 1
        """, [a.case.source_name]).fetchone()
        if not exists:
            missing_aliases.append(a.case.source_name)
    if missing_aliases:
        print(f"\n  [WARN] Missing preserving aliases ({len(missing_aliases)}):")
        for name in missing_aliases[:10]:
            print(f"    - {name}")
        if len(missing_aliases) > 10:
            print(f"    ... and {len(missing_aliases) - 10} more")
    else:
        print(f"  [OK] All {len(results)} preserving aliases exist")

    # 4. No NULL agency_id on spots for processed customers
    null_agency_spots = conn.execute(f"""
        SELECT s.customer_id, COUNT(*) AS cnt
        FROM spots s
        WHERE s.customer_id IN ({placeholders}) AND s.agency_id IS NULL
        GROUP BY s.customer_id
    """, source_ids).fetchall()
    if null_agency_spots:
        # Only warn — some spots may legitimately have other agency assignment patterns
        total_null = sum(r["cnt"] for r in null_agency_spots)
        print(f"\n  [INFO] {total_null} spots still have NULL agency_id across {len(null_agency_spots)} customers")
    else:
        print(f"  [OK] No NULL agency_id spots on processed customers")

    # 5. Collision records renamed
    collision_ids = [a.case.collision_id for a in results if a.case.collision_id]
    if collision_ids:
        c_placeholders = ",".join("?" * len(collision_ids))
        not_retired = conn.execute(f"""
            SELECT customer_id, normalized_name FROM customers
            WHERE customer_id IN ({c_placeholders})
              AND normalized_name NOT LIKE '%#retired-%'
        """, collision_ids).fetchall()
        if not_retired:
            print(f"\n  [FAIL] Collision records not renamed ({len(not_retired)}):")
            for r in not_retired:
                print(f"    #{r['customer_id']}: {r['normalized_name']}")
        else:
            print(f"  [OK] All {len(collision_ids)} collision records renamed")

    print(f"\n{'-' * w}")


def execute_all_scenario2(conn: sqlite3.Connection, results: List[Scenario2Analysis]):
    """Execute all Scenario 2 cases with user confirmation."""
    # Filter out cases with dangerous collisions (spots on collision record)
    valid = [a for a in results if a.collision_spots == 0]
    skipped = [a for a in results if a.collision_spots > 0]

    if skipped:
        print(f"\nSkipping {len(skipped)} case(s) with spots on collision record:")
        for a in skipped:
            print(f"  - #{a.case.source_id}: collision #{a.case.collision_id} has {a.collision_spots} spots")

    if not valid:
        print("\nNo valid cases to execute.")
        return

    answer = input(f"\nApply Scenario 2 changes to {len(valid)} cases? [yes/NO]: ").strip().lower()
    if answer != "yes":
        print("Aborted.")
        return

    affected_customers = set()
    affected_agencies = set()
    succeeded = 0
    failed = 0

    for i, a in enumerate(valid, 1):
        stats = execute_scenario2_case(conn, a)
        if stats["success"]:
            succeeded += 1
            affected_customers.add(a.case.source_id)
            if a.case.collision_id:
                affected_customers.add(a.case.collision_id)
            affected_agencies.add(a.case.agency_id)
            print(f"  [{i}/{len(valid)}] OK: #{a.case.source_id} '{a.case.source_name}' -> '{a.case.customer_part}' ({len(stats['steps'])} steps)")
        else:
            failed += 1
            print(f"  [{i}/{len(valid)}] FAILED: #{a.case.source_id} -> {stats['error']}")
            print(f"\nStopping execution after failure. {succeeded} succeeded, {failed} failed, {len(valid) - i} remaining.")
            break

    print(f"\nExecution complete: {succeeded} succeeded, {failed} failed")

    if succeeded > 0:
        rebuild_metrics(conn, affected_customers, affected_agencies)
        verify_scenario2(conn, [a for a in valid[:succeeded]])


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Unsplit colon-name customers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Scenarios:
  1  Both agency and standalone customer exist — merge colon-name into standalone
  2  Agency exists but no standalone customer — rename + assign agency

Examples:
  python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 1                    # dry-run S1
  python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2                    # dry-run S2
  python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --customer-id 268  # one case
  python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --limit 5          # first 5
  python tools/unsplit_colon_customers.py --db .data/dev.db --scenario 2 --execute          # apply S2
""",
    )
    parser.add_argument("--db", required=True, help="Path to SQLite database")
    parser.add_argument("--scenario", choices=["1", "2", "all"], default="all",
                        help="Which scenario to run (default: all)")
    parser.add_argument("--customer-id", type=int, help="Process a single source customer ID")
    parser.add_argument("--limit", type=int, help="Limit number of cases to process")
    parser.add_argument("--execute", action="store_true", help="Apply changes (default: dry-run)")
    return parser.parse_args()


def run_scenario1(conn: sqlite3.Connection, args):
    """Run Scenario 1: merge colon-name into existing standalone customer."""
    print(f"Discovering colon-name customers in {args.db}...")
    cases = discover_cases(conn, customer_id=args.customer_id, limit=args.limit)
    print(f"Found {len(cases)} qualifying case(s) (Scenario 1)")

    if not cases:
        print("Nothing to do for Scenario 1.")
        return

    results = []
    for case in cases:
        results.append(analyze_case(conn, case))

    for i, a in enumerate(results, 1):
        print_case(i, len(results), a)

    print_summary(results)

    if args.execute:
        execute_all(conn, results)
    else:
        print(f"\nScenario 1 dry-run complete. Use --execute to apply changes.")


def run_scenario2(conn: sqlite3.Connection, args):
    """Run Scenario 2: rename colon-name customer + assign agency."""
    print(f"\nDiscovering Scenario 2 cases in {args.db}...")
    cases = discover_scenario2_cases(conn, customer_id=args.customer_id, limit=args.limit)
    print(f"Found {len(cases)} qualifying case(s) (Scenario 2: rename + assign agency)")

    if not cases:
        print("Nothing to do for Scenario 2.")
        return

    results = []
    for case in cases:
        results.append(analyze_scenario2(conn, case))

    for i, a in enumerate(results, 1):
        print_scenario2_case(i, len(results), a)

    print_scenario2_summary(results)

    if args.execute:
        execute_all_scenario2(conn, results)
    else:
        print(f"\nScenario 2 dry-run complete. Use --execute to apply changes.")


def main():
    args = parse_args()

    readonly = not args.execute
    conn = open_db(args.db, readonly=readonly)

    try:
        if args.scenario in ("1", "all"):
            run_scenario1(conn, args)
        if args.scenario in ("2", "all"):
            run_scenario2(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
