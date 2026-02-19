"""
Parse placement confirmation text files from the comDiff repo.
Used to drive "contracts added" dates from confirmation report dates instead of spots.load_date.
"""

import os
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional

# Default path to comDiff repo (placement_confirmation_YYYYMMDD.txt files)
DEFAULT_PLACEMENT_DIR = os.environ.get("PLACEMENT_CONFIRMATION_DIR", "/home/jellee26/comDiff")


def _parse_amount(s: str) -> float:
    """Parse '$1,234.56' or '-$1,234.56' to float."""
    s = s.replace("$", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def _date_from_filename(filename: str) -> Optional[date]:
    """Extract date from placement_confirmation_YYYYMMDD.txt."""
    m = re.search(r"placement_confirmation_(\d{8})\.txt", filename)
    if not m:
        return None
    try:
        y, mo, d = int(m.group(1)[:4]), int(m.group(1)[4:6]), int(m.group(1)[6:8])
        return date(y, mo, d)
    except (ValueError, IndexError):
        return None


def parse_placement_file(filepath: str) -> List[Tuple[str, str, str, float]]:
    """
    Parse a single placement confirmation file.
    Returns list of (ae_name, client_bill_code, contract_id, total).
    """
    results = []
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError:
        return results

    current_ae: Optional[str] = None
    current_client: Optional[str] = None
    current_contract: Optional[str] = None
    # WorldLink clients appear under other AEs (e.g. House); attribute them to AE "WorldLink"
    effective_ae: Optional[str] = None
    in_worldlink_summary = False
    # Regex for "  1. WorldLink:Client Name - $1,225 → $700 ($-525.00) (date)" in MODIFICATIONS section (→ or ->)
    worldlink_mod_line = re.compile(r"^\s+\d+\.\s+(WorldLink:[^-]+?)\s+-\s+\$?[\d,.]+\s*(?:→|->)\s*\$?([\d,.-]+)")

    for line in content.splitlines():
        line_stripped = line.strip()
        # AE: Name
        if line.startswith("AE: ") and "=" not in line[:10]:
            current_ae = line[4:].strip()
            current_client = None
            current_contract = None
            effective_ae = current_ae
            in_worldlink_summary = False
            continue
        # Skip non-AE sections unless this is a WorldLink client line (no "AE: WorldLink" in file; section has its own header)
        if current_ae is None and not in_worldlink_summary:
            is_worldlink_client_line = bool(re.match(r"^  [^ ].+:$", line) and line.strip().rstrip(":").startswith("WorldLink:"))
            if not is_worldlink_client_line:
                continue
        if line_stripped.startswith("WORLDLINK LINES SUMMARY"):
            current_ae = None
            effective_ae = None
            in_worldlink_summary = True
            continue
        if line_stripped.startswith("Generated from") or line_stripped == "*all totals" or line_stripped.startswith("Monthly Breakdown"):
            in_worldlink_summary = False
            if line_stripped.startswith("Generated from") or line_stripped == "*all totals":
                current_ae = None
                effective_ae = None
            continue
        # In WORLDLINK LINES SUMMARY MODIFICATIONS: "  N. WorldLink:Client - $old → $new ($change) (date)"
        if in_worldlink_summary:
            m_wl = worldlink_mod_line.match(line)
            if m_wl:
                client = m_wl.group(1).strip()
                total = _parse_amount(m_wl.group(2))
                # Avoid double-counting if we already have this client from "Worldlink New Revenue by Bill Code"
                if total != 0 and not any(r[0] == "WorldLink" and r[1] == client for r in results):
                    results.append(("WorldLink", client, "—", total))
                continue
        # "  BillCode:" (two spaces, then client name, then colon)
        if re.match(r"^  [^ ].+:$", line) and not line.strip().startswith("Contract") and "Total:" not in line and "---" not in line:
            # Line like "  Daviselen:Capital Business Unit:" or "  WorldLink:Direct Donor X:"
            current_client = line.strip().rstrip(":")
            current_contract = None
            # Client names starting with "WorldLink:" belong to AE WorldLink (separate from House/others)
            if current_client.startswith("WorldLink:"):
                effective_ae = "WorldLink"
            else:
                effective_ae = current_ae
            continue
        # "    Contract: 2414" or "    Contract(s): " (optional id in WorldLink section)
        m_contract = re.match(r"^\s+Contract(?:\(s\))?:\s*(\S*)", line)
        if m_contract:
            current_contract = m_contract.group(1).strip() or "—"
            continue
        # "      Total: $2,040.00"
        m_total = re.match(r"^\s+Total:\s*\$?([\d,.-]+)", line)
        if m_total and effective_ae and current_client and current_contract is not None:
            total = _parse_amount(m_total.group(1))
            results.append((effective_ae, current_client, current_contract, total))
            current_contract = None  # next contract or bill code
            continue

    return results


def list_placement_files(
    placement_dir: str = DEFAULT_PLACEMENT_DIR,
    days_back: int = 30,
) -> List[Tuple[date, str]]:
    """List placement_confirmation_*.txt files in dir, (file_date, path), within days_back."""
    if not os.path.isdir(placement_dir):
        return []
    cutoff = date.today() - timedelta(days=days_back)
    out = []
    for name in os.listdir(placement_dir):
        if not name.startswith("placement_confirmation_") or not name.endswith(".txt"):
            continue
        d = _date_from_filename(name)
        if d is None or d < cutoff:
            continue
        out.append((d, os.path.join(placement_dir, name)))
    out.sort(key=lambda x: x[0])
    return out


def load_contracts_from_placement_files(
    placement_dir: str = DEFAULT_PLACEMENT_DIR,
    days_back: int = 30,
    ae_name_filter: Optional[str] = None,
) -> List[Dict]:
    """
    Load all contracts from placement confirmation files in the last days_back days.
    Returns list of dicts: client, contract, total, added_date (date of file).
    If ae_name_filter is set, only include that AE (exact match after strip).
    """
    files = list_placement_files(placement_dir=placement_dir, days_back=days_back)
    # For each (ae, client, contract, total) we want the earliest file_date as "added_date"
    # Key: (ae, client, contract), value: (total, first_date)
    by_key: Dict[Tuple[str, str, str], Tuple[float, date]] = {}

    for file_date, path in files:
        for ae_name, client, contract_id, total in parse_placement_file(path):
            if ae_name_filter is not None and ae_name.strip() != ae_name_filter.strip():
                continue
            key = (ae_name, client, contract_id)
            if key not in by_key:
                by_key[key] = (total, file_date)
            else:
                old_total, old_date = by_key[key]
                by_key[key] = (old_total + total, min(old_date, file_date))

    return [
        {
            "client": client,
            "contract": contract_id,
            "total": total,
            "added_date": first_date.isoformat() if first_date else None,
        }
        for (_, client, contract_id), (total, first_date) in by_key.items()
    ]


def contracts_highlight_7_days(
    placement_dir: str = DEFAULT_PLACEMENT_DIR,
    ae_name: Optional[str] = None,
) -> List[Dict]:
    """
    Contracts that appear in placement confirmation files from the last 7 days.
    Returns flat list: client, contract, total, added_date (from file date).
    """
    files = list_placement_files(placement_dir=placement_dir, days_back=7)
    rows = []
    for file_date, path in files:
        for ae, client, contract_id, total in parse_placement_file(path):
            if ae_name is not None and ae.strip() != ae_name.strip():
                continue
            rows.append({
                "client": client,
                "contract": contract_id,
                "total": total,
                "added_date": file_date.isoformat(),
            })
    # Dedupe by (client, contract), summing total and keeping one added_date (e.g. earliest)
    by_key: Dict[Tuple[str, str], Dict] = {}
    for r in rows:
        key = (r["client"], r["contract"])
        if key not in by_key:
            by_key[key] = {**r}
        else:
            by_key[key]["total"] += r["total"]
            if r["added_date"] < by_key[key]["added_date"]:
                by_key[key]["added_date"] = r["added_date"]
    return list(by_key.values())


def contracts_by_client_15_days(
    placement_dir: str = DEFAULT_PLACEMENT_DIR,
    ae_name: Optional[str] = None,
) -> List[Dict]:
    """
    Contracts from placement files in the last 15 days, grouped by client.
    Returns list of { client, total, contracts: [ { contract, total, added_date }, ... ] }.
    """
    raw = load_contracts_from_placement_files(
        placement_dir=placement_dir,
        days_back=15,
        ae_name_filter=ae_name,
    )
    # Group by client
    client_map: Dict[str, Dict] = {}
    for r in raw:
        c = r["client"]
        if c not in client_map:
            client_map[c] = {"total": 0.0, "contracts": []}
        client_map[c]["total"] += r["total"]
        client_map[c]["contracts"].append({
            "contract": r["contract"],
            "total": r["total"],
            "added_date": r.get("added_date"),
        })
    # Sort clients by total desc, contracts by total desc
    for c in client_map:
        client_map[c]["contracts"].sort(key=lambda x: x["total"], reverse=True)
    return [
        {"client": c, "total": data["total"], "contracts": data["contracts"]}
        for c, data in sorted(client_map.items(), key=lambda x: x[1]["total"], reverse=True)
    ]
