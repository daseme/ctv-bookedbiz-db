"""Service for CSV export and contact import operations.

Extracted from address_book.py routes. All methods accept a conn parameter
(sqlite3.Connection with Row factory) and return plain data.
"""

import csv
import io
import logging

from src.services.base_service import BaseService

logger = logging.getLogger(__name__)

VALID_IMPORT_ROLES = [
    "decision_maker", "account_manager", "billing", "technical", "other",
]


class ExportService(BaseService):
    """Handles CSV export of entities and CSV import of contacts."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def export_entities_csv(self, conn, filters, metrics_map):
        """Build CSV content string from entity data.

        Args:
            conn: database connection (read-only)
            filters: dict with optional keys: search, type, has_contacts,
                     has_address, sector_id, market, ae
            metrics_map: dict keyed by (entity_type, entity_id) tuples
                         as returned by EntityMetricsService.get_metrics_map

        Returns CSV string ready to send as response body.
        """
        agency_client_ids = set()
        for key, m in metrics_map.items():
            etype = key[0] if isinstance(key, tuple) else m.get("entity_type")
            eid = key[1] if isinstance(key, tuple) else key
            if (etype == "customer"
                    and m.get("agency_spot_count") == m.get("spot_count")
                    and m.get("spot_count", 0) > 0):
                agency_client_ids.add(eid)

        # Build flat lookup by (entity_type, entity_id) for convenience
        flat_map = {}
        for key, m in metrics_map.items():
            if isinstance(key, tuple):
                flat_map[key] = m
            else:
                etype = m.get("entity_type", "customer")
                flat_map[(etype, key)] = m

        results = []
        entity_type_filter = filters.get("type", "all")

        if entity_type_filter in ("all", "agency"):
            results.extend(
                self._load_agencies_for_export(conn, flat_map)
            )

        if entity_type_filter in ("all", "customer"):
            results.extend(
                self._load_customers_for_export(
                    conn, flat_map, agency_client_ids,
                )
            )

        results = self._apply_filters(conn, results, filters)
        results.sort(key=lambda r: r["entity_name"].lower())
        return self._build_csv(results)

    def _load_agencies_for_export(self, conn, flat_map):
        """Load agency rows with primary contact for export."""
        rows = conn.execute("""
            SELECT
                a.agency_id as entity_id,
                'agency' as entity_type,
                a.agency_name as entity_name,
                a.address, a.city, a.state, a.zip,
                a.notes, a.assigned_ae,
                a.po_number, a.edi_billing, a.edi_code,
                NULL as sector_name,
                (SELECT contact_name FROM entity_contacts ec
                 WHERE ec.entity_type = 'agency'
                   AND ec.entity_id = a.agency_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_contact,
                (SELECT email FROM entity_contacts ec
                 WHERE ec.entity_type = 'agency'
                   AND ec.entity_id = a.agency_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_email,
                (SELECT phone FROM entity_contacts ec
                 WHERE ec.entity_type = 'agency'
                   AND ec.entity_id = a.agency_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_phone
            FROM agencies a WHERE a.is_active = 1
        """).fetchall()

        results = []
        for row in rows:
            d = dict(row)
            m = flat_map.get(("agency", d["entity_id"]), {})
            d["last_active"] = m.get("last_active", "")
            d["total_revenue"] = float(m.get("total_revenue") or 0)
            d["markets"] = m.get("markets", "")
            results.append(d)
        return results

    def _load_customers_for_export(
        self, conn, flat_map, agency_client_ids,
    ):
        """Load customer rows (excluding agency clients) for export."""
        rows = conn.execute("""
            SELECT
                c.customer_id as entity_id,
                'customer' as entity_type,
                c.normalized_name as entity_name,
                c.address, c.city, c.state, c.zip,
                c.notes, c.assigned_ae,
                c.po_number, c.edi_billing,
                s.sector_name,
                (SELECT contact_name FROM entity_contacts ec
                 WHERE ec.entity_type = 'customer'
                   AND ec.entity_id = c.customer_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_contact,
                (SELECT email FROM entity_contacts ec
                 WHERE ec.entity_type = 'customer'
                   AND ec.entity_id = c.customer_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_email,
                (SELECT phone FROM entity_contacts ec
                 WHERE ec.entity_type = 'customer'
                   AND ec.entity_id = c.customer_id
                   AND ec.is_active = 1 AND ec.is_primary = 1
                 LIMIT 1) as primary_phone
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE c.is_active = 1
        """).fetchall()

        results = []
        for row in rows:
            d = dict(row)
            if (":" in d["entity_name"]
                    or d["entity_id"] in agency_client_ids):
                continue
            m = flat_map.get(("customer", d["entity_id"]), {})
            d["last_active"] = m.get("last_active", "")
            d["total_revenue"] = float(m.get("total_revenue") or 0)
            d["markets"] = m.get("markets", "")
            results.append(d)
        return results

    def _apply_filters(self, conn, results, filters):
        """Apply search, contact, address, sector, market, AE filters."""
        search = filters.get("search", "").strip()
        if search:
            q = search.lower()
            results = [
                r for r in results
                if (q in r["entity_name"].lower()
                    or q in (r.get("notes") or "").lower()
                    or q in (r.get("sector_name") or "").lower())
            ]

        has_contacts = filters.get("has_contacts", "all")
        if has_contacts == "yes":
            results = [
                r for r in results if r.get("primary_contact")
            ]
        elif has_contacts == "no":
            results = [
                r for r in results if not r.get("primary_contact")
            ]

        has_address = filters.get("has_address", "all")
        if has_address == "yes":
            results = [
                r for r in results
                if r.get("address") or r.get("city")
            ]
        elif has_address == "no":
            results = [
                r for r in results
                if not r.get("address") and not r.get("city")
            ]

        sector_filter = filters.get("sector_id", "")
        if sector_filter:
            try:
                sid = int(sector_filter)
                sector_customer_ids = set(
                    r["customer_id"]
                    for r in conn.execute(
                        "SELECT customer_id FROM customer_sectors "
                        "WHERE sector_id = ?",
                        [sid],
                    ).fetchall()
                )
                results = [
                    r for r in results
                    if (r.get("entity_type") == "customer"
                        and r.get("entity_id") in sector_customer_ids)
                ]
            except ValueError:
                pass

        market_filter = filters.get("market", "")
        if market_filter:
            results = [
                r for r in results
                if market_filter in (r.get("markets") or "")
            ]

        ae_filter = filters.get("ae", "")
        if ae_filter:
            if ae_filter == "__none__":
                results = [
                    r for r in results if not r.get("assigned_ae")
                ]
            else:
                results = [
                    r for r in results
                    if r.get("assigned_ae") == ae_filter
                ]

        return results

    def _build_csv(self, results):
        """Build CSV string from result list."""
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Entity Name", "Type", "Sector", "Assigned AE",
            "Primary Contact", "Email", "Phone",
            "Address", "City", "State", "ZIP",
            "PO Number", "EDI Billing", "EDI Code",
            "Markets", "Last Active", "Total Revenue", "Notes",
        ])

        for r in results:
            rev = r.get("total_revenue", 0)
            writer.writerow([
                r.get("entity_name", ""),
                r.get("entity_type", ""),
                r.get("sector_name", ""),
                r.get("assigned_ae", ""),
                r.get("primary_contact", ""),
                r.get("primary_email", ""),
                r.get("primary_phone", ""),
                r.get("address", ""),
                r.get("city", ""),
                r.get("state", ""),
                r.get("zip", ""),
                r.get("po_number", ""),
                "Yes" if r.get("edi_billing") else "No",
                r.get("edi_code", ""),
                r.get("markets", ""),
                r.get("last_active", ""),
                f"${rev:,.2f}" if rev else "",
                r.get("notes", ""),
            ])

        return output.getvalue()

    def import_contacts_csv(self, conn, csv_content, created_by):
        """Parse CSV and create contacts for matching entities.

        Args:
            conn: writable database connection
            csv_content: decoded CSV string
            created_by: username for audit trail

        Returns dict with imported, skipped, errors keys.
        """
        reader = csv.DictReader(io.StringIO(csv_content))
        required_cols = {"Entity Name", "Type", "Contact Name"}
        if not required_cols.issubset(set(reader.fieldnames or [])):
            return {
                "error": "CSV must have columns: "
                         + ", ".join(sorted(required_cols)),
            }

        imported = 0
        skipped = 0
        errors = []

        for i, row in enumerate(reader, start=2):
            entity_name = (row.get("Entity Name") or "").strip()
            entity_type = (row.get("Type") or "").strip().lower()
            contact_name = (row.get("Contact Name") or "").strip()

            if not entity_name or not entity_type or not contact_name:
                errors.append(
                    f"Row {i}: Missing required field "
                    f"(Entity Name, Type, or Contact Name)"
                )
                skipped += 1
                continue

            if entity_type not in ("agency", "customer"):
                errors.append(
                    f"Row {i}: Type must be 'agency' or 'customer', "
                    f"got '{entity_type}'"
                )
                skipped += 1
                continue

            entity_id = self._lookup_entity(
                conn, entity_type, entity_name,
            )
            if not entity_id:
                errors.append(
                    f"Row {i}: Entity '{entity_name}' "
                    f"({entity_type}) not found"
                )
                skipped += 1
                continue

            role = (row.get("Role") or "").strip().lower() or None
            if role and role not in VALID_IMPORT_ROLES:
                role = None

            try:
                conn.execute("""
                    INSERT INTO entity_contacts
                        (entity_type, entity_id, contact_name,
                         contact_title, email, phone,
                         contact_role, is_primary, created_by)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)
                """, [
                    entity_type, entity_id, contact_name,
                    (row.get("Title") or "").strip() or None,
                    (row.get("Email") or "").strip() or None,
                    (row.get("Phone") or "").strip() or None,
                    role, created_by,
                ])
                imported += 1
            except Exception as e:
                errors.append(f"Row {i}: {e}")
                skipped += 1

        return {
            "imported": imported,
            "skipped": skipped,
            "errors": errors[:20],
        }

    def _lookup_entity(self, conn, entity_type, entity_name):
        """Look up entity ID by name. Returns int or None."""
        if entity_type == "agency":
            row = conn.execute(
                "SELECT agency_id FROM agencies "
                "WHERE agency_name = ? COLLATE NOCASE "
                "AND is_active = 1",
                [entity_name],
            ).fetchone()
            return row["agency_id"] if row else None

        row = conn.execute(
            "SELECT customer_id FROM customers "
            "WHERE normalized_name = ? COLLATE NOCASE "
            "AND is_active = 1",
            [entity_name],
        ).fetchone()
        return row["customer_id"] if row else None
