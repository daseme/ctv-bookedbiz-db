"""Service for entity (agency/customer) CRUD operations.

Extracted from address_book.py routes. All methods accept a conn parameter
(sqlite3.Connection with Row factory) and return plain dicts.
"""

import logging

from src.services.base_service import BaseService
from src.services.customer_resolution_service import _score_name
from src.utils.formatting import client_portion

logger = logging.getLogger(__name__)


class EntityService(BaseService):
    """Handles all entity CRUD: list, detail, create, update, deactivate."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def list_entities(self, conn, include_inactive=False):
        """List all entities with batch contact stats, sectors, metrics,
        and signals.

        Returns dict with top-level list (agencies then customers).
        """
        results = []

        # Batch: contact stats for all entities
        contact_stats = {}
        for row in conn.execute("""
            SELECT entity_type, entity_id, COUNT(*) as contact_count,
                   MAX(CASE WHEN is_primary = 1
                       THEN contact_name END) as primary_contact
            FROM entity_contacts WHERE is_active = 1
            GROUP BY entity_type, entity_id
        """).fetchall():
            contact_stats[(row["entity_type"], row["entity_id"])] = {
                "contact_count": row["contact_count"],
                "primary_contact": row["primary_contact"]
            }

        # Batch: sector counts + sector_ids per customer
        sector_counts = {}
        customer_sector_ids = {}
        for row in conn.execute("""
            SELECT customer_id, COUNT(*) as cnt,
                   GROUP_CONCAT(sector_id) as sids
            FROM customer_sectors GROUP BY customer_id
        """).fetchall():
            sector_counts[row["customer_id"]] = row["cnt"]
            customer_sector_ids[row["customer_id"]] = row["sids"] or ""

        # Batch: client count per agency
        client_counts = {}
        for row in conn.execute("""
            SELECT agency_id, COUNT(*) as cnt
            FROM customers
            WHERE is_active = 1 AND agency_id IS NOT NULL
            GROUP BY agency_id
        """).fetchall():
            client_counts[row["agency_id"]] = row["cnt"]

        # Load entity_metrics
        agency_markets = {}
        agency_metrics = {}
        customer_markets = {}
        customer_metrics = {}
        agency_client_ids_from_spots = set()

        for row in conn.execute(
            "SELECT * FROM entity_metrics"
        ).fetchall():
            eid = row["entity_id"]
            if row["entity_type"] == "agency":
                agency_markets[eid] = row["markets"] or ""
                agency_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "spot_count": row["spot_count"]
                }
            else:
                customer_markets[eid] = row["markets"] or ""
                customer_metrics[eid] = {
                    "last_active": row["last_active"],
                    "total_revenue": float(row["total_revenue"] or 0),
                    "spot_count": row["spot_count"]
                }
                if row["agency_spot_count"] == row["spot_count"]:
                    agency_client_ids_from_spots.add(eid)

        # Load entity signals
        entity_signals = {}
        for srow in conn.execute(
            "SELECT entity_type, entity_id, signal_type, "
            "signal_label, signal_priority "
            "FROM entity_signals ORDER BY signal_priority"
        ).fetchall():
            key = (srow["entity_type"], srow["entity_id"])
            if key not in entity_signals:
                entity_signals[key] = []
            entity_signals[key].append({
                "signal_type": srow["signal_type"],
                "signal_label": srow["signal_label"],
                "signal_priority": srow["signal_priority"]
            })

        # Agencies
        active_clause = (
            "" if include_inactive else "WHERE a.is_active = 1"
        )
        agencies = conn.execute(f"""
            SELECT
                a.agency_id as entity_id,
                'agency' as entity_type,
                a.agency_name as entity_name,
                a.address, a.city, a.state, a.zip,
                a.notes, a.assigned_ae, a.is_active,
                NULL as sector_id,
                NULL as sector_name,
                NULL as sector_code
            FROM agencies a
            {active_clause}
            ORDER BY a.agency_name
        """).fetchall()

        for a in agencies:
            row = dict(a)
            cs = contact_stats.get(
                ("agency", row["entity_id"]), {}
            )
            row["contact_count"] = cs.get("contact_count", 0)
            row["primary_contact"] = cs.get("primary_contact")
            row["sector_count"] = 0
            row["sector_ids"] = ""
            row["client_count"] = client_counts.get(
                row["entity_id"], 0
            )
            row["markets"] = agency_markets.get(
                row["entity_id"], ""
            )
            metrics = agency_metrics.get(row["entity_id"], {})
            row["last_active"] = metrics.get("last_active")
            row["total_revenue"] = metrics.get("total_revenue", 0)
            row["spot_count"] = metrics.get("spot_count", 0)
            row["signals"] = entity_signals.get(
                ("agency", row["entity_id"]), []
            )
            results.append(row)

        # Customers (exclude agency-booked)
        active_clause = (
            "" if include_inactive else "WHERE c.is_active = 1"
        )
        customers = conn.execute(f"""
            SELECT
                c.customer_id as entity_id,
                'customer' as entity_type,
                c.normalized_name as entity_name,
                c.address, c.city, c.state, c.zip,
                c.notes, c.assigned_ae, c.is_active,
                c.sector_id,
                s.sector_name,
                s.sector_code,
                c.agency_id,
                ag.agency_name
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            LEFT JOIN agencies ag ON c.agency_id = ag.agency_id
            {active_clause}
            ORDER BY c.normalized_name
        """).fetchall()

        for c in customers:
            row = dict(c)
            if (':' in row["entity_name"]
                    or row["entity_id"]
                    in agency_client_ids_from_spots):
                continue
            cs = contact_stats.get(
                ("customer", row["entity_id"]), {}
            )
            row["contact_count"] = cs.get("contact_count", 0)
            row["primary_contact"] = cs.get("primary_contact")
            row["sector_count"] = sector_counts.get(
                row["entity_id"], 0
            )
            row["sector_ids"] = customer_sector_ids.get(
                row["entity_id"], ""
            )
            row["markets"] = customer_markets.get(
                row["entity_id"], ""
            )
            metrics = customer_metrics.get(row["entity_id"], {})
            row["last_active"] = metrics.get("last_active")
            row["total_revenue"] = metrics.get("total_revenue", 0)
            row["spot_count"] = metrics.get("spot_count", 0)
            row["signals"] = entity_signals.get(
                ("customer", row["entity_id"]), []
            )
            results.append(row)

        return results

    def get_entity_detail(self, conn, entity_type, entity_id):
        """Full detail for one entity including contacts, addresses,
        markets, sectors, signals.

        Returns dict or None if not found.
        """
        if entity_type not in ("agency", "customer"):
            return None

        if entity_type == "agency":
            entity = conn.execute("""
                SELECT agency_id as entity_id,
                       'agency' as entity_type,
                       agency_name as entity_name,
                       address, city, state, zip, notes,
                       assigned_ae,
                       po_number, edi_billing,
                       commission_rate, order_rate_basis,
                       is_active,
                       NULL as sector_id, NULL as sector_name
                FROM agencies WHERE agency_id = ?
            """, [entity_id]).fetchone()
        else:
            entity = conn.execute("""
                SELECT c.customer_id as entity_id,
                       'customer' as entity_type,
                       c.normalized_name as entity_name,
                       c.address, c.city, c.state, c.zip,
                       c.notes, c.assigned_ae,
                       c.po_number, c.edi_billing,
                       c.affidavit_required,
                       c.commission_rate, c.order_rate_basis,
                       c.is_active,
                       c.sector_id, s.sector_name,
                       c.agency_id, a.agency_name,
                       a.commission_rate AS agency_commission_rate,
                       a.order_rate_basis AS agency_order_rate_basis
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                LEFT JOIN agencies a ON c.agency_id = a.agency_id
                WHERE c.customer_id = ?
            """, [entity_id]).fetchone()

        if not entity:
            return None

        result = dict(entity)

        # Contacts
        contacts = conn.execute("""
            SELECT contact_id, contact_name, contact_title,
                   email, phone, contact_role, is_primary,
                   last_contacted
            FROM entity_contacts
            WHERE entity_type = ? AND entity_id = ?
                  AND is_active = 1
            ORDER BY is_primary DESC, contact_name
        """, [entity_type, entity_id]).fetchall()
        result["contacts"] = [dict(c) for c in contacts]

        # Additional addresses
        addresses = conn.execute("""
            SELECT address_id, address_label, address, city,
                   state, zip, is_primary, notes
            FROM entity_addresses
            WHERE entity_type = ? AND entity_id = ?
                  AND is_active = 1
            ORDER BY is_primary DESC, address_label
        """, [entity_type, entity_id]).fetchall()
        result["addresses"] = [dict(a) for a in addresses]

        # Markets from spots
        if entity_type == "agency":
            markets = conn.execute("""
                SELECT DISTINCT market_name
                FROM spots
                WHERE agency_id = ?
                  AND market_name IS NOT NULL
                  AND market_name != ''
                ORDER BY market_name
            """, [entity_id]).fetchall()
        else:
            markets = conn.execute("""
                SELECT DISTINCT market_name
                FROM spots
                WHERE customer_id = ?
                  AND market_name IS NOT NULL
                  AND market_name != ''
                ORDER BY market_name
            """, [entity_id]).fetchall()
        result["markets"] = [m["market_name"] for m in markets]

        # Sectors from junction table (customers only)
        if entity_type == "customer":
            sectors = conn.execute("""
                SELECT cs.sector_id, s.sector_name,
                       s.sector_code, cs.is_primary
                FROM customer_sectors cs
                JOIN sectors s ON cs.sector_id = s.sector_id
                WHERE cs.customer_id = ?
                ORDER BY cs.is_primary DESC, s.sector_name
            """, [entity_id]).fetchall()
            result["sectors"] = [dict(s) for s in sectors]

        return result

    def create_entity(self, conn, data, actor):
        """Create agency or customer with optional contact, sector,
        AE assignment. Fuzzy duplicate detection.

        Returns dict with result or error info.
        """
        entity_type = (data.get("entity_type") or "").strip()
        name = (data.get("name") or "").strip()
        force = data.get("force", False)

        if entity_type not in ("agency", "customer"):
            return {
                "error": "entity_type must be 'agency' or 'customer'"
            }
        if not name:
            return {"error": "Name is required"}

        # Parse common fields
        notes = (data.get("notes") or "").strip() or None
        po_number = (
            (data.get("po_number") or "").strip() or None
        )
        affidavit_required = (
            1 if data.get("affidavit_required") else 0
        )
        assigned_ae = (
            (data.get("assigned_ae") or "").strip() or None
        )
        address = (data.get("address") or "").strip() or None
        city = (data.get("city") or "").strip() or None
        state = (data.get("state") or "").strip() or None
        zip_code = (data.get("zip") or "").strip() or None

        # Commission fields
        commission_rate = data.get("commission_rate")
        if commission_rate is not None and commission_rate != "":
            try:
                commission_rate = float(commission_rate)
            except (ValueError, TypeError):
                commission_rate = None
            if (commission_rate is not None
                    and not (0 <= commission_rate <= 100)):
                return {
                    "error": "Commission rate must be 0-100"
                }
        else:
            commission_rate = None

        order_rate_basis = data.get("order_rate_basis") or None
        if (order_rate_basis is not None
                and order_rate_basis not in ("gross", "net")):
            return {
                "error": "Order rate basis must be 'gross' or 'net'"
            }

        # Contact fields
        contact_name = (
            (data.get("contact_name") or "").strip() or None
        )
        contact_title = (
            (data.get("contact_title") or "").strip() or None
        )
        contact_email = (
            (data.get("contact_email") or "").strip() or None
        )
        contact_phone = (
            (data.get("contact_phone") or "").strip() or None
        )
        contact_role = (
            (data.get("contact_role") or "").strip() or None
        )

        sector_id = data.get("sector_id")
        agency_id = data.get("agency_id")

        if entity_type == "agency":
            return self._create_agency(
                conn, name, force, po_number, assigned_ae,
                address, city, state, zip_code, notes,
                commission_rate, order_rate_basis,
                contact_name, contact_title, contact_email,
                contact_phone, contact_role, actor
            )

        return self._create_customer(
            conn, name, force, sector_id, agency_id,
            po_number, affidavit_required, assigned_ae,
            address, city, state, zip_code, notes,
            commission_rate, order_rate_basis,
            contact_name, contact_title, contact_email,
            contact_phone, contact_role, actor
        )

    def _create_agency(
        self, conn, name, force, po_number, assigned_ae,
        address, city, state, zip_code, notes,
        commission_rate, order_rate_basis,
        contact_name, contact_title, contact_email,
        contact_phone, contact_role, actor
    ):
        """Create an agency entity."""
        # Exact duplicate check
        existing = conn.execute(
            "SELECT agency_id FROM agencies "
            "WHERE agency_name = ? COLLATE NOCASE",
            [name]
        ).fetchone()
        if existing:
            return {
                "error": f"Agency '{name}' already exists",
                "existing_id": existing["agency_id"],
                "status": 409
            }

        # Fuzzy duplicate check
        if not force:
            similar = self._fuzzy_check_agencies(conn, name)
            if similar:
                return {
                    "needs_confirmation": True,
                    "similar_entities": similar
                }

        conn.execute("""
            INSERT INTO agencies
                (agency_name, po_number, assigned_ae,
                 address, city, state, zip, notes, is_active,
                 commission_rate, order_rate_basis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """, [
            name, po_number, assigned_ae, address, city,
            state, zip_code, notes, commission_rate,
            order_rate_basis
        ])
        entity_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]

        self._post_create(
            conn, "agency", entity_id, name, assigned_ae,
            contact_name, contact_title, contact_email,
            contact_phone, contact_role, None, actor
        )

        return {
            "entity_type": "agency",
            "entity_id": entity_id,
            "name": name,
            "status": 201
        }

    def _create_customer(
        self, conn, name, force, sector_id, agency_id,
        po_number, affidavit_required, assigned_ae,
        address, city, state, zip_code, notes,
        commission_rate, order_rate_basis,
        contact_name, contact_title, contact_email,
        contact_phone, contact_role, actor
    ):
        """Create a customer entity."""
        # Exact duplicate check
        existing = conn.execute(
            "SELECT customer_id FROM customers "
            "WHERE normalized_name = ? COLLATE NOCASE",
            [name]
        ).fetchone()
        if existing:
            return {
                "error": f"Advertiser '{name}' already exists",
                "existing_id": existing["customer_id"],
                "status": 409
            }

        # Fuzzy duplicate check
        if not force:
            similar = self._fuzzy_check_customers(conn, name)
            if similar:
                return {
                    "needs_confirmation": True,
                    "similar_entities": similar
                }

        # Validate sector_id
        if sector_id is not None:
            try:
                sector_id = (
                    int(sector_id) if sector_id else None
                )
            except (ValueError, TypeError):
                return {"error": "Invalid sector_id"}

        # Validate agency_id
        if agency_id is not None:
            try:
                agency_id = (
                    int(agency_id) if agency_id else None
                )
            except (ValueError, TypeError):
                return {"error": "Invalid agency_id"}
            if agency_id:
                agency_check = conn.execute(
                    "SELECT agency_id FROM agencies "
                    "WHERE agency_id = ? AND is_active = 1",
                    [agency_id]
                ).fetchone()
                if not agency_check:
                    return {
                        "error": "Selected agency does not "
                        "exist or is inactive"
                    }

        conn.execute("""
            INSERT INTO customers
                (normalized_name, sector_id, agency_id,
                 po_number, affidavit_required, assigned_ae,
                 address, city, state, zip, notes, is_active,
                 commission_rate, order_rate_basis)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """, [
            name, sector_id, agency_id, po_number,
            affidavit_required, assigned_ae, address, city,
            state, zip_code, notes, commission_rate,
            order_rate_basis
        ])
        entity_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]

        # Sector junction table
        if sector_id:
            conn.execute("""
                INSERT INTO customer_sectors
                    (customer_id, sector_id, is_primary,
                     assigned_by)
                VALUES (?, ?, 1, ?)
            """, [entity_id, sector_id, actor])

        self._post_create(
            conn, "customer", entity_id, name, assigned_ae,
            contact_name, contact_title, contact_email,
            contact_phone, contact_role, sector_id, actor
        )

        return {
            "entity_type": "customer",
            "entity_id": entity_id,
            "name": name,
            "status": 201
        }

    def _post_create(
        self, conn, entity_type, entity_id, name,
        assigned_ae, contact_name, contact_title,
        contact_email, contact_phone, contact_role,
        sector_id, actor
    ):
        """Shared post-creation steps: AE history, contact, audit."""
        if assigned_ae:
            conn.execute("""
                INSERT INTO ae_assignments
                    (entity_type, entity_id, ae_name,
                     created_by)
                VALUES (?, ?, ?, ?)
            """, [entity_type, entity_id, assigned_ae, actor])

        if contact_name:
            conn.execute("""
                INSERT INTO entity_contacts
                    (entity_type, entity_id, contact_name,
                     contact_title, email, phone,
                     is_primary, contact_role, created_by)
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, [
                entity_type, entity_id, contact_name,
                contact_title, contact_email, contact_phone,
                contact_role, actor
            ])

        conn.execute("""
            INSERT INTO canon_audit
                (actor, action, key, value, extra)
            VALUES (?, 'CREATE_ENTITY', ?, ?, ?)
        """, [
            actor,
            f"{entity_type}:{entity_id}",
            name,
            f"type={entity_type}"
            f"|sector_id={sector_id or 'none'}"
            f"|agency_id=none"
        ])

    def _fuzzy_check_agencies(self, conn, name):
        """Check for fuzzy duplicate agencies."""
        rows = conn.execute(
            "SELECT agency_id, agency_name FROM agencies "
            "WHERE is_active = 1"
        ).fetchall()
        similar = []
        for row in rows:
            score = _score_name(name, row["agency_name"])
            if score >= 0.60:
                similar.append({
                    "id": row["agency_id"],
                    "name": row["agency_name"],
                    "score": round(score * 100)
                })
        if similar:
            similar.sort(
                key=lambda x: x["score"], reverse=True
            )
            return similar[:5]
        return []

    def _fuzzy_check_customers(self, conn, name):
        """Check for fuzzy duplicate customers."""
        rows = conn.execute(
            "SELECT customer_id, normalized_name "
            "FROM customers WHERE is_active = 1"
        ).fetchall()
        similar = []
        for row in rows:
            score = _score_name(
                name, row["normalized_name"]
            )
            if score >= 0.60:
                similar.append({
                    "id": row["customer_id"],
                    "name": row["normalized_name"],
                    "score": round(score * 100)
                })
        if similar:
            similar.sort(
                key=lambda x: x["score"], reverse=True
            )
            return similar[:5]
        return []

    def deactivate_entity(self, conn, entity_type, entity_id,
                          actor):
        """Set is_active=0 with audit trail.

        Returns dict with success/error info.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )
        name_col = (
            "agency_name" if entity_type == "agency"
            else "normalized_name"
        )

        row = conn.execute(
            f"SELECT {name_col} AS name, is_active "
            f"FROM {table} WHERE {id_col} = ?",
            [entity_id]
        ).fetchone()

        if not row:
            return {"error": "Entity not found", "status": 404}
        if not row["is_active"]:
            return {
                "error": "Entity is already inactive",
                "status": 400
            }

        conn.execute(
            f"UPDATE {table} SET is_active = 0 "
            f"WHERE {id_col} = ?",
            [entity_id]
        )

        conn.execute("""
            INSERT INTO canon_audit
                (actor, action, key, value, extra)
            VALUES (?, 'DEACTIVATE_ENTITY', ?, ?, ?)
        """, [
            actor,
            f"{entity_type}:{entity_id}",
            row["name"],
            f"type={entity_type}"
        ])

        return {
            "success": True,
            "message": f"{row['name']} has been deactivated"
        }

    def reactivate_entity(self, conn, entity_type, entity_id,
                          actor):
        """Set is_active=1 with audit trail.

        Returns dict with success/error info.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )
        name_col = (
            "agency_name" if entity_type == "agency"
            else "normalized_name"
        )

        row = conn.execute(
            f"SELECT {name_col} AS name, is_active "
            f"FROM {table} WHERE {id_col} = ?",
            [entity_id]
        ).fetchone()

        if not row:
            return {"error": "Entity not found", "status": 404}
        if row["is_active"]:
            return {
                "error": "Entity is already active",
                "status": 400
            }

        conn.execute(
            f"UPDATE {table} SET is_active = 1 "
            f"WHERE {id_col} = ?",
            [entity_id]
        )

        conn.execute("""
            INSERT INTO canon_audit
                (actor, action, key, value, extra)
            VALUES (?, 'REACTIVATE_ENTITY', ?, ?, ?)
        """, [
            actor,
            f"{entity_type}:{entity_id}",
            row["name"],
            f"type={entity_type}"
        ])

        return {
            "success": True,
            "message": f"{row['name']} has been reactivated"
        }

    def update_address(self, conn, entity_type, entity_id,
                       data):
        """Update address/city/state/zip on the main entity table.

        Returns dict with success/error.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )

        conn.execute(f"""
            UPDATE {table}
            SET address = ?, city = ?, state = ?, zip = ?
            WHERE {id_col} = ?
        """, [
            data.get("address"),
            data.get("city"),
            data.get("state"),
            data.get("zip"),
            entity_id
        ])

        return {"success": True}

    def update_notes(self, conn, entity_type, entity_id,
                     notes):
        """Update notes field on the main entity table.

        Returns dict with success/error.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )

        conn.execute(f"""
            UPDATE {table} SET notes = ? WHERE {id_col} = ?
        """, [notes, entity_id])

        return {"success": True}

    def update_billing_info(self, conn, entity_type,
                            entity_id, data):
        """Update PO, EDI, commission, affidavit fields.

        Returns dict with success/error.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        po_number = (
            (data.get("po_number") or "").strip() or None
        )
        edi_billing = 1 if data.get("edi_billing") else 0
        affidavit_required = (
            1 if data.get("affidavit_required") else 0
        )

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )

        if entity_type == "customer":
            set_parts = [
                "po_number = ?", "edi_billing = ?",
                "affidavit_required = ?"
            ]
            params = [po_number, edi_billing,
                      affidavit_required]

            if "commission_rate" in data:
                cr = data.get("commission_rate")
                if cr is not None and cr != "":
                    try:
                        cr = float(cr)
                    except (ValueError, TypeError):
                        return {
                            "error": "Commission rate must "
                            "be a number"
                        }
                    if not (0 <= cr <= 100):
                        return {
                            "error": "Commission rate must "
                            "be 0-100"
                        }
                else:
                    cr = None
                set_parts.append("commission_rate = ?")
                params.append(cr)

            if "order_rate_basis" in data:
                orb = data.get("order_rate_basis") or None
                if (orb is not None
                        and orb not in ("gross", "net")):
                    return {
                        "error": "Order rate basis must be "
                        "'gross' or 'net'"
                    }
                set_parts.append("order_rate_basis = ?")
                params.append(orb)

            params.append(entity_id)
            conn.execute(f"""
                UPDATE {table}
                SET {', '.join(set_parts)}
                WHERE {id_col} = ?
            """, params)
        else:
            # Agency: always update all billing fields
            commission_rate = data.get("commission_rate")
            if (commission_rate is not None
                    and commission_rate != ""):
                try:
                    commission_rate = float(commission_rate)
                except (ValueError, TypeError):
                    return {
                        "error": "Commission rate must "
                        "be a number"
                    }
                if not (0 <= commission_rate <= 100):
                    return {
                        "error": "Commission rate must "
                        "be 0-100"
                    }
            else:
                commission_rate = None

            order_rate_basis = (
                data.get("order_rate_basis") or None
            )
            if (order_rate_basis is not None
                    and order_rate_basis
                    not in ("gross", "net")):
                return {
                    "error": "Order rate basis must be "
                    "'gross' or 'net'"
                }

            conn.execute(f"""
                UPDATE {table}
                SET po_number = ?, edi_billing = ?,
                    commission_rate = ?,
                    order_rate_basis = ?
                WHERE {id_col} = ?
            """, [
                po_number, edi_billing, commission_rate,
                order_rate_basis, entity_id
            ])

        return {"success": True}

    def update_sector(self, conn, entity_id, sector_id,
                      actor):
        """Update primary sector via junction table
        (backward compat).

        Returns dict with success/error and sector_name.
        """
        if sector_id is not None:
            try:
                sector_id = (
                    int(sector_id) if sector_id else None
                )
            except (ValueError, TypeError):
                return {"error": "Invalid sector_id"}

        if sector_id:
            conn.execute("""
                INSERT INTO customer_sectors
                    (customer_id, sector_id, is_primary,
                     assigned_by)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(customer_id, sector_id)
                DO UPDATE SET is_primary = 1
            """, [entity_id, sector_id, actor])
        else:
            conn.execute(
                "DELETE FROM customer_sectors "
                "WHERE customer_id = ?",
                [entity_id]
            )
            conn.execute(
                "UPDATE customers SET sector_id = NULL "
                "WHERE customer_id = ?",
                [entity_id]
            )

        sector_name = None
        if sector_id:
            sector = conn.execute(
                "SELECT sector_name FROM sectors "
                "WHERE sector_id = ?",
                [sector_id]
            ).fetchone()
            sector_name = (
                sector["sector_name"] if sector else None
            )

        return {"success": True, "sector_name": sector_name}

    def update_sectors(self, conn, entity_id, sectors, actor):
        """Bulk replace all sector assignments for a customer.

        sectors: list of dicts with sector_id and optional
        is_primary.

        Returns dict with success/error and updated sectors.
        """
        if not isinstance(sectors, list):
            return {"error": "sectors must be an array"}

        primary_count = sum(
            1 for s in sectors if s.get("is_primary")
        )
        if len(sectors) > 0 and primary_count != 1:
            return {
                "error": "Exactly one sector must be marked "
                "as primary"
            }

        # Get current state for audit
        old_sectors = conn.execute(
            "SELECT sector_id, is_primary "
            "FROM customer_sectors WHERE customer_id = ?",
            [entity_id]
        ).fetchall()

        # Delete existing
        conn.execute(
            "DELETE FROM customer_sectors "
            "WHERE customer_id = ?",
            [entity_id]
        )

        if not sectors:
            conn.execute(
                "UPDATE customers SET sector_id = NULL "
                "WHERE customer_id = ?",
                [entity_id]
            )
        else:
            # Insert new (primary first so trigger fires)
            for s in sorted(
                sectors,
                key=lambda x: not x.get("is_primary", False)
            ):
                sid = int(s["sector_id"])
                is_primary = (
                    1 if s.get("is_primary") else 0
                )
                conn.execute("""
                    INSERT INTO customer_sectors
                        (customer_id, sector_id, is_primary,
                         assigned_by)
                    VALUES (?, ?, ?, ?)
                """, [entity_id, sid, is_primary, actor])

        # Audit
        old_ids = [r["sector_id"] for r in old_sectors]
        new_ids = [s["sector_id"] for s in sectors]
        conn.execute("""
            INSERT INTO canon_audit
                (actor, action, key, value, extra)
            VALUES (?, 'SECTOR_ASSIGN', ?, ?, ?)
        """, [
            actor,
            f"customer:{entity_id}",
            f"sectors={new_ids}",
            f"old_sectors={old_ids}"
        ])

        # Return updated sectors
        rows = conn.execute("""
            SELECT cs.sector_id, s.sector_name,
                   s.sector_code, cs.is_primary
            FROM customer_sectors cs
            JOIN sectors s ON cs.sector_id = s.sector_id
            WHERE cs.customer_id = ?
            ORDER BY cs.is_primary DESC, s.sector_name
        """, [entity_id]).fetchall()

        return {
            "success": True,
            "sectors": [dict(r) for r in rows]
        }

    def update_agency(self, conn, entity_id, agency_id):
        """Update agency_id on a customer.

        Returns dict with success/error and agency_name.
        """
        if agency_id is not None:
            try:
                agency_id = (
                    int(agency_id) if agency_id else None
                )
            except (ValueError, TypeError):
                return {"error": "Invalid agency_id"}

        agency_name = None
        if agency_id:
            agency = conn.execute(
                "SELECT agency_name FROM agencies "
                "WHERE agency_id = ? AND is_active = 1",
                [agency_id]
            ).fetchone()
            if not agency:
                return {
                    "error": "Agency not found or inactive",
                    "status": 400
                }
            agency_name = agency["agency_name"]

        conn.execute(
            "UPDATE customers SET agency_id = ? "
            "WHERE customer_id = ?",
            [agency_id, entity_id]
        )

        return {
            "success": True, "agency_name": agency_name
        }

    def get_agency_customers(self, conn, agency_id):
        """Get all customers linked to agency via 3 sources:
        spots, name prefix, and direct agency_id.

        Returns dict with agency info and customers list,
        or error dict.
        """
        agency = conn.execute(
            "SELECT agency_name, commission_rate, "
            "order_rate_basis FROM agencies "
            "WHERE agency_id = ? AND is_active = 1",
            [agency_id]
        ).fetchone()
        if not agency:
            return {"error": "Agency not found", "status": 404}

        # Collect agency name variants
        agency_names = [agency["agency_name"]]
        alias_rows = conn.execute("""
            SELECT alias_name FROM entity_aliases
            WHERE entity_type = 'agency'
              AND target_entity_id = ? AND is_active = 1
        """, [agency_id]).fetchall()
        for ar in alias_rows:
            agency_names.append(ar["alias_name"])

        # Source 1: spots with this agency_id
        spot_customers = conn.execute("""
            SELECT
                c.customer_id,
                c.normalized_name as customer_name,
                c.sector_id, s.sector_name,
                c.po_number, c.edi_billing,
                c.commission_rate, c.order_rate_basis,
                SUM(CASE
                    WHEN sp.revenue_type != 'Trade'
                         OR sp.revenue_type IS NULL
                    THEN sp.gross_rate ELSE 0
                END) as revenue_via_agency,
                COUNT(sp.spot_id) as spot_count,
                MAX(sp.air_date) as last_active
            FROM spots sp
            JOIN customers c
                ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.agency_id = ? AND c.is_active = 1
            GROUP BY c.customer_id
        """, [agency_id]).fetchall()

        seen_ids = set()
        result_customers = []
        for c in spot_customers:
            result_customers.append(dict(c))
            seen_ids.add(c["customer_id"])

        # Source 2: name prefix match
        for name in agency_names:
            name_customers = conn.execute("""
                SELECT
                    c.customer_id,
                    c.normalized_name as customer_name,
                    c.sector_id, s.sector_name,
                    c.po_number, c.edi_billing,
                    c.commission_rate, c.order_rate_basis,
                    COALESCE((
                        SELECT SUM(
                            CASE WHEN sp.revenue_type != 'Trade'
                                 OR sp.revenue_type IS NULL
                            THEN sp.gross_rate ELSE 0 END
                        ) FROM spots sp
                        WHERE sp.customer_id = c.customer_id
                    ), 0) as revenue_via_agency,
                    COALESCE((
                        SELECT COUNT(*)
                        FROM spots sp
                        WHERE sp.customer_id = c.customer_id
                    ), 0) as spot_count,
                    (SELECT MAX(sp.air_date) FROM spots sp
                     WHERE sp.customer_id = c.customer_id
                    ) as last_active
                FROM customers c
                LEFT JOIN sectors s
                    ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
                  AND c.normalized_name LIKE ? || ':%'
            """, [name]).fetchall()

            for c in name_customers:
                if c["customer_id"] not in seen_ids:
                    result_customers.append(dict(c))
                    seen_ids.add(c["customer_id"])

        # Source 3: directly assigned agency_id
        assigned_customers = conn.execute("""
            SELECT
                c.customer_id,
                c.normalized_name as customer_name,
                c.sector_id, s.sector_name,
                c.po_number, c.edi_billing,
                c.commission_rate, c.order_rate_basis,
                COALESCE((
                    SELECT SUM(
                        CASE WHEN sp.revenue_type != 'Trade'
                             OR sp.revenue_type IS NULL
                        THEN sp.gross_rate ELSE 0 END
                    ) FROM spots sp
                    WHERE sp.customer_id = c.customer_id
                ), 0) as revenue_via_agency,
                COALESCE((
                    SELECT COUNT(*)
                    FROM spots sp
                    WHERE sp.customer_id = c.customer_id
                ), 0) as spot_count,
                (SELECT MAX(sp.air_date) FROM spots sp
                 WHERE sp.customer_id = c.customer_id
                ) as last_active
            FROM customers c
            LEFT JOIN sectors s
                ON c.sector_id = s.sector_id
            WHERE c.agency_id = ? AND c.is_active = 1
        """, [agency_id]).fetchall()

        for c in assigned_customers:
            if c["customer_id"] not in seen_ids:
                result_customers.append(dict(c))
                seen_ids.add(c["customer_id"])

        # Sort by revenue descending
        result_customers.sort(
            key=lambda x: -(x.get("revenue_via_agency") or 0)
        )

        return {
            "agency_id": agency_id,
            "agency_name": agency["agency_name"],
            "commission_rate": agency["commission_rate"],
            "order_rate_basis": agency["order_rate_basis"],
            "customers": result_customers
        }

    def get_agency_duplicates(self, conn, agency_id):
        """Find potential duplicate clients within an agency
        using fuzzy name matching.

        Returns dict with agency info and duplicates list.
        """
        agency = conn.execute(
            "SELECT agency_name FROM agencies "
            "WHERE agency_id = ? AND is_active = 1",
            [agency_id]
        ).fetchone()
        if not agency:
            return {"error": "Agency not found", "status": 404}

        # Collect agency name variants
        agency_names = [agency["agency_name"]]
        for ar in conn.execute(
            "SELECT alias_name FROM entity_aliases "
            "WHERE entity_type = 'agency' "
            "AND target_entity_id = ? AND is_active = 1",
            [agency_id]
        ).fetchall():
            agency_names.append(ar["alias_name"])

        # Gather clients from 3 sources
        seen_ids = set()
        clients = []

        # Source 1: spots
        for row in conn.execute("""
            SELECT c.customer_id,
                   c.normalized_name AS customer_name,
                   c.sector_id, s.sector_name,
                   SUM(CASE
                       WHEN sp.revenue_type != 'Trade'
                            OR sp.revenue_type IS NULL
                       THEN sp.gross_rate ELSE 0
                   END) AS revenue,
                   COUNT(sp.spot_id) AS spot_count
            FROM spots sp
            JOIN customers c
                ON sp.customer_id = c.customer_id
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE sp.agency_id = ? AND c.is_active = 1
            GROUP BY c.customer_id
        """, [agency_id]).fetchall():
            if row["customer_id"] not in seen_ids:
                clients.append(dict(row))
                seen_ids.add(row["customer_id"])

        # Source 2: name prefix match
        for name in agency_names:
            for row in conn.execute("""
                SELECT c.customer_id,
                       c.normalized_name AS customer_name,
                       c.sector_id, s.sector_name,
                       COALESCE((
                           SELECT SUM(
                               CASE WHEN sp.revenue_type != 'Trade'
                                    OR sp.revenue_type IS NULL
                               THEN sp.gross_rate ELSE 0 END
                           ) FROM spots sp
                           WHERE sp.customer_id = c.customer_id
                       ), 0) AS revenue,
                       COALESCE((
                           SELECT COUNT(*)
                           FROM spots sp
                           WHERE sp.customer_id = c.customer_id
                       ), 0) AS spot_count
                FROM customers c
                LEFT JOIN sectors s
                    ON c.sector_id = s.sector_id
                WHERE c.is_active = 1
                  AND c.normalized_name LIKE ? || ':%'
            """, [name]).fetchall():
                if row["customer_id"] not in seen_ids:
                    clients.append(dict(row))
                    seen_ids.add(row["customer_id"])

        # Source 3: directly assigned
        for row in conn.execute("""
            SELECT c.customer_id,
                   c.normalized_name AS customer_name,
                   c.sector_id, s.sector_name,
                   COALESCE((
                       SELECT SUM(
                           CASE WHEN sp.revenue_type != 'Trade'
                                OR sp.revenue_type IS NULL
                           THEN sp.gross_rate ELSE 0 END
                       ) FROM spots sp
                       WHERE sp.customer_id = c.customer_id
                   ), 0) AS revenue,
                   COALESCE((
                       SELECT COUNT(*)
                       FROM spots sp
                       WHERE sp.customer_id = c.customer_id
                   ), 0) AS spot_count
            FROM customers c
            LEFT JOIN sectors s
                ON c.sector_id = s.sector_id
            WHERE c.agency_id = ? AND c.is_active = 1
        """, [agency_id]).fetchall():
            if row["customer_id"] not in seen_ids:
                clients.append(dict(row))
                seen_ids.add(row["customer_id"])

        # Compare all pairs
        duplicates = []
        for i in range(len(clients)):
            for j in range(i + 1, len(clients)):
                a, b = clients[i], clients[j]
                portion_a = client_portion(
                    a["customer_name"]
                )
                portion_b = client_portion(
                    b["customer_name"]
                )
                score = _score_name(portion_a, portion_b)
                if score >= 0.50:
                    if ((a["revenue"] or 0)
                            >= (b["revenue"] or 0)):
                        target, source = a, b
                    else:
                        target, source = b, a
                    duplicates.append({
                        "score": round(score, 2),
                        "source": {
                            "customer_id":
                                source["customer_id"],
                            "customer_name":
                                source["customer_name"],
                            "client_portion":
                                client_portion(
                                    source["customer_name"]
                                ),
                            "revenue":
                                source["revenue"] or 0,
                            "spot_count":
                                source["spot_count"] or 0,
                            "sector_name":
                                source.get(
                                    "sector_name") or ""
                        },
                        "target": {
                            "customer_id":
                                target["customer_id"],
                            "customer_name":
                                target["customer_name"],
                            "client_portion":
                                client_portion(
                                    target["customer_name"]
                                ),
                            "revenue":
                                target["revenue"] or 0,
                            "spot_count":
                                target["spot_count"] or 0,
                            "sector_name":
                                target.get(
                                    "sector_name") or ""
                        }
                    })

        duplicates.sort(key=lambda d: -d["score"])

        return {
            "agency_id": agency_id,
            "agency_name": agency["agency_name"],
            "duplicates": duplicates
        }

    def update_ae(self, conn, entity_type, entity_id,
                  ae_name, actor):
        """Update assigned AE with history tracking.

        Returns dict with success/error.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        table = (
            "agencies" if entity_type == "agency"
            else "customers"
        )
        id_col = (
            "agency_id" if entity_type == "agency"
            else "customer_id"
        )
        name_col = (
            "agency_name" if entity_type == "agency"
            else "normalized_name"
        )

        row = conn.execute(
            f"SELECT {name_col} as name, assigned_ae "
            f"FROM {table} WHERE {id_col} = ?",
            [entity_id]
        ).fetchone()
        if not row:
            return {"error": "Entity not found", "status": 404}

        old_ae = row["assigned_ae"]

        # End current active assignment
        conn.execute("""
            UPDATE ae_assignments
            SET ended_date = datetime('now')
            WHERE entity_type = ? AND entity_id = ?
              AND ended_date IS NULL
        """, [entity_type, entity_id])

        # Insert new assignment if AE is set
        if ae_name:
            conn.execute("""
                INSERT INTO ae_assignments
                    (entity_type, entity_id, ae_name,
                     created_by)
                VALUES (?, ?, ?, ?)
            """, [entity_type, entity_id, ae_name, actor])

        # Update denormalized value
        conn.execute(
            f"UPDATE {table} SET assigned_ae = ? "
            f"WHERE {id_col} = ?",
            [ae_name, entity_id]
        )

        # Audit
        conn.execute("""
            INSERT INTO canon_audit
                (actor, action, key, value, extra)
            VALUES (?, 'AE_ASSIGN', ?, ?, ?)
        """, [
            actor,
            f"{entity_type}:{entity_id}",
            ae_name or "(cleared)",
            f"name={row['name']}"
            f"|old_ae={old_ae or '(none)'}"
            f"|new_ae={ae_name or '(none)'}"
        ])

        return {"success": True, "assigned_ae": ae_name}

    def get_ae_history(self, conn, entity_type, entity_id):
        """Get AE assignment history for an entity.

        Returns list of dicts.
        """
        if entity_type not in ("agency", "customer"):
            return []

        rows = conn.execute("""
            SELECT ae_name, assigned_date, ended_date,
                   created_by, notes
            FROM ae_assignments
            WHERE entity_type = ? AND entity_id = ?
            ORDER BY assigned_date DESC
        """, [entity_type, entity_id]).fetchall()

        return [dict(r) for r in rows]

    def get_ae_list(self, conn):
        """Get sorted unique AE names from spots + existing
        assignments.

        Returns sorted list of strings.
        """
        spot_aes = conn.execute("""
            SELECT DISTINCT sales_person
            FROM spots
            WHERE sales_person IS NOT NULL
              AND sales_person != ''
            ORDER BY sales_person
        """).fetchall()

        assigned_aes = conn.execute("""
            SELECT DISTINCT assigned_ae FROM agencies
            WHERE assigned_ae IS NOT NULL
              AND assigned_ae != ''
            UNION
            SELECT DISTINCT assigned_ae FROM customers
            WHERE assigned_ae IS NOT NULL
              AND assigned_ae != ''
        """).fetchall()

        ae_set = set()
        for row in spot_aes:
            ae_set.add(row["sales_person"])
        for row in assigned_aes:
            ae_set.add(row["assigned_ae"])

        return sorted(ae_set, key=str.lower)

    def get_sectors(self, conn):
        """Active sectors for dropdown.

        Returns list of dicts.
        """
        sectors = conn.execute("""
            SELECT sector_id, sector_code, sector_name,
                   sector_group
            FROM sectors
            WHERE is_active = 1
            ORDER BY CASE sector_group
                WHEN 'Commercial' THEN 1
                WHEN 'Financial' THEN 2
                WHEN 'Healthcare' THEN 3
                WHEN 'Outreach' THEN 4
                WHEN 'Political' THEN 5
                WHEN 'Other' THEN 6 ELSE 7
            END, sector_name
        """).fetchall()
        return [dict(s) for s in sectors]

    def get_markets(self, conn):
        """Distinct markets from spots.

        Returns list of market name strings.
        """
        markets = conn.execute("""
            SELECT DISTINCT market_name
            FROM spots
            WHERE market_name IS NOT NULL
              AND market_name != ''
            ORDER BY market_name
        """).fetchall()
        return [row["market_name"] for row in markets]

    def get_spots_link(self, entity_type, entity_id):
        """URL to filtered spots view. No SQL needed.

        Returns dict with url or error.
        """
        if entity_type not in ("agency", "customer"):
            return {"error": "Invalid entity type"}

        if entity_type == "customer":
            url = f"/datasette/dev/spots?customer_id={entity_id}"
        else:
            url = f"/datasette/dev/spots?agency_id={entity_id}"

        return {"url": url}
