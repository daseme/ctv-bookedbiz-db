"""Service for entity additional address operations.

Extracted from address_book.py routes. All methods accept a conn parameter
(sqlite3.Connection with Row factory) and return plain dicts.
"""

import logging

from src.services.base_service import BaseService

logger = logging.getLogger(__name__)

VALID_ADDRESS_LABELS = [
    "Billing", "Shipping", "PO Box", "Office", "Other",
]


class AddressService(BaseService):
    """Manages additional addresses for agencies and customers."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_addresses(self, conn, entity_type, entity_id):
        """Get active additional addresses for an entity.

        Returns list of address dicts.
        """
        rows = conn.execute("""
            SELECT address_id, address_label, address, city, state, zip,
                   is_primary, notes
            FROM entity_addresses
            WHERE entity_type = ? AND entity_id = ? AND is_active = 1
            ORDER BY is_primary DESC, address_label
        """, [entity_type, entity_id]).fetchall()
        return [dict(r) for r in rows]

    def create_address(
        self, conn, entity_type, entity_id, data, created_by,
    ):
        """Create an additional address for an entity.

        Args:
            data: dict with address_label, address, city, state, zip,
                  is_primary, notes

        Returns dict with address_id on success or error key on failure.
        """
        label = (data.get("address_label") or "").strip()
        if label not in VALID_ADDRESS_LABELS:
            return {
                "error": "Invalid label. Must be one of: "
                         f"{VALID_ADDRESS_LABELS}",
            }

        conn.execute("""
            INSERT INTO entity_addresses
                (entity_type, entity_id, address_label, address,
                 city, state, zip, is_primary, created_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            entity_type, entity_id, label,
            (data.get("address") or "").strip() or None,
            (data.get("city") or "").strip() or None,
            (data.get("state") or "").strip() or None,
            (data.get("zip") or "").strip() or None,
            1 if data.get("is_primary") else 0,
            created_by,
            (data.get("notes") or "").strip() or None,
        ])
        address_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]
        return {"success": True, "address_id": address_id}

    def update_address(self, conn, address_id, data):
        """Update an additional address.

        Returns dict with success or error key.
        """
        existing = conn.execute(
            "SELECT 1 FROM entity_addresses "
            "WHERE address_id = ? AND is_active = 1",
            [address_id],
        ).fetchone()
        if not existing:
            return {"error": "Address not found", "status": 404}

        label = (data.get("address_label") or "").strip()
        if label and label not in VALID_ADDRESS_LABELS:
            return {
                "error": "Invalid label. Must be one of: "
                         f"{VALID_ADDRESS_LABELS}",
            }

        conn.execute("""
            UPDATE entity_addresses
            SET address_label = COALESCE(?, address_label),
                address = ?,
                city = ?,
                state = ?,
                zip = ?,
                is_primary = ?,
                notes = ?,
                updated_date = CURRENT_TIMESTAMP
            WHERE address_id = ?
        """, [
            label or None,
            (data.get("address") or "").strip() or None,
            (data.get("city") or "").strip() or None,
            (data.get("state") or "").strip() or None,
            (data.get("zip") or "").strip() or None,
            1 if data.get("is_primary") else 0,
            (data.get("notes") or "").strip() or None,
            address_id,
        ])
        return {"success": True}

    def delete_address(self, conn, address_id):
        """Soft-delete an additional address.

        Returns dict with success key.
        """
        conn.execute("""
            UPDATE entity_addresses
            SET is_active = 0, updated_date = CURRENT_TIMESTAMP
            WHERE address_id = ?
        """, [address_id])
        return {"success": True}
