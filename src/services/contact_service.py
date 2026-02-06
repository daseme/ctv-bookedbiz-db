# src/services/contact_service.py
"""
Contact Service

Unified contact management for agencies and customers.
Provides CRUD operations for entity_contacts table.
"""

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import sqlite3
from contextlib import contextmanager


@dataclass
class Contact:
    """A contact record for an entity."""
    contact_id: int
    entity_type: str
    entity_id: int
    contact_name: str
    contact_title: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    contact_role: Optional[str]  # decision_maker, account_manager, billing, technical, other
    is_primary: bool
    is_active: bool
    created_by: str
    created_date: str
    updated_date: str
    last_contacted: Optional[str]
    notes: Optional[str]


class ContactService:
    """
    Manages contacts for agencies and customers.

    Supports primary contact designation with automatic
    demotion of existing primary contacts.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def _db_ro(self):
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _db_rw(self):
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def get_contacts(
        self,
        entity_type: str,
        entity_id: int,
        include_inactive: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get all contacts for an entity.

        Returns contacts ordered by: primary first, then by name.
        """
        if entity_type not in ('customer', 'agency'):
            raise ValueError(f"Invalid entity_type: {entity_type}")

        sql = """
            SELECT
                contact_id, entity_type, entity_id,
                contact_name, contact_title, email, phone,
                contact_role, is_primary, is_active, created_by, created_date,
                updated_date, last_contacted, notes
            FROM entity_contacts
            WHERE entity_type = ? AND entity_id = ?
        """
        params = [entity_type, entity_id]

        if not include_inactive:
            sql += " AND is_active = 1"

        sql += " ORDER BY is_primary DESC, contact_name ASC"

        with self._db_ro() as db:
            rows = db.execute(sql, params).fetchall()

        return [
            {
                "contact_id": r["contact_id"],
                "entity_type": r["entity_type"],
                "entity_id": r["entity_id"],
                "contact_name": r["contact_name"],
                "contact_title": r["contact_title"],
                "email": r["email"],
                "phone": r["phone"],
                "contact_role": r["contact_role"],
                "is_primary": bool(r["is_primary"]),
                "is_active": bool(r["is_active"]),
                "created_by": r["created_by"],
                "created_date": r["created_date"],
                "updated_date": r["updated_date"],
                "last_contacted": r["last_contacted"],
                "notes": r["notes"],
            }
            for r in rows
        ]

    def get_contact(self, contact_id: int) -> Optional[Dict[str, Any]]:
        """Get a single contact by ID."""
        with self._db_ro() as db:
            row = db.execute("""
                SELECT
                    contact_id, entity_type, entity_id,
                    contact_name, contact_title, email, phone,
                    contact_role, is_primary, is_active, created_by, created_date,
                    updated_date, last_contacted, notes
                FROM entity_contacts
                WHERE contact_id = ?
            """, [contact_id]).fetchone()

        if not row:
            return None

        return {
            "contact_id": row["contact_id"],
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "contact_name": row["contact_name"],
            "contact_title": row["contact_title"],
            "email": row["email"],
            "phone": row["phone"],
            "contact_role": row["contact_role"],
            "is_primary": bool(row["is_primary"]),
            "is_active": bool(row["is_active"]),
            "created_by": row["created_by"],
            "created_date": row["created_date"],
            "updated_date": row["updated_date"],
            "last_contacted": row["last_contacted"],
            "notes": row["notes"],
        }

    VALID_ROLES = ['decision_maker', 'account_manager', 'billing', 'technical', 'other', None]

    def create_contact(
        self,
        entity_type: str,
        entity_id: int,
        contact_name: str,
        contact_title: str = None,
        email: str = None,
        phone: str = None,
        contact_role: str = None,
        is_primary: bool = False,
        notes: str = None,
        created_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Create a new contact for an entity.

        If is_primary=True, demotes any existing primary contact.
        contact_role can be: decision_maker, account_manager, billing, technical, other
        """
        if entity_type not in ('customer', 'agency'):
            return {"success": False, "error": f"Invalid entity_type: {entity_type}"}

        contact_name = contact_name.strip() if contact_name else ""
        if not contact_name:
            return {"success": False, "error": "contact_name is required"}

        if contact_role and contact_role not in self.VALID_ROLES:
            return {"success": False, "error": f"Invalid contact_role: {contact_role}"}

        with self._db_rw() as db:
            # Verify entity exists
            if entity_type == 'customer':
                exists = db.execute("""
                    SELECT customer_id FROM customers
                    WHERE customer_id = ? AND is_active = 1
                """, [entity_id]).fetchone()
            else:
                exists = db.execute("""
                    SELECT agency_id FROM agencies
                    WHERE agency_id = ? AND is_active = 1
                """, [entity_id]).fetchone()

            if not exists:
                return {"success": False, "error": f"{entity_type} {entity_id} not found"}

            # If setting as primary, demote existing primary
            if is_primary:
                db.execute("""
                    UPDATE entity_contacts
                    SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
                    WHERE entity_type = ? AND entity_id = ? AND is_primary = 1
                """, [entity_type, entity_id])

            # Create contact
            db.execute("""
                INSERT INTO entity_contacts
                (entity_type, entity_id, contact_name, contact_title,
                 email, phone, contact_role, is_primary, is_active, created_by, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, [
                entity_type, entity_id, contact_name, contact_title,
                email, phone, contact_role, 1 if is_primary else 0, created_by, notes
            ])

            contact_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

            db.commit()

        return {
            "success": True,
            "contact_id": contact_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "contact_name": contact_name,
            "contact_role": contact_role,
            "is_primary": is_primary,
        }

    def update_contact(
        self,
        contact_id: int,
        contact_name: str = None,
        contact_title: str = None,
        email: str = None,
        phone: str = None,
        contact_role: str = None,
        notes: str = None,
        updated_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Update an existing contact.

        Only updates provided fields (None = keep existing).
        Use empty string to clear a field.
        """
        with self._db_rw() as db:
            # Get existing contact
            contact = db.execute("""
                SELECT contact_id, entity_type, entity_id, contact_name
                FROM entity_contacts
                WHERE contact_id = ? AND is_active = 1
            """, [contact_id]).fetchone()

            if not contact:
                return {"success": False, "error": "Contact not found"}

            # Build update query dynamically
            updates = []
            params = []

            if contact_name is not None:
                contact_name = contact_name.strip()
                if not contact_name:
                    return {"success": False, "error": "contact_name cannot be empty"}
                updates.append("contact_name = ?")
                params.append(contact_name)

            if contact_title is not None:
                updates.append("contact_title = ?")
                params.append(contact_title if contact_title else None)

            if email is not None:
                updates.append("email = ?")
                params.append(email if email else None)

            if phone is not None:
                updates.append("phone = ?")
                params.append(phone if phone else None)

            if contact_role is not None:
                if contact_role and contact_role not in self.VALID_ROLES:
                    return {"success": False, "error": f"Invalid contact_role: {contact_role}"}
                updates.append("contact_role = ?")
                params.append(contact_role if contact_role else None)

            if notes is not None:
                updates.append("notes = ?")
                params.append(notes if notes else None)

            if not updates:
                return {"success": False, "error": "No fields to update"}

            updates.append("updated_date = CURRENT_TIMESTAMP")
            params.append(contact_id)

            sql = f"UPDATE entity_contacts SET {', '.join(updates)} WHERE contact_id = ?"
            db.execute(sql, params)
            db.commit()

        return {
            "success": True,
            "contact_id": contact_id,
        }

    def delete_contact(
        self,
        contact_id: int,
        deleted_by: str = "web_user"
    ) -> Dict[str, Any]:
        """Soft-delete a contact (set is_active = 0)."""
        with self._db_rw() as db:
            # Get contact info
            contact = db.execute("""
                SELECT contact_id, contact_name, entity_type, entity_id
                FROM entity_contacts
                WHERE contact_id = ? AND is_active = 1
            """, [contact_id]).fetchone()

            if not contact:
                return {"success": False, "error": "Contact not found"}

            # Soft delete
            db.execute("""
                UPDATE entity_contacts
                SET is_active = 0,
                    is_primary = 0,
                    updated_date = CURRENT_TIMESTAMP,
                    notes = COALESCE(notes, '') || ' | Deleted by ' || ? || ' at ' || datetime('now')
                WHERE contact_id = ?
            """, [deleted_by, contact_id])

            db.commit()

        return {
            "success": True,
            "contact_id": contact_id,
            "contact_name": contact["contact_name"],
        }

    def set_primary(
        self,
        contact_id: int,
        updated_by: str = "web_user"
    ) -> Dict[str, Any]:
        """
        Set a contact as the primary contact.

        Demotes existing primary contact for the same entity.
        """
        with self._db_rw() as db:
            # Get contact info
            contact = db.execute("""
                SELECT contact_id, contact_name, entity_type, entity_id, is_primary
                FROM entity_contacts
                WHERE contact_id = ? AND is_active = 1
            """, [contact_id]).fetchone()

            if not contact:
                return {"success": False, "error": "Contact not found"}

            if contact["is_primary"]:
                return {"success": True, "contact_id": contact_id, "message": "Already primary"}

            # Demote existing primary
            db.execute("""
                UPDATE entity_contacts
                SET is_primary = 0, updated_date = CURRENT_TIMESTAMP
                WHERE entity_type = ? AND entity_id = ? AND is_primary = 1
            """, [contact["entity_type"], contact["entity_id"]])

            # Set new primary
            db.execute("""
                UPDATE entity_contacts
                SET is_primary = 1, updated_date = CURRENT_TIMESTAMP
                WHERE contact_id = ?
            """, [contact_id])

            db.commit()

        return {
            "success": True,
            "contact_id": contact_id,
            "contact_name": contact["contact_name"],
        }

    def get_primary_contact(
        self,
        entity_type: str,
        entity_id: int
    ) -> Optional[Dict[str, Any]]:
        """Get the primary contact for an entity, if any."""
        if entity_type not in ('customer', 'agency'):
            return None

        with self._db_ro() as db:
            row = db.execute("""
                SELECT
                    contact_id, entity_type, entity_id,
                    contact_name, contact_title, email, phone,
                    contact_role, is_primary, is_active, created_by, created_date,
                    updated_date, last_contacted, notes
                FROM entity_contacts
                WHERE entity_type = ? AND entity_id = ?
                  AND is_primary = 1 AND is_active = 1
            """, [entity_type, entity_id]).fetchone()

        if not row:
            return None

        return {
            "contact_id": row["contact_id"],
            "entity_type": row["entity_type"],
            "entity_id": row["entity_id"],
            "contact_name": row["contact_name"],
            "contact_title": row["contact_title"],
            "email": row["email"],
            "phone": row["phone"],
            "contact_role": row["contact_role"],
            "is_primary": True,
            "is_active": True,
            "created_by": row["created_by"],
            "created_date": row["created_date"],
            "updated_date": row["updated_date"],
            "last_contacted": row["last_contacted"],
            "notes": row["notes"],
        }

    def update_last_contacted(
        self,
        contact_id: int,
        contacted_date: str = None
    ) -> Dict[str, Any]:
        """
        Update the last_contacted timestamp for a contact.

        If contacted_date is None, uses current timestamp.
        """
        with self._db_rw() as db:
            # Verify contact exists
            contact = db.execute("""
                SELECT contact_id, contact_name
                FROM entity_contacts
                WHERE contact_id = ? AND is_active = 1
            """, [contact_id]).fetchone()

            if not contact:
                return {"success": False, "error": "Contact not found"}

            if contacted_date:
                db.execute("""
                    UPDATE entity_contacts
                    SET last_contacted = ?, updated_date = CURRENT_TIMESTAMP
                    WHERE contact_id = ?
                """, [contacted_date, contact_id])
            else:
                db.execute("""
                    UPDATE entity_contacts
                    SET last_contacted = CURRENT_TIMESTAMP, updated_date = CURRENT_TIMESTAMP
                    WHERE contact_id = ?
                """, [contact_id])

            db.commit()

        return {
            "success": True,
            "contact_id": contact_id,
            "contact_name": contact["contact_name"],
        }
