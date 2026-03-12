"""Service for saved filter preset operations.

Extracted from address_book.py routes. All methods accept a conn parameter
(sqlite3.Connection with Row factory) and return plain dicts.
"""

import json
import logging

from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class SavedFilterService(BaseService):
    """Manages saved filter presets for the address book."""

    def __init__(self, db_connection):
        super().__init__(db_connection)

    def get_filters(self, conn, filter_type="address_book"):
        """Get saved filter presets.

        Returns list of filter dicts with parsed filter_config.
        """
        rows = conn.execute("""
            SELECT filter_id, filter_name, filter_config,
                   created_by, created_date, is_shared
            FROM saved_filters
            WHERE filter_type = ?
            ORDER BY filter_name
        """, [filter_type]).fetchall()

        result = []
        for row in rows:
            d = dict(row)
            d["filter_config"] = (
                json.loads(d["filter_config"])
                if d["filter_config"]
                else {}
            )
            result.append(d)
        return result

    def save_filter(
        self, conn, name, config, created_by, is_shared=False,
    ):
        """Save a filter preset.

        Returns dict with filter_id on success or error key on failure.
        """
        if not name:
            return {"error": "Filter name required"}

        conn.execute("""
            INSERT INTO saved_filters
                (filter_name, filter_type, filter_config,
                 created_by, is_shared)
            VALUES (?, 'address_book', ?, ?, ?)
        """, [name, json.dumps(config), created_by, is_shared])
        filter_id = conn.execute(
            "SELECT last_insert_rowid()"
        ).fetchone()[0]
        return {"success": True, "filter_id": filter_id}

    def delete_filter(self, conn, filter_id):
        """Delete a saved filter.

        Returns dict with success key.
        """
        conn.execute(
            "DELETE FROM saved_filters WHERE filter_id = ?",
            [filter_id],
        )
        return {"success": True}
