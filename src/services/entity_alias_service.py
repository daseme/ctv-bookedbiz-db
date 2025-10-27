#!/usr/bin/env python3
"""
Entity Alias Service - Manages entity name variations and mappings.
"""

import logging
from typing import Optional
from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


class EntityAliasService(BaseService):
    """Service for managing entity aliases and resolving name variations."""

    def lookup_customer_by_alias(self, alias_name: str, conn=None) -> Optional[int]:
        """
        Look up customer_id by alias name.

        Args:
            alias_name: The alias name to look up
            conn: Optional database connection to reuse (prevents locking)

        Returns:
            customer_id if found, None otherwise
        """
        try:
            if conn:
                # Use the passed connection (we're in a transaction)
                cursor = conn.execute(
                    """
                    SELECT target_entity_id 
                    FROM entity_aliases 
                    WHERE alias_name = ? 
                      AND entity_type = 'customer' 
                      AND is_active = 1
                """,
                    (alias_name.strip(),),
                )

                result = cursor.fetchone()
                return result[0] if result else None
            else:
                # Create our own connection (standalone usage)
                with self.safe_connection() as conn:
                    cursor = conn.execute(
                        """
                        SELECT target_entity_id 
                        FROM entity_aliases 
                        WHERE alias_name = ? 
                          AND entity_type = 'customer' 
                          AND is_active = 1
                    """,
                        (alias_name.strip(),),
                    )

                    result = cursor.fetchone()
                    return result[0] if result else None

        except Exception as e:
            logger.error(f"Failed to lookup customer alias '{alias_name}': {e}")
            return None

    def lookup_agency_by_alias(self, alias_name: str, conn=None) -> Optional[int]:
        """Look up agency_id by alias name."""
        try:
            if conn:
                # Use the passed connection (we're in a transaction)
                cursor = conn.execute(
                    """
                    SELECT target_entity_id 
                    FROM entity_aliases 
                    WHERE alias_name = ? 
                      AND entity_type = 'agency' 
                      AND is_active = 1
                """,
                    (alias_name.strip(),),
                )

                result = cursor.fetchone()
                return result[0] if result else None
            else:
                # Create our own connection (standalone usage)
                with self.safe_connection() as conn:
                    cursor = conn.execute(
                        """
                        SELECT target_entity_id 
                        FROM entity_aliases 
                        WHERE alias_name = ? 
                          AND entity_type = 'agency' 
                          AND is_active = 1
                    """,
                        (alias_name.strip(),),
                    )

                    result = cursor.fetchone()
                    return result[0] if result else None

        except Exception as e:
            logger.error(f"Failed to lookup agency alias '{alias_name}': {e}")
            return None
