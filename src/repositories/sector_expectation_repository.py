"""
Sector Expectation Repository - Data access for sector-level budget expectations.

Handles CRUD operations for the sector_expectations table.
"""

import sqlite3
import logging
from typing import List, Dict, Any
from datetime import datetime
from decimal import Decimal

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.models.planning import (
    SectorExpectation,
    EntitySectorExpectations,
    EntityType,
)

logger = logging.getLogger(__name__)


class SectorExpectationRepository(BaseService):
    """Repository for sector expectation data access."""

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

    # =========================================================================
    # Read Operations
    # =========================================================================

    def get_by_entity_year(self, ae_name: str, year: int) -> List[SectorExpectation]:
        """
        Get all sector expectations for an entity and year.

        Args:
            ae_name: Entity name (AE or House)
            year: Planning year

        Returns:
            List of SectorExpectation objects
        """
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 
                    se.expectation_id,
                    se.ae_name,
                    se.sector_id,
                    s.sector_code,
                    s.sector_name,
                    se.year,
                    se.month,
                    se.expected_amount,
                    se.notes,
                    se.created_date,
                    se.updated_date,
                    se.updated_by
                FROM sector_expectations se
                JOIN sectors s ON se.sector_id = s.sector_id
                WHERE se.ae_name = ? AND se.year = ?
                ORDER BY s.sector_name, se.month
            """,
                (ae_name, year),
            )

            return [self._row_to_expectation(row) for row in cursor.fetchall()]

    def get_by_entity_month(
        self, ae_name: str, year: int, month: int
    ) -> List[SectorExpectation]:
        """Get sector expectations for a specific entity/month."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 
                    se.expectation_id,
                    se.ae_name,
                    se.sector_id,
                    s.sector_code,
                    s.sector_name,
                    se.year,
                    se.month,
                    se.expected_amount,
                    se.notes,
                    se.created_date,
                    se.updated_date,
                    se.updated_by
                FROM sector_expectations se
                JOIN sectors s ON se.sector_id = s.sector_id
                WHERE se.ae_name = ? AND se.year = ? AND se.month = ?
                ORDER BY s.sector_name
            """,
                (ae_name, year, month),
            )

            return [self._row_to_expectation(row) for row in cursor.fetchall()]

    def get_all_for_year(self, year: int) -> Dict[str, EntitySectorExpectations]:
        """
        Get all sector expectations for a year, grouped by entity.

        Args:
            year: Planning year

        Returns:
            Dict mapping entity_name -> EntitySectorExpectations
        """
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 
                    se.expectation_id,
                    se.ae_name,
                    se.sector_id,
                    s.sector_code,
                    s.sector_name,
                    se.year,
                    se.month,
                    se.expected_amount,
                    se.notes,
                    se.created_date,
                    se.updated_date,
                    se.updated_by,
                    re.entity_type
                FROM sector_expectations se
                JOIN sectors s ON se.sector_id = s.sector_id
                JOIN revenue_entities re ON se.ae_name = re.entity_name
                WHERE se.year = ?
                ORDER BY se.ae_name, s.sector_name, se.month
            """,
                (year,),
            )

            result: Dict[str, EntitySectorExpectations] = {}

            for row in cursor.fetchall():
                ae_name = row["ae_name"]

                if ae_name not in result:
                    result[ae_name] = EntitySectorExpectations(
                        entity_name=ae_name,
                        entity_type=EntityType(row["entity_type"]),
                        year=year,
                        expectations=[],
                    )

                result[ae_name].expectations.append(self._row_to_expectation(row))

            return result

    def get_by_sector_year(self, sector_id: int, year: int) -> List[SectorExpectation]:
        """Get all expectations for a sector across all entities."""
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT 
                    se.expectation_id,
                    se.ae_name,
                    se.sector_id,
                    s.sector_code,
                    s.sector_name,
                    se.year,
                    se.month,
                    se.expected_amount,
                    se.notes,
                    se.created_date,
                    se.updated_date,
                    se.updated_by
                FROM sector_expectations se
                JOIN sectors s ON se.sector_id = s.sector_id
                WHERE se.sector_id = ? AND se.year = ?
                ORDER BY se.ae_name, se.month
            """,
                (sector_id, year),
            )

            return [self._row_to_expectation(row) for row in cursor.fetchall()]

    # =========================================================================
    # Write Operations
    # =========================================================================

    def save_expectations(
        self,
        ae_name: str,
        year: int,
        expectations: List[SectorExpectation],
        updated_by: str,
    ) -> int:
        """
        Save sector expectations for an entity/year.

        Uses delete + insert pattern to handle adds, updates, and removes
        in a single transaction.

        Args:
            ae_name: Entity name
            year: Planning year
            expectations: Complete list of expectations for this entity/year
            updated_by: User making the change

        Returns:
            Number of rows saved
        """
        with self.safe_transaction() as conn:
            # Delete existing expectations for this entity/year
            conn.execute(
                """
                DELETE FROM sector_expectations
                WHERE ae_name = ? AND year = ?
            """,
                (ae_name, year),
            )

            # Insert new expectations
            saved_count = 0
            for exp in expectations:
                if exp.expected_amount > 0:  # Only save non-zero amounts
                    conn.execute(
                        """
                        INSERT INTO sector_expectations 
                        (ae_name, sector_id, year, month, expected_amount, notes, updated_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            ae_name,
                            exp.sector_id,
                            year,
                            exp.month,
                            float(exp.expected_amount),
                            exp.notes,
                            updated_by,
                        ),
                    )
                    saved_count += 1

            logger.info(f"Saved {saved_count} sector expectations for {ae_name} {year}")

            return saved_count

    def save_single_expectation(
        self, expectation: SectorExpectation, updated_by: str
    ) -> int:
        """
        Save or update a single sector expectation.

        Args:
            expectation: The expectation to save
            updated_by: User making the change

        Returns:
            The expectation_id
        """
        with self.safe_transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sector_expectations 
                (ae_name, sector_id, year, month, expected_amount, notes, updated_by)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ae_name, sector_id, year, month) DO UPDATE SET
                    expected_amount = excluded.expected_amount,
                    notes = excluded.notes,
                    updated_date = CURRENT_TIMESTAMP,
                    updated_by = excluded.updated_by
                RETURNING expectation_id
            """,
                (
                    expectation.ae_name,
                    expectation.sector_id,
                    expectation.year,
                    expectation.month,
                    float(expectation.expected_amount),
                    expectation.notes,
                    updated_by,
                ),
            )

            row = cursor.fetchone()
            return row["expectation_id"] if row else 0

    def delete_sector_for_entity(self, ae_name: str, sector_id: int, year: int) -> int:
        """
        Remove a sector's expectations for an entity/year.

        Args:
            ae_name: Entity name
            sector_id: Sector to remove
            year: Planning year

        Returns:
            Number of rows deleted
        """
        with self.safe_transaction() as conn:
            cursor = conn.execute(
                """
                DELETE FROM sector_expectations
                WHERE ae_name = ? AND sector_id = ? AND year = ?
            """,
                (ae_name, sector_id, year),
            )

            deleted = cursor.rowcount
            logger.info(
                f"Deleted {deleted} expectations for {ae_name}/sector {sector_id}/{year}"
            )
            return deleted

    def delete_for_entity_year(self, ae_name: str, year: int) -> int:
        """Delete all expectations for an entity/year."""
        with self.safe_transaction() as conn:
            cursor = conn.execute(
                """
                DELETE FROM sector_expectations
                WHERE ae_name = ? AND year = ?
            """,
                (ae_name, year),
            )

            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} expectations for {ae_name}/{year}")
            return deleted

    # =========================================================================
    # Sector Reference Data
    # =========================================================================

    def get_all_sectors(self) -> List[Dict[str, Any]]:
        """Get all active sectors for dropdown population."""
        with self.safe_connection() as conn:
            cursor = conn.execute("""
                SELECT sector_id, sector_code, sector_name
                FROM sectors
                WHERE is_active = 1
                ORDER BY sector_name
            """)

            return [dict(row) for row in cursor.fetchall()]

    def get_available_sectors_for_entity(
        self, ae_name: str, year: int
    ) -> List[Dict[str, Any]]:
        """
        Get sectors not yet used by this entity for the year.

        Used for the "Add Sector" dropdown.
        """
        with self.safe_connection() as conn:
            cursor = conn.execute(
                """
                SELECT s.sector_id, s.sector_code, s.sector_name
                FROM sectors s
                WHERE s.is_active = 1
                AND s.sector_id NOT IN (
                    SELECT DISTINCT sector_id 
                    FROM sector_expectations 
                    WHERE ae_name = ? AND year = ?
                )
                ORDER BY s.sector_name
            """,
                (ae_name, year),
            )

            return [dict(row) for row in cursor.fetchall()]

    # =========================================================================
    # Helpers
    # =========================================================================

    def _row_to_expectation(self, row: sqlite3.Row) -> SectorExpectation:
        """Convert database row to SectorExpectation object."""
        return SectorExpectation(
            expectation_id=row["expectation_id"],
            ae_name=row["ae_name"],
            sector_id=row["sector_id"],
            sector_code=row["sector_code"],
            sector_name=row["sector_name"],
            year=row["year"],
            month=row["month"],
            expected_amount=Decimal(str(row["expected_amount"])),
            notes=row["notes"] if "notes" in row.keys() else None,
            created_date=datetime.fromisoformat(row["created_date"])
            if row["created_date"]
            else None,
            updated_date=datetime.fromisoformat(row["updated_date"])
            if row["updated_date"]
            else None,
            updated_by=row["updated_by"] if "updated_by" in row.keys() else None,
        )
