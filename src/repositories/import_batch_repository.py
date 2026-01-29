#!/usr/bin/env python3
"""
Import Batch Repository - Data access layer for import_batches table.

Handles all SQL operations related to import batch tracking.
"""

import sqlite3
from typing import List, Optional
from dataclasses import dataclass
from enum import Enum


# ============================================================================
# Value Objects
# ============================================================================


class ImportStatus(Enum):
    """Status of an import batch."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ImportBatchRecord:
    """Immutable record of an import batch."""

    batch_id: str
    import_mode: str
    source_file: str
    import_date: str
    status: ImportStatus
    records_imported: int
    records_deleted: int
    months_affected: List[str]
    error_summary: Optional[str] = None
    completed_at: Optional[str] = None


# ============================================================================
# Repository
# ============================================================================


class ImportBatchRepository:
    """
    Data access layer for import_batches table.

    Connections are passed in - this class doesn't manage connections.
    """

    def create_batch(
        self,
        batch_id: str,
        import_mode: str,
        source_file: str,
        months_affected: List[str],
        conn: sqlite3.Connection,
    ) -> Optional[int]:
        """
        Create a new import batch record.

        Args:
            batch_id: Unique batch identifier
            import_mode: WEEKLY_UPDATE, HISTORICAL, or MANUAL
            source_file: Path to source Excel file
            months_affected: List of broadcast months being imported
            conn: Active database connection

        Returns:
            The inserted row ID
        """
        cursor = conn.execute(
            """
            INSERT INTO import_batches 
            (batch_id, import_mode, source_file, import_date, status, broadcast_months_affected)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        """,
            (
                batch_id,
                import_mode,
                source_file,
                ImportStatus.RUNNING.value,
                str(months_affected),
            ),
        )

        return cursor.lastrowid

    def batch_exists(self, batch_id: str, conn: sqlite3.Connection) -> bool:
        """Check if a batch ID exists in the database."""
        cursor = conn.execute(
            "SELECT COUNT(*) FROM import_batches WHERE batch_id = ?", (batch_id,)
        )
        return cursor.fetchone()[0] > 0

    def mark_completed(
        self,
        batch_id: str,
        records_imported: int,
        records_deleted: int,
        conn: sqlite3.Connection,
    ) -> None:
        """
        Mark an import batch as completed with statistics.

        Args:
            batch_id: The batch to update
            records_imported: Number of records imported
            records_deleted: Number of records deleted
            conn: Active database connection
        """
        conn.execute(
            """
            UPDATE import_batches 
            SET status = ?,
                completed_at = CURRENT_TIMESTAMP,
                records_imported = ?,
                records_deleted = ?
            WHERE batch_id = ?
        """,
            (ImportStatus.COMPLETED.value, records_imported, records_deleted, batch_id),
        )

    def mark_failed(
        self, batch_id: str, error_message: str, conn: sqlite3.Connection
    ) -> None:
        """
        Mark an import batch as failed.

        Args:
            batch_id: The batch to update
            error_message: Description of the failure
            conn: Active database connection
        """
        conn.execute(
            """
            UPDATE import_batches 
            SET status = ?,
                completed_at = CURRENT_TIMESTAMP,
                error_summary = ?
            WHERE batch_id = ?
        """,
            (ImportStatus.FAILED.value, error_message, batch_id),
        )

    def get_recent_batches(
        self, limit: int, conn: sqlite3.Connection
    ) -> List[ImportBatchRecord]:
        """
        Get recent import batch history.

        Args:
            limit: Maximum number of records to return
            conn: Active database connection

        Returns:
            List of ImportBatchRecord objects, newest first
        """
        cursor = conn.execute(
            """
            SELECT 
                batch_id, import_mode, source_file, import_date, status,
                records_imported, records_deleted, broadcast_months_affected,
                error_summary, completed_at
            FROM import_batches 
            ORDER BY import_date DESC 
            LIMIT ?
        """,
            (limit,),
        )

        results = []
        for row in cursor.fetchall():
            # Parse months_affected from string representation
            months_str = row[7]
            try:
                months = eval(months_str) if months_str else []
            except:
                months = []

            results.append(
                ImportBatchRecord(
                    batch_id=row[0],
                    import_mode=row[1],
                    source_file=row[2],
                    import_date=row[3],
                    status=ImportStatus(row[4]) if row[4] else ImportStatus.RUNNING,
                    records_imported=row[5] or 0,
                    records_deleted=row[6] or 0,
                    months_affected=months,
                    error_summary=row[8],
                    completed_at=row[9],
                )
            )

        return results

    def cleanup_stale_batches(
        self, hours_threshold: int, conn: sqlite3.Connection
    ) -> int:
        """
        Clean up import batches that have been RUNNING for too long.

        Args:
            hours_threshold: Mark as failed if running longer than this
            conn: Active database connection

        Returns:
            Number of batches cleaned up
        """
        cursor = conn.execute(
            f"""
            UPDATE import_batches 
            SET status = ?,
                error_summary = 'Import timed out or was interrupted',
                completed_at = CURRENT_TIMESTAMP
            WHERE status = ? 
              AND import_date < datetime('now', '-{hours_threshold} hours')
        """,
            (ImportStatus.FAILED.value, ImportStatus.RUNNING.value),
        )

        return cursor.rowcount
