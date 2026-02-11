#!/usr/bin/env python3
"""
Broadcast Month Import Service - Refactored with single-responsibility methods.

This service orchestrates the complete import workflow with clear separation
between analysis, filtering, validation, and execution phases.

Enhanced with:
- Multi-sheet source tracking using source_file field
- Month preservation safeguard for unclosed months without Excel data
- Previous month filtering to skip stray MC operational data
- Progress bars and clean output
"""

import re
import sys
import logging
import contextlib
import io
import sqlite3
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Set, Optional, Dict, Any, Callable

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConnection
from src.services.base_service import BaseService
from src.services.month_closure_service import (
    MonthClosureService,
    ValidationResult,
    MonthClosureError,
)
from src.services.import_integration_utilities import (
    extract_display_months_from_excel,
    get_excel_worksheet_flexible,
)
from src.utils.broadcast_month_utils import (
    BroadcastMonthParser,
    normalize_broadcast_day,
)
from src.services.entity_alias_service import EntityAliasService
from src.services.import_performance_optimization import BatchEntityResolver

from src.repositories.spot_repository import SpotRepository
from src.repositories.import_batch_repository import ImportBatchRepository

from src.models.import_workflow import (
    ExcelAnalysis,
    MonthClassification,
    MonthFilterResult,
    PreservedMonth,
    ImportContext,
    ImportResult,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Constants
# ============================================================================

EXCEL_COLUMN_POSITIONS = {
    0: "bill_code",
    1: "air_date",
    2: "end_date",
    3: "day_of_week",
    4: "time_in",
    5: "time_out",
    6: "length_seconds",
    7: "media",
    8: "comments",
    9: "language_code",
    10: "format",
    11: "sequence_number",
    12: "line_number",
    13: "spot_type",
    14: "estimate",
    15: "gross_rate",
    16: "make_good",
    17: "spot_value",
    18: "broadcast_month",
    19: "broker_fees",
    20: "priority",
    21: "station_net",
    22: "sales_person",
    23: "revenue_type",
    24: "billing_type",
    25: "agency_flag",
    26: "affidavit_flag",
    27: "contract",
    28: "market_name",
    29: "sheet_source",
}


# ============================================================================
# Context Managers
# ============================================================================


@contextlib.contextmanager
def suppress_verbose_logging():
    """Context manager to suppress verbose logging during import operations."""
    root_logger = logging.getLogger()
    original_level = root_logger.level
    root_logger.setLevel(logging.WARNING)

    noisy_loggers = ["openpyxl", "xlrd", "pandas", "services", "utils", "__main__"]
    original_levels = {}
    for logger_name in noisy_loggers:
        logger_obj = logging.getLogger(logger_name)
        original_levels[logger_name] = logger_obj.level
        logger_obj.setLevel(logging.WARNING)

    try:
        yield
    finally:
        root_logger.setLevel(original_level)
        for logger_name, level in original_levels.items():
            logging.getLogger(logger_name).setLevel(level)


@contextlib.contextmanager
def suppress_stdout_stderr():
    """Context manager to suppress stdout/stderr during noisy operations."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    try:
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


# ============================================================================
# Value Objects
# ============================================================================


class SourceFileFormatter:
    """Value object for creating standardized source file tracking strings."""

    @staticmethod
    def format_source_file(filename: str, sheet_name: Optional[str]) -> str:
        if sheet_name and sheet_name.strip():
            return f"{filename}:{sheet_name.strip()}"
        return filename

    @staticmethod
    def extract_filename_from_path(file_path: str) -> str:
        return Path(file_path).name


# ============================================================================
# Pure Functions for Normalization
# ============================================================================


def build_revenue_type_normalizer() -> Callable[[Optional[str]], Optional[str]]:
    """Pure normalizer: A&O variants → 'Internal Ad Sales'; else passthrough."""

    def _norm(v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        key = re.sub(r"[\s\./-]+", "", v.lower()).replace("&", "and")
        return "Internal Ad Sales" if key in {"ao", "aando"} else v

    return _norm


def build_spot_type_normalizer() -> Callable[[Optional[str]], str]:
    """Pure normalizer for CHECK constraint on spots.spot_type."""
    allowed = {"AV", "BB", "BNS", "COM", "CRD", "PKG", "PRD", "PRG", "SVC", ""}
    aliases = {
        "SPOT": "COM",
        "COMMERCIAL": "COM",
        "BONUS": "BNS",
        "PKG": "PKG",
        "PROD": "PRD",
        "PROGRAM": "PRG",
        "SERVICE": "SVC",
    }

    def _norm(v: Optional[str]) -> str:
        x = (v or "").strip().upper()
        x = aliases.get(x, x)
        return x if x in allowed else ""

    return _norm


# ============================================================================
# Custom Exception
# ============================================================================


class BroadcastMonthImportError(Exception):
    """Raised when there's an error with import operations."""

    pass


# ============================================================================
# Service Implementation
# ============================================================================


class BroadcastMonthImportService(BaseService):
    """
    Service for managing broadcast month imports.

    Orchestrates the complete import workflow with clear separation between:
    - Analysis: Examining Excel file contents
    - Classification: Determining closed vs open months
    - Filtering: Applying preservation and skip rules
    - Execution: Delete, import, validate within transaction
    """

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)

        # Dependencies
        self.closure_service = MonthClosureService(db_connection)
        self.parser = BroadcastMonthParser()
        self.alias_service = EntityAliasService(db_connection)
        self.batch_resolver = BatchEntityResolver(db_connection)

        # Repositories
        self._spot_repo = SpotRepository()
        self._batch_repo = ImportBatchRepository()

        # Normalizers
        self._normalize_revenue_type = build_revenue_type_normalizer()
        self._normalize_spot_type = build_spot_type_normalizer()

    # ========================================================================
    # Public API: Validation
    # ========================================================================

    def validate_import(self, excel_file: str, import_mode: str) -> ValidationResult:
        """
        Validate Excel file for import based on mode.

        Args:
            excel_file: Path to Excel file
            import_mode: 'HISTORICAL', 'WEEKLY_UPDATE', or 'MANUAL'

        Returns:
            ValidationResult with detailed validation info
        """
        logger.info(f"Validating {excel_file} for {import_mode} import")

        try:
            with suppress_verbose_logging():
                display_months = list(extract_display_months_from_excel(excel_file))

            if not display_months:
                raise BroadcastMonthImportError(
                    "No broadcast months found in Excel file"
                )

            logger.info(
                f"Found {len(display_months)} months in Excel: {sorted(display_months)}"
            )

            return self.closure_service.validate_months_for_import(
                display_months, import_mode
            )

        except Exception as e:
            error_msg = f"Failed to validate import: {str(e)}"
            logger.error(error_msg)
            raise BroadcastMonthImportError(error_msg)

    # ========================================================================
    # Main Orchestrator
    # ========================================================================

    def execute_month_replacement(
        self,
        excel_file: str,
        import_mode: str,
        closed_by: Optional[str] = None,
        dry_run: bool = False,
    ) -> ImportResult:
        """
        Orchestrates the complete import workflow.

        Each step delegates to a focused method with single responsibility.
        """
        start_time = datetime.now()
        batch_id = self._generate_batch_id(import_mode, start_time)
        result = ImportResult.create_empty(batch_id, import_mode)

        try:
            # Step 1: Analyze Excel file
            excel_analysis = self._analyze_excel_file(excel_file)
            if not excel_analysis.has_data:
                raise BroadcastMonthImportError(
                    "No broadcast months found in Excel file"
                )

            # Step 2: Classify months by closure status
            month_classification = self._classify_months(excel_analysis.display_months)

            # Step 3: Build import context
            context = ImportContext(
                batch_id=batch_id,
                import_mode=import_mode,
                excel_analysis=excel_analysis,
                month_classification=month_classification,
                closed_by=closed_by,
                dry_run=dry_run,
            )

            # Step 4: Determine which months to process based on mode
            filter_result = self._determine_months_to_process(context)
            if filter_result is None:
                result.success = True
                return result

            context.filter_result = filter_result
            result.broadcast_months_affected = context.months_to_process

            # Step 5: Validate mode-specific requirements
            validation_error = self._validate_import_mode_requirements(context)
            if validation_error:
                result.add_error(validation_error)
                return result

            tqdm.write(
                f"Processing {len(context.months_to_process)} months: "
                f"{', '.join(sorted(context.months_to_process))}"
            )

            # Step 6: Handle dry run
            if dry_run:
                tqdm.write("DRY RUN - No changes will be made")
                result.success = True
                return result

            # Step 7: Execute the actual import
            result = self._execute_import_workflow(context, result)

        except BroadcastMonthImportError:
            raise
        except Exception as e:
            error_msg = f"Import failed: {str(e)}"
            tqdm.write(f"❌ {error_msg}")
            result.add_error(error_msg)
            self._fail_import_batch(batch_id, str(e))

        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result

    # ========================================================================
    # Step 1: Excel Analysis
    # ========================================================================

    def _analyze_excel_file(self, excel_file: str) -> ExcelAnalysis:
        """Analyze Excel file to extract months and record counts."""
        tqdm.write(f"Analyzing Excel file: {Path(excel_file).name}")

        with suppress_verbose_logging():
            display_months = list(extract_display_months_from_excel(excel_file))

        tqdm.write(
            f"Found {len(display_months)} months: {', '.join(sorted(display_months))}"
        )

        month_counts = self._count_excel_records_by_month(excel_file)

        return ExcelAnalysis.from_file(
            file_path=excel_file,
            display_months=display_months,
            month_counts=month_counts,
        )

    def _count_excel_records_by_month(self, excel_file: str) -> Dict[str, int]:
        """Count records per broadcast month in the Excel file."""
        month_counts: Dict[str, int] = {}

        try:
            with suppress_verbose_logging(), suppress_stdout_stderr():
                worksheet, sheet_name, workbook = get_excel_worksheet_flexible(
                    excel_file
                )

            month_col_indices = [
                k for k, v in EXCEL_COLUMN_POSITIONS.items() if v == "broadcast_month"
            ]
            if not month_col_indices:
                workbook.close()
                return month_counts
            month_col = month_col_indices[0]

            for row in worksheet.iter_rows(min_row=2, values_only=True):
                if not any(row):
                    continue

                month_value = row[month_col] if month_col < len(row) else None
                if not month_value:
                    continue

                display_month = self._parse_month_value(month_value)
                if display_month:
                    month_counts[display_month] = month_counts.get(display_month, 0) + 1

            workbook.close()

        except Exception as e:
            tqdm.write(f"Warning: Could not count Excel records by month: {e}")

        return month_counts

    def _parse_month_value(self, month_value: Any) -> Optional[str]:
        """Parse a month cell value into display format (e.g., 'Dec-25')."""
        try:
            bm_date: Optional[date] = None

            if hasattr(month_value, "date"):
                bm_date = month_value.date()
            elif isinstance(month_value, str):
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        bm_date = datetime.strptime(month_value.strip(), fmt).date()
                        break
                    except ValueError:
                        continue
            elif isinstance(month_value, date):
                bm_date = month_value

            if bm_date is None:
                return None

            return bm_date.strftime("%b-%y")
        except Exception:
            return None

    # ========================================================================
    # Step 2: Month Classification
    # ========================================================================

    def _classify_months(self, display_months: List[str]) -> MonthClassification:
        """Classify months into closed vs open."""
        closed_months = self.closure_service.get_closed_months(display_months)
        open_months = [m for m in display_months if m not in closed_months]

        if closed_months:
            tqdm.write(
                f"Closed months: {len(closed_months)} "
                f"({', '.join(sorted(closed_months))})"
            )
        if open_months:
            tqdm.write(
                f"Open months: {len(open_months)} ({', '.join(sorted(open_months))})"
            )

        return MonthClassification(
            closed_months=list(closed_months), open_months=open_months
        )

    # ========================================================================
    # Step 4: Month Filtering (Mode-Specific)
    # ========================================================================

    def _determine_months_to_process(
        self, context: ImportContext
    ) -> Optional[MonthFilterResult]:
        """Determine which months to process based on import mode."""
        if context.is_weekly_update_mode:
            return self._filter_months_for_weekly_update(context)

        return MonthFilterResult(
            months_to_process=context.excel_analysis.display_months,
            preserved_months={},
            skipped_previous_month=None,
        )

    def _filter_months_for_weekly_update(
        self, context: ImportContext
    ) -> Optional[MonthFilterResult]:
        """Apply WEEKLY_UPDATE filtering rules."""
        classification = context.month_classification
        excel_analysis = context.excel_analysis

        if classification.has_closed:
            tqdm.write(
                f"WEEKLY_UPDATE: Auto-skipping "
                f"{len(classification.closed_months)} closed months"
            )

        if classification.all_closed:
            tqdm.write("No open months to import - all months are closed")
            return None

        working_months = list(classification.open_months)

        tqdm.write("Analyzing Excel data by month for preservation check...")
        preserved = self._identify_months_to_preserve(
            working_months, excel_analysis.month_record_counts
        )

        if preserved:
            self._log_preserved_months(preserved)
            working_months = [m for m in working_months if m not in preserved]

        skipped_previous = self._check_previous_month_skip(
            working_months, excel_analysis.month_record_counts
        )

        if skipped_previous:
            working_months = [m for m in working_months if m != skipped_previous]

        if not working_months:
            tqdm.write(
                "✅ No months to update after filtering - existing data protected"
            )
            return None

        tqdm.write(
            f"Proceeding with {len(working_months)} month(s): "
            f"{', '.join(sorted(working_months))}"
        )

        return MonthFilterResult(
            months_to_process=working_months,
            preserved_months=preserved,
            skipped_previous_month=skipped_previous,
        )

    def _identify_months_to_preserve(
        self, open_months: List[str], excel_month_counts: Dict[str, int]
    ) -> Dict[str, PreservedMonth]:
        """Identify months that should be preserved."""
        preserved: Dict[str, PreservedMonth] = {}

        with self.safe_connection() as conn:
            for month in open_months:
                excel_count = excel_month_counts.get(month, 0)

                if excel_count > 0:
                    tqdm.write(f"   {month}: {excel_count:,} records in Excel")
                else:
                    tqdm.write(f"   {month}: NO DATA in Excel")

                    summary = self._spot_repo.get_month_summary(month, conn)

                    if summary.has_data:
                        preserved[month] = PreservedMonth(
                            month=month,
                            existing_count=summary.count,
                            existing_revenue=summary.gross_revenue,
                            reason="No data in Excel - preserving existing",
                        )

        return preserved

    def _log_preserved_months(self, preserved: Dict[str, PreservedMonth]) -> None:
        """Log information about months being preserved."""
        tqdm.write(
            f"⚠️  PRESERVATION: Protecting {len(preserved)} open month(s) "
            f"with no Excel data:"
        )
        for pm in sorted(preserved.values(), key=lambda x: x.month):
            tqdm.write(
                f"   {pm.month}: {pm.existing_count:,} existing records, "
                f"${pm.existing_revenue:,.2f} revenue - PRESERVED"
            )

    def _check_previous_month_skip(
        self, open_months: List[str], excel_month_counts: Dict[str, int]
    ) -> Optional[str]:
        """Check if previous calendar month should be skipped."""
        previous_month = self._get_previous_calendar_month()
        current_month = date.today().strftime("%b-%y")

        if previous_month not in open_months:
            return None

        excel_count = excel_month_counts.get(previous_month, 0)

        with self.safe_connection() as conn:
            summary = self._spot_repo.get_month_summary(previous_month, conn)

        tqdm.write(f"⚠️  SKIPPING PREVIOUS MONTH: {previous_month}")
        tqdm.write(
            f"   Excel has {excel_count:,} records (likely stray MC operational data)"
        )
        tqdm.write(
            f"   Preserving {summary.count:,} existing records, "
            f"${summary.gross_revenue:,.2f} revenue"
        )
        tqdm.write(
            f"   Reason: Once in {current_month}, previous month data "
            f"is not authoritative"
        )

        return previous_month

    def _get_previous_calendar_month(
        self, reference_date: Optional[date] = None
    ) -> str:
        """Get the previous calendar month in display format."""
        if reference_date is None:
            reference_date = date.today()

        first_of_month = reference_date.replace(day=1)
        last_of_previous = first_of_month - timedelta(days=1)

        return last_of_previous.strftime("%b-%y")

    # ========================================================================
    # Step 5: Mode Validation
    # ========================================================================

    def _validate_import_mode_requirements(
        self, context: ImportContext
    ) -> Optional[str]:
        """Validate mode-specific requirements."""
        if context.is_historical_mode and not context.closed_by:
            return "HISTORICAL mode requires --closed-by parameter"

        if context.is_manual_mode and context.month_classification.has_closed:
            closed = context.month_classification.closed_months
            return (
                f"Manual import contains closed months: {closed}. "
                f"Use --force to override or filter the Excel file."
            )

        return None

    # ========================================================================
    # Step 7: Execute Import Transaction
    # ========================================================================

    def _execute_import_workflow(
        self, context: ImportContext, result: ImportResult
    ) -> ImportResult:
        """Execute the actual import within a transaction."""
        # Build entity cache for performance
        tqdm.write("🚀 Phase 1: Building high-performance entity cache...")
        self.batch_resolver.build_entity_cache_from_excel(
            context.excel_analysis.file_path
        )
        cache_stats = self.batch_resolver.get_performance_stats()
        tqdm.write(
            f"✅ Cache ready: {cache_stats['cache_size']} entities "
            f"pre-resolved ({cache_stats['batch_resolved']} found)"
        )

        # Create batch record
        self._create_import_batch(
            context.batch_id,
            context.import_mode,
            context.excel_analysis.file_path,
            context.months_to_process,
        )

        try:
            with self.safe_transaction() as conn:
                # Delete existing data
                result.records_deleted = self._delete_months_with_progress(
                    context.months_to_process, conn
                )

                # Import new data
                result.records_imported = self._import_excel_data_with_progress(
                    context.excel_analysis.file_path,
                    context.batch_id,
                    conn,
                    context.months_to_process,
                )

                # Log net change
                if context.is_weekly_update_mode:
                    tqdm.write(
                        f"Import complete: {result.records_imported:,} imported, "
                        f"{result.records_deleted:,} deleted "
                        f"(net: {result.net_change:+,})"
                    )

                # Validate and correct customer alignment
                self._validate_and_correct_customers(context.batch_id, conn)

                # Close months for HISTORICAL mode
                if context.is_historical_mode:
                    result.closed_months = self._close_months(
                        context.months_to_process, context.closed_by, conn
                    )

                # Complete batch record
                self._complete_import_batch(context.batch_id, result, conn)

                # Refresh materialized entity metrics for address book
                from src.web.routes.address_book import refresh_entity_metrics
                refresh_entity_metrics(conn)
                tqdm.write("✅ Entity metrics cache refreshed")

                result.success = True
                tqdm.write("✅ Import completed successfully")

        except Exception as e:
            error_msg = f"Transaction failed: {str(e)}"
            tqdm.write(f"❌ {error_msg}")
            result.add_error(error_msg)
            self._fail_import_batch(context.batch_id, str(e))
            raise BroadcastMonthImportError(error_msg)

        return result

    def _delete_months_with_progress(
        self, months: List[str], conn: sqlite3.Connection
    ) -> int:
        """Delete existing data for specified months with progress tracking."""
        if not months:
            return 0

        summaries = self._spot_repo.get_month_summaries(months, conn)
        total_to_delete = sum(s.count for s in summaries.values())

        if total_to_delete == 0:
            tqdm.write("No existing records to delete")
            return 0

        with tqdm(
            total=total_to_delete, desc="Deleting existing records", unit=" records"
        ) as pbar:
            deleted = 0
            for month in months:
                count = summaries[month].count
                if count > 0:
                    cursor = conn.execute(
                        "DELETE FROM spots WHERE broadcast_month = ?", (month,)
                    )
                    deleted += cursor.rowcount
                    pbar.update(cursor.rowcount)
                    pbar.set_description(f"Deleted {deleted:,}/{total_to_delete:,}")

        tqdm.write(f"Deleted {total_to_delete:,} existing records")
        return total_to_delete

    def _validate_and_correct_customers(
        self, batch_id: str, conn: sqlite3.Connection
    ) -> None:
        """Validate customer alignment and auto-correct if needed."""
        tqdm.write("Validating customer alignment...")

        validation = self._spot_repo.validate_customer_alignment(batch_id, conn)

        if validation.is_valid:
            tqdm.write("✅ Customer alignment validation passed")
            return

        tqdm.write("⚠️ Customer alignment issues detected:")
        tqdm.write(
            f"   {validation.mismatch_count} mismatches affecting "
            f"{validation.total_spots_affected:,} spots"
        )
        tqdm.write(f"   Revenue affected: ${validation.total_revenue_affected:,.2f}")

        tqdm.write("🔧 Auto-correcting customer_id mismatches...")
        corrections = self._spot_repo.correct_customer_mismatches(batch_id, conn)
        tqdm.write(f"   Corrected {corrections} records")

        validation = self._spot_repo.validate_customer_alignment(batch_id, conn)

        if validation.is_valid:
            tqdm.write("✅ All customer_id mismatches corrected")
        else:
            raise BroadcastMonthImportError(
                f"Failed to correct {validation.mismatch_count} "
                f"customer alignment issues"
            )

    def _close_months(
        self, months: List[str], closed_by: Optional[str], conn: sqlite3.Connection
    ) -> List[str]:
        """Close months for HISTORICAL mode."""
        closed: List[str] = []

        if not closed_by:
            logger.warning("closed_by is None, skipping month closure")
            return closed

        with tqdm(total=len(months), desc="Closing months", unit=" months") as pbar:
            for month in months:
                try:
                    self.closure_service.close_broadcast_month_with_connection(
                        month, closed_by, conn
                    )
                    closed.append(month)
                    pbar.set_description(f"Closed {month}")
                except MonthClosureError as e:
                    tqdm.write(f"Failed to close month {month}: {e}")
                finally:
                    pbar.update(1)

        tqdm.write(f"Closed {len(closed)} months")
        return closed

    # ========================================================================
    # Row-by-Row Import (to be refactored in future iteration)
    # ========================================================================

    def _import_excel_data_with_progress(
        self,
        excel_file: str,
        batch_id: str,
        conn: sqlite3.Connection,
        allowed_months: List[str],
    ) -> int:
        """
        Import Excel data using fixed column positions with progress tracking.
        Supports sheet source tracking using source_file field.
        """
        # Verify batch exists
        cursor = conn.execute(
            "SELECT COUNT(*) FROM import_batches WHERE batch_id = ?", (batch_id,)
        )
        if cursor.fetchone()[0] == 0:
            raise BroadcastMonthImportError(
                f"batch_id {batch_id} not found in import_batches table"
            )

        filename = SourceFileFormatter.extract_filename_from_path(excel_file)

        try:
            with suppress_verbose_logging(), suppress_stdout_stderr():
                worksheet, sheet_name, workbook = get_excel_worksheet_flexible(
                    excel_file
                )
                tqdm.write(f"Using sheet: {sheet_name}")

            total_records = worksheet.max_row - 1
            imported_count = 0
            skipped_count = 0
            filtered_count = 0
            unmatched_customers: Set[str] = set()
            unmatched_agencies: Set[str] = set()
            sheet_source_stats: Dict[str, int] = {}

            with tqdm(
                total=total_records, desc="Processing Excel rows", unit=" rows"
            ) as pbar:
                for row_num, row in enumerate(
                    worksheet.iter_rows(min_row=2, values_only=True), start=2
                ):
                    pbar.update(1)

                    try:
                        if not any(row):
                            continue

                        # Get broadcast_month
                        month_col_index = [
                            k
                            for k, v in EXCEL_COLUMN_POSITIONS.items()
                            if v == "broadcast_month"
                        ]
                        if not month_col_index:
                            skipped_count += 1
                            continue
                        month_value = row[month_col_index[0]]

                        if not month_value:
                            skipped_count += 1
                            continue

                        # Parse broadcast_month
                        broadcast_month_display = self._parse_month_value(month_value)
                        if not broadcast_month_display:
                            skipped_count += 1
                            continue

                        if broadcast_month_display not in allowed_months:
                            filtered_count += 1
                            continue

                        spot_data: Dict[str, Any] = {
                            "import_batch_id": batch_id,
                            "broadcast_month": broadcast_month_display,
                        }

                        sheet_source: Optional[str] = None

                        for col_idx, field_name in EXCEL_COLUMN_POSITIONS.items():
                            if field_name and col_idx < len(row):
                                val = row[col_idx]
                                if val is None or val == "":
                                    continue

                                if field_name == "sheet_source":
                                    sheet_source = str(val).strip() if val else None
                                    if sheet_source:
                                        sheet_source_stats[sheet_source] = (
                                            sheet_source_stats.get(sheet_source, 0) + 1
                                        )
                                    continue

                                if field_name == "bill_code":
                                    spot_data[field_name] = str(val).strip()

                                elif field_name == "air_date":
                                    if hasattr(val, "date"):
                                        spot_data[field_name] = val.date().isoformat()
                                    else:
                                        spot_data[field_name] = str(val).strip()

                                elif field_name in [
                                    "gross_rate",
                                    "station_net",
                                    "spot_value",
                                    "broker_fees",
                                ]:
                                    try:
                                        spot_data[field_name] = float(val)
                                    except:
                                        spot_data[field_name] = None

                                elif field_name == "day_of_week":
                                    try:
                                        spot_data[field_name] = normalize_broadcast_day(
                                            str(val).strip()
                                        )
                                    except:
                                        spot_data[field_name] = str(val).strip()

                                elif field_name == "revenue_type":
                                    spot_data[field_name] = (
                                        self._normalize_revenue_type(str(val).strip())
                                    )

                                elif field_name == "spot_type":
                                    spot_data[field_name] = self._normalize_spot_type(
                                        str(val).strip()
                                    )

                                elif field_name != "broadcast_month":
                                    spot_data[field_name] = str(val).strip()

                        spot_data["source_file"] = (
                            SourceFileFormatter.format_source_file(
                                filename, sheet_source
                            )
                        )

                        if "market_name" in spot_data:
                            market_id = self._lookup_market_id(
                                spot_data["market_name"], conn
                            )
                            if market_id:
                                spot_data["market_id"] = market_id

                        if "bill_code" in spot_data:
                            entity_result = self.batch_resolver.lookup_entities_cached(
                                spot_data["bill_code"], conn
                            )
                            if entity_result.customer_id:
                                spot_data["customer_id"] = entity_result.customer_id
                            else:
                                unmatched_customers.add(spot_data["bill_code"])
                            if entity_result.agency_id:
                                spot_data["agency_id"] = entity_result.agency_id
                            else:
                                if ":" in spot_data["bill_code"]:
                                    unmatched_agencies.add(
                                        spot_data["bill_code"].split(":", 1)[0].strip()
                                    )

                        if "language_id" not in spot_data:
                            spot_data["language_id"] = 1

                        if not spot_data.get("bill_code") or not spot_data.get(
                            "air_date"
                        ):
                            skipped_count += 1
                            continue

                        fields = list(spot_data.keys())
                        placeholders = ", ".join(["?"] * len(fields))
                        field_names = ", ".join(fields)
                        values = [spot_data[field] for field in fields]

                        conn.execute(
                            f"INSERT INTO spots ({field_names}) VALUES ({placeholders})",
                            values,
                        )
                        imported_count += 1

                        if imported_count % 1000 == 0:
                            pbar.set_description(f"Imported {imported_count:,} records")

                    except Exception as row_error:
                        skipped_count += 1
                        if skipped_count <= 5:
                            tqdm.write(
                                f"Row {row_num} error: {str(row_error)[:100]}..."
                            )
                        continue

            workbook.close()

            # Log completion statistics
            final_stats = self.batch_resolver.get_performance_stats()
            tqdm.write(f"Import complete: {imported_count:,} records imported")
            tqdm.write("Entity resolution performance:")
            tqdm.write(f"   Cache hit rate: {final_stats['cache_hit_rate_percent']}%")
            tqdm.write(f"   Total lookups: {final_stats['total_lookups']:,}")
            tqdm.write(f"   Cache hits: {final_stats['cache_hits']:,}")

            if sheet_source_stats:
                tqdm.write("Sheet breakdown:")
                for sheet, count in sorted(sheet_source_stats.items()):
                    tqdm.write(f"   {sheet}: {count:,} records")

            if skipped_count:
                tqdm.write(f"Skipped: {skipped_count:,} records (errors)")
            if filtered_count:
                tqdm.write(f"Filtered: {filtered_count:,} records (closed months)")
            if unmatched_customers:
                tqdm.write(f"Unmatched customers: {len(unmatched_customers)}")
            if unmatched_agencies:
                tqdm.write(f"Unmatched agencies: {len(unmatched_agencies)}")

            return imported_count

        except Exception as e:
            raise BroadcastMonthImportError(f"Excel import failed: {e}")

    # ========================================================================
    # Entity Lookup Methods
    # ========================================================================

    def _lookup_market_id(
        self, market_name: str, conn: sqlite3.Connection
    ) -> Optional[int]:
        """Look up market_id from market name or code."""
        if not market_name:
            return None

        if len(market_name) > 50 or any(c in market_name for c in [";", "--", "/*"]):
            logger.warning(f"Suspicious market_name rejected: {market_name}")
            return None

        cursor = conn.execute(
            """
            SELECT market_id FROM markets 
            WHERE market_code = ? OR market_name = ?
            """,
            (market_name.strip(), market_name.strip()),
        )

        result = cursor.fetchone()
        return result[0] if result else None

    # ========================================================================
    # Batch Management Methods
    # ========================================================================

    def _generate_batch_id(self, import_mode: str, timestamp: datetime) -> str:
        """Generate a unique batch ID."""
        return f"{import_mode.lower()}_{int(timestamp.timestamp())}"

    def _create_import_batch(
        self, batch_id: str, import_mode: str, source_file: str, months: List[str]
    ) -> Optional[int]:
        """Create import batch record."""
        try:
            with self.safe_transaction() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO import_batches 
                    (batch_id, import_mode, source_file, import_date, status, broadcast_months_affected)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'RUNNING', ?)
                    """,
                    (batch_id, import_mode, source_file, str(months)),
                )
                return cursor.lastrowid

        except Exception as e:
            error_msg = f"Failed to create import batch: {str(e)}"
            tqdm.write(f"{error_msg}")
            raise BroadcastMonthImportError(error_msg)

    def _complete_import_batch(
        self, batch_id: str, result: ImportResult, conn: sqlite3.Connection
    ) -> None:
        """Mark import batch as completed with statistics."""
        try:
            conn.execute(
                """
                UPDATE import_batches 
                SET status = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP,
                    records_imported = ?,
                    records_deleted = ?
                WHERE batch_id = ?
                """,
                (result.records_imported, result.records_deleted, batch_id),
            )
        except Exception as e:
            logger.error(f"Failed to update batch completion: {e}")

    def _fail_import_batch(self, batch_id: str, error_message: str) -> None:
        """Mark import batch as failed."""
        try:
            with self.safe_transaction() as conn:
                conn.execute(
                    """
                    UPDATE import_batches 
                    SET status = 'FAILED',
                        completed_at = CURRENT_TIMESTAMP,
                        error_summary = ?
                    WHERE batch_id = ?
                    """,
                    (error_message, batch_id),
                )
        except Exception as e:
            logger.error(f"Failed to mark batch as failed: {e}")

    # ========================================================================
    # Public API Methods
    # ========================================================================

    def get_import_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent import history."""
        try:
            with self.safe_connection() as conn:
                cursor = conn.execute(
                    """
                    SELECT batch_id, import_mode, source_file, import_date, status,
                        records_imported, records_deleted, broadcast_months_affected
                    FROM import_batches 
                    ORDER BY import_date DESC 
                    LIMIT ?
                    """,
                    (limit,),
                )

                history: List[Dict[str, Any]] = []
                for row in cursor.fetchall():
                    months_str = row[7]
                    try:
                        months_affected = eval(months_str) if months_str else []
                    except Exception:
                        months_affected = []

                    history.append(
                        {
                            "batch_id": row[0],
                            "import_mode": row[1],
                            "source_file": row[2],
                            "import_date": row[3],
                            "status": row[4],
                            "records_imported": row[5] or 0,
                            "records_deleted": row[6] or 0,
                            "months_affected": months_affected,
                        }
                    )

                return history

        except Exception as e:
            logger.error(f"Failed to get import history: {e}")
            return []

    def cleanup_failed_imports(self) -> int:
        """Clean up any failed or stuck import batches."""
        try:
            with self.safe_transaction() as conn:
                cursor = conn.execute("""
                    UPDATE import_batches 
                    SET status = 'FAILED',
                        error_summary = 'Import timed out or was interrupted',
                        completed_at = CURRENT_TIMESTAMP
                    WHERE status = 'RUNNING' 
                      AND import_date < datetime('now', '-1 hour')
                """)

                cleaned_count = cursor.rowcount

                if cleaned_count > 0:
                    tqdm.write(f"Cleaned up {cleaned_count} failed import batches")

                return cleaned_count

        except Exception as e:
            logger.error(f"Failed to cleanup imports: {e}")
            return 0


# ============================================================================
# Convenience Functions
# ============================================================================


def execute_import(
    excel_file: str,
    import_mode: str,
    db_path: str,
    closed_by: Optional[str] = None,
    dry_run: bool = False,
) -> ImportResult:
    """Simple function to execute an import."""
    db_connection = DatabaseConnection(db_path)
    service = BroadcastMonthImportService(db_connection)

    try:
        return service.execute_month_replacement(
            excel_file, import_mode, closed_by, dry_run
        )
    finally:
        db_connection.close()


# ============================================================================
# CLI Entry Point
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Broadcast month import service")
    parser.add_argument("excel_file", nargs="?", help="Excel file to import")
    parser.add_argument(
        "--db-path", default="data/database/production.db", help="Database path"
    )
    parser.add_argument(
        "--mode",
        choices=["WEEKLY_UPDATE", "HISTORICAL", "MANUAL"],
        default="WEEKLY_UPDATE",
        help="Import mode",
    )
    parser.add_argument("--closed-by", help="Required for HISTORICAL mode")
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate and preview without importing"
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Only validate the import"
    )
    parser.add_argument("--history", action="store_true", help="Show import history")
    parser.add_argument("--cleanup", action="store_true", help="Cleanup failed imports")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    if not Path(args.db_path).exists():
        print(f"Database not found: {args.db_path}")
        sys.exit(1)

    db_connection = DatabaseConnection(args.db_path)
    service = BroadcastMonthImportService(db_connection)

    try:
        if args.history:
            history = service.get_import_history()
            if history:
                print("Recent import history:")
                for record in history:
                    print(
                        f"  {record['batch_id']}: {record['import_mode']} - "
                        f"{record['status']} - {record['records_imported']} imported"
                    )
            else:
                print("No import history found")

        elif args.cleanup:
            cleaned = service.cleanup_failed_imports()
            print(f"Cleaned up {cleaned} failed import batches")

        elif args.validate_only:
            if not args.excel_file:
                print("Error: excel_file required for --validate-only")
                sys.exit(1)
            validation = service.validate_import(args.excel_file, args.mode)
            if validation.is_valid:
                print(f"Validation passed for {args.mode} mode")
                if validation.closed_months_found:
                    print(f"Includes closed months: {validation.closed_months_found}")
            else:
                print(f"Validation failed: {validation.error_message}")
                print(f"Solution: {validation.suggested_action}")

        else:
            if not args.excel_file:
                print("Error: excel_file required")
                sys.exit(1)

            result = service.execute_month_replacement(
                args.excel_file, args.mode, args.closed_by, args.dry_run
            )

            print("\nImport Results:")
            print(f"  Status: {'Success' if result.success else 'Failed'}")
            print(f"  Batch ID: {result.batch_id}")
            print(f"  Mode: {result.import_mode}")
            print(f"  Duration: {result.duration_seconds:.1f} seconds")
            print(f"  Months: {', '.join(result.broadcast_months_affected)}")
            print(f"  Records deleted: {result.records_deleted:,}")
            print(f"  Records imported: {result.records_imported:,}")

            if result.closed_months:
                print(f"  Months closed: {', '.join(result.closed_months)}")

            if result.error_messages:
                print("  Errors:")
                for error in result.error_messages:
                    print(f"    - {error}")

            if not result.success:
                sys.exit(1)

    finally:
        db_connection.close()
