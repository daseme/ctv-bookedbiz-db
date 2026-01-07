#!/usr/bin/env python3
"""
Import Workflow Models - Value objects for the import process.

These immutable dataclasses encapsulate state at each stage of the import workflow,
making data flow explicit and testable.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from pathlib import Path


# ============================================================================
# Excel Analysis Models
# ============================================================================

@dataclass(frozen=True)
class ExcelAnalysis:
    """
    Immutable result of analyzing an Excel file for import.
    
    Captures what months exist in the file and how many records each has.
    """
    file_path: str
    filename: str
    display_months: List[str]
    month_record_counts: Dict[str, int]
    
    @property
    def has_data(self) -> bool:
        """Returns True if any broadcast months were found."""
        return len(self.display_months) > 0
    
    @classmethod
    def from_file(
        cls, 
        file_path: str, 
        display_months: List[str],
        month_counts: Dict[str, int]
    ) -> 'ExcelAnalysis':
        """Factory method to create from file path and extracted data."""
        return cls(
            file_path=file_path,
            filename=Path(file_path).name,
            display_months=display_months,
            month_record_counts=month_counts
        )


# ============================================================================
# Month Classification Models
# ============================================================================

@dataclass(frozen=True)
class MonthClassification:
    """
    Immutable classification of months by closure status.
    
    Separates months into closed (no modifications allowed) and 
    open (available for import).
    """
    closed_months: List[str]
    open_months: List[str]
    
    @property
    def all_closed(self) -> bool:
        """Returns True if all months are closed."""
        return len(self.open_months) == 0
    
    @property
    def has_closed(self) -> bool:
        """Returns True if any months are closed."""
        return len(self.closed_months) > 0


@dataclass(frozen=True)
class PreservedMonth:
    """Information about a month being preserved (not deleted)."""
    month: str
    existing_count: int
    existing_revenue: float
    reason: str


@dataclass(frozen=True)
class MonthFilterResult:
    """
    Immutable result of filtering months for processing.
    
    Captures which months will be processed and which are being
    protected (preserved or skipped).
    """
    months_to_process: List[str]
    preserved_months: Dict[str, PreservedMonth]
    skipped_previous_month: Optional[str]
    
    @property
    def has_work(self) -> bool:
        """Returns True if there are months to process."""
        return len(self.months_to_process) > 0
    
    @property
    def preserved_count(self) -> int:
        """Number of months being preserved."""
        return len(self.preserved_months)


# ============================================================================
# Import Context Model
# ============================================================================

@dataclass
class ImportContext:
    """
    Mutable context accumulated through the import workflow.
    
    This is the only mutable model - it accumulates state as the 
    workflow progresses through each stage.
    """
    batch_id: str
    import_mode: str
    excel_analysis: ExcelAnalysis
    month_classification: MonthClassification
    filter_result: Optional[MonthFilterResult] = None
    closed_by: Optional[str] = None
    dry_run: bool = False
    
    @property
    def months_to_process(self) -> List[str]:
        """Get the final list of months to process."""
        if self.filter_result:
            return self.filter_result.months_to_process
        return self.month_classification.open_months
    
    @property
    def is_historical_mode(self) -> bool:
        return self.import_mode == "HISTORICAL"
    
    @property
    def is_weekly_update_mode(self) -> bool:
        return self.import_mode == "WEEKLY_UPDATE"
    
    @property
    def is_manual_mode(self) -> bool:
        return self.import_mode == "MANUAL"


# ============================================================================
# Import Result Model
# ============================================================================

@dataclass
class ImportResult:
    """
    Comprehensive result of import operation.
    
    Captures all outcomes including success/failure, statistics,
    and any error messages.
    """
    success: bool
    batch_id: str
    import_mode: str
    broadcast_months_affected: List[str] = field(default_factory=list)
    records_deleted: int = 0
    records_imported: int = 0
    duration_seconds: float = 0.0
    error_messages: List[str] = field(default_factory=list)
    closed_months: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Ensure lists are not None."""
        if self.error_messages is None:
            self.error_messages = []
        if self.closed_months is None:
            self.closed_months = []
        if self.broadcast_months_affected is None:
            self.broadcast_months_affected = []
    
    @property
    def net_change(self) -> int:
        """Net change in records (imported - deleted)."""
        return self.records_imported - self.records_deleted
    
    @property
    def has_errors(self) -> bool:
        """Returns True if there are error messages."""
        return len(self.error_messages) > 0
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.error_messages.append(message)
    
    @classmethod
    def create_empty(cls, batch_id: str, import_mode: str) -> 'ImportResult':
        """Factory method to create an empty result with defaults."""
        return cls(
            success=False,
            batch_id=batch_id,
            import_mode=import_mode
        )