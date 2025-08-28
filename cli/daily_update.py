#!/usr/bin/env python3
"""
Daily update command with automatic market setup and language assignment processing.
Clean Architecture implementation following established coding style guide.
Replaces open month data while protecting closed historical months.
"""

from __future__ import annotations
import sys
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Set, List, Optional, Any, Protocol, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Add tqdm for progress bars
from tqdm import tqdm

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from services.broadcast_month_import_service import BroadcastMonthImportService, BroadcastMonthImportError
from services.import_integration_utilities import get_excel_import_summary, validate_excel_for_import
from database.connection import DatabaseConnection

# ============================================================================
# Domain Models
# ============================================================================

class BatchType(Enum):
    ENHANCED_DAILY = ("enhanced_daily", "Enhanced Daily Update")
    HISTORICAL_IMPORT = ("historical_import", "Historical Import")
    MANUAL_UPDATE = ("manual_update", "Manual Update")
    
    def __init__(self, value: str, display_name: str):
        self.value = value
        self.display_name = display_name

@dataclass
class MarketData:
    market_code: str
    earliest_date: Optional[date]
    latest_date: Optional[date]
    spot_count: int = 0
    
    @classmethod
    def create_new(cls, market_code: str) -> MarketData:
        return cls(
            market_code=market_code,
            earliest_date=None,
            latest_date=None,
            spot_count=0
        )
    
    def add_spot_date(self, air_date: Optional[date]) -> None:
        """Add a spot date to this market data"""
        self.spot_count += 1
        if air_date:
            if not self.earliest_date or air_date < self.earliest_date:
                self.earliest_date = air_date
            if not self.latest_date or air_date > self.latest_date:
                self.latest_date = air_date

@dataclass 
class DailyUpdateConfig:
    excel_file: Path
    auto_setup_markets: bool = True
    dry_run: bool = False
    force: bool = False
    verbose: bool = False
    db_path: Path = Path("data/database/production.db")
    
    @classmethod
    def from_args(cls, args) -> DailyUpdateConfig:
        """Create configuration from CLI arguments"""
        return cls(
            excel_file=Path(args.excel_file),
            auto_setup_markets=args.auto_setup,
            dry_run=args.dry_run,
            force=args.force,
            verbose=args.verbose,
            db_path=Path(args.db_path)
        )

@dataclass
class MarketSetupResult:
    new_markets_found: int
    markets_created: int
    schedules_created: int
    duration_seconds: float
    new_markets: Dict[str, MarketData] = field(default_factory=dict)
    
    @property
    def summary(self) -> str:
        """Human readable summary"""
        if self.new_markets_found == 0:
            return "No new markets needed"
        return f"{self.markets_created} markets, {self.schedules_created} assignments created"

@dataclass  
class ImportResult:
    success: bool
    records_imported: int
    records_deleted: int
    batch_id: str
    duration_seconds: float
    months_processed: List[str] = field(default_factory=list)
    error_message: Optional[str] = None
    
    @property
    def net_change(self) -> int:
        """Calculate net change in records"""
        return self.records_imported - self.records_deleted

@dataclass
class LanguageAssignmentResult:
    success: bool
    categorized: int
    processed: int
    language_assigned: int
    default_english_assigned: int
    flagged_for_review: int
    duration_seconds: float = 0.0
    error_messages: List[str] = field(default_factory=list)
    
    @property
    def summary(self) -> str:
        """Human readable summary"""
        return f"Processed: {self.processed:,}, Language assigned: {self.language_assigned:,}, Review needed: {self.flagged_for_review:,}"

@dataclass
class DailyUpdateResult:
    success: bool
    batch_id: str
    market_setup: Optional[MarketSetupResult] = None
    import_result: Optional[ImportResult] = None
    language_assignment: Optional[LanguageAssignmentResult] = None
    duration_seconds: float = 0.0
    error_messages: List[str] = field(default_factory=list)
    
    @property
    def summary_line(self) -> str:
        """One-line summary for logging"""
        status = "✅ Success" if self.success else "❌ Failed"
        duration = f"{self.duration_seconds:.1f}s"
        return f"{status} | Duration: {duration} | Batch: {self.batch_id}"

# ============================================================================
# Value Objects and Business Rules
# ============================================================================

class MarketNameGenerator:
    """Value object for generating market names from codes"""
    
    NAME_MAPPINGS = {
        'NYC': 'NEW YORK',
        'LAX': 'LOS ANGELES', 
        'SFO': 'SAN FRANCISCO',
        'SEA': 'SEATTLE',
        'CHI': 'CHICAGO',
        'MSP': 'MINNEAPOLIS',
        'DAL': 'DALLAS',
        'HOU': 'HOUSTON',
        'WDC': 'WASHINGTON DC',
        'CVC': 'CENTRAL VALLEY',
        'CMP': 'CHI MSP',
        'MMT': 'MAMMOTH',
        'ADMIN': 'ADMINISTRATIVE'
    }
    
    @classmethod
    def generate_name(cls, market_code: str) -> str:
        """Generate proper market name from code"""
        return cls.NAME_MAPPINGS.get(market_code, market_code.upper().replace('_', ' '))

class BatchIdGenerator:
    """Value object for generating standardized batch IDs"""
    
    @staticmethod
    def generate(batch_type: BatchType, timestamp: Optional[datetime] = None) -> str:
        """Generate standardized batch ID"""
        if timestamp is None:
            timestamp = datetime.now()
        return f"{batch_type.value}_{int(timestamp.timestamp())}"

# ============================================================================
# Data Access Layer  
# ============================================================================

class MarketRepository:
    """Repository for market-related database operations"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def get_existing_markets(self) -> Dict[str, int]:
        """Get existing markets from database"""
        with self.db.transaction() as conn:
            cursor = conn.execute("SELECT market_code, market_id FROM markets")
            return {row[0]: row[1] for row in cursor.fetchall()}
    
    def create_market(self, market_code: str, market_name: str) -> int:
        """Create new market and return its ID"""
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "INSERT INTO markets (market_code, market_name) VALUES (?, ?)",
                (market_code, market_name)
            )
            return cursor.lastrowid
    
    def create_schedule_assignment(self, market_id: int, effective_date: date) -> None:
        """Create schedule assignment for market"""
        with self.db.transaction() as conn:
            conn.execute("""
                INSERT INTO schedule_market_assignments 
                (schedule_id, market_id, effective_start_date, assignment_priority)
                VALUES (1, ?, ?, 1)
            """, (market_id, effective_date))

class SpotRepository:
    """Repository for spot-related database operations"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db = db_connection
    
    def get_uncategorized_spots_by_batch(self, batch_id: str) -> List[int]:
        """Get uncategorized spot IDs for a specific batch"""
        with self.db.transaction() as conn:
            cursor = conn.execute("""
                SELECT spot_id FROM spots 
                WHERE import_batch_id = ? AND spot_category IS NULL
            """, (batch_id,))
            return [row[0] for row in cursor.fetchall()]

# ============================================================================
# Business Logic Layer
# ============================================================================

class ExcelMarketScanner:
    """Service for scanning Excel files for new markets"""
    
    def __init__(self, progress_reporter: ProgressReporter):
        self.progress_reporter = progress_reporter
    
    def scan_for_new_markets(self, excel_file: Path, existing_markets: Set[str]) -> Dict[str, MarketData]:
        """
        Scan Excel file to detect any new market codes not in existing set.
        Optimized for daily import files.
        """
        try:
            from openpyxl import load_workbook
            
            workbook = load_workbook(excel_file, read_only=True, data_only=True)
            worksheet = workbook.active
            
            # Find required columns
            market_col_index, air_date_col_index = self._find_columns(worksheet)
            if market_col_index is None:
                return {}
            
            new_markets = {}
            total_rows = worksheet.max_row - 1  # Exclude header
            
            # Process rows to find new markets with progress bar
            with self.progress_reporter.create_progress("🔍 Scanning for new markets", total_rows) as pbar:
                for row_num, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True), start=2):
                    pbar.update(1)
                    
                    if not any(cell for cell in row):
                        continue
                    
                    market_code = self._extract_market_code(row, market_col_index)
                    if not market_code or market_code in existing_markets:
                        continue
                    
                    # New market found - create or update data
                    if market_code not in new_markets:
                        new_markets[market_code] = MarketData.create_new(market_code)
                    
                    air_date = self._extract_air_date(row, air_date_col_index)
                    new_markets[market_code].add_spot_date(air_date)
                    
                    # Update progress description with found markets
                    if len(new_markets) > 0:
                        pbar.set_description(f"🔍 Scanning ({len(new_markets)} new markets found)")
            
            workbook.close()
            
            if new_markets:
                total_spots = sum(data.spot_count for data in new_markets.values())
                self.progress_reporter.write(f"✅ Found {len(new_markets)} new markets with {total_spots:,} spots")
            
            return new_markets
            
        except Exception as e:
            self.progress_reporter.write(f"⚠️  Warning: Could not scan for new markets: {str(e)}")
            return {}
    
    def _find_columns(self, worksheet) -> Tuple[Optional[int], Optional[int]]:
        """Find market and date column indices"""
        header_row = next(worksheet.iter_rows(min_row=1, max_row=1, values_only=True))
        market_col_index = None
        air_date_col_index = None
        
        for i, header in enumerate(header_row):
            if header:
                header_str = str(header).strip().lower()
                if header_str in ['market_name', 'market', 'market_code']:
                    market_col_index = i
                elif header_str in ['air_date', 'date', 'airdate']:
                    air_date_col_index = i
        
        return market_col_index, air_date_col_index
    
    def _extract_market_code(self, row: tuple, market_col: int) -> Optional[str]:
        """Extract and clean market code from row"""
        if market_col < len(row) and row[market_col]:
            return str(row[market_col]).strip()
        return None
    
    def _extract_air_date(self, row: tuple, date_col: Optional[int]) -> Optional[date]:
        """Extract air date from row if available"""
        if date_col is None or date_col >= len(row):
            return None
        
        air_date_value = row[date_col]
        if not air_date_value:
            return None
        
        try:
            if isinstance(air_date_value, datetime):
                return air_date_value.date()
            else:
                return datetime.strptime(str(air_date_value), '%Y-%m-%d').date()
        except:
            return None

class MarketSetupService:
    """Service for setting up new markets and schedules"""
    
    def __init__(self, 
                 market_repository: MarketRepository, 
                 progress_reporter: ProgressReporter):
        self.market_repository = market_repository
        self.progress_reporter = progress_reporter
    
    def execute_daily_market_setup(self, excel_file: Path) -> MarketSetupResult:
        """Execute lightweight market setup for daily data"""
        start_time = datetime.now()
        
        # Step 1: Scan for new markets
        existing_markets = set(self.market_repository.get_existing_markets().keys())
        scanner = ExcelMarketScanner(self.progress_reporter)
        new_markets = scanner.scan_for_new_markets(excel_file, existing_markets)
        
        if not new_markets:
            return MarketSetupResult(
                new_markets_found=0,
                markets_created=0,
                schedules_created=0,
                duration_seconds=(datetime.now() - start_time).total_seconds()
            )
        
        # Step 2: Create new markets and schedules
        created_markets = self._create_markets(new_markets)
        schedules_created = self._create_schedule_assignments(new_markets, created_markets)
        
        duration = (datetime.now() - start_time).total_seconds()
        
        return MarketSetupResult(
            new_markets_found=len(new_markets),
            markets_created=len(created_markets),
            schedules_created=schedules_created,
            duration_seconds=duration,
            new_markets=new_markets
        )
    
    def _create_markets(self, new_markets: Dict[str, MarketData]) -> Dict[str, int]:
        """Create new markets with progress tracking"""
        created_markets = {}
        
        with self.progress_reporter.create_progress("🏗️  Creating markets", len(new_markets)) as pbar:
            for market_code, market_data in sorted(new_markets.items()):
                market_name = MarketNameGenerator.generate_name(market_code)
                market_id = self.market_repository.create_market(market_code, market_name)
                created_markets[market_code] = market_id
                
                pbar.update(1)
                pbar.set_description(f"🏗️  Created {market_code}")
        
        self.progress_reporter.write(f"✅ Created {len(created_markets)} new markets")
        return created_markets
    
    def _create_schedule_assignments(self, 
                                   new_markets: Dict[str, MarketData], 
                                   market_mapping: Dict[str, int]) -> int:
        """Setup schedule assignments for newly created markets"""
        assignments_created = 0
        
        with self.progress_reporter.create_progress("🗓️  Setting up schedules", len(new_markets)) as pbar:
            for market_code, market_data in new_markets.items():
                market_id = market_mapping[market_code]
                
                # Use earliest date if available, otherwise use current date
                effective_date = market_data.earliest_date if market_data.earliest_date else datetime.now().date()
                
                self.market_repository.create_schedule_assignment(market_id, effective_date)
                assignments_created += 1
                
                pbar.update(1)
                pbar.set_description(f"🗓️  Setup {market_code}")
        
        self.progress_reporter.write(f"✅ Created {assignments_created} schedule assignments")
        return assignments_created

class ImportService:
    """Service for handling data imports with progress tracking"""
    
    def __init__(self, 
                 broadcast_import_service: BroadcastMonthImportService,
                 progress_reporter: ProgressReporter):
        self.broadcast_service = broadcast_import_service
        self.progress_reporter = progress_reporter
    
    def execute_import_with_progress(self, excel_file: Path, batch_id: str) -> ImportResult:
        """Execute import with progress tracking"""
        start_time = datetime.now()
        
        # Get summary first for progress setup
        summary = get_excel_import_summary(str(excel_file), self.broadcast_service.db_connection.db_path)
        total_spots = summary['total_existing_spots_affected']
        
        # Create a progress bar for the overall import
        with self.progress_reporter.create_progress("📦 Importing data", 100) as pbar:
            pbar.set_description("📦 Preparing import")
            pbar.update(10)
            
            pbar.set_description("📦 Deleting existing data")
            pbar.update(20)
            
            # Execute actual import
            import_result = self.broadcast_service.execute_month_replacement(
                str(excel_file),
                'DAILY_UPDATE',
                closed_by=None,
                dry_run=False
            )
            
            pbar.set_description("📦 Importing new data")
            pbar.update(50)
            
            pbar.set_description("📦 Finalizing import")
            pbar.update(20)
            
            pbar.set_description("📦 Import complete")
            pbar.update(100)
        
        # Convert to our domain model
        result = ImportResult(
            success=import_result.success,
            records_imported=import_result.records_imported,
            records_deleted=import_result.records_deleted,
            batch_id=batch_id,
            duration_seconds=(datetime.now() - start_time).total_seconds(),
            months_processed=summary.get('months_in_excel', [])
        )
        
        if result.success:
            self.progress_reporter.write(f"📊 Import: {result.records_imported:,} imported, {result.records_deleted:,} deleted (net: {result.net_change:+,})")
        
        return result
    
    def simulate_import(self, excel_file: Path) -> ImportResult:
        """Simulate import for dry run"""
        summary = get_excel_import_summary(str(excel_file), self.broadcast_service.db_connection.db_path)
        
        return ImportResult(
            success=True,
            records_imported=summary['total_existing_spots_affected'],
            records_deleted=summary['total_existing_spots_affected'],
            batch_id="dry_run",
            duration_seconds=0.0,
            months_processed=summary['months_in_excel']
        )

class LanguageAssignmentService:
    """Service for processing language assignments after import"""
    
    def __init__(self, 
                 db_connection: DatabaseConnection,
                 progress_reporter: ProgressReporter):
        self.db = db_connection
        self.progress_reporter = progress_reporter
    
    def process_language_assignments(self, batch_id: str) -> LanguageAssignmentResult:
        """Process language assignments after import with comprehensive progress tracking"""
        start_time = datetime.now()
        
        result = LanguageAssignmentResult(
            success=False,
            categorized=0,
            processed=0,
            language_assigned=0,
            default_english_assigned=0,
            flagged_for_review=0
        )
        
        try:
            # Import language assignment services
            from services.spot_categorization_service import SpotCategorizationService
            from services.language_processing_orchestrator import LanguageProcessingOrchestrator
            
            conn = sqlite3.connect(self.db.db_path)
            
            # Step 1: Categorization with progress
            result.categorized = self._categorize_batch_spots(conn, batch_id)
            
            # Step 2: Process all categories with progress
            language_results = self._process_batch_categories(conn, batch_id)
            
            # Update result with language processing data
            summary = language_results['summary']
            result.processed = summary['total_processed']
            result.language_assigned = summary['language_assigned']
            result.default_english_assigned = summary['default_english_assigned']
            result.flagged_for_review = summary['flagged_for_review']
            result.success = True
            
            # Display clean summary
            self.progress_reporter.write(f"✅ Language assignment complete:")
            self.progress_reporter.write(f"   🎯 Processed: {result.processed:,}")
            self.progress_reporter.write(f"   🔤 Language assigned: {result.language_assigned:,}")
            self.progress_reporter.write(f"   🇺🇸 Default English: {result.default_english_assigned:,}")
            if result.flagged_for_review > 0:
                self.progress_reporter.write(f"   📋 Review required: {result.flagged_for_review:,}")
            
            conn.close()
            
        except Exception as e:
            error_msg = f"Language assignment processing failed: {str(e)}"
            result.error_messages.append(error_msg)
            self.progress_reporter.write(f"⚠️  {error_msg}")
            
            # Try to provide partial success info
            try:
                if 'conn' in locals():
                    conn.close()
            except:
                pass
        
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    def _categorize_batch_spots(self, conn: sqlite3.Connection, batch_id: str) -> int:
        """Categorize uncategorized spots from current batch"""
        from services.spot_categorization_service import SpotCategorizationService
        
        categorization_service = SpotCategorizationService(conn)
        
        # Get uncategorized spots from current batch only
        cursor = conn.execute("""
            SELECT spot_id FROM spots 
            WHERE import_batch_id = ? AND spot_category IS NULL
        """, (batch_id,))
        uncategorized_spots = [row[0] for row in cursor.fetchall()]
        
        if not uncategorized_spots:
            self.progress_reporter.write(f"✅ No uncategorized spots found")
            return 0
        
        # Categorize in batches with progress tracking
        batch_size = 1000
        total_categorized = 0
        
        with self.progress_reporter.create_progress("🏷️  Categorizing spots", len(uncategorized_spots)) as pbar:
            for i in range(0, len(uncategorized_spots), batch_size):
                batch = uncategorized_spots[i:i + batch_size]
                categorization_service.categorize_spots_batch(batch)
                total_categorized += len(batch)
                pbar.update(len(batch))
                pbar.set_description(f"🏷️  Categorized {total_categorized:,}/{len(uncategorized_spots):,}")
        
        self.progress_reporter.write(f"✅ Categorized {len(uncategorized_spots):,} spots")
        return len(uncategorized_spots)
    
    def _process_batch_categories(self, conn: sqlite3.Connection, batch_id: str) -> Dict[str, Any]:
        """Process all categories for the batch"""
        from services.language_processing_orchestrator import LanguageProcessingOrchestrator
        
        orchestrator = LanguageProcessingOrchestrator(conn)
        return orchestrator.process_batch_categories(batch_id)

class DailyUpdateOrchestrator:
    """Main orchestrator for daily update process following Clean Architecture"""
    
    def __init__(self,
                 market_setup_service: MarketSetupService,
                 import_service: ImportService,
                 language_service: LanguageAssignmentService,
                 progress_reporter: ProgressReporter):
        self.market_setup_service = market_setup_service
        self.import_service = import_service
        self.language_service = language_service
        self.progress_reporter = progress_reporter
    
    def execute_daily_update(self, config: DailyUpdateConfig) -> DailyUpdateResult:
        """Execute the complete daily update process"""
        start_time = datetime.now()
        batch_id = BatchIdGenerator.generate(BatchType.ENHANCED_DAILY, start_time)
        
        self._display_update_header(config, batch_id)
        
        result = DailyUpdateResult(success=False, batch_id=batch_id)
        
        try:
            # Step 1: Market setup (if enabled)
            if config.auto_setup_markets and not config.dry_run:
                self.progress_reporter.write(f"🏗️  STEP 1: Automatic Market Setup")
                result.market_setup = self.market_setup_service.execute_daily_market_setup(config.excel_file)
                self.progress_reporter.write(f"📊 Setup: {result.market_setup.summary}")
                self.progress_reporter.write("")
            
            # Step 2: Daily data import
            self.progress_reporter.write(f"📦 STEP 2: Daily Data Import")
            
            if config.dry_run:
                self.progress_reporter.write(f"🔍 DRY RUN - No changes would be made")
                result.import_result = self.import_service.simulate_import(config.excel_file)
                self.progress_reporter.write(f"📊 Would import {result.import_result.records_imported:,} spots across {len(result.import_result.months_processed)} months")
            else:
                result.import_result = self.import_service.execute_import_with_progress(config.excel_file, batch_id)
            
            # Step 3: Language Assignment Processing (if import succeeded)
            if result.import_result and result.import_result.success and not config.dry_run:
                self.progress_reporter.write(f"\n🎯 STEP 3: Language Assignment Processing")
                result.language_assignment = self.language_service.process_language_assignments(batch_id)
            
            result.success = True
            
        except Exception as e:
            error_msg = f"Enhanced daily update failed: {str(e)}"
            result.error_messages.append(error_msg)
            self.progress_reporter.write(f"❌ {error_msg}")
        
        # Calculate total duration
        result.duration_seconds = (datetime.now() - start_time).total_seconds()
        return result
    
    def _display_update_header(self, config: DailyUpdateConfig, batch_id: str) -> None:
        """Display clean header without excessive printing"""
        self.progress_reporter.write(f"🔄 Enhanced Daily Update Starting")
        self.progress_reporter.write(f"📁 File: {config.excel_file.name}")
        self.progress_reporter.write(f"🔧 Auto-setup: {config.auto_setup_markets} | Dry run: {config.dry_run}")
        self.progress_reporter.write(f"🆔 Batch ID: {batch_id}")
        self.progress_reporter.write("=" * 60)

# ============================================================================
# Presentation Layer
# ============================================================================

class ProgressReporter(Protocol):
    """Interface for progress reporting - can be implemented for CLI, web, etc."""
    
    def create_progress(self, description: str, total: int) -> ProgressContext:
        """Create a progress tracking context"""
        ...
    
    def write(self, message: str) -> None:
        """Write a message to output"""
        ...

class ProgressContext(Protocol):
    """Context manager for progress tracking"""
    
    def update(self, increment: int) -> None:
        """Update progress by increment"""
        ...
    
    def set_description(self, description: str) -> None:
        """Update progress description"""
        ...

class TqdmProgressReporter:
    """Tqdm implementation of progress reporting"""
    
    def create_progress(self, description: str, total: int):
        """Create tqdm progress bar context"""
        return tqdm(total=total, desc=description, unit=" items")
    
    def write(self, message: str) -> None:
        """Write message using tqdm.write to avoid conflicts with progress bars"""
        tqdm.write(message)

class DailyUpdatePreviewService:
    """Service for displaying preview of what daily update would do"""
    
    def __init__(self, db_connection: DatabaseConnection, progress_reporter: ProgressReporter):
        self.db = db_connection
        self.progress_reporter = progress_reporter
    
    def display_enhanced_daily_preview(self, config: DailyUpdateConfig) -> bool:
        """Display what the enhanced daily update would do"""
        self.progress_reporter.write(f"📋 Enhanced Daily Update Preview")
        self.progress_reporter.write(f"=" * 60)
        self.progress_reporter.write(f"📁 File: {config.excel_file.name}")
        self.progress_reporter.write(f"🔧 Auto-setup: {config.auto_setup_markets}")
        self.progress_reporter.write("")
        
        try:
            # Market setup preview
            if config.auto_setup_markets:
                self._display_market_setup_preview(config.excel_file)
            
            # Standard import preview
            can_proceed = self._display_import_preview(config.excel_file)
            
            # Language assignment preview
            if can_proceed:
                self.progress_reporter.write(f"🎯 Language Assignment Preview:")
                self.progress_reporter.write(f"   📋 All spots will be categorized and processed automatically")
                self.progress_reporter.write(f"   🔤 Business rules applied, manual review flagged as needed")
            
            return can_proceed
            
        except Exception as e:
            self.progress_reporter.write(f"❌ Error generating preview: {e}")
            return False
    
    def _display_market_setup_preview(self, excel_file: Path) -> None:
        """Display market setup preview"""
        try:
            market_repo = MarketRepository(self.db)
            existing_markets = set(market_repo.get_existing_markets().keys())
            
            scanner = ExcelMarketScanner(self.progress_reporter)
            new_markets = scanner.scan_for_new_markets(excel_file, existing_markets)
            
            if new_markets:
                self.progress_reporter.write(f"🏗️  Market Setup Preview:")
                self.progress_reporter.write(f"   🆕 New markets: {len(new_markets)} ({', '.join(sorted(new_markets.keys()))})")
                self.progress_reporter.write("")
        except Exception as e:
            self.progress_reporter.write(f"⚠️  Could not preview market setup: {e}")
    
    def _display_import_preview(self, excel_file: Path) -> bool:
        """Display import preview"""
        self.progress_reporter.write(f"📦 Daily Update Preview:")
        
        try:
            # Get Excel summary
            summary = get_excel_import_summary(str(excel_file), self.db.db_path)
            
            # Validation check
            validation = validate_excel_for_import(str(excel_file), 'DAILY_UPDATE', self.db.db_path)
            
            if validation.is_valid:
                self.progress_reporter.write(f"   ✅ Daily update allowed")
                self.progress_reporter.write(f"   📊 {summary['total_existing_spots_affected']:,} spots across {len(summary['months_in_excel'])} months")
            else:
                self.progress_reporter.write(f"   ❌ Daily update BLOCKED")
                self.progress_reporter.write(f"      Reason: {validation.error_message}")
                self.progress_reporter.write(f"      Solution: {validation.suggested_action}")
                return False
            
            # Show month details concisely
            open_months = summary['open_months']
            closed_months = summary['closed_months']
            
            self.progress_reporter.write(f"   📂 Open months: {len(open_months)} ({', '.join(open_months) if len(open_months) <= 5 else f'{len(open_months)} months'})")
            if closed_months:
                self.progress_reporter.write(f"   🔒 Closed months: {len(closed_months)} (protected)")
            self.progress_reporter.write("")
            
            return True
            
        except Exception as e:
            self.progress_reporter.write(f"❌ Error generating preview: {e}")
            return False

class UserConfirmationService:
    """Service for getting user confirmation"""
    
    @staticmethod
    def get_user_confirmation(total_spots: int, open_months: int, new_markets: int = 0, force: bool = False) -> bool:
        """Get user confirmation for the enhanced update"""
        if force:
            return True
        
        tqdm.write(f"🚨 CONFIRMATION REQUIRED")
        actions = [
            f"REPLACE {total_spots:,} existing spots",
            f"Update {open_months} open months",
            "Process language assignments automatically",
            "Preserve all closed/historical months"
        ]
        
        if new_markets > 0:
            actions.insert(0, f"Create {new_markets} new markets")
        
        tqdm.write(f"This will:")
        for action in actions:
            tqdm.write(f"  • {action}")
        
        while True:
            response = input(f"\nProceed with enhanced daily update? (yes/no): ").strip().lower()
            if response in ['yes', 'y']:
                return True
            elif response in ['no', 'n', '']:
                return False
            else:
                tqdm.write("Please enter 'yes' or 'no'")

class DailyUpdateResultsPresenter:
    """Service for presenting daily update results"""
    
    def __init__(self, progress_reporter: ProgressReporter):
        self.progress_reporter = progress_reporter
    
    def display_final_results(self, result: DailyUpdateResult, dry_run: bool) -> None:
        """Display clean final results"""
        self.progress_reporter.write(f"\n{'='*60}")
        if dry_run:
            self.progress_reporter.write(f"🔍 ENHANCED DAILY UPDATE DRY RUN COMPLETED")
        else:
            self.progress_reporter.write(f"🎉 ENHANCED DAILY UPDATE COMPLETED")
        self.progress_reporter.write(f"{'='*60}")
        
        self.progress_reporter.write(f"📊 Results Summary:")
        self.progress_reporter.write(f"  Status: {'✅ Success' if result.success else '❌ Failed'}")
        self.progress_reporter.write(f"  Duration: {result.duration_seconds:.1f} seconds")
        
        # Market setup results (concise)
        if result.market_setup and result.market_setup.markets_created > 0:
            self.progress_reporter.write(f"  Markets created: {result.market_setup.markets_created}")
        
        # Import results (concise)
        if result.import_result and not dry_run:
            import_res = result.import_result
            if import_res.success:
                self.progress_reporter.write(f"  Spots imported: {import_res.records_imported:,}")
                self.progress_reporter.write(f"  Net change: {import_res.net_change:+,}")
        
        # Language assignment results (concise)
        if result.language_assignment and result.language_assignment.success:
            lang_res = result.language_assignment
            self.progress_reporter.write(f"  Language processed: {lang_res.processed:,}")
            if lang_res.flagged_for_review > 0:
                self.progress_reporter.write(f"  Review needed: {lang_res.flagged_for_review:,}")
        
        if result.error_messages:
            self.progress_reporter.write(f"\n❌ Errors:")
            for error in result.error_messages:
                self.progress_reporter.write(f"  • {error}")
        
        if result.success and not dry_run:
            self.progress_reporter.write(f"\n✅ Update completed successfully!")
            if result.language_assignment and result.language_assignment.flagged_for_review > 0:
                self.progress_reporter.write(f"💡 Next: Review flagged spots with 'uv run python cli/assign_languages.py --review-required'")

# ============================================================================
# Application Layer (CLI Interface)
# ============================================================================

class DailyUpdateCLI:
    """CLI interface for daily update - separated from business logic"""
    
    def __init__(self, db_connection: DatabaseConnection):
        self.db_connection = db_connection
        self.progress_reporter = TqdmProgressReporter()
    
    def execute_from_args(self, args) -> int:
        """Execute daily update from command line arguments"""
        try:
            config = DailyUpdateConfig.from_args(args)
            
            # Validate inputs
            if not config.excel_file.exists():
                print(f"❌ Excel file not found: {config.excel_file}")
                return 1
            
            if not config.db_path.exists():
                print(f"❌ Database not found: {config.db_path}")
                print(f"Run: uv run python scripts/setup_database.py --db-path {config.db_path}")
                return 1
            
            # Display header
            print(f"🔄 Enhanced Daily Update Tool")
            print(f"📁 File: {config.excel_file.name}")
            print(f"🗃️  Database: {config.db_path.name}")
            if config.dry_run:
                print(f"🔍 Mode: DRY RUN (no changes will be made)")
            print()
            
            # Display enhanced preview and validate
            preview_service = DailyUpdatePreviewService(self.db_connection, self.progress_reporter)
            can_proceed = preview_service.display_enhanced_daily_preview(config)
            
            if not can_proceed:
                self._display_validation_error_help()
                return 1
            
            # Get confirmation unless forced or dry run
            if not config.dry_run and not config.force:
                confirmed = self._get_confirmation(config)
                if not confirmed:
                    print(f"❌ Daily update cancelled by user")
                    return 0
            
            # Create services with dependency injection
            orchestrator = self._create_orchestrator()
            
            # Execute the enhanced update
            result = orchestrator.execute_daily_update(config)
            
            # Display results
            presenter = DailyUpdateResultsPresenter(self.progress_reporter)
            presenter.display_final_results(result, config.dry_run)
            
            return 0 if result.success else 1
            
        except BroadcastMonthImportError as e:
            print(f"❌ Import error: {e}")
            return 1
        except KeyboardInterrupt:
            print(f"\n❌ Daily update cancelled by user")
            return 1
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            if hasattr(args, 'verbose') and args.verbose:
                import traceback
                traceback.print_exc()
            return 1
    
    def _create_orchestrator(self) -> DailyUpdateOrchestrator:
        """Create orchestrator with all dependencies injected"""
        # Create repositories
        market_repo = MarketRepository(self.db_connection)
        
        # Create services
        market_setup_service = MarketSetupService(market_repo, self.progress_reporter)
        import_service = ImportService(
            BroadcastMonthImportService(self.db_connection),
            self.progress_reporter
        )
        language_service = LanguageAssignmentService(self.db_connection, self.progress_reporter)
        
        # Create orchestrator
        return DailyUpdateOrchestrator(
            market_setup_service,
            import_service,
            language_service,
            self.progress_reporter
        )
    
    def _get_confirmation(self, config: DailyUpdateConfig) -> bool:
        """Get user confirmation for the update"""
        summary = get_excel_import_summary(str(config.excel_file), config.db_path)
        new_market_count = 0
        
        if config.auto_setup_markets:
            # Estimate new markets
            market_repo = MarketRepository(self.db_connection)
            existing_markets = set(market_repo.get_existing_markets().keys())
            scanner = ExcelMarketScanner(self.progress_reporter)
            new_markets = scanner.scan_for_new_markets(config.excel_file, existing_markets)
            new_market_count = len(new_markets)
        
        return UserConfirmationService.get_user_confirmation(
            summary['total_existing_spots_affected'],
            len(summary['open_months']),
            new_market_count,
            config.force
        )
    
    def _display_validation_error_help(self) -> None:
        """Display validation error help"""
        print(f"\n❌ Daily update cannot proceed due to validation errors")
        print(f"💡 Common solutions:")
        print(f"  • Remove closed month data from Excel file")
        print(f"  • Use historical import mode for closed months")
        print(f"  • Check if months need to be manually closed first")

# ============================================================================
# Custom Exceptions
# ============================================================================

class DailyUpdateError(Exception):
    """Base exception for daily update operations"""
    pass

class MarketScanError(DailyUpdateError):
    """Error scanning Excel file for markets"""
    pass

class MarketSetupError(DailyUpdateError):
    """Error setting up new markets"""
    pass

class ImportValidationError(DailyUpdateError):
    """Error validating import data"""
    pass

# ============================================================================
# Main Entry Point
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Enhanced daily update with automatic market setup and language assignment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard daily update with language assignment
  python daily_update.py data/booked_business_current.xlsx

  # Enhanced daily update with automatic market setup and language assignment
  python daily_update.py data/booked_business_current.xlsx --auto-setup

  # Preview what would happen
  python daily_update.py data/daily_data.xlsx --auto-setup --dry-run

Enhanced Features:
  • Automatic detection of new markets in daily data
  • Missing market creation with proper naming
  • Schedule assignment setup for new markets
  • Integrated language assignment processing with business rules
  • Comprehensive progress tracking with tqdm progress bars
  • Clean, professional output with minimal screen flooding
  • Clean Architecture implementation with proper separation of concerns
        """
    )
    
    parser.add_argument("excel_file", help="Path to Excel file to import")
    parser.add_argument("--auto-setup", action="store_true",
                       help="Automatically create missing markets and schedule assignments")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database path (default: production.db)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Preview import without making changes")
    parser.add_argument("--force", action="store_true",
                       help="Skip confirmation prompts")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    import logging
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')
    
    # Create database connection
    try:
        db_connection = DatabaseConnection(args.db_path)
        
        # Create and execute CLI
        cli = DailyUpdateCLI(db_connection)
        exit_code = cli.execute_from_args(args)
        
        db_connection.close()
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"❌ Failed to initialize daily update: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()