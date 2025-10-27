"""
Basic import service that combines Excel reading, bill code parsing, and database storage.
This is the first complete data flow from Excel to Database.
"""

import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from dataclasses import dataclass

# Handle imports for both package use and direct execution
try:
    from ..importers.excel_reader import ExcelReader, ExcelReadError
    from ..services.bill_code_parser import BillCodeParser, BillCodeParseError
    from ..repositories.sqlite_repositories import (
        SQLiteSpotRepository,
        SQLiteCustomerRepository,
        ReferenceDataRepository,
    )
    from ..database.connection import DatabaseConnection
    from ..models.entities import Spot, Customer
    from ..models.validators import SpotValidator, CustomerValidator
except ImportError:
    # Fall back to absolute imports for direct execution
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).parent.parent))

    from importers.excel_reader import ExcelReader, ExcelReadError
    from src.services.bill_code_parser import BillCodeParser, BillCodeParseError
    from src.repositories.sqlite_repositories import (
        SQLiteSpotRepository,
        SQLiteCustomerRepository,
        ReferenceDataRepository,
    )
    from src.database.connection import DatabaseConnection
    from src.models.entities import Spot, Customer
    from src.models.validators import SpotValidator, CustomerValidator
    from src.services.base_service import BaseService

logger = logging.getLogger(__name__)


@dataclass
class ImportResult:
    """Result of an import operation."""

    success: bool
    spots_processed: int = 0
    spots_imported: int = 0
    spots_skipped: int = 0
    new_customers_created: int = 0
    new_agencies_created: int = 0
    validation_errors: int = 0
    processing_errors: int = 0
    duration_seconds: float = 0.0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class BasicImportService(BaseService):
    """
    Basic import service that handles the complete flow:
    Excel → Bill Code Parsing → Validation → Database Storage
    """

    def __init__(self, db_connection: DatabaseConnection):
        super().__init__(db_connection)  # ADD THIS LINE

        # Initialize repositories
        self.spot_repo = SQLiteSpotRepository(db_connection)
        self.customer_repo = SQLiteCustomerRepository(db_connection)
        self.reference_repo = ReferenceDataRepository(db_connection)

        # Initialize services
        self.bill_code_parser = BillCodeParser(normalize_customer_names=True)

        # Initialize validators
        self.spot_validator = SpotValidator()
        self.customer_validator = CustomerValidator()

    def import_excel_file(
        self,
        excel_file_path: str,
        limit: Optional[int] = None,
        validate_spots: bool = True,
    ) -> ImportResult:
        """
        Import data from an Excel file into the database.

        Args:
            excel_file_path: Path to the Excel file
            limit: Optional limit on number of spots to process (for testing)
            validate_spots: Whether to validate spots before importing

        Returns:
            ImportResult with statistics and any errors
        """
        start_time = datetime.now()
        result = ImportResult(success=False)

        logger.info(f"Starting import from: {excel_file_path}")
        if limit:
            logger.info(f"Processing limited to {limit} spots for testing")

        try:
            # Step 1: Read Excel file
            with ExcelReader(excel_file_path) as reader:
                logger.info(f"Reading Excel file: {reader.get_file_info()}")

                # Read spots (with optional limit for testing)
                if limit:
                    spots = []
                    for i, spot in enumerate(reader.read_spots()):
                        spots.append(spot)
                        if i >= limit - 1:
                            break
                else:
                    spots = reader.read_all_spots()

                result.spots_processed = len(spots)
                logger.info(f"Read {result.spots_processed} spots from Excel")

            # Step 2: Process spots with bill code parsing and validation
            processed_spots = self._process_spots(spots, result, validate_spots)

            # Step 3: Import to database
            self._import_spots_to_database(processed_spots, result)

            # Step 4: Calculate final statistics
            end_time = datetime.now()
            result.duration_seconds = (end_time - start_time).total_seconds()
            result.success = True

            logger.info(
                f"Import completed successfully in {result.duration_seconds:.2f} seconds"
            )
            logger.info(
                f"Imported {result.spots_imported} spots, skipped {result.spots_skipped}"
            )

        except ExcelReadError as e:
            error_msg = f"Excel reading error: {str(e)}"
            result.errors.append(error_msg)
            logger.error(error_msg)

        except Exception as e:
            error_msg = f"Unexpected import error: {str(e)}"
            result.errors.append(error_msg)
            logger.error(error_msg, exc_info=True)

        return result

    def _process_spots(
        self, spots: List[Spot], result: ImportResult, validate_spots: bool
    ) -> List[Spot]:
        """Process spots: parse bill codes, create customers/agencies, validate."""
        processed_spots = []

        logger.info(f"Processing {len(spots)} spots...")

        for i, spot in enumerate(spots):
            try:
                # Parse bill code
                agency_name, customer_name = self.bill_code_parser.parse(spot.bill_code)

                # Get or create agency if present
                if agency_name:
                    agency = self.reference_repo.get_or_create_agency(agency_name)
                    spot.agency_id = agency.agency_id
                    if (
                        agency.created_date
                        and agency.created_date.date() == date.today()
                    ):
                        result.new_agencies_created += 1

                # Get or create customer
                customer = self.customer_repo.find_by_normalized_name(customer_name)
                if not customer:
                    # Create new customer
                    customer = Customer(normalized_name=customer_name)

                    # Validate customer before saving
                    if validate_spots:
                        validation_result = self.customer_validator.validate(customer)
                        if not validation_result.is_valid():
                            logger.warning(
                                f"Customer validation failed for '{customer_name}': {validation_result.errors}"
                            )
                            result.validation_errors += 1
                            continue

                    customer = self.customer_repo.save(customer)
                    result.new_customers_created += 1
                    logger.debug(f"Created new customer: {customer_name}")

                spot.customer_id = customer.customer_id

                # Handle market mapping
                if spot.market_name:
                    market = self.reference_repo.get_market_by_name(spot.market_name)
                    if market:
                        spot.market_id = market.market_id

                # Validate spot if requested
                if validate_spots:
                    validation_result = self.spot_validator.validate(spot)
                    if not validation_result.is_valid():
                        logger.warning(
                            f"Spot validation failed: {validation_result.errors}"
                        )
                        result.validation_errors += 1
                        result.spots_skipped += 1
                        continue

                processed_spots.append(spot)

            except BillCodeParseError as e:
                logger.warning(
                    f"Failed to parse bill code '{spot.bill_code}': {str(e)}"
                )
                result.processing_errors += 1
                result.spots_skipped += 1

            except Exception as e:
                logger.error(f"Error processing spot {i}: {str(e)}")
                result.processing_errors += 1
                result.spots_skipped += 1

        logger.info(f"Processed {len(processed_spots)} spots successfully")
        return processed_spots

    def _import_spots_to_database(self, spots: List[Spot], result: ImportResult):
        """FIXED: Import processed spots to database using BaseService."""
        logger.info(f"Importing {len(spots)} spots to database...")

        with self.safe_transaction() as conn:
            for spot in spots:
                try:
                    self.spot_repo.save(spot)
                    result.spots_imported += 1

                except Exception as e:
                    logger.error(f"Failed to save spot to database: {str(e)}")
                    result.processing_errors += 1
                    result.spots_skipped += 1

        logger.info(f"Database import completed: {result.spots_imported} spots saved")

    def get_import_statistics(self) -> Dict[str, Any]:
        """Get bill code parsing statistics."""
        return self.bill_code_parser.get_statistics()

    def reset_statistics(self):
        """Reset all statistics."""
        self.bill_code_parser.reset_statistics()


# Convenience function for simple usage
def import_excel_to_database(
    excel_file_path: str, database_path: str, limit: Optional[int] = None
) -> ImportResult:
    """
    Simple function to import an Excel file to database.

    Args:
        excel_file_path: Path to Excel file
        database_path: Path to SQLite database
        limit: Optional limit for testing

    Returns:
        ImportResult with statistics
    """
    db_connection = DatabaseConnection(database_path)
    service = BasicImportService(db_connection)

    try:
        result = service.import_excel_file(excel_file_path, limit=limit)
        return result
    finally:
        db_connection.close()


# Test script
if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description="Test basic import service")
    parser.add_argument("excel_file", help="Path to Excel file")
    parser.add_argument(
        "--database", default="data/database/production.db", help="Database path"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of spots to import (for testing)"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")

    if not Path(args.excel_file).exists():
        print(f"Error: Excel file not found: {args.excel_file}")
        sys.exit(1)

    if not Path(args.database).exists():
        print(f"Error: Database not found: {args.database}")
        print("Run: python scripts/setup_database.py --db-path {args.database}")
        sys.exit(1)

    print(f"Testing Basic Import Service")
    print(f"Excel file: {args.excel_file}")
    print(f"Database: {args.database}")
    if args.limit:
        print(f"Limit: {args.limit} spots")
    print("=" * 50)

    try:
        result = import_excel_to_database(args.excel_file, args.database, args.limit)

        print(f"\nImport Results:")
        print(f"  Success: {result.success}")
        print(f"  Duration: {result.duration_seconds:.2f} seconds")
        print(f"  Spots processed: {result.spots_processed}")
        print(f"  Spots imported: {result.spots_imported}")
        print(f"  Spots skipped: {result.spots_skipped}")
        print(f"  New customers: {result.new_customers_created}")
        print(f"  New agencies: {result.new_agencies_created}")
        print(f"  Validation errors: {result.validation_errors}")
        print(f"  Processing errors: {result.processing_errors}")

        if result.errors:
            print(f"\nErrors:")
            for error in result.errors:
                print(f"  - {error}")

        if result.success:
            print(f"\n✅ Import completed successfully!")
        else:
            print(f"\n❌ Import failed!")
            sys.exit(1)

    except Exception as e:
        print(f"Import failed: {e}")
        sys.exit(1)
