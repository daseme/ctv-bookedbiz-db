# src/cli/update_command.py
from ..database.connection import DatabaseConnection
from ..repositories.sqlite_repositories import SQLiteSpotRepository, SQLiteCustomerRepository, ReferenceDataRepository
from ..services.data_import_service import DataImportService
from ..config.settings import get_settings

def update_command(excel_file_path: str):
    # Setup dependencies
    settings = get_settings()
    db_conn = DatabaseConnection(settings.database.db_path)
    spot_repo = SQLiteSpotRepository(db_conn)
    customer_repo = SQLiteCustomerRepository(db_conn)
    reference_repo = ReferenceDataRepository(db_conn)
   
    # Create service
    import_service = DataImportService(db_conn, spot_repo, customer_repo, reference_repo)
   
    # Your existing Excel parsing logic here
    spots = parse_excel_file(excel_file_path)
   
    # Execute import
    results = import_service.execute_weekly_import(spots, date(2025, 6, 1))
    print(f"Import completed: {results}")