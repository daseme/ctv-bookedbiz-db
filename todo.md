# Sales Database Tool - Development Checklist

## Project Setup and Prerequisites

- [x ] Install Python 3.9 or higher
- [x ] Install `uv` package manager
- [x ] Set up Windows PowerShell environment
- [x ] Verify Git is installed
- [x ] Create project directory
- [x ] Initialize Git repository

## Phase 1: Foundation Setup

### Step 1: Project Structure and Environment Setup
- [x ] Create directory structure
  - [x ] `src/` directory
  - [x ] `src/database/` directory
  - [x ] `src/utils/` directory
  - [x ] `tests/` directory
  - [x ] `data/` directory
  - [x ] `data/raw/` directory
  - [x ] `data/processed/` directory
  - [x ] `config/` directory
- [x ] Create `pyproject.toml` with project configuration
- [x ] Create `.gitignore` file
- [x ] Create `README.md` with placeholders
- [x ] Add `__init__.py` files to all packages
- [x ] Run `uv pip install -e .` to verify setup
- [ ] Commit initial project structure

### Step 2: Database Schema Definition
- [ ] Create `src/database/schema.py`
  - [ ] Define spots table schema
  - [ ] Define customers table schema
  - [ ] Define customer_mappings table schema
  - [ ] Define budget table schema
  - [ ] Define pipeline table schema
  - [ ] Define markets table schema
  - [ ] Add proper indexes
- [ ] Create `src/database/models.py`
  - [ ] Create dataclass for each table
  - [ ] Add type hints
  - [ ] Add validation methods
- [ ] Write docstrings for all schemas
- [ ] Test schema creation in SQLite
- [ ] Commit schema definitions

### Step 3: Database Connection Manager
- [ ] Create `src/database/connection.py`
  - [ ] Implement DatabaseConnection class
  - [ ] Add context manager support
  - [ ] Add transaction management
  - [ ] Add connection pooling
- [ ] Create `src/database/initialize.py`
  - [ ] Function to create all tables
  - [ ] Function to verify database integrity
  - [ ] Function to create market mappings
  - [ ] Function to set up indexes
- [ ] Create `src/utils/config.py`
  - [ ] Database path configuration
  - [ ] Logging configuration
  - [ ] Environment settings
- [ ] Add comprehensive error handling
- [ ] Test database creation and connections
- [ ] Commit connection management code

### Step 4: Excel File Reader
- [ ] Install openpyxl dependency
- [ ] Create `src/importers/excel_reader.py`
  - [ ] Implement ExcelReader class
  - [ ] Add file validation method
  - [ ] Add column reading method
  - [ ] Add row iterator
- [ ] Create `src/importers/data_validator.py`
  - [ ] Add column validation
  - [ ] Add date format validation
  - [ ] Add currency validation
  - [ ] Add revenue type checking
- [ ] Handle data type conversions
  - [ ] Date conversion (m/d/yy)
  - [ ] Currency to Decimal
  - [ ] Text encoding (UTF-8)
- [ ] Add error handling for corrupted files
- [ ] Create unit tests for Excel reading
- [ ] Test with sample Excel file
- [ ] Commit Excel reader implementation

### Step 5: Basic Data Import
- [ ] Create `src/importers/basic_importer.py`
  - [ ] Implement DataImporter class
  - [ ] Add Excel reading integration
  - [ ] Add row validation
  - [ ] Add batch insert method
- [ ] Create `src/cli/import_command.py`
  - [ ] Add command-line argument parsing
  - [ ] Add progress bar
  - [ ] Add error reporting
  - [ ] Add success summary
- [ ] Implement transaction management
- [ ] Add import statistics tracking
- [ ] Create test import script
- [ ] Test full import workflow
- [ ] Verify data in database
- [ ] Commit basic import functionality

## Phase 1 Checkpoint
- [ ] All Phase 1 tests passing
- [ ] Can successfully import Excel file
- [ ] Database contains raw data
- [ ] No crashes or data loss
- [ ] Documentation updated

## Phase 2: Customer Normalization

### Step 6: Customer Normalization Core
- [ ] Create `src/normalization/similarity.py`
  - [ ] Implement string preprocessing
  - [ ] Add suffix removal function
  - [ ] Add Levenshtein distance calculation
  - [ ] Add fuzzy matching algorithm
- [ ] Create `src/normalization/customer_normalizer.py`
  - [ ] Implement CustomerNormalizer class
  - [ ] Add similarity calculation
  - [ ] Add match ranking
  - [ ] Add caching mechanism
- [ ] Create `src/database/customer_ops.py`
  - [ ] Function to get existing customers
  - [ ] Function to create customer
  - [ ] Function to create mapping
  - [ ] Function to get mappings
- [ ] Add normalization configuration
- [ ] Create unit tests with real examples
- [ ] Test similarity matching accuracy
- [ ] Commit normalization core

### Step 7: Interactive Customer Mapping CLI
- [ ] Enhance `src/normalization/customer_normalizer.py`
  - [ ] Add InteractiveNormalizer class
  - [ ] Add choice presentation method
  - [ ] Add decision memory
- [ ] Create `src/cli/interactive_mapper.py`
  - [ ] Display similarity matches
  - [ ] Get user input
  - [ ] Validate choices
  - [ ] Add create new option
- [ ] Create `src/normalization/mapping_cache.py`
  - [ ] Implement session cache
  - [ ] Add cache application
  - [ ] Add import/export methods
- [ ] Update import process for normalization
- [ ] Add colorized output for better UX
- [ ] Test interactive flow
- [ ] Commit interactive CLI

### Step 8: Historical Mapping Storage
- [ ] Create `src/database/mapping_ops.py`
  - [ ] Save mapping function
  - [ ] Load mappings function
  - [ ] Update mapping function
  - [ ] Find mapping function
- [ ] Create `src/normalization/mapping_manager.py`
  - [ ] Implement MappingManager class
  - [ ] Add historical loading
  - [ ] Add auto-apply logic
  - [ ] Add export functionality
- [ ] Create `src/reports/mapping_audit.py`
  - [ ] Generate mapping report
  - [ ] Show mappings by date
  - [ ] Identify duplicates
- [ ] Add mapping management commands
- [ ] Test mapping persistence
- [ ] Verify auto-application
- [ ] Commit mapping storage

### Step 9: Complete Import with Normalization
- [ ] Create `src/importers/normalized_importer.py`
  - [ ] Extend DataImporter class
  - [ ] Add customer extraction
  - [ ] Add normalization phase
  - [ ] Add normalized import
- [ ] Update import workflow
  - [ ] Extract unique customers
  - [ ] Load existing mappings
  - [ ] Interactive normalization
  - [ ] Apply mappings
  - [ ] Import normalized data
- [ ] Create `src/cli/update_command.py`
  - [ ] Full update command
  - [ ] Auto/interactive modes
  - [ ] Dry-run option
  - [ ] Verbose logging
- [ ] Add comprehensive statistics
- [ ] Test with real data files
- [ ] Verify normalized data
- [ ] Commit complete normalization

## Phase 2 Checkpoint
- [ ] Customer normalization working
- [ ] Mappings persist between runs
- [ ] Interactive CLI functional
- [ ] All tests passing
- [ ] Documentation updated

## Phase 3: Data Management

### Step 10: Market Standardization
- [ ] Create `src/normalization/market_standardizer.py`
  - [ ] Add market mapping dictionary
  - [ ] Implement standardization function
  - [ ] Add validation function
  - [ ] Handle unknown markets
- [ ] Update database initialization
  - [ ] Populate markets table
  - [ ] Add all standard mappings
- [ ] Enhance import process
  - [ ] Apply market standardization
  - [ ] Report unknown markets
  - [ ] Add new mappings option
- [ ] Create `src/reports/market_report.py`
  - [ ] Show all mappings
  - [ ] Identify unmapped markets
  - [ ] Market distribution stats
- [ ] Test market standardization
- [ ] Verify all markets mapped
- [ ] Commit market standardization

### Step 11: Historical Data Management
- [ ] Create `src/database/historical_ops.py`
  - [ ] Mark month as historical
  - [ ] Prevent historical updates
  - [ ] Identify month types
  - [ ] Archive function
- [ ] Create `src/utils/date_utils.py`
  - [ ] Broadcast month calculation
  - [ ] Calendar month calculation
  - [ ] Month boundary detection
  - [ ] Current month identification
- [ ] Update import for historical data
  - [ ] Check historical months
  - [ ] Skip historical updates
  - [ ] Preserve historical records
- [ ] Create `src/cli/finalize_command.py`
  - [ ] Finalize month command
  - [ ] List historical months
  - [ ] Undo finalization
- [ ] Add integrity checks
- [ ] Test finalization process
- [ ] Commit historical management

### Step 12: Budget and Pipeline Data
- [ ] Create `src/importers/budget_importer.py`
  - [ ] Excel/CSV reader
  - [ ] Budget validation
  - [ ] AE normalization
  - [ ] Annual distribution
- [ ] Create `src/importers/pipeline_importer.py`
  - [ ] Pipeline reader
  - [ ] Update handling
  - [ ] Version tracking
  - [ ] Delta reporting
- [ ] Create `src/database/budget_ops.py`
  - [ ] CRUD operations
  - [ ] Query functions
  - [ ] Calculations
  - [ ] Summaries
- [ ] Create `src/database/pipeline_ops.py`
  - [ ] Update functions
  - [ ] Version tracking
  - [ ] Historical queries
  - [ ] Trend analysis
- [ ] Create CLI commands
  - [ ] Import budget
  - [ ] Update pipeline
  - [ ] View status
- [ ] Test budget/pipeline import
- [ ] Verify calculations
- [ ] Commit budget/pipeline features

### Step 13: Update Process Orchestration
- [ ] Create `src/workflow/update_orchestrator.py`
  - [ ] Workflow class
  - [ ] Step execution
  - [ ] Rollback capability
  - [ ] Progress tracking
- [ ] Implement update steps
  - [ ] Database backup
  - [ ] Data validation
  - [ ] Month checking
  - [ ] Data deletion
  - [ ] Import execution
  - [ ] Report generation
- [ ] Create `src/utils/backup.py`
  - [ ] Backup function
  - [ ] Rotation policy
  - [ ] Restoration
  - [ ] Verification
- [ ] Create `src/cli/weekly_update.py`
  - [ ] Single update command
  - [ ] Component options
  - [ ] Dry-run mode
  - [ ] Automation support
- [ ] Add update reporting
- [ ] Test complete workflow
- [ ] Verify rollback works
- [ ] Commit orchestration

## Phase 3 Checkpoint
- [ ] Market standardization complete
- [ ] Historical data protected
- [ ] Budget/pipeline integrated
- [ ] Weekly update automated
- [ ] All tests passing

## Phase 4: Basic Reporting

### Step 14: Report Generator Framework
- [ ] Create `src/reports/base_report.py`
  - [ ] BaseReport abstract class
  - [ ] Common methods
  - [ ] Template handling
  - [ ] CSS integration
- [ ] Create `src/reports/html_generator.py`
  - [ ] HTML structure
  - [ ] Table formatting
  - [ ] Chart placeholders
  - [ ] Responsive CSS
- [ ] Create report templates
  - [ ] `base_template.html`
  - [ ] `report_styles.css`
  - [ ] JavaScript files
  - [ ] Print styling
- [ ] Create `src/reports/data_formatter.py`
  - [ ] Currency formatting
  - [ ] Date formatting
  - [ ] Percentage calculations
  - [ ] Number formatting
- [ ] Test HTML generation
- [ ] Verify responsive design
- [ ] Commit report framework

### Step 15: Monthly Revenue Report
- [ ] Create `src/reports/monthly_revenue.py`
  - [ ] MonthlyRevenueReport class
  - [ ] Customer queries
  - [ ] AE queries
  - [ ] Market queries
  - [ ] MoM calculations
- [ ] Implement report sections
  - [ ] Executive summary
  - [ ] Top 10 customers
  - [ ] AE rankings
  - [ ] Market breakdown
  - [ ] Revenue types
- [ ] Add visualizations
  - [ ] Revenue trends
  - [ ] Market charts
  - [ ] AE performance
  - [ ] YoY indicators
- [ ] Create `src/database/revenue_queries.py`
  - [ ] Optimized queries
  - [ ] Aggregations
  - [ ] Comparisons
- [ ] Test report generation
- [ ] Verify calculations
- [ ] Commit monthly report

### Step 16: AE Performance Report
- [ ] Create `src/reports/ae_performance.py`
  - [ ] AEPerformanceReport class
  - [ ] Individual metrics
  - [ ] Budget comparison
  - [ ] Pipeline tracking
  - [ ] Portfolio analysis
- [ ] Implement components
  - [ ] Summary card
  - [ ] Performance table
  - [ ] Budget gauge
  - [ ] Customer analysis
  - [ ] Type breakdown
- [ ] Add metrics
  - [ ] Current bookings
  - [ ] QTD/YTD totals
  - [ ] Budget variance
  - [ ] Pipeline coverage
  - [ ] Growth rates
- [ ] Add comparisons
  - [ ] Previous month
  - [ ] Last year
  - [ ] Team average
  - [ ] Peer ranking
- [ ] Test AE reports
- [ ] Verify email formatting
- [ ] Commit AE reports

### Step 17: Report CLI and Scheduling
- [ ] Create `src/cli/report_command.py`
  - [ ] Report selection
  - [ ] Parameter input
  - [ ] Output options
  - [ ] Batch generation
- [ ] Implement CLI commands
  - [ ] Monthly reports
  - [ ] AE reports
  - [ ] Weekly emails
  - [ ] List reports
- [ ] Create `src/scheduler/report_scheduler.py`
  - [ ] Schedule config
  - [ ] Queue management
  - [ ] Email settings
  - [ ] Error handling
- [ ] Create `src/utils/email_sender.py`
  - [ ] HTML formatting
  - [ ] Attachments
  - [ ] Distribution lists
  - [ ] Confirmations
- [ ] Add configuration
- [ ] Test report generation
- [ ] Test email sending
- [ ] Commit CLI and scheduling

## Phase 4 Checkpoint
- [ ] Report framework complete
- [ ] Monthly reports working
- [ ] AE reports functional
- [ ] CLI operational
- [ ] Email delivery tested

## Phase 5: Advanced Features

### Step 18: Datasette Integration
- [ ] Install Datasette
- [ ] Create `src/datasette/setup.py`
  - [ ] Configuration
  - [ ] Metadata
  - [ ] Descriptions
- [ ] Create `datasette_config.json`
  - [ ] Database path
  - [ ] Authentication
  - [ ] Plugins
  - [ ] Custom CSS
- [ ] Create `src/datasette/custom_queries.sql`
  - [ ] Business queries
  - [ ] Revenue summaries
  - [ ] Customer analysis
  - [ ] Trends
- [ ] Install plugins
  - [ ] datasette-vega
  - [ ] datasette-export
  - [ ] datasette-saved-queries
- [ ] Create deployment script
- [ ] Write user guide
- [ ] Test Datasette access
- [ ] Commit Datasette integration

### Step 19: Advanced Analytics
- [ ] Create `src/analytics/yoy_analysis.py`
  - [ ] YoY calculations
  - [ ] Growth analysis
  - [ ] Seasonal adjustments
  - [ ] Trend identification
- [ ] Create `src/analytics/sector_analysis.py`
  - [ ] Sector revenue
  - [ ] Market share
  - [ ] Growth trends
  - [ ] Competitive analysis
- [ ] Create `src/reports/analytics_dashboard.py`
  - [ ] Analytics view
  - [ ] Time comparisons
  - [ ] Predictive indicators
  - [ ] Exception reporting
- [ ] Add statistical functions
  - [ ] Moving averages
  - [ ] Standard deviations
  - [ ] Percentiles
  - [ ] Correlations
- [ ] Enhance visualizations
- [ ] Test analytics
- [ ] Commit advanced analytics

### Step 20: System Integration and Finalization
- [ ] Create `src/main.py`
  - [ ] Command dispatcher
  - [ ] Help system
  - [ ] Version info
  - [ ] Config validation
- [ ] Create documentation
  - [ ] `docs/user_guide.md`
  - [ ] `docs/admin_guide.md`
  - [ ] `docs/troubleshooting.md`
  - [ ] `docs/api_reference.md`
- [ ] Add health checks
  - [ ] Database integrity
  - [ ] Data quality
  - [ ] Performance monitoring
  - [ ] Error analysis
- [ ] Create deployment package
  - [ ] Finalize requirements.txt
  - [ ] Installation script
  - [ ] Sample data
  - [ ] Quick start guide
- [ ] Final features
  - [ ] Activity logging
  - [ ] Audit trail
  - [ ] Backup automation
  - [ ] Report archive
- [ ] Create video tutorials
- [ ] Final testing
- [ ] Commit final integration

## Final Project Checklist

### Testing
- [ ] All unit tests passing
- [ ] Integration tests complete
- [ ] Performance benchmarks met
- [ ] Error scenarios handled
- [ ] Edge cases covered

### Documentation
- [ ] Code fully documented
- [ ] User guide complete
- [ ] Admin guide complete
- [ ] API reference done
- [ ] Video tutorials created

### Deployment
- [ ] Installation tested on clean system
- [ ] Backup/restore verified
- [ ] Email delivery confirmed
- [ ] Datasette accessible
- [ ] Sample data works

### Quality Assurance
- [ ] Code follows PEP 8
- [ ] No SQL injection vulnerabilities
- [ ] Error messages helpful
- [ ] Logging comprehensive
- [ ] Performance acceptable

### Business Requirements
- [ ] Weekly updates working
- [ ] Customer normalization accurate
- [ ] Historical data protected
- [ ] All reports generating
- [ ] Budget/pipeline tracking functional

## Project Complete! 🎉