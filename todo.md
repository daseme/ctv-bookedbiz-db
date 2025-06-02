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
- [ x] Add `__init__.py` files to all packages
- [x ] Run `uv pip install -e .` to verify setup
- [x ] Commit initial project structure

### Step 2: Database Schema Definition
- [ ] Create `src/database/schema.py`
  - [ ] Define agencies table schema
  - [ ] Define customers table schema with sector field
  - [ ] Define customer_mappings table schema with confidence scores
  - [ ] Define spots table schema with all Excel columns
  - [ ] Define budget table schema
  - [ ] Define pipeline table schema with current flag
  - [ ] Define markets table schema
  - [ ] Define sectors table schema
  - [ ] Define languages table schema
  - [ ] Add proper indexes including sector and normalized_agency
  - [ ] Add unique constraints on agency_name and normalized_name
- [ ] Create `src/database/models.py`
  - [ ] Create dataclass for each table
  - [ ] Add sector enum (AUTO, CPG, INS, OUTR, etc.)
  - [ ] Add type hints
  - [ ] Add validation methods
- [ ] Write docstrings for all schemas
- [ ] Document agency-customer relationships
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
  - [ ] Function to populate sectors table
  - [ ] Function to populate languages table
  - [ ] Function to set up indexes
- [ ] Create `src/utils/config.py`
  - [ ] Database path configuration
  - [ ] Logging configuration
  - [ ] Environment settings
  - [ ] Sector definitions
- [ ] Add comprehensive error handling
- [ ] Test database creation and connections
- [ ] Commit connection management code

### Step 4: Excel File Reader with Bill Code Parsing
- [ ] Install openpyxl dependency
- [ ] Create `src/importers/excel_reader.py`
  - [ ] Implement ExcelReader class
  - [ ] Add file validation method
  - [ ] Add column reading method
  - [ ] Add row iterator
  - [ ] Preserve all Excel columns
- [ ] Create `src/importers/bill_code_parser.py`
  - [ ] Parse "Agency:Customer" format
  - [ ] Handle "Customer" only format
  - [ ] Return (agency, customer) tuple
  - [ ] Handle edge cases (multiple colons, empty)
- [ ] Create `src/importers/data_validator.py`
  - [ ] Add all column validation
  - [ ] Add date format validation
  - [ ] Add currency validation
  - [ ] Add time field validation (HH:MM:SS)
  - [ ] Add revenue type checking (exclude "Trade")
  - [ ] Add bill code format validation
- [ ] Handle data type conversions
  - [ ] Date conversion (m/d/yy)
  - [ ] Currency to Decimal
  - [ ] Text encoding (UTF-8)
  - [ ] Time fields (time_in, time_out)
- [ ] Add error handling for corrupted files
- [ ] Create unit tests for bill code parsing
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

### Step 6: Customer and Agency Normalization Core
- [ ] Create `src/normalization/similarity.py`
  - [ ] Implement string preprocessing
  - [ ] Add suffix removal function
  - [ ] Add Levenshtein distance calculation
  - [ ] Add fuzzy matching algorithm
- [ ] Create `src/normalization/normalizer_base.py`
  - [ ] Implement BaseNormalizer class
  - [ ] Add shared normalization methods
  - [ ] Add similarity calculation
  - [ ] Add caching mechanism
- [ ] Create `src/normalization/agency_normalizer.py`
  - [ ] Implement AgencyNormalizer class
  - [ ] Add agency-specific rules
  - [ ] Add get or create method
- [ ] Create `src/normalization/customer_normalizer.py`
  - [ ] Implement CustomerNormalizer class
  - [ ] Add customer-specific rules
  - [ ] Add sector assignment
  - [ ] Add agency linking
- [ ] Create `src/database/agency_ops.py`
  - [ ] Function to get existing agencies
  - [ ] Function to create agency
  - [ ] Function to find by name
- [ ] Create `src/database/customer_ops.py`
  - [ ] Function to get existing customers
  - [ ] Function to create customer with sector
  - [ ] Function to create mapping
  - [ ] Function to update sector
- [ ] Add normalization configuration
- [ ] Create unit tests with real examples
- [ ] Test similarity matching accuracy
- [ ] Commit normalization core

### Step 7: Interactive Customer and Agency Mapping CLI
- [ ] Enhance `src/normalization/customer_normalizer.py`
  - [ ] Add InteractiveNormalizer class
  - [ ] Add agency choice presentation
  - [ ] Add customer choice presentation
  - [ ] Add sector choice presentation
  - [ ] Add decision memory
- [ ] Create `src/cli/interactive_mapper.py`
  - [ ] Display bill code parsing results
  - [ ] Display agency similarity matches
  - [ ] Display customer similarity matches
  - [ ] Display sector selection menu
  - [ ] Get and validate user input
  - [ ] Add create new option
- [ ] Create `src/normalization/sector_manager.py`
  - [ ] Define sectors (AUTO, CPG, INS, OUTR, etc.)
  - [ ] Display sector menu method
  - [ ] Validate sector selection
  - [ ] Suggest sector based on name
- [ ] Create `src/normalization/mapping_cache.py`
  - [ ] Implement session cache
  - [ ] Cache agency mappings
  - [ ] Cache customer mappings
  - [ ] Cache sector assignments
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
  - [ ] Add bill code parsing phase
  - [ ] Add agency extraction
  - [ ] Add customer extraction
  - [ ] Add normalization phases
  - [ ] Add sector assignment
- [ ] Update import workflow
  - [ ] Parse all bill codes
  - [ ] Extract unique agencies
  - [ ] Extract unique customers
  - [ ] Load existing mappings
  - [ ] Interactive agency normalization
  - [ ] Interactive customer normalization
  - [ ] Sector assignment/confirmation
  - [ ] Apply all mappings
  - [ ] Import normalized data
- [ ] Create `src/cli/update_command.py`
  - [ ] Full update command
  - [ ] Auto/interactive modes
  - [ ] Dry-run option
  - [ ] Verbose logging
  - [ ] Sector review option
- [ ] Add comprehensive statistics
  - [ ] Agencies created
  - [ ] Customers created
  - [ ] Sectors assigned
  - [ ] Mappings applied
- [ ] Handle special cases
  - [ ] Customer-only bill codes
  - [ ] Multiple colons
  - [ ] Empty bill codes
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
  - [ ] Handle billing_type field
- [ ] Update import for historical data
  - [ ] Check historical months
  - [ ] Skip historical updates
  - [ ] Preserve historical records
  - [ ] Consider billing_type (Calendar/Broadcast)
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

### Step 15: Monthly Revenue Report with Sector Analysis
- [ ] Create `src/reports/monthly_revenue.py`
  - [ ] MonthlyRevenueReport class
  - [ ] Customer queries with sectors
  - [ ] Agency performance queries
  - [ ] Sector breakdown queries
  - [ ] AE queries
  - [ ] Market queries
  - [ ] MoM calculations
- [ ] Implement report sections
  - [ ] Executive summary
  - [ ] Top 10 customers with sectors
  - [ ] Agency performance ranking
  - [ ] Sector breakdown pie chart
  - [ ] AE rankings
  - [ ] Market breakdown
  - [ ] Revenue types
  - [ ] Agency vs direct split
- [ ] Add visualizations
  - [ ] Revenue trends
  - [ ] Sector pie chart
  - [ ] Market charts
  - [ ] Agency contribution
  - [ ] AE performance
  - [ ] YoY indicators
- [ ] Create `src/database/revenue_queries.py`
  - [ ] Optimized queries
  - [ ] Sector aggregations
  - [ ] Agency aggregations
  - [ ] Customer concentration
  - [ ] Comparisons
- [ ] Add drill-down features
  - [ ] Sector to customers
  - [ ] Agency to clients
  - [ ] Market details
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

### Step 19: Advanced Analytics with Sector Performance
- [ ] Create `src/analytics/yoy_analysis.py`
  - [ ] YoY calculations by sector
  - [ ] Growth analysis by market/sector
  - [ ] Seasonal adjustments
  - [ ] Trend identification
  - [ ] Agency YoY comparisons
- [ ] Create `src/analytics/sector_analysis.py`
  - [ ] Sector revenue breakdowns
  - [ ] Market share calculations
  - [ ] Growth trends by sector
  - [ ] Customer concentration
  - [ ] Competitive analysis
  - [ ] Historical performance
- [ ] Create `src/analytics/language_analysis.py`
  - [ ] Revenue by language code
  - [ ] Language trends
  - [ ] Market-language correlations
  - [ ] Growth rates
  - [ ] Multi-language customers
- [ ] Create `src/reports/analytics_dashboard.py`
  - [ ] Analytics view
  - [ ] Sector heatmap
  - [ ] Language charts
  - [ ] Agency matrix
  - [ ] Time comparisons
  - [ ] Predictive indicators
  - [ ] Exception reporting
- [ ] Add statistical functions
  - [ ] Moving averages by sector
  - [ ] Standard deviations
  - [ ] Percentiles
  - [ ] Correlations
  - [ ] Volatility metrics
- [ ] Enhance visualizations
  - [ ] Sector trends
  - [ ] Language maps
  - [ ] Market-sector heatmaps
  - [ ] Agency diagrams
- [ ] Sector-specific metrics
  - [ ] AUTO seasonality
  - [ ] CPG consistency
  - [ ] INS patterns
  - [ ] OUTR effectiveness
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
- [ ] Agency:Customer parsing accurate
- [ ] Customer normalization accurate
- [ ] Sector assignment functional
- [ ] Historical data protected
- [ ] All reports generating
- [ ] Budget/pipeline tracking functional
- [ ] Language field preserved for future use
- [ ] All Excel columns preserved in database

## Project Complete! 🎉