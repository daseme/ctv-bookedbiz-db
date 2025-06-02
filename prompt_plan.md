# Sales Database Tool - Development Blueprint and Prompts

## Project Overview

This blueprint provides a structured approach to building a read-only sales database tool for a television network. The system will track booked revenue, budget balancing, and pipeline numbers across multiple markets and revenue types.

## Development Philosophy

1. **Incremental Progress**: Each step builds on the previous one
2. **Test as You Go**: Verify each component works before moving forward
3. **No Orphaned Code**: Every piece connects to the main system
4. **Best Practices First**: Proper structure, error handling, and documentation from the start

## Phase 1: Foundation Setup (Steps 1-5)

### Step 1: Project Structure and Environment Setup

**Goal**: Create the basic project structure and set up the development environment.

```text
Create a Python project structure for a sales database tool using SQLite. 

Requirements:
1. Create a project directory structure with the following layout:
   - src/ (main source code)
   - src/database/ (database models and connections)
   - src/utils/ (utility functions)
   - tests/ (unit tests)
   - data/ (data storage)
   - data/raw/ (raw Excel files)
   - data/processed/ (processed data)
   - config/ (configuration files)

2. Create a pyproject.toml file for project configuration that:
   - Sets Python requirement to >=3.9
   - Defines the project name as "sales-database-tool"
   - Lists initial dependencies: sqlite3 (built-in), python-dateutil, openpyxl

3. Create a .gitignore file appropriate for Python projects

4. Create a README.md with:
   - Project title and description
   - Installation instructions placeholder
   - Usage instructions placeholder

5. Create an empty __init__.py in each Python package directory

Ensure all files follow Python best practices and include appropriate docstrings.
```

### Step 2: Database Schema Definition

**Goal**: Define the SQLite database schema with all required tables.

```text
Create SQLite database schema definitions for the sales database tool.

Requirements:
1. Create src/database/schema.py with SQL CREATE TABLE statements for:
   - spots table (with all Excel columns plus normalized fields)
   - customers table (customer_id, normalized_name, timestamps)
   - customer_mappings table (original to normalized mapping)
   - budget table (AE budgets by month/year)
   - pipeline table (management expectations)
   - markets table (market name to code mapping)

2. Include proper:
   - Primary keys and foreign keys
   - Indexes on frequently queried columns (air_date, normalized_customer, ae_name, market_code)
   - NOT NULL constraints where appropriate
   - Default values where sensible

3. Create src/database/models.py with:
   - Python dataclass definitions matching each table
   - Type hints for all fields
   - Validation methods for data integrity

4. Add comprehensive docstrings explaining each table's purpose and relationships

Use SQLite-specific syntax and ensure all date fields can handle the m/d/yy format from Excel.
```

### Step 3: Database Connection Manager

**Goal**: Create a robust database connection system with proper error handling.

```text
Create a database connection manager for the SQLite database.

Requirements:
1. Create src/database/connection.py with:
   - DatabaseConnection class using context manager pattern
   - Connection pooling for concurrent access
   - Automatic database creation if not exists
   - Transaction management methods

2. Create src/database/initialize.py with:
   - Function to create all tables from schema
   - Function to verify database integrity
   - Function to create initial market mappings
   - Function to set up indexes

3. Implement error handling for:
   - Database file permissions
   - Disk space issues
   - Concurrent access conflicts
   - Schema version mismatches

4. Create src/utils/config.py with:
   - Database path configuration
   - Logging configuration
   - Environment-specific settings

5. Add logging throughout for debugging and monitoring

Include comprehensive error messages and recovery strategies.
```

### Step 4: Excel File Reader

**Goal**: Build a robust Excel file reader that handles the booked business report format.

```text
Create an Excel file reader for the booked business report.

Requirements:
1. Create src/importers/excel_reader.py with:
   - ExcelReader class that uses openpyxl
   - Method to validate Excel file structure
   - Method to read all columns preserving original names
   - Iterator pattern for memory-efficient row processing

2. Handle data type conversions:
   - Air Date (m/d/yy format) to Python date
   - Currency fields (Gross Rate, Station Net) to Decimal
   - Text fields with proper encoding (UTF-8)
   - Empty cells to None

3. Create src/importers/data_validator.py with:
   - Validation for required columns
   - Date format validation
   - Currency format validation
   - Revenue type checking (exclude "Trade")
   - Data completeness checks

4. Implement error handling for:
   - Missing columns
   - Invalid date formats
   - Malformed currency values
   - File access issues
   - Corrupted Excel files

5. Add progress reporting for large files

Include unit tests for common data issues and edge cases.
```

### Step 5: Basic Data Import

**Goal**: Implement the core import functionality without normalization.

```text
Create the basic data import functionality to load Excel data into the database.

Requirements:
1. Create src/importers/basic_importer.py with:
   - DataImporter class
   - Method to read Excel file using ExcelReader
   - Method to validate all rows
   - Method to insert data into spots table
   - Transaction management for atomic updates

2. Implement the import workflow:
   - Open database connection
   - Begin transaction
   - Validate Excel structure
   - Process rows in batches (1000 rows)
   - Commit on success, rollback on failure

3. Create src/cli/import_command.py with:
   - Command-line interface for import
   - Progress bar for user feedback
   - Error reporting
   - Success summary (rows imported, skipped, errors)

4. Add data tracking:
   - load_date timestamp
   - Source file name
   - Import statistics

5. Create a simple test script that:
   - Creates the database
   - Imports a sample Excel file
   - Queries and displays row count

This establishes the foundation before adding normalization complexity.
```

## Phase 2: Customer Normalization (Steps 6-9)

### Step 6: Customer Normalization Core

**Goal**: Build the string similarity matching system for customer names.

```text
Create a customer name normalization system with similarity matching.

Requirements:
1. Create src/normalization/similarity.py with:
   - String preprocessing (lowercase, remove punctuation)
   - Suffix removal function (LLC, Co., Corporation, etc.)
   - Revenue type suffix removal (Production, etc.)
   - Levenshtein distance calculation
   - Fuzzy matching with configurable threshold

2. Create src/normalization/customer_normalizer.py with:
   - CustomerNormalizer class
   - Method to find similar existing customers
   - Method to calculate similarity scores
   - Method to rank matches by similarity
   - Caching for performance

3. Implement normalization rules:
   - Remove common business suffixes
   - Strip revenue type indicators
   - Handle special characters consistently
   - Preserve important distinctions

4. Create src/database/customer_ops.py with:
   - Function to get all existing customers
   - Function to create new customer
   - Function to create mapping
   - Function to get existing mappings

5. Add configuration in src/utils/config.py:
   - Similarity threshold (default 70%)
   - List of removable suffixes
   - List of revenue type indicators

Include unit tests with real-world examples from the specification.
```

### Step 7: Interactive Customer Mapping CLI

**Goal**: Create an interactive command-line interface for customer normalization.

```text
Create an interactive CLI for customer name normalization during import.

Requirements:
1. Enhance src/normalization/customer_normalizer.py with:
   - InteractiveNormalizer class
   - Method to present choices to user
   - Method to remember user decisions
   - Method to apply mappings to data

2. Create src/cli/interactive_mapper.py with:
   - Function to display similarity matches
   - Function to get user input
   - Function to validate user choice
   - Option to create new customer
   - Option to skip/cancel

3. Implement the interaction flow:
   - Show new customer name
   - Display top 5 similar matches with percentages
   - Allow selection by number
   - Confirm mapping before saving
   - Remember for current session

4. Create src/normalization/mapping_cache.py with:
   - In-memory cache for session
   - Method to apply cached mappings
   - Method to export mappings
   - Method to import previous mappings

5. Update import process to:
   - Collect unique customer names first
   - Process normalizations interactively
   - Apply mappings during import
   - Show mapping summary

Format output clearly with colors/formatting for better UX.
```

### Step 8: Historical Mapping Storage

**Goal**: Persist customer mappings for future imports.

```text
Implement persistent storage for customer name mappings.

Requirements:
1. Create src/database/mapping_ops.py with:
   - Function to save new mapping
   - Function to load all mappings
   - Function to update existing mapping
   - Function to find mapping by original name

2. Enhance src/normalization/mapping_manager.py with:
   - MappingManager class
   - Load historical mappings on startup
   - Auto-apply known mappings
   - Only prompt for new/uncertain names
   - Export mappings to file

3. Create src/reports/mapping_audit.py with:
   - Function to generate mapping report
   - Show all mappings by date
   - Identify potential duplicates
   - Suggest consolidation opportunities

4. Update the import workflow:
   - Load existing mappings
   - Auto-apply known mappings silently
   - Only interact for new names
   - Save new mappings immediately
   - Generate mapping summary

5. Add mapping management commands:
   - List all mappings
   - Edit existing mapping
   - Merge duplicate customers
   - Export/import mappings file

Include validation to prevent circular or conflicting mappings.
```

### Step 9: Complete Import with Normalization

**Goal**: Integrate normalization into the full import process.

```text
Update the import process to include full customer normalization.

Requirements:
1. Create src/importers/normalized_importer.py with:
   - NormalizedDataImporter class extending DataImporter
   - Pre-import customer extraction
   - Interactive normalization phase
   - Normalized data import
   - Post-import verification

2. Implement the complete workflow:
   - Read Excel file
   - Extract unique customers
   - Load existing mappings
   - Interactive normalization for new customers
   - Apply all mappings
   - Import with normalized_customer field
   - Update customer and mapping tables

3. Add transaction management:
   - Atomic customer creation
   - Atomic mapping creation
   - Rollback on any failure
   - Detailed error reporting

4. Create src/cli/update_command.py with:
   - Full update command
   - Options for automatic/interactive mode
   - Dry-run option
   - Verbose logging option

5. Add import statistics:
   - Total rows processed
   - New customers created
   - Mappings applied
   - Errors encountered
   - Time elapsed

Test with real data files and various edge cases.
```

## Phase 3: Data Management (Steps 10-13)

### Step 10: Market Standardization

**Goal**: Implement market code standardization system.

```text
Create market standardization functionality.

Requirements:
1. Create src/normalization/market_standardizer.py with:
   - Market mapping dictionary
   - Function to standardize market names
   - Function to validate market codes
   - Function to handle unknown markets

2. Update database initialization:
   - Populate markets table with standard mappings
   - NEW YORK -> NYC
   - Central Valley -> CVC
   - SAN FRANCISCO -> SFO
   - etc. (all from specification)

3. Enhance import process:
   - Apply market standardization during import
   - Report unknown markets
   - Option to add new market mappings

4. Create src/reports/market_report.py with:
   - Function to show all market mappings
   - Function to identify unmapped markets
   - Market distribution statistics

5. Update the spots table population:
   - Set market_code based on market column
   - Handle null/empty markets
   - Log standardization actions

Include configuration option for strict vs. lenient market matching.
```

### Step 11: Historical Data Management

**Goal**: Implement the monthly data finalization process.

```text
Create historical data management system for finalized months.

Requirements:
1. Create src/database/historical_ops.py with:
   - Function to mark month as historical
   - Function to prevent updates to historical data
   - Function to identify current vs. historical months
   - Function to archive historical data

2. Create src/utils/date_utils.py with:
   - Broadcast month calculation
   - Calendar month calculation
   - Month boundary detection
   - Current month identification

3. Update import process with:
   - Check for historical months
   - Skip historical data updates
   - Only update future months
   - Preserve historical records

4. Create src/cli/finalize_command.py with:
   - Command to mark month as finalized
   - Confirmation prompt
   - Ability to list historical months
   - Undo finalization (admin only)

5. Add data integrity checks:
   - Prevent historical data modification
   - Validate month continuity
   - Alert on missing months
   - Backup before finalization

Include comprehensive logging of all finalization actions.
```

### Step 12: Budget and Pipeline Data

**Goal**: Implement budget and pipeline data management.

```text
Create budget and pipeline data management functionality.

Requirements:
1. Create src/importers/budget_importer.py with:
   - Excel/CSV budget file reader
   - Budget validation
   - AE name normalization
   - Annual budget distribution

2. Create src/importers/pipeline_importer.py with:
   - Pipeline data reader
   - Bi-weekly update handling
   - Historical pipeline tracking
   - Delta reporting

3. Create src/database/budget_ops.py with:
   - Budget CRUD operations
   - Budget by AE/month queries
   - Budget vs. actual calculations
   - Annual budget summaries

4. Create src/database/pipeline_ops.py with:
   - Pipeline data updates
   - Version tracking
   - Historical pipeline queries
   - Pipeline trends

5. Create CLI commands:
   - Import budget command
   - Update pipeline command
   - View budget/pipeline status
   - Compare versions

Include validation to ensure AE names match between systems.
```

### Step 13: Update Process Orchestration

**Goal**: Create a complete weekly update workflow.

```text
Create an orchestrated update process for weekly data refreshes.

Requirements:
1. Create src/workflow/update_orchestrator.py with:
   - Complete update workflow class
   - Step-by-step execution
   - Rollback capabilities
   - Progress tracking

2. Implement update steps:
   - Backup current database
   - Validate new data file
   - Check for finalized months
   - Delete future month data
   - Import new data with normalization
   - Update pipeline if provided
   - Generate update report

3. Create src/utils/backup.py with:
   - Database backup function
   - Backup rotation (keep last 5)
   - Backup restoration
   - Backup verification

4. Create src/cli/weekly_update.py with:
   - Single command for weekly update
   - Options for components to update
   - Dry-run mode
   - Automated mode for scheduling

5. Add update reporting:
   - Changes summary
   - New customers added
   - Data quality metrics
   - Error summary
   - Email notification option

Include error recovery and partial update handling.
```

## Phase 4: Basic Reporting (Steps 14-17)

### Step 14: Report Generator Framework

**Goal**: Create the foundation for HTML report generation.

```text
Create a report generation framework with HTML output.

Requirements:
1. Create src/reports/base_report.py with:
   - BaseReport abstract class
   - Common report methods
   - HTML template handling
   - CSS styling integration

2. Create src/reports/html_generator.py with:
   - HTML page structure
   - Table formatting functions
   - Chart placeholder functions
   - Responsive design CSS

3. Create src/reports/templates/ directory with:
   - base_template.html
   - report_styles.css
   - JavaScript for interactivity
   - Print-friendly styling

4. Create src/reports/data_formatter.py with:
   - Currency formatting
   - Date formatting
   - Percentage calculations
   - Number formatting with commas

5. Implement report features:
   - Company branding header
   - Report metadata (date, parameters)
   - Sortable tables
   - Summary statistics
   - Export to PDF button

Use modern, clean design with good typography and spacing.
```

### Step 15: Monthly Revenue Report

**Goal**: Implement the monthly revenue dashboard report.

```text
Create a monthly revenue dashboard report.

Requirements:
1. Create src/reports/monthly_revenue.py with:
   - MonthlyRevenueReport class
   - Query for revenue by customer
   - Query for revenue by AE
   - Query for revenue by market
   - Month-over-month calculations

2. Implement report sections:
   - Executive summary box
   - Top 10 customers table
   - AE performance ranking
   - Market breakdown
   - Revenue type analysis

3. Add data visualizations using HTML/CSS:
   - Revenue trend line (CSS bars)
   - Market pie chart (CSS circles)
   - AE performance bars
   - YoY comparison indicators

4. Create src/database/revenue_queries.py with:
   - Optimized revenue queries
   - Gross vs Net calculations
   - Period comparisons
   - Aggregation functions

5. Add report parameters:
   - Month selection
   - Gross vs Net toggle
   - Market filter
   - AE filter
   - Export options

Include drill-down links to detailed views.
```

### Step 16: AE Performance Report

**Goal**: Create individual Account Executive performance reports.

```text
Create an AE performance tracking report.

Requirements:
1. Create src/reports/ae_performance.py with:
   - AEPerformanceReport class
   - Individual AE metrics
   - Budget vs Actual comparison
   - Pipeline tracking
   - Customer portfolio analysis

2. Implement report components:
   - AE summary card
   - Monthly performance table
   - Budget attainment gauge
   - Customer concentration analysis
   - Revenue by type breakdown

3. Add performance metrics:
   - Current month booking
   - QTD and YTD totals
   - Budget variance
   - Pipeline coverage
   - Growth rate

4. Create comparison features:
   - Vs. previous month
   - Vs. same month last year
   - Vs. team average
   - Ranking among peers

5. Add visual indicators:
   - Green/red for above/below budget
   - Trend arrows
   - Progress bars
   - Achievement badges

Format for easy reading in email clients.
```

### Step 17: Report CLI and Scheduling

**Goal**: Create command-line interface for report generation.

```text
Create CLI for report generation and scheduling infrastructure.

Requirements:
1. Create src/cli/report_command.py with:
   - Report type selection
   - Parameter input
   - Output format options
   - Batch report generation

2. Implement CLI commands:
   - generate-report monthly --month 2025-01
   - generate-report ae --name "John Doe" --quarter Q1
   - generate-report weekly-email --auto
   - list-available-reports

3. Create src/scheduler/report_scheduler.py with:
   - Schedule configuration
   - Report queue management
   - Email delivery settings
   - Error handling

4. Create src/utils/email_sender.py with:
   - HTML email formatting
   - Attachment handling
   - Distribution list management
   - Send confirmation

5. Add configuration file:
   - Report schedules
   - Email recipients
   - Report parameters
   - Output directories

Include dry-run mode and preview options.
```

## Phase 5: Advanced Features (Steps 18-20)

### Step 18: Datasette Integration

**Goal**: Deploy Datasette for ad-hoc SQL queries.

```text
Integrate Datasette for ad-hoc database exploration.

Requirements:
1. Create src/datasette/setup.py with:
   - Datasette configuration
   - Custom metadata
   - Table descriptions
   - Column descriptions

2. Create datasette_config.json with:
   - Database path
   - Authentication settings
   - Plugin configuration
   - Custom CSS

3. Create src/datasette/custom_queries.sql with:
   - Common business queries
   - Revenue summaries
   - Customer analysis
   - Trend queries

4. Add Datasette plugins:
   - datasette-vega for charts
   - datasette-export for Excel export
   - datasette-saved-queries

5. Create deployment script:
   - Database copy for read-only access
   - Datasette launch
   - Port configuration
   - Access documentation

Include user guide for non-technical users.
```

### Step 19: Advanced Analytics

**Goal**: Add year-over-year analysis and sector performance tracking.

```text
Implement advanced analytics and comparison features.

Requirements:
1. Create src/analytics/yoy_analysis.py with:
   - Year-over-year calculations
   - Growth rate analysis
   - Seasonal adjustments
   - Trend identification

2. Create src/analytics/sector_analysis.py with:
   - Revenue by sector/type
   - Market share calculations
   - Sector growth trends
   - Competitive analysis

3. Create src/reports/analytics_dashboard.py with:
   - Comprehensive analytics view
   - Multiple time period comparisons
   - Predictive indicators
   - Exception reporting

4. Add statistical functions:
   - Moving averages
   - Standard deviations
   - Percentile rankings
   - Correlation analysis

5. Create visualization enhancements:
   - Trend charts
   - Heat maps
   - Comparison matrices
   - Performance scorecards

Include interpretation guides for metrics.
```

### Step 20: System Integration and Finalization

**Goal**: Complete system integration with all features working together.

```text
Finalize system integration and create comprehensive documentation.

Requirements:
1. Create src/main.py with:
   - Central command dispatcher
   - Help system
   - Version information
   - Configuration validation

2. Create comprehensive documentation:
   - docs/user_guide.md
   - docs/admin_guide.md
   - docs/troubleshooting.md
   - docs/api_reference.md

3. Add system health checks:
   - Database integrity verification
   - Data quality metrics
   - Performance monitoring
   - Error log analysis

4. Create deployment package:
   - requirements.txt finalization
   - Installation script
   - Sample data for testing
   - Quick start guide

5. Implement final features:
   - User activity logging
   - Audit trail for changes
   - Backup automation
   - Report archive

Include video tutorials for common tasks.
```

## Testing Strategy

Each step should include:
1. Unit tests for new functions
2. Integration tests for workflows
3. Sample data for testing
4. Error scenario validation
5. Performance benchmarks

## Success Criteria

After each step:
- Code runs without errors
- Tests pass
- Feature is usable
- Documentation is complete
- Integration with previous steps works

## Notes for Implementation

1. **Always start with imports and proper structure**
2. **Include error handling from the beginning**
3. **Add logging statements for debugging**
4. **Write docstrings for all functions**
5. **Follow PEP 8 style guidelines**
6. **Test with edge cases**
7. **Keep security in mind (SQL injection prevention)**
8. **Make code maintainable and readable**

This blueprint provides a structured path from empty project to fully functional sales database tool, with each step building upon the previous ones and no orphaned code.