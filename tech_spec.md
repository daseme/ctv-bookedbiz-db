# Sales Database Tool - Technical Specification

## Executive Summary

A read-only sales database tool for a television network to track and report on a 3-part revenue structure: booked revenue, budget balancing mechanism, and pipeline numbers. The system will support multiple markets, revenue types, and provide reporting for sales management, business management, and finance departments.

## Technology Stack

- **Database**: SQLite
- **Programming Language**: Python
- **Development Environment**: Windows with PowerShell, using `uv` package manager
- **Reporting**: Command line → HTML reports, Datasette for ad-hoc queries
- **Email Reports**: HTML formatted emails (future phase)

## Core Requirements

### 1. Data Structure

#### 1.1 Primary Data Sources
- **Booked Business Report**: Excel file (~16,000 rows) containing forward-looking bookings
- **Update Frequency**: Weekly updates replace all future month data
- **Historical Data**: Once a month is finalized/closed, it becomes permanent historical data

#### 1.2 Revenue Components
1. **Booked Revenue**: From Gross Rate column (also track Station Net for certain reports)
2. **Budget Numbers**: Static annual budget by AE and month
3. **Pipeline Numbers**: Management's current expectations, updated bi-weekly

#### 1.3 Key Data Columns

| Column | Type | Business Use | Notes |
|--------|------|--------------|-------|
| Bill Code | Text | Customer identifier | Requires normalization |
| Air Date | Date (m/d/yy) | Spot air date | Key for time-based reporting |
| Gross Rate | Currency | Primary revenue metric | Excludes broker fees |
| Station Net | Currency | Net revenue metric | For specific reports |
| Market | Text | Geographic market | Must map to standard codes |
| Revenue Type | Text | Revenue category | Exclude "Trade" type |
| Sales Person | Text | Account Executive | Links to budget/pipeline |
| Lang. | Code | Language identifier | Important for future reporting |
| Month | Text (mmm-yy) | Reporting period | Broadcast or Calendar |
| Billing Type | Text | "Calendar" or "Broadcast" | Affects month calculation |
| Type | Text | "COM" or "BNS" | Commercial vs Bonus |

*Note: All columns preserved in database even if current use unclear*

### 2. Data Normalization Requirements

#### 2.1 Agency and Customer Parsing
- **Bill Code Format**: Contains either "Agency:Client" or just "Client"
- **Examples**:
  - "IW Group:CMS" → Agency: "IW Group", Customer: "CMS"
  - "CMS" → Agency: None, Customer: "CMS"
- **Agency tracking**: Separate agencies from customers for proper attribution

#### 2.2 Customer Name Normalization
- **Problem**: Inconsistent customer names across years and entries
- **Examples**:
  - "IW Group:CMS" and "IW Group:CMS Production" → "IW Group:CMS"
  - "Hoffman Lewis" (2017) → "H&L Partners" (2025)
  - Remove suffixes like "LLC", "Co.", "Corporation"
  - Strip revenue type suffixes (e.g., "Production")

#### 2.3 Sector Assignment
- **Customer Categorization**: Each customer assigned to a business sector
- **Sectors**: Automotive, CPG, Insurance, Outreach, Technology, Financial Services, Healthcare, Retail, etc.
- **Purpose**: Enable sector performance analysis and reporting

#### 2.4 Normalization System Features
1. Smart suggestion system using similarity matching
2. Present user with choice to map to existing or create new
3. Remember mappings for future updates
4. Assign/confirm sector during normalization
5. Single user, command-line interface

#### 2.5 Market Standardization
```
NEW YORK = NYC
Central Valley = CVC
SAN FRANCISCO = SFO
CHI MSP = CMP
HOUSTON = HOU
LOS ANGELES = LAX
SEATTLE = SEA
DALLAS = DAL
```

#### 2.6 Language Mapping (Future Development)
```
Chinese = M
Filipino = T
Hmong = Hm
South Asian = SA
Vietnamese = V
Mandarin = M
Cantonese = C
Korean = K
Japanese = J
Tagalog = T
Hindi = SA
Punjabi = SA
Bengali = SA
Gujarati = SA
default = E
```

### 3. Database Schema

#### 3.1 Core Tables

**agencies** (Advertising agencies)
- agency_id (primary key)
- agency_name (unique)
- created_date
- updated_date
- notes

**customers** (Normalized customer entities)
- customer_id (primary key)
- normalized_name (unique)
- sector (AUTO, CPG, INS, OUTR, etc.)
- agency_id (foreign key - if customer comes through agency)
- created_date
- updated_date
- customer_type
- notes

**customer_mappings** (Original to normalized name mappings)
- mapping_id (primary key)
- original_name (unique)
- normalized_name (foreign key)
- created_date
- created_by
- confidence_score (similarity score if auto-matched)

**spots** (Historical and current booked data)
- spot_id (primary key)
- All columns from Excel source including:
  - bill_code (Agency:Client or Client format)
  - air_date, end_date
  - time_in, time_out (HH:MM:SS format)
  - length, media, program, lang
  - format, number_field (#), line, type (COM/BNS)
  - estimate, gross_rate, make_good, spot_value
  - month (mmm-yy), broker_fees, priority, station_net
  - sales_person (AE name), revenue_type, billing_type
  - agency_flag, affidavit_flag (Y/N), contract, market
- normalized_customer (links to customers table)
- normalized_agency (extracted from bill_code if present)
- market_code (standardized market)
- is_historical (boolean - true once month is closed)
- load_date (when record was loaded)
- effective_date (for tracking forward-looking snapshots)

**budget** (Annual budget by AE and month)
- budget_id (primary key)
- ae_name
- year
- month
- budget_amount
- created_date
- updated_date
- source

**pipeline** (Management expectations by AE and month)
- pipeline_id (primary key)
- ae_name
- year
- month
- pipeline_amount
- update_date
- is_current (boolean)
- created_date
- created_by
- notes

**markets** (Market code standardization)
- market_id (primary key)
- market_name (as appears in Excel)
- market_code (standardized: NYC, CMP, etc.)
- region
- is_active
- created_date

**sectors** (Business sector categories)
- sector_id (primary key)
- sector_code (AUTO, CPG, INS, OUTR, etc.)
- sector_name (full descriptive name)
- sector_group (higher level grouping if needed)
- is_active
- created_date

**languages** (Language mappings for future use)
- language_id (primary key)
- language_code (E, M, T, Hm, SA, V, etc.)
- language_name (English, Mandarin, etc.)
- language_group (South Asian, etc.)
- created_date

### 4. Business Rules

#### 4.1 Data Processing Rules
- Parse bill_code to separate agency and customer names
- Exclude all records where Revenue Type = "Trade"
- Preserve all data columns even if usage unclear (for future needs)
- Monthly finalized data never changes
- Future months completely replaced on each weekly update
- Pipeline updates roughly bi-weekly
- Budget remains static (annual process)
- Language field is important for future language-based revenue reporting

#### 4.2 Update Process
1. Load new booked business report
2. Parse bill_code into agency and customer components
3. Identify and normalize new agency names
4. Identify and normalize new customer names
5. Assign/confirm customer sectors
6. Delete all non-historical future month data
7. Insert new forward-looking data
8. Update pipeline numbers when provided
9. Mark previous month as historical when closed

### 5. Reporting Requirements

#### 5.1 Standard Reports
1. **Monthly Revenue Dashboard**
   - Revenue by customer, AE, market
   - Gross vs Net views
   - Month-over-month comparisons
   - Agency vs direct client breakdowns

2. **Sector Performance Analysis**
   - Year-over-year by market and sector
   - Revenue type breakdowns
   - Trend analysis with visualizations
   - Sector concentration analysis (pie charts)
   - Historical performance tracking

3. **Management Expectation Tracking**
   - Budget vs Current Expectation vs Assigned Revenue
   - Quarterly and annual views
   - Individual AE tracking with progress indicators
   - Completion percentages
   - Pipeline analysis

4. **Weekly Email Reports**
   - Company performance summary
   - Quarterly performance with pacing
   - Individual AE performance (Charmaine, WorldLink, House)
   - Budget vs actual with variance analysis
   - HTML formatted for email viewing

#### 5.2 Ad-Hoc Reporting
- Command line interface generating HTML reports
- Datasette deployment for SQL query access
- Export capabilities to Excel/CSV
- Language-based revenue analysis (future)
- Agency performance reports

### 6. User Interface Specifications

#### 6.1 Customer Normalization CLI
```
New customer found: "IW Group:CMS Production"
Agency: IW Group
Customer: CMS Production

Suggested agency matches:
1. IW Group (100% match - existing)

Suggested customer matches:
1. CMS (85% similarity)
2. CMS Marketing (42% similarity)
3. [Create new customer]

Select customer option (1-3): 1

Select sector for CMS:
1. AUTO - Automotive
2. CPG - Consumer Packaged Goods
3. INS - Insurance
4. OUTR - Outreach
5. TECH - Technology
[... more sectors ...]

Select sector (1-14): 2

Mapped:
- Agency: "IW Group" → "IW Group"
- Customer: "IW Group:CMS Production" → "CMS" (Sector: CPG)
```

#### 6.2 Report Generation CLI
```
python reports.py --type monthly --month 2025-05 --output revenue_report.html
python reports.py --type ae-performance --ae "Charmaine" --quarter Q2-2025
```

### 7. Technical Considerations

#### 7.1 Performance
- ~16,000 rows per weekly update
- SQLite adequate for data volume
- Index on: air_date, normalized_customer, normalized_agency, ae_name, market_code, sector

#### 7.2 Data Integrity
- Validate date formats on import
- Ensure currency fields handle nulls
- UTF-8 encoding throughout (avoid Unicode issues in PowerShell)
- Transaction-wrapped updates
- Validate bill_code parsing for agency:customer format
- Maintain referential integrity for agency-customer relationships

#### 7.3 Error Handling
- Log all normalization decisions
- Graceful handling of malformed Excel data
- Handle missing agency in bill_code format
- Rollback capability for failed updates
- Validate sector assignments

### 8. Development Phases

#### Phase 1: Core Database and Import
- Database schema creation
- Excel import functionality
- Customer normalization system
- Basic data validation

#### Phase 2: Reporting Foundation
- Command line report generation
- HTML report templates
- Datasette deployment
- Core report types (monthly, quarterly)

#### Phase 3: Advanced Features
- Email report automation
- Pipeline tracking integration
- Language-based reporting
- Performance optimizations

### 9. Testing Requirements

#### 9.1 Data Validation Tests
- Agency:Customer parsing accuracy
- Customer normalization accuracy
- Market code mapping
- Revenue calculations
- Month boundary handling (broadcast vs calendar)
- Sector assignment validation
- Language code validation

#### 9.2 Report Accuracy Tests
- Budget vs actual calculations
- Quarter/annual rollups
- YoY comparison logic
- Pipeline tracking
- Sector performance calculations
- Agency vs direct client attribution

### 10. Maintenance Considerations

- Weekly update process documentation
- Customer mapping audit trail
- Month-end closing procedures
- Report template modifications
- Database backup strategy

## Appendix A: Sample Code Structure

```
sales_database/
├── src/
│   ├── database/
│   │   ├── models.py
│   │   ├── schema.py
│   │   └── connection.py
│   ├── importers/
│   │   ├── excel_reader.py
│   │   └── data_validator.py
│   ├── normalization/
│   │   ├── customer_normalizer.py
│   │   └── similarity.py
│   ├── reports/
│   │   ├── generator.py
│   │   ├── templates/
│   │   └── formatters.py
│   └── cli/
│       ├── update_command.py
│       └── report_command.py
├── tests/
├── data/
│   ├── mappings/
│   └── archives/
├── requirements.txt
└── README.md
```

## Summary of Key Updates

This specification has been updated to include:

1. **Agency Support**: Proper parsing and tracking of agencies separate from customers
2. **Sector Categorization**: Each customer assigned to a business sector (AUTO, CPG, INS, OUTR, etc.)
3. **Enhanced Schema**: Added agencies, sectors, and languages tables with proper relationships
4. **Bill Code Parsing**: Handles "Agency:Client" format with normalization for both components
5. **Language Tracking**: Preserves language codes for future revenue-by-language reporting
6. **Complete Column Preservation**: All Excel columns retained, even those with unclear current usage

The system now fully supports the complex reporting requirements shown in the examples, including sector performance analysis, agency attribution, and management expectation tracking across multiple dimensions.