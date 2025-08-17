-- Enhanced database schema for Broadcast Month Import System
-- Includes month closure protection, import batch tracking, and audit trail capabilities

-- 1. AGENCIES TABLE
CREATE TABLE IF NOT EXISTS agencies (
    agency_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agency_name TEXT NOT NULL UNIQUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    notes TEXT
);

-- 2. SECTORS TABLE (Reference data)
CREATE TABLE IF NOT EXISTS sectors (
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_code TEXT NOT NULL UNIQUE,  -- AUTO, CPG, INS, OUTR, etc.
    sector_name TEXT NOT NULL,         -- Full descriptive name
    sector_group TEXT,                 -- Higher level grouping
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. MARKETS TABLE (Reference data)
CREATE TABLE IF NOT EXISTS markets (
    market_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_name TEXT NOT NULL UNIQUE,  -- As appears in Excel
    market_code TEXT NOT NULL UNIQUE,  -- Standardized: NYC, CMP, etc.
    region TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. LANGUAGES TABLE (Reference data)
CREATE TABLE IF NOT EXISTS languages (
    language_id INTEGER PRIMARY KEY AUTOINCREMENT,
    language_code TEXT NOT NULL UNIQUE,  -- E, M, T, Hm, SA, V, etc.
    language_name TEXT NOT NULL,         -- English, Mandarin, etc.
    language_group TEXT,                 -- Chinese, South Asian, etc.
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. CUSTOMERS TABLE (With proper foreign key relationships)
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_name TEXT NOT NULL UNIQUE,
    sector_id INTEGER,
    agency_id INTEGER,  -- If customer comes through agency
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    customer_type TEXT,
    is_active BOOLEAN DEFAULT 1,
    notes TEXT,
    
    -- Proper foreign key constraints
    FOREIGN KEY (sector_id) REFERENCES sectors(sector_id) ON DELETE RESTRICT,
    FOREIGN KEY (agency_id) REFERENCES agencies(agency_id) ON DELETE RESTRICT
);

-- 6. CUSTOMER_MAPPINGS TABLE (For name normalization tracking)
CREATE TABLE IF NOT EXISTS customer_mappings (
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_name TEXT NOT NULL UNIQUE,
    customer_id INTEGER NOT NULL,  -- Use ID instead of name
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'system',
    confidence_score REAL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    
    -- Proper foreign key constraint
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);

-- 7. MONTH CLOSURES TABLE (NEW: For broadcast month protection)
CREATE TABLE IF NOT EXISTS month_closures (
    broadcast_month TEXT PRIMARY KEY,  -- Format: 'Nov-24', 'Dec-24', etc.
    closed_date DATE NOT NULL,
    closed_by TEXT NOT NULL,
    notes TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. IMPORT BATCHES TABLE (NEW: For complete audit trail)
CREATE TABLE IF NOT EXISTS import_batches (
    batch_id TEXT PRIMARY KEY,
    import_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    import_mode TEXT NOT NULL CHECK (import_mode IN ('HISTORICAL', 'WEEKLY_UPDATE', 'MANUAL')),
    source_file TEXT NOT NULL,
    broadcast_months_affected TEXT,  -- JSON array of affected months
    records_imported INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    started_by TEXT,
    completed_at TIMESTAMP,
    notes TEXT,
    error_summary TEXT  -- JSON of any errors encountered
);

-- 9. SPOTS TABLE (Core transactional data with batch tracking)
CREATE TABLE IF NOT EXISTS spots (
    spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Excel source fields (preserved but cleaned up)
    bill_code TEXT NOT NULL,  -- Original customer field from Excel
    air_date DATE NOT NULL,
    end_date DATE,
    day_of_week TEXT,
    time_in TEXT,  -- HH:MM:SS format
    time_out TEXT, -- HH:MM:SS format
    
    -- Spot details
    length_seconds TEXT,
    media TEXT,
    program TEXT,
    language_code TEXT,  -- More descriptive than 'lang'
    format TEXT,
    sequence_number INTEGER,  -- More descriptive than 'number_field'
    line_number INTEGER,
    spot_type TEXT CHECK (spot_type IN ('AV', 'BB', 'BNS', 'COM', 'CRD', 'PKG', 'PRD', 'PRG', 'SVC', '')),
    estimate TEXT,
    
    -- Financial fields (allows negative values for business accuracy)
    gross_rate DECIMAL(12, 2),
    make_good TEXT,
    spot_value DECIMAL(12, 2),
    broadcast_month TEXT,  -- mmm-yy format, more descriptive than 'month'
    broker_fees DECIMAL(12, 2),
    priority INTEGER,
    station_net DECIMAL(12, 2),
    
    -- Business fields
    sales_person TEXT,  -- AE name
    revenue_type TEXT,
    billing_type TEXT CHECK (billing_type IN ('Calendar', 'Broadcast', '')),
    agency_flag TEXT,
    affidavit_flag TEXT CHECK (affidavit_flag IN ('Y', 'N', '')),
    contract TEXT,
    market_name TEXT,  -- Original market name from Excel
    
    -- Normalized relationships (using proper foreign keys)
    customer_id INTEGER,
    agency_id INTEGER,
    market_id INTEGER,
    language_id INTEGER,
    
    -- Metadata
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file TEXT,
    is_historical BOOLEAN DEFAULT 0,
    effective_date DATE,  -- When this forward-looking data was loaded
    
    -- NEW: Import batch tracking for complete audit trail
    import_batch_id TEXT,
    
    -- Business rule constraints
    CHECK (revenue_type != 'Trade' OR revenue_type IS NULL),  -- Exclude Trade per business rules
    
    -- Foreign key constraints with proper cascading
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
    FOREIGN KEY (agency_id) REFERENCES agencies(agency_id) ON DELETE RESTRICT,
    FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE RESTRICT,
    FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE RESTRICT,
    FOREIGN KEY (import_batch_id) REFERENCES import_batches(batch_id) ON DELETE SET NULL
);

-- 10. BUDGET TABLE (With enhanced constraints)
CREATE TABLE IF NOT EXISTS budget (
    budget_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 2000 AND year <= 2100),
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    budget_amount DECIMAL(12, 2) NOT NULL CHECK (budget_amount >= 0),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT,
    
    -- Ensure unique budget per AE/year/month
    UNIQUE(ae_name, year, month)
);

-- 11. PIPELINE TABLE (With enhanced constraints)
CREATE TABLE IF NOT EXISTS pipeline (
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL CHECK (year >= 2000 AND year <= 2100),
    month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
    pipeline_amount DECIMAL(12, 2) NOT NULL CHECK (pipeline_amount >= 0),
    update_date DATE NOT NULL,
    is_current BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    notes TEXT
);

-- ===================================================================
-- INDEXES - Optimized for broadcast month operations and performance
-- ===================================================================

-- Existing spots table indexes
CREATE INDEX IF NOT EXISTS idx_spots_air_date ON spots(air_date);
CREATE INDEX IF NOT EXISTS idx_spots_customer_id ON spots(customer_id);
CREATE INDEX IF NOT EXISTS idx_spots_sales_person ON spots(sales_person);
CREATE INDEX IF NOT EXISTS idx_spots_historical ON spots(is_historical);

-- NEW: Critical index for broadcast month operations
CREATE INDEX IF NOT EXISTS idx_spots_broadcast_month_historical ON spots(broadcast_month, is_historical);

-- NEW: Index for batch tracking and audit
CREATE INDEX IF NOT EXISTS idx_spots_import_batch ON spots(import_batch_id);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_spots_performance_report ON spots(sales_person, air_date, market_id) 
WHERE is_historical = 0 AND revenue_type != 'Trade';

CREATE INDEX IF NOT EXISTS idx_spots_customer_timeline ON spots(customer_id, air_date, revenue_type);

CREATE INDEX IF NOT EXISTS idx_spots_monthly_rollup ON spots(broadcast_month, market_id, revenue_type) 
WHERE revenue_type != 'Trade';

CREATE INDEX IF NOT EXISTS idx_spots_agency_performance ON spots(agency_id, air_date, gross_rate)
WHERE agency_id IS NOT NULL;

-- Reference table indexes
CREATE INDEX IF NOT EXISTS idx_customers_sector ON customers(sector_id);
CREATE INDEX IF NOT EXISTS idx_customers_agency ON customers(agency_id);
CREATE INDEX IF NOT EXISTS idx_customer_mappings_original ON customer_mappings(original_name);

-- Budget and pipeline indexes
CREATE INDEX IF NOT EXISTS idx_budget_ae_year_month ON budget(ae_name, year, month);
CREATE INDEX IF NOT EXISTS idx_pipeline_ae_year_month ON pipeline(ae_name, year, month);
CREATE INDEX IF NOT EXISTS idx_pipeline_current ON pipeline(is_current) WHERE is_current = 1;

-- NEW: Import batch indexes for audit and monitoring
CREATE INDEX IF NOT EXISTS idx_import_batches_date ON import_batches(import_date);
CREATE INDEX IF NOT EXISTS idx_import_batches_mode ON import_batches(import_mode);
CREATE INDEX IF NOT EXISTS idx_import_batches_status ON import_batches(status);

-- ===================================================================
-- REFERENCE DATA INSERTS - Standard mappings for consistent data
-- ===================================================================

-- Insert standard market mappings
INSERT OR IGNORE INTO markets (market_name, market_code, region) VALUES
('NEW YORK', 'NYC', 'Northeast'),
('New York', 'NYC', 'Northeast'),
('Central Valley', 'CVC', 'West'),
('SAN FRANCISCO', 'SFO', 'West'),
('San Francisco', 'SFO', 'West'),
('CHI MSP', 'CMP', 'Midwest'),
('HOUSTON', 'HOU', 'South'),
('Houston', 'HOU', 'South'),
('LOS ANGELES', 'LAX', 'West'),
('Los Angeles', 'LAX', 'West'),
('SEATTLE', 'SEA', 'West'),
('Seattle', 'SEA', 'West'),
('DALLAS', 'DAL', 'South'),
('Dallas', 'DAL', 'South');

-- Insert standard language mappings
INSERT OR IGNORE INTO languages (language_code, language_name, language_group) VALUES
('E', 'English', 'English'),
('M', 'Mandarin', 'Chinese'),
('C', 'Cantonese', 'Chinese'),
('T', 'Tagalog', 'Filipino'),
('Hm', 'Hmong', 'Hmong'),
('SA', 'South Asian', 'South Asian'),
('V', 'Vietnamese', 'Vietnamese'),
('K', 'Korean', 'Korean'),
('J', 'Japanese', 'Japanese');

-- Insert standard sector mappings
INSERT OR IGNORE INTO sectors (sector_code, sector_name, sector_group) VALUES
('AUTO', 'Automotive', 'Commercial'),
('CPG', 'Consumer Packaged Goods', 'Commercial'),
('INS', 'Insurance', 'Financial'),
('OUTR', 'Outreach', 'Community'),
('TECH', 'Technology', 'Commercial'),
('FIN', 'Financial Services', 'Financial'),
('HEALTH', 'Healthcare', 'Commercial'),
('RETAIL', 'Retail', 'Commercial'),
('TELCO', 'Telecommunications', 'Commercial'),
('MEDIA', 'Media & Entertainment', 'Commercial'),
('GOV', 'Government', 'Public'),
('EDU', 'Education', 'Public'),
('NPO', 'Non-Profit', 'Community'),
('OTHER', 'Other', 'Other');

-- ===================================================================
-- ENHANCED REPORTING VIEW - Includes batch and closure information
-- ===================================================================

-- Main reporting view with all joins resolved including new audit fields
CREATE VIEW IF NOT EXISTS spots_reporting AS
SELECT 
    s.spot_id,
    s.bill_code,
    s.air_date,
    s.gross_rate,
    s.station_net,
    s.sales_person,
    s.revenue_type,
    s.broadcast_month,
    s.is_historical,
    s.import_batch_id,
    
    -- Customer information
    c.normalized_name as customer_name,
    sect.sector_code,
    sect.sector_name,
    
    -- Agency information
    a.agency_name,
    
    -- Market information
    m.market_code,
    m.market_name as market_display_name,
    m.region,
    
    -- Language information
    l.language_code,
    l.language_name,
    
    -- NEW: Import batch information for audit trail
    ib.import_mode,
    ib.import_date,
    ib.started_by,
    
    -- NEW: Month closure information for protection status
    mc.closed_date,
    mc.closed_by
    
FROM spots s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN markets m ON s.market_id = m.market_id
LEFT JOIN languages l ON s.language_id = l.language_id
LEFT JOIN import_batches ib ON s.import_batch_id = ib.batch_id
LEFT JOIN month_closures mc ON s.broadcast_month = mc.broadcast_month
WHERE s.revenue_type != 'Trade' OR s.revenue_type IS NULL;