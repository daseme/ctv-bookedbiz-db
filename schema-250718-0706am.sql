-- SQLite Database Schema Export
-- Source Database: ./data/database/production.db
-- Generated on: 2025-07-18 07:06:03
-- 
-- SQLite Version: 3.40.1
-- 

-- DIAGNOSTIC: Column type verification
-- Use these commands to verify column types:
-- PRAGMA table_info(spots);
-- SELECT typeof(broadcast_month) FROM spots LIMIT 5;
-- 

PRAGMA foreign_keys = ON;

-- Tables (18)
-- ============================================================

-- Table: agencies
-- ----------------------------------------
CREATE TABLE agencies (
        agency_id INTEGER PRIMARY KEY AUTOINCREMENT,
        agency_name TEXT NOT NULL UNIQUE,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT 1,
        notes TEXT
    );

-- Table: budget
-- ----------------------------------------
CREATE TABLE budget (
        budget_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ae_name TEXT NOT NULL,
        year INTEGER NOT NULL CHECK (year >= 2000 AND year <= 2100),
        month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
        budget_amount DECIMAL(12, 2) NOT NULL CHECK (budget_amount >= 0),
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source TEXT,
        
        UNIQUE(ae_name, year, month)
    );

-- Indexes for table: budget
CREATE INDEX idx_budget_ae_year_month ON budget(ae_name, year, month);

-- Table: budget_data
-- ----------------------------------------
CREATE TABLE budget_data (
            budget_data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_id INTEGER NOT NULL,
            ae_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            budget_amount DECIMAL(12, 2) NOT NULL,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (version_id) REFERENCES budget_versions(version_id),
            UNIQUE(version_id, ae_name, year, month)
        );

-- Indexes for table: budget_data
CREATE INDEX idx_budget_data_ae_year ON budget_data(ae_name, year);
CREATE INDEX idx_budget_data_quarter ON budget_data(year, quarter);

-- Table: budget_versions
-- ----------------------------------------
CREATE TABLE budget_versions (
            version_id INTEGER PRIMARY KEY AUTOINCREMENT,
            version_name TEXT NOT NULL,
            year INTEGER NOT NULL,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            description TEXT,
            is_active BOOLEAN DEFAULT 1,
            source_file TEXT,
            UNIQUE(version_name, year)
        );

-- Indexes for table: budget_versions
CREATE INDEX idx_budget_versions_active ON budget_versions(year, is_active);

-- Table: customer_mappings
-- ----------------------------------------
CREATE TABLE customer_mappings (
        mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_name TEXT NOT NULL UNIQUE,
        customer_id INTEGER NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT 'system',
        confidence_score REAL CHECK (confidence_score >= 0 AND confidence_score <= 1),
        
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
    );

-- Indexes for table: customer_mappings
CREATE INDEX idx_customer_mappings_original ON customer_mappings(original_name);

-- Table: customers
-- ----------------------------------------
CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
        normalized_name TEXT NOT NULL UNIQUE,
        sector_id INTEGER,
        agency_id INTEGER,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        customer_type TEXT,
        is_active BOOLEAN DEFAULT 1,
        notes TEXT,
        
        FOREIGN KEY (sector_id) REFERENCES sectors(sector_id) ON DELETE RESTRICT,
        FOREIGN KEY (agency_id) REFERENCES agencies(agency_id) ON DELETE RESTRICT
    );

-- Indexes for table: customers
CREATE INDEX idx_customers_agency ON customers(agency_id);
CREATE INDEX idx_customers_sector ON customers(sector_id);

-- Table: import_batches
-- ----------------------------------------
CREATE TABLE import_batches (
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

-- Indexes for table: import_batches
CREATE INDEX idx_import_batches_date ON import_batches(import_date);
CREATE INDEX idx_import_batches_mode ON import_batches(import_mode);
CREATE INDEX idx_import_batches_status ON import_batches(status);

-- Table: language_blocks
-- ----------------------------------------
CREATE TABLE language_blocks (
    block_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,             -- 'monday', 'tuesday', etc.
    time_start TIME NOT NULL,              -- '06:00:00'
    time_end TIME NOT NULL,                -- '07:00:00'
    language_id INTEGER NOT NULL,          -- FK to languages table
    block_name TEXT NOT NULL,              -- e.g., "Mandarin Prime", "Phoenix Evening Express"
    block_type TEXT NOT NULL,              -- e.g., "News", "Children", "Prime", "Drama", "Variety"
    day_part TEXT,                         -- e.g., "Morning", "Afternoon", "Prime", "Late Night"
    display_order INTEGER DEFAULT 0,       -- For UI ordering within day
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,           -- Allow disabling blocks without deletion
    
    -- Constraints
    FOREIGN KEY (schedule_id) REFERENCES programming_schedules(schedule_id) ON DELETE CASCADE,
    FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE RESTRICT,
    
    -- Ensure no overlapping time blocks for same day/schedule
    UNIQUE(schedule_id, day_of_week, time_start, time_end),
    
    -- Ensure logical time ordering (handle midnight rollover)
    CHECK (time_start < time_end OR (time_start > time_end AND time_end = '23:59:59')),
    
    -- Validate day part values
    CHECK (day_part IN ('Early Morning', 'Morning', 'Midday', 'Afternoon', 'Early Evening', 'Prime', 'Late Night', 'Overnight'))
);

-- Indexes for table: language_blocks
CREATE INDEX idx_language_blocks_day_part 
ON language_blocks(day_part, language_id);
CREATE INDEX idx_language_blocks_language 
ON language_blocks(language_id);
CREATE INDEX idx_language_blocks_schedule_day 
ON language_blocks(schedule_id, day_of_week, is_active);
CREATE INDEX idx_language_blocks_time_lookup 
ON language_blocks(day_of_week, time_start, time_end) WHERE is_active = 1;

-- Table: languages
-- ----------------------------------------
CREATE TABLE languages (
        language_id INTEGER PRIMARY KEY AUTOINCREMENT,
        language_code TEXT NOT NULL UNIQUE,
        language_name TEXT NOT NULL,
        language_group TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Table: markets
-- ----------------------------------------
CREATE TABLE markets (
        market_id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_name TEXT NOT NULL UNIQUE,
        market_code TEXT NOT NULL UNIQUE,
        region TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Table: month_closures
-- ----------------------------------------
CREATE TABLE month_closures (
        broadcast_month TEXT PRIMARY KEY,  -- Format: 'Nov-24', 'Dec-24', etc.
        closed_date DATE NOT NULL,
        closed_by TEXT NOT NULL,
        notes TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Table: pipeline
-- ----------------------------------------
CREATE TABLE pipeline (
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

-- Indexes for table: pipeline
CREATE INDEX idx_pipeline_ae_year_month ON pipeline(ae_name, year, month);
CREATE INDEX idx_pipeline_current ON pipeline(is_current) WHERE is_current = 1;

-- Table: programming_schedules
-- ----------------------------------------
CREATE TABLE programming_schedules (
    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_name TEXT NOT NULL,           -- e.g., "Standard Grid", "Dallas Grid", "Holiday Grid"
    schedule_version TEXT NOT NULL,        -- e.g., "2025-v1.0", "2025-v2.1"
    schedule_type TEXT NOT NULL,           -- e.g., "standard", "market_specific", "seasonal"
    effective_start_date DATE NOT NULL,    -- When this schedule becomes active
    effective_end_date DATE,               -- When this schedule expires (NULL = current)
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    notes TEXT,
    
    -- Collision prevention constraints
    UNIQUE(schedule_name, schedule_type, effective_start_date),
    
    -- Ensure end date is after start date
    CHECK (effective_end_date IS NULL OR effective_end_date > effective_start_date)
);

-- Indexes for table: programming_schedules
CREATE INDEX idx_programming_schedules_active 
ON programming_schedules(is_active, effective_start_date, effective_end_date);
CREATE INDEX idx_programming_schedules_dates
ON programming_schedules(effective_start_date, effective_end_date);
CREATE INDEX idx_programming_schedules_type 
ON programming_schedules(schedule_type, is_active);

-- Table: schedule_collision_log
-- ----------------------------------------
CREATE TABLE schedule_collision_log (
    collision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    collision_type TEXT NOT NULL,          -- 'market_overlap', 'schedule_gap', 'date_conflict'
    severity TEXT NOT NULL,                -- 'warning', 'error', 'info'
    market_id INTEGER,
    schedule_id_1 INTEGER,
    schedule_id_2 INTEGER,
    conflict_start_date DATE,
    conflict_end_date DATE,
    description TEXT NOT NULL,
    resolution_status TEXT DEFAULT 'unresolved', -- 'unresolved', 'resolved', 'ignored'
    detected_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_date TIMESTAMP,
    resolved_by TEXT,
    resolution_notes TEXT,
    
    FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
    FOREIGN KEY (schedule_id_1) REFERENCES programming_schedules(schedule_id) ON DELETE CASCADE,
    FOREIGN KEY (schedule_id_2) REFERENCES programming_schedules(schedule_id) ON DELETE CASCADE,
    
    CHECK (collision_type IN ('market_overlap', 'schedule_gap', 'date_conflict')),
    CHECK (severity IN ('warning', 'error', 'info')),
    CHECK (resolution_status IN ('unresolved', 'resolved', 'ignored'))
);

-- Indexes for table: schedule_collision_log
CREATE INDEX idx_collision_log_market
ON schedule_collision_log(market_id, collision_type);
CREATE INDEX idx_collision_log_unresolved
ON schedule_collision_log(resolution_status, detected_date) WHERE resolution_status = 'unresolved';

-- Table: schedule_market_assignments
-- ----------------------------------------
CREATE TABLE schedule_market_assignments (
    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    effective_start_date DATE NOT NULL,    -- When this market assignment starts
    effective_end_date DATE,               -- When this assignment ends (NULL = current)
    assignment_priority INTEGER DEFAULT 1, -- Higher priority wins if overlapping assignments
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'system',
    notes TEXT,
    
    -- Constraints
    FOREIGN KEY (schedule_id) REFERENCES programming_schedules(schedule_id) ON DELETE CASCADE,
    FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE CASCADE,
    
    -- Prevent exact duplicate assignments
    UNIQUE(market_id, schedule_id, effective_start_date),
    
    -- Ensure end date is after start date
    CHECK (effective_end_date IS NULL OR effective_end_date > effective_start_date)
);

-- Indexes for table: schedule_market_assignments
CREATE INDEX idx_schedule_markets_market_date 
ON schedule_market_assignments(market_id, effective_start_date, effective_end_date);
CREATE INDEX idx_schedule_markets_priority 
ON schedule_market_assignments(market_id, assignment_priority, effective_start_date);
CREATE INDEX idx_schedule_markets_schedule 
ON schedule_market_assignments(schedule_id, effective_start_date);

-- Table: sectors
-- ----------------------------------------
CREATE TABLE sectors (
        sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sector_code TEXT NOT NULL UNIQUE,
        sector_name TEXT NOT NULL,
        sector_group TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Table: spot_language_blocks
-- ----------------------------------------
CREATE TABLE spot_language_blocks (
    assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    spot_id INTEGER NOT NULL,
    schedule_id INTEGER NOT NULL,          -- Which grid version was used for assignment
    block_id INTEGER,                      -- NULL if spans multiple blocks or no assignment
    
    -- Customer Intent Analysis
    customer_intent TEXT NOT NULL,         -- 'language_specific', 'time_specific', 'indifferent', 'no_grid_coverage'
    intent_confidence REAL DEFAULT 1.0,   -- 0.0-1.0 confidence in intent classification
    
    -- Multi-block handling
    spans_multiple_blocks BOOLEAN DEFAULT 0,
    blocks_spanned TEXT,                   -- JSON array of block_ids if spans multiple
    primary_block_id INTEGER,              -- Most relevant block if spanning multiple
    
    -- Assignment metadata
    assignment_method TEXT NOT NULL,       -- 'auto_computed', 'manual_override', 'no_grid_available'
    assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    assigned_by TEXT DEFAULT 'system',
    notes TEXT,
    
    -- NEW: Alert flags for reporting
    requires_attention BOOLEAN DEFAULT 0,  -- Flag for spots needing manual review
    alert_reason TEXT, business_rule_applied TEXT DEFAULT NULL, auto_resolved_date TIMESTAMP DEFAULT NULL, campaign_type TEXT DEFAULT 'language_specific',                     -- Why this spot needs attention
    
    -- Constraints
    FOREIGN KEY (spot_id) REFERENCES spots(spot_id) ON DELETE CASCADE,
    FOREIGN KEY (schedule_id) REFERENCES programming_schedules(schedule_id) ON DELETE RESTRICT,
    FOREIGN KEY (block_id) REFERENCES language_blocks(block_id) ON DELETE SET NULL,
    FOREIGN KEY (primary_block_id) REFERENCES language_blocks(block_id) ON DELETE SET NULL,
    
    -- Prevent duplicate spot assignments
    UNIQUE(spot_id),
    
    -- Validate customer intent values
    CHECK (customer_intent IN ('language_specific', 'time_specific', 'indifferent', 'no_grid_coverage')),
    
    -- Validate assignment method
    CHECK (assignment_method IN ('auto_computed', 'manual_override', 'no_grid_available')),
    
    -- Business rule: if spans_multiple_blocks, then block_id should be NULL
    CHECK (
        (spans_multiple_blocks = 0 AND block_id IS NOT NULL) OR
        (spans_multiple_blocks = 1 AND block_id IS NULL AND blocks_spanned IS NOT NULL) OR
        (customer_intent = 'no_grid_coverage' AND block_id IS NULL)
    )
);

-- Indexes for table: spot_language_blocks
CREATE INDEX idx_spot_blocks_attention
ON spot_language_blocks(requires_attention) WHERE requires_attention = 1;
CREATE INDEX idx_spot_blocks_auto_resolved 
ON spot_language_blocks(auto_resolved_date) 
WHERE auto_resolved_date IS NOT NULL;
CREATE INDEX idx_spot_blocks_business_rule 
ON spot_language_blocks(business_rule_applied) 
WHERE business_rule_applied IS NOT NULL;
CREATE INDEX idx_spot_blocks_campaign_type ON spot_language_blocks(campaign_type);
CREATE INDEX idx_spot_blocks_intent 
ON spot_language_blocks(customer_intent);
CREATE INDEX idx_spot_blocks_schedule 
ON spot_language_blocks(schedule_id);
CREATE INDEX idx_spot_blocks_spot 
ON spot_language_blocks(spot_id);

-- Table: spots
-- ----------------------------------------
CREATE TABLE spots (
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
        
        -- Financial fields (REMOVED negative value constraints)
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
        
        -- Metadata (existing)
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT,
        is_historical BOOLEAN DEFAULT 0,
        effective_date DATE,  -- When this forward-looking data was loaded
        
        -- NEW: Import batch tracking
        import_batch_id TEXT,
        
        -- Business rule constraints (excludes Trade revenue)
        CHECK (revenue_type != 'Trade' OR revenue_type IS NULL),
        
        -- Foreign key constraints with proper cascading
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
        FOREIGN KEY (agency_id) REFERENCES agencies(agency_id) ON DELETE RESTRICT,
        FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE RESTRICT,
        FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE RESTRICT,
        FOREIGN KEY (import_batch_id) REFERENCES import_batches(batch_id) ON DELETE SET NULL
    );

-- Indexes for table: spots
CREATE INDEX idx_spots_ae_broadcast_covering ON spots(sales_person, broadcast_month, customer_id, revenue_type, gross_rate, station_net) WHERE gross_rate > 0 AND (revenue_type != 'Trade' OR revenue_type IS NULL);
CREATE INDEX idx_spots_ae_broadcast_month_revenue ON spots(sales_person, broadcast_month, gross_rate, station_net) WHERE gross_rate > 0 AND (revenue_type != 'Trade' OR revenue_type IS NULL);
CREATE INDEX idx_spots_agency_performance ON spots(agency_id, air_date, gross_rate)
    WHERE agency_id IS NOT NULL
    ;
CREATE INDEX idx_spots_air_date ON spots(air_date);
CREATE INDEX idx_spots_broadcast_month_historical ON spots(broadcast_month, is_historical);
CREATE INDEX idx_spots_customer_broadcast ON spots(customer_id, broadcast_month, gross_rate, station_net) WHERE gross_rate > 0;
CREATE INDEX idx_spots_customer_id ON spots(customer_id);
CREATE INDEX idx_spots_customer_timeline ON spots(customer_id, air_date, revenue_type);
CREATE INDEX idx_spots_historical ON spots(is_historical);
CREATE INDEX idx_spots_import_batch ON spots(import_batch_id);
CREATE INDEX idx_spots_monthly_rollup ON spots(broadcast_month, market_id, revenue_type) 
    WHERE revenue_type != 'Trade'
    ;
CREATE INDEX idx_spots_performance_report ON spots(sales_person, air_date, market_id) 
    WHERE is_historical = 0 AND revenue_type != 'Trade'
    ;
CREATE INDEX idx_spots_sales_person ON spots(sales_person);
CREATE INDEX idx_spots_time_market_day 
ON spots(market_id, day_of_week, time_in, time_out) 
WHERE day_of_week IS NOT NULL AND time_in IS NOT NULL;

-- Views (10)
-- ============================================================

CREATE VIEW business_rule_analytics AS
SELECT 
    COALESCE(business_rule_applied, 'no_rule') as rule_applied,
    COUNT(*) as spots_affected,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_blocks) as percentage,
    AVG(intent_confidence) as avg_confidence,
    COUNT(CASE WHEN requires_attention = 1 THEN 1 END) as flagged_count,
    COUNT(CASE WHEN requires_attention = 0 THEN 1 END) as auto_resolved_count,
    MIN(assigned_date) as earliest_assignment,
    MAX(assigned_date) as latest_assignment
FROM spot_language_blocks 
GROUP BY COALESCE(business_rule_applied, 'no_rule')
ORDER BY spots_affected DESC;

CREATE VIEW business_rule_summary AS
SELECT 
    'Total spots' as metric,
    COUNT(*) as value,
    '' as notes
FROM spot_language_blocks
UNION ALL
SELECT 
    'Business rule applied' as metric,
    COUNT(*) as value,
    'Auto-resolved by business rules' as notes
FROM spot_language_blocks
WHERE business_rule_applied IS NOT NULL
UNION ALL
SELECT 
    'Manual assignments' as metric,
    COUNT(*) as value,
    'Standard assignment process' as notes
FROM spot_language_blocks
WHERE business_rule_applied IS NULL;

CREATE VIEW enhanced_rule_analytics AS
                SELECT 
                    COALESCE(business_rule_applied, 'standard_assignment') as rule_type,
                    COUNT(*) as spots_affected,
                    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM spot_language_blocks) as percentage,
                    AVG(intent_confidence) as avg_confidence,
                    COUNT(CASE WHEN requires_attention = 1 THEN 1 END) as flagged_count,
                    COUNT(CASE WHEN requires_attention = 0 THEN 1 END) as auto_resolved_count,
                    MIN(assigned_date) as earliest_assignment,
                    MAX(assigned_date) as latest_assignment,
                    MIN(auto_resolved_date) as earliest_auto_resolved,
                    MAX(auto_resolved_date) as latest_auto_resolved
                FROM spot_language_blocks 
                GROUP BY COALESCE(business_rule_applied, 'standard_assignment')
                ORDER BY spots_affected DESC;

CREATE VIEW language_block_dashboard AS
SELECT 
    lbrs.block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.schedule_name,
    lbrs.market_code,
    lbrs.market_display_name,
    lbrs.block_type,
    lbrs.day_part,
    lbrs.time_start,
    lbrs.time_end,
    -- Current year metrics
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) as current_year_revenue,
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_spots ELSE 0 END) as current_year_spots,
    SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.unique_customers ELSE 0 END) as current_year_customers,
    
    -- Previous year metrics
    SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END) as previous_year_revenue,
    SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_spots ELSE 0 END) as previous_year_spots,
    
    -- Calculate year-over-year growth
    CASE 
        WHEN SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END) > 0 THEN
            ROUND(
                ((SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) - 
                  SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END)) * 100.0 / 
                  SUM(CASE WHEN lbrs.year = CAST(strftime('%Y', 'now') - 1 AS TEXT) THEN lbrs.total_revenue ELSE 0 END)), 2
            )
        ELSE NULL
    END as yoy_revenue_growth_pct,
    
    -- Performance rating
    CASE 
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 100000 THEN 'Excellent'
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 50000 THEN 'Good'
        WHEN SUM(CASE WHEN lbrs.year = strftime('%Y', 'now') THEN lbrs.total_revenue ELSE 0 END) >= 10000 THEN 'Fair'
        ELSE 'Needs Attention'
    END as performance_rating,
    
    -- Last activity date
    MAX(lbrs.last_air_date) as last_activity_date
FROM language_block_revenue_summary lbrs
GROUP BY 
    lbrs.block_id,
    lbrs.block_name,
    lbrs.language_name,
    lbrs.schedule_name,
    lbrs.market_code,
    lbrs.market_display_name,
    lbrs.block_type,
    lbrs.day_part,
    lbrs.time_start,
    lbrs.time_end
ORDER BY current_year_revenue DESC;

CREATE VIEW language_block_revenue_analysis AS
SELECT 
    ps.schedule_name,
    ps.schedule_type,
    m.market_code,
    lb.day_of_week,
    lb.day_part,
    lb.block_name,
    lb.block_type,
    lb.time_start,
    lb.time_end,
    bl.language_code,
    bl.language_name,
    
    -- Revenue metrics
    COUNT(DISTINCT s.spot_id) as total_spots,
    ROUND(SUM(s.gross_rate), 2) as total_revenue,
    ROUND(AVG(s.gross_rate), 2) as average_spot_rate,
    ROUND(SUM(s.station_net), 2) as total_net_revenue,
    ROUND(AVG(s.station_net), 2) as average_net_rate,
    
    -- Customer intent breakdown
    COUNT(DISTINCT CASE WHEN slb.customer_intent = 'language_specific' THEN s.spot_id END) as language_targeted_spots,
    COUNT(DISTINCT CASE WHEN slb.customer_intent = 'indifferent' THEN s.spot_id END) as flexible_spots,
    COUNT(DISTINCT CASE WHEN slb.spans_multiple_blocks = 1 THEN s.spot_id END) as multi_block_spots,
    
    -- Customer diversity
    COUNT(DISTINCT s.customer_id) as unique_customers,
    COUNT(DISTINCT s.sales_person) as unique_sales_people,
    
    -- Date range
    MIN(s.air_date) as earliest_spot_date,
    MAX(s.air_date) as latest_spot_date

FROM language_blocks lb
JOIN programming_schedules ps ON lb.schedule_id = ps.schedule_id
JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
JOIN markets m ON sma.market_id = m.market_id
JOIN languages bl ON lb.language_id = bl.language_id
LEFT JOIN spot_language_blocks slb ON lb.block_id = slb.block_id
LEFT JOIN spots s ON slb.spot_id = s.spot_id
WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL OR s.spot_id IS NULL)
  AND lb.is_active = 1
  AND ps.is_active = 1
GROUP BY ps.schedule_name, ps.schedule_type, m.market_code, lb.day_of_week, 
         lb.day_part, lb.block_name, lb.block_type, lb.time_start, lb.time_end,
         bl.language_code, bl.language_name
ORDER BY ps.schedule_name, m.market_code, lb.day_of_week, lb.time_start;

CREATE VIEW language_block_revenue_summary AS
SELECT 
    lb.block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.schedule_id,
    ps.schedule_name,
    swlb.market_code,
    swlb.market_display_name,
    lb.block_type,
    lb.day_part,
    lb.time_start,
    lb.time_end,
    COUNT(DISTINCT swlb.spot_id) as total_spots,
    COUNT(DISTINCT swlb.customer_name) as unique_customers,
    SUM(swlb.station_net) as total_revenue,
    AVG(swlb.station_net) as avg_spot_revenue,
    MIN(swlb.air_date) as first_air_date,
    MAX(swlb.air_date) as last_air_date,
    strftime('%Y', swlb.air_date) as year,
    strftime('%Y-%m', swlb.air_date) as year_month,
    lb.is_active
FROM language_blocks lb
LEFT JOIN languages l ON lb.language_id = l.language_id
LEFT JOIN programming_schedules ps ON lb.schedule_id = ps.schedule_id
LEFT JOIN spots_with_language_blocks_enhanced swlb ON lb.block_id = swlb.block_id
WHERE lb.is_active = 1 AND swlb.spot_id IS NOT NULL
GROUP BY 
    lb.block_id,
    lb.block_name,
    lb.language_id,
    l.language_name,
    lb.schedule_id,
    ps.schedule_name,
    swlb.market_code,
    swlb.market_display_name,
    lb.block_type,
    lb.day_part,
    lb.time_start,
    lb.time_end,
    strftime('%Y', swlb.air_date),
    strftime('%Y-%m', swlb.air_date),
    lb.is_active;

CREATE VIEW schedule_collision_monitor AS
SELECT 
    scl.collision_id,
    scl.collision_type,
    scl.severity,
    m.market_code,
    m.market_name,
    ps1.schedule_name as schedule_1,
    ps2.schedule_name as schedule_2,
    scl.conflict_start_date,
    scl.conflict_end_date,
    scl.description,
    scl.resolution_status,
    scl.detected_date,
    scl.resolved_date,
    scl.resolved_by
FROM schedule_collision_log scl
LEFT JOIN markets m ON scl.market_id = m.market_id
LEFT JOIN programming_schedules ps1 ON scl.schedule_id_1 = ps1.schedule_id
LEFT JOIN programming_schedules ps2 ON scl.schedule_id_2 = ps2.schedule_id
ORDER BY scl.detected_date DESC;

CREATE VIEW spots_reporting AS
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
        
        -- Import batch information
        ib.import_mode,
        ib.import_date,
        ib.started_by,
        
        -- Month closure information
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

CREATE VIEW spots_with_language_blocks_enhanced AS
SELECT 
    s.spot_id,
    s.bill_code,
    s.air_date,
    s.day_of_week,
    s.time_in,
    s.time_out,
    s.gross_rate,
    s.station_net,
    s.sales_person,
    s.revenue_type,
    s.broadcast_month,
    
    -- Customer information
    c.normalized_name as customer_name,
    
    -- Market information  
    m.market_code,
    m.market_name as market_display_name,
    
    -- Original spot language
    sl.language_code as spot_language_code,
    sl.language_name as spot_language_name,
    
    -- Programming schedule information
    ps.schedule_name,
    ps.schedule_version,
    ps.schedule_type,
    
    -- Language block information
    lb.block_id,
    lb.block_name,
    lb.block_type,
    lb.day_part,
    lb.time_start as block_time_start,
    lb.time_end as block_time_end,
    
    -- Block language (may differ from spot language)
    bl.language_code as block_language_code,
    bl.language_name as block_language_name,
    
    -- Customer intent analysis
    slb.customer_intent,
    slb.intent_confidence,
    slb.spans_multiple_blocks,
    slb.assignment_method,
    
    -- NEW: Alert information
    slb.requires_attention,
    slb.alert_reason,
    
    -- Grid coverage status
    CASE 
        WHEN slb.customer_intent = 'no_grid_coverage' THEN 'No Grid Coverage'
        WHEN slb.spans_multiple_blocks = 1 THEN 'Multi-Block (Customer Indifferent)'
        WHEN slb.customer_intent = 'language_specific' THEN 'Language Targeted'
        WHEN slb.customer_intent = 'time_specific' THEN 'Time Slot Specific'
        WHEN slb.customer_intent = 'indifferent' THEN 'Flexible Placement'
        ELSE 'Unknown Intent'
    END as intent_description,
    
    -- Grid assignment status
    CASE 
        WHEN sma.assignment_id IS NOT NULL THEN 'Covered by Grid'
        ELSE 'No Grid Assignment'
    END as grid_coverage_status
    
FROM spots s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN markets m ON s.market_id = m.market_id
LEFT JOIN languages sl ON s.language_id = sl.language_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
LEFT JOIN languages bl ON lb.language_id = bl.language_id
LEFT JOIN programming_schedules ps ON slb.schedule_id = ps.schedule_id
LEFT JOIN schedule_market_assignments sma ON m.market_id = sma.market_id 
    AND s.air_date >= sma.effective_start_date 
    AND (s.air_date <= sma.effective_end_date OR sma.effective_end_date IS NULL)
WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL);

CREATE VIEW top_language_blocks AS
SELECT 
    lbd.block_id,
    lbd.block_name,
    lbd.language_name,
    lbd.schedule_name,
    lbd.market_display_name,
    lbd.block_type,
    lbd.day_part,
    lbd.time_start,
    lbd.time_end,
    lbd.current_year_revenue,
    lbd.current_year_spots,
    lbd.current_year_customers,
    lbd.yoy_revenue_growth_pct,
    lbd.performance_rating,
    lbd.last_activity_date,
    -- Rank by revenue
    ROW_NUMBER() OVER (ORDER BY lbd.current_year_revenue DESC) as revenue_rank,
    -- Rank by growth
    ROW_NUMBER() OVER (ORDER BY lbd.yoy_revenue_growth_pct DESC) as growth_rank
FROM language_block_dashboard lbd
WHERE lbd.current_year_revenue > 0
ORDER BY lbd.current_year_revenue DESC
LIMIT 20;

-- Triggers (3)
-- ============================================================

CREATE TRIGGER detect_market_assignment_collision
AFTER INSERT ON schedule_market_assignments
FOR EACH ROW
BEGIN
    -- Check for overlapping assignments for the same market
    INSERT INTO schedule_collision_log (
        collision_type, severity, market_id, schedule_id_1, schedule_id_2,
        conflict_start_date, conflict_end_date, description
    )
    SELECT 
        'market_overlap' as collision_type,
        'error' as severity,
        NEW.market_id,
        NEW.schedule_id as schedule_id_1,
        existing.schedule_id as schedule_id_2,
        MAX(NEW.effective_start_date, existing.effective_start_date) as conflict_start_date,
        MIN(
            COALESCE(NEW.effective_end_date, '2099-12-31'), 
            COALESCE(existing.effective_end_date, '2099-12-31')
        ) as conflict_end_date,
        'Market ' || (SELECT market_code FROM markets WHERE market_id = NEW.market_id) || 
        ' has overlapping schedule assignments from ' ||
        MAX(NEW.effective_start_date, existing.effective_start_date) || ' to ' ||
        MIN(
            COALESCE(NEW.effective_end_date, '2099-12-31'), 
            COALESCE(existing.effective_end_date, '2099-12-31')
        ) as description
    FROM schedule_market_assignments existing
    WHERE existing.assignment_id != NEW.assignment_id
      AND existing.market_id = NEW.market_id
      AND existing.effective_start_date < COALESCE(NEW.effective_end_date, '2099-12-31')
      AND COALESCE(existing.effective_end_date, '2099-12-31') > NEW.effective_start_date;
END;

CREATE TRIGGER validate_broadcast_month_insert
BEFORE INSERT ON spots
FOR EACH ROW
WHEN NEW.broadcast_month IS NOT NULL AND NEW.broadcast_month NOT LIKE '___-__'
BEGIN
    SELECT RAISE(ABORT, 'broadcast_month must be in mmm-yy format (e.g., Jan-24)');
END;

CREATE TRIGGER validate_broadcast_month_update
BEFORE UPDATE ON spots
FOR EACH ROW
WHEN NEW.broadcast_month IS NOT NULL AND NEW.broadcast_month NOT LIKE '___-__'
BEGIN
    SELECT RAISE(ABORT, 'broadcast_month must be in mmm-yy format (e.g., Jan-24)');
END;

-- End of schema export
-- Total tables: 18
-- Total views: 10
-- Total triggers: 3
