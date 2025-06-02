"""
Revised database schema definitions for the sales database tool.
Addresses all Excel columns and business requirements discussed.
"""

# SQL statements for creating database tables

SPOTS_TABLE = """
CREATE TABLE IF NOT EXISTS spots (
    -- Primary key
    spot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- All Excel columns (preserving exact names even if usage unclear)
    bill_code TEXT NOT NULL,  -- This is the customer field
    air_date DATE NOT NULL,
    end_date DATE,
    day TEXT,  -- Day of week
    time_in TEXT,  -- HH:MM:SS format
    time_out TEXT,  -- HH:MM:SS format
    length TEXT,  -- Length in seconds
    media TEXT,
    program TEXT,
    lang TEXT,  -- Language code (E, M, T, Hm, SA, V, etc.)
    format TEXT,
    number_field INTEGER,  -- The '#' column
    line INTEGER,
    type TEXT,  -- COM or BNS
    estimate TEXT,
    gross_rate DECIMAL(10, 2),
    make_good TEXT,
    spot_value DECIMAL(10, 2),
    month TEXT,  -- mmm-yy format
    broker_fees DECIMAL(10, 2),
    priority INTEGER,
    station_net DECIMAL(10, 2),
    sales_person TEXT,  -- AE name
    revenue_type TEXT,
    billing_type TEXT,  -- Calendar or Broadcast
    agency_flag TEXT,  -- The 'Agency?' column
    affidavit_flag TEXT,  -- The 'Affidavit?' column (Y/N)
    contract TEXT,
    market TEXT,
    
    -- Normalized fields for consistency
    normalized_customer TEXT,
    market_code TEXT,
    
    -- Metadata fields
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file TEXT,
    is_historical BOOLEAN DEFAULT 0,
    effective_date DATE,  -- For tracking when this forward-looking data was loaded
    
    -- Foreign key constraint
    FOREIGN KEY (normalized_customer) REFERENCES customers(normalized_name)
);

-- Create indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_spots_air_date ON spots(air_date);
CREATE INDEX IF NOT EXISTS idx_spots_normalized_customer ON spots(normalized_customer);
CREATE INDEX IF NOT EXISTS idx_spots_sales_person ON spots(sales_person);
CREATE INDEX IF NOT EXISTS idx_spots_market_code ON spots(market_code);
CREATE INDEX IF NOT EXISTS idx_spots_revenue_type ON spots(revenue_type);
CREATE INDEX IF NOT EXISTS idx_spots_month ON spots(month);
CREATE INDEX IF NOT EXISTS idx_spots_is_historical ON spots(is_historical);
"""

CUSTOMERS_TABLE = """
CREATE TABLE IF NOT EXISTS customers (
    -- Primary key
    customer_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Normalized customer name (unique)
    normalized_name TEXT NOT NULL UNIQUE,
    
    -- Business categorization
    sector TEXT,  -- Auto, Outreach, CPG, Insurance, etc.
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Additional fields for future use
    customer_type TEXT,
    notes TEXT
);

-- Indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_customers_normalized_name ON customers(normalized_name);
CREATE INDEX IF NOT EXISTS idx_customers_sector ON customers(sector);
"""

CUSTOMER_MAPPINGS_TABLE = """
CREATE TABLE IF NOT EXISTS customer_mappings (
    -- Primary key
    mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Original name from Excel (as it appears in bill_code)
    original_name TEXT NOT NULL UNIQUE,
    
    -- Normalized name it maps to
    normalized_name TEXT NOT NULL,
    
    -- Mapping metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'system',
    confidence_score REAL,  -- Similarity score if auto-matched
    
    -- Foreign key constraint
    FOREIGN KEY (normalized_name) REFERENCES customers(normalized_name)
);

-- Indexes for mapping lookups
CREATE INDEX IF NOT EXISTS idx_mappings_original ON customer_mappings(original_name);
CREATE INDEX IF NOT EXISTS idx_mappings_normalized ON customer_mappings(normalized_name);
"""

BUDGET_TABLE = """
CREATE TABLE IF NOT EXISTS budget (
    -- Primary key
    budget_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Budget dimensions
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Budget amount
    budget_amount DECIMAL(12, 2) NOT NULL,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source TEXT,  -- Where this budget came from
    
    -- Ensure unique budget per AE/year/month
    UNIQUE(ae_name, year, month)
);

-- Indexes for budget queries
CREATE INDEX IF NOT EXISTS idx_budget_ae_name ON budget(ae_name);
CREATE INDEX IF NOT EXISTS idx_budget_year_month ON budget(year, month);
"""

PIPELINE_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline (
    -- Primary key
    pipeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Pipeline dimensions (by AE and month as specified)
    ae_name TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    
    -- Pipeline amount (management's current expectation)
    pipeline_amount DECIMAL(12, 2) NOT NULL,
    
    -- Version tracking (bi-weekly updates)
    update_date DATE NOT NULL,
    is_current BOOLEAN DEFAULT 1,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    notes TEXT,
    
    -- Ensure one current pipeline per AE/year/month
    UNIQUE(ae_name, year, month, is_current)
);

-- Indexes for pipeline queries
CREATE INDEX IF NOT EXISTS idx_pipeline_ae_name ON pipeline(ae_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_year_month ON pipeline(year, month);
CREATE INDEX IF NOT EXISTS idx_pipeline_current ON pipeline(is_current);
CREATE INDEX IF NOT EXISTS idx_pipeline_update_date ON pipeline(update_date);
"""

MARKETS_TABLE = """
CREATE TABLE IF NOT EXISTS markets (
    -- Primary key
    market_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Market mapping
    market_name TEXT NOT NULL UNIQUE,  -- As it appears in Excel
    market_code TEXT NOT NULL,         -- Standardized code (NYC, CMP, etc.)
    
    -- Additional market info
    region TEXT,
    is_active BOOLEAN DEFAULT 1,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for market lookups
CREATE INDEX IF NOT EXISTS idx_markets_name ON markets(market_name);
CREATE INDEX IF NOT EXISTS idx_markets_code ON markets(market_code);
"""

# Add a sectors reference table for standardization
SECTORS_TABLE = """
CREATE TABLE IF NOT EXISTS sectors (
    -- Primary key
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Sector information
    sector_code TEXT NOT NULL UNIQUE,  -- Short code (AUTO, CPG, INS, etc.)
    sector_name TEXT NOT NULL,         -- Full name (Automotive, Consumer Packaged Goods, etc.)
    sector_group TEXT,                 -- Higher level grouping if needed
    
    -- Metadata
    is_active BOOLEAN DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for sector lookups
CREATE INDEX IF NOT EXISTS idx_sectors_code ON sectors(sector_code);
"""
CREATE TABLE IF NOT EXISTS languages (
    -- Primary key
    language_id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Language mapping
    language_code TEXT NOT NULL UNIQUE,  -- Code from Excel (E, M, T, etc.)
    language_name TEXT NOT NULL,         -- Full name (English, Mandarin, etc.)
    language_group TEXT,                 -- Group (South Asian, etc.)
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for language lookups
CREATE INDEX IF NOT EXISTS idx_languages_code ON languages(language_code);
"""

# Aggregate all table creation statements
ALL_TABLES = [
    ("sectors", SECTORS_TABLE),
    ("customers", CUSTOMERS_TABLE),
    ("customer_mappings", CUSTOMER_MAPPINGS_TABLE),
    ("markets", MARKETS_TABLE),
    ("languages", LANGUAGES_TABLE),
    ("spots", SPOTS_TABLE),
    ("budget", BUDGET_TABLE),
    ("pipeline", PIPELINE_TABLE)
]

# Market code mappings (matching your specifications)
MARKET_MAPPINGS = [
    ("NEW YORK", "NYC"),
    ("Central Valley", "CVC"), 
    ("SAN FRANCISCO", "SFO"),
    ("CHI MSP", "CMP"),  -- As specified
    ("HOUSTON", "HOU"),
    ("LOS ANGELES", "LAX"),
    ("SEATTLE", "SEA"),
    ("DALLAS", "DAL"),
    # Add variations for case-insensitive matching
    ("New York", "NYC"),
    ("new york", "NYC"),
    ("San Francisco", "SFO"),
    ("san francisco", "SFO"),
    ("Los Angeles", "LAX"),
    ("los angeles", "LAX"),
    ("Houston", "HOU"),
    ("houston", "HOU"),
    ("Dallas", "DAL"),
    ("dallas", "DAL"),
    ("Seattle", "SEA"),
    ("seattle", "SEA"),
    ("chi msp", "CMP"),
    ("central valley", "CVC"),
]

# Language mappings for future implementation
LANGUAGE_MAPPINGS = [
    ("E", "English", "English"),
    ("M", "Mandarin", "Chinese"),
    ("C", "Cantonese", "Chinese"),
    ("T", "Tagalog", "Filipino"),
    ("Hm", "Hmong", "Hmong"),
    ("SA", "South Asian", "South Asian"),
    ("V", "Vietnamese", "Vietnamese"),
    ("K", "Korean", "Korean"),
    ("J", "Japanese", "Japanese"),
    # South Asian languages
    ("SA", "Hindi", "South Asian"),
    ("SA", "Punjabi", "South Asian"),
    ("SA", "Bengali", "South Asian"),
    ("SA", "Gujarati", "South Asian"),
]

# Sample sector mappings
SECTOR_MAPPINGS = [
    ("AUTO", "Automotive", None),
    ("CPG", "Consumer Packaged Goods", None),
    ("INS", "Insurance", None),
    ("OUTR", "Outreach", None),
    ("TECH", "Technology", None),
    ("FIN", "Financial Services", None),
    ("HEALTH", "Healthcare", None),
    ("RETAIL", "Retail", None),
    ("TELCO", "Telecommunications", None),
    ("MEDIA", "Media & Entertainment", None),
    ("GOV", "Government", None),
    ("EDU", "Education", None),
    ("NPO", "Non-Profit", None),
    ("OTHER", "Other", None),
]