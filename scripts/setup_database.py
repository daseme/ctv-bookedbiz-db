#!/usr/bin/env python3
"""
Database setup script - creates tables and populates reference data.
Run this once to initialize the database.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.connection import DatabaseConnection
from database.reference_data import ReferenceDataManager


def create_tables(conn):
    """Create all database tables."""
    
    # 1. AGENCIES TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS agencies (
        agency_id INTEGER PRIMARY KEY AUTOINCREMENT,
        agency_name TEXT NOT NULL UNIQUE,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_active BOOLEAN DEFAULT 1,
        notes TEXT
    )
    """)
    
    # 2. SECTORS TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS sectors (
        sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sector_code TEXT NOT NULL UNIQUE,
        sector_name TEXT NOT NULL,
        sector_group TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 3. MARKETS TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS markets (
        market_id INTEGER PRIMARY KEY AUTOINCREMENT,
        market_name TEXT NOT NULL UNIQUE,
        market_code TEXT NOT NULL UNIQUE,
        region TEXT,
        is_active BOOLEAN DEFAULT 1,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 4. LANGUAGES TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS languages (
        language_id INTEGER PRIMARY KEY AUTOINCREMENT,
        language_code TEXT NOT NULL UNIQUE,
        language_name TEXT NOT NULL,
        language_group TEXT,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    # 5. CUSTOMERS TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS customers (
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
    )
    """)
    
    # 6. CUSTOMER_MAPPINGS TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS customer_mappings (
        mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_name TEXT NOT NULL UNIQUE,
        customer_id INTEGER NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT 'system',
        confidence_score REAL CHECK (confidence_score >= 0 AND confidence_score <= 1),
        
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
    )
    """)
    
    # 7. SPOTS TABLE (Core transactional data - fixed relationships)
    conn.execute("""
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
        
        -- Metadata
        load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source_file TEXT,
        is_historical BOOLEAN DEFAULT 0,
        effective_date DATE,  -- When this forward-looking data was loaded
        
        -- Business rule constraints (UPDATED - removed financial >= 0 checks)
        CHECK (revenue_type != 'Trade' OR revenue_type IS NULL),  -- Exclude Trade per business rules
        
        -- Foreign key constraints with proper cascading
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE RESTRICT,
        FOREIGN KEY (agency_id) REFERENCES agencies(agency_id) ON DELETE RESTRICT,
        FOREIGN KEY (market_id) REFERENCES markets(market_id) ON DELETE RESTRICT,
        FOREIGN KEY (language_id) REFERENCES languages(language_id) ON DELETE RESTRICT
    )
    """)
    
    # 8. BUDGET TABLE
    conn.execute("""
    CREATE TABLE IF NOT EXISTS budget (
        budget_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ae_name TEXT NOT NULL,
        year INTEGER NOT NULL CHECK (year >= 2000 AND year <= 2100),
        month INTEGER NOT NULL CHECK (month >= 1 AND month <= 12),
        budget_amount DECIMAL(12, 2) NOT NULL CHECK (budget_amount >= 0),
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        source TEXT,
        
        UNIQUE(ae_name, year, month)
    )
    """)
    
    # 9. PIPELINE TABLE
    conn.execute("""
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
    )
    """)


def create_indexes(conn):
    """Create performance indexes."""
    
    # Spots table indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spots_air_date ON spots(air_date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spots_customer_id ON spots(customer_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spots_sales_person ON spots(sales_person)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spots_historical ON spots(is_historical)")
    
    # Composite indexes for common query patterns
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_spots_performance_report ON spots(sales_person, air_date, market_id) 
    WHERE is_historical = 0 AND revenue_type != 'Trade'
    """)
    
    conn.execute("CREATE INDEX IF NOT EXISTS idx_spots_customer_timeline ON spots(customer_id, air_date, revenue_type)")
    
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_spots_monthly_rollup ON spots(broadcast_month, market_id, revenue_type) 
    WHERE revenue_type != 'Trade'
    """)
    
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_spots_agency_performance ON spots(agency_id, air_date, gross_rate)
    WHERE agency_id IS NOT NULL
    """)
    
    # Reference table indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_sector ON customers(sector_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_customers_agency ON customers(agency_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_customer_mappings_original ON customer_mappings(original_name)")
    
    # Budget and pipeline indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_budget_ae_year_month ON budget(ae_name, year, month)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_ae_year_month ON pipeline(ae_name, year, month)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_current ON pipeline(is_current) WHERE is_current = 1")


def populate_reference_data(conn):
    """Populate reference data tables."""
    
    # Insert standard market mappings
    markets = [
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
        ('Dallas', 'DAL', 'South')
    ]
    
    for market_name, market_code, region in markets:
        conn.execute(
            "INSERT OR IGNORE INTO markets (market_name, market_code, region) VALUES (?, ?, ?)",
            (market_name, market_code, region)
        )
    
    # Insert standard language mappings
    languages = [
        ('E', 'English', 'English'),
        ('M', 'Mandarin', 'Chinese'),
        ('C', 'Cantonese', 'Chinese'),
        ('T', 'Tagalog', 'Filipino'),
        ('Hm', 'Hmong', 'Hmong'),
        ('SA', 'South Asian', 'South Asian'),
        ('V', 'Vietnamese', 'Vietnamese'),
        ('K', 'Korean', 'Korean'),
        ('J', 'Japanese', 'Japanese')
    ]
    
    for language_code, language_name, language_group in languages:
        conn.execute(
            "INSERT OR IGNORE INTO languages (language_code, language_name, language_group) VALUES (?, ?, ?)",
            (language_code, language_name, language_group)
        )
    
    # Insert standard sector mappings
    sectors = [
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
        ('OTHER', 'Other', 'Other')
    ]
    
    for sector_code, sector_name, sector_group in sectors:
        conn.execute(
            "INSERT OR IGNORE INTO sectors (sector_code, sector_name, sector_group) VALUES (?, ?, ?)",
            (sector_code, sector_name, sector_group)
        )


def create_reporting_view(conn):
    """Create the reporting view for fast queries."""
    conn.execute("""
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
        l.language_name
        
    FROM spots s
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    LEFT JOIN markets m ON s.market_id = m.market_id
    LEFT JOIN languages l ON s.language_id = l.language_id
    WHERE s.revenue_type != 'Trade' OR s.revenue_type IS NULL
    """)


def setup_database(db_path: str):
    """Create database schema and populate reference data."""
    print(f"Setting up database: {db_path}")
    
    # Ensure directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    db_conn = DatabaseConnection(db_path)
    
    with db_conn.transaction() as conn:
        print("Creating tables...")
        create_tables(conn)
        
        print("Creating indexes...")
        create_indexes(conn)
        
        print("Populating reference data...")
        populate_reference_data(conn)
        
        print("Creating reporting view...")
        create_reporting_view(conn)
        
        print("Database setup complete!")
    
    db_conn.close()


def verify_database(db_path: str):
    """Verify database was created correctly."""
    print(f"Verifying database: {db_path}")
    
    db_conn = DatabaseConnection(db_path)
    conn = db_conn.connect()
    
    # Check tables exist
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    expected_tables = ['agencies', 'sectors', 'markets', 'languages', 'customers', 
                      'customer_mappings', 'spots', 'budget', 'pipeline']
    
    print("Tables created:")
    for table in expected_tables:
        if table in tables:
            print(f"  ✓ {table}")
        else:
            print(f"  ✗ {table} (MISSING)")
    
    # Check reference data
    cursor = conn.execute("SELECT COUNT(*) FROM markets")
    market_count = cursor.fetchone()[0]
    print(f"Markets populated: {market_count}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM sectors")
    sector_count = cursor.fetchone()[0]
    print(f"Sectors populated: {sector_count}")
    
    cursor = conn.execute("SELECT COUNT(*) FROM languages")
    language_count = cursor.fetchone()[0]
    print(f"Languages populated: {language_count}")
    
    # Check view exists
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='spots_reporting'")
    view_exists = cursor.fetchone() is not None
    print(f"Reporting view: {'✓' if view_exists else '✗'}")
    
    db_conn.close()
    print("Verification complete!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup sales database")
    parser.add_argument("--db-path", default="data/database/production.db",
                       help="Database file path")
    parser.add_argument("--verify", action="store_true",
                       help="Verify database after setup")
    
    args = parser.parse_args()
    
    try:
        setup_database(args.db_path)
        
        if args.verify:
            verify_database(args.db_path)
            
    except Exception as e:
        print(f"Database setup failed: {e}")
        sys.exit(1)