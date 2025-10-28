#!/usr/bin/env python3
"""
REFACTORED: Enhanced Quarterly Language Block Report
===================================================

Refactored to address:
- Issue #2: Centralized year filter logic
- Issue #3: Separated concerns and smaller focused functions
- FIXED: Month counting logic to respect year filters

Usage:
    python cli_enhanced_quarterly_refactored.py --year 2024                    # Single year analysis
    python cli_enhanced_quarterly_refactored.py --year 2025                    # Single year analysis
    python cli_enhanced_quarterly_refactored.py --compare 2024 2025            # Compare two years
    python cli_enhanced_quarterly_refactored.py --all                          # All available data
    python cli_enhanced_quarterly_refactored.py --interactive                  # Interactive mode
"""

import sqlite3
import os
import argparse
from datetime import datetime
from collections import defaultdict
from typing import Optional, List, Union, Tuple, Dict, Any

# =============================================================================
# CENTRALIZED YEAR FILTER LOGIC (Issue #2 Fix)
# =============================================================================


def build_year_conditions(
    year_filter: Optional[Union[str, List[str]]],
) -> Tuple[str, str, str]:
    """
    Centralized year filter logic to eliminate code duplication.
    FIXED: Proper handling for yyyy-mm-dd datetime format

    Args:
        year_filter: None, single year string, or list of year strings

    Returns:
        Tuple of (where_condition, case_condition, month_case_condition) for SQL queries
        month_case_condition extracts YYYY-MM for proper month counting
    """
    if not year_filter:
        return "", "1=1", "SUBSTR(s.broadcast_month, 1, 7)"

    if isinstance(year_filter, list):
        year_list = "', '".join(year_filter)
        where_condition = f"AND SUBSTR(s.broadcast_month, 1, 4) IN ('{year_list}')"
        case_condition = f"SUBSTR(s.broadcast_month, 1, 4) IN ('{year_list}')"
        month_case_condition = f"CASE WHEN SUBSTR(s.broadcast_month, 1, 4) IN ('{year_list}') THEN SUBSTR(s.broadcast_month, 1, 7) END"
    else:
        where_condition = f"AND SUBSTR(s.broadcast_month, 1, 4) = '{year_filter}'"
        case_condition = f"SUBSTR(s.broadcast_month, 1, 4) = '{year_filter}'"
        month_case_condition = f"CASE WHEN SUBSTR(s.broadcast_month, 1, 4) = '{year_filter}' THEN SUBSTR(s.broadcast_month, 1, 7) END"

    return where_condition, case_condition, month_case_condition


# =============================================================================
# DATABASE CONNECTION AND UTILITIES
# =============================================================================


def get_database_connection(
    db_path: str = "data/database/production.db",
) -> Optional[sqlite3.Connection]:
    """Get database connection"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found: {db_path}")
        return None
    return sqlite3.connect(db_path)


def get_available_years(conn: sqlite3.Connection) -> List[str]:
    """Get list of available years in the database"""
    cursor = conn.cursor()
    query = """
    SELECT DISTINCT SUBSTR(broadcast_month, 1, 4) as year
    FROM spots 
    WHERE broadcast_month IS NOT NULL
      AND (revenue_type != 'Trade' OR revenue_type IS NULL)
    ORDER BY year DESC
    """
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]


# =============================================================================
# DATA RETRIEVAL FUNCTIONS (Issue #3 Fix - Separated Data Layer)
# =============================================================================


def get_performance_overview_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> Dict[str, Any]:
    """Retrieve performance overview data"""
    cursor = conn.cursor()
    year_condition, _, _ = build_year_conditions(year_filter)

    query = f"""
    SELECT 
        COUNT(DISTINCT slb.block_id) as total_blocks,
        COUNT(DISTINCT s.language_id) as total_languages,
        COUNT(DISTINCT s.market_id) as total_markets,
        SUM(s.gross_rate) as total_revenue,
        COUNT(DISTINCT s.spot_id) as total_spots,
        ROUND(AVG(s.gross_rate), 2) as avg_spot_revenue
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      {year_condition}
    """

    cursor.execute(query)
    result = cursor.fetchone()

    return {
        "total_blocks": result[0] or 0,
        "total_languages": result[1] or 0,
        "total_markets": result[2] or 0,
        "total_revenue": result[3] or 0,
        "total_spots": result[4] or 0,
        "avg_spot_revenue": result[5] or 0,
    }


def get_language_performance_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> List[Tuple]:
    """Retrieve language performance data"""
    cursor = conn.cursor()
    year_condition, _, _ = build_year_conditions(year_filter)

    query = f"""
    SELECT 
        l.language_name,
        COUNT(DISTINCT slb.block_id) as blocks,
        SUM(s.gross_rate) as total_revenue,
        COUNT(DISTINCT s.spot_id) as total_spots,
        ROUND(SUM(s.gross_rate) / COUNT(DISTINCT s.spot_id), 2) as revenue_per_spot,
        ROUND(AVG(s.gross_rate), 2) as avg_spot_revenue
    FROM spots s
    LEFT JOIN languages l ON s.language_id = l.language_id
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND l.language_name IS NOT NULL
      {year_condition}
    GROUP BY l.language_name
    ORDER BY total_revenue DESC
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_top_blocks_data(
    conn: sqlite3.Connection,
    year_filter: Optional[Union[str, List[str]]],
    limit: int = 15,
) -> List[Tuple]:
    """Retrieve top performing blocks data - FIXED month counting"""
    cursor = conn.cursor()
    year_condition, case_condition, month_case_condition = build_year_conditions(
        year_filter
    )

    query = f"""
    SELECT 
        lb.block_name,
        l.language_name,
        m.market_code,
        lb.time_start || '-' || lb.time_end as time_slot,
        SUM(s.gross_rate) as revenue,
        COUNT(DISTINCT s.spot_id) as spots,
        COUNT(DISTINCT {month_case_condition}) as active_months_in_period
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    JOIN language_blocks lb ON slb.block_id = lb.block_id
    LEFT JOIN languages l ON lb.language_id = l.language_id
    LEFT JOIN markets m ON s.market_id = m.market_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      {year_condition}
    GROUP BY lb.block_id, lb.block_name, l.language_name, m.market_code
    ORDER BY revenue DESC
    LIMIT {limit}
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_market_performance_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> List[Tuple]:
    """Retrieve market performance data"""
    cursor = conn.cursor()
    year_condition, _, _ = build_year_conditions(year_filter)

    query = f"""
    SELECT 
        m.market_name,
        m.market_code,
        COUNT(DISTINCT slb.block_id) as blocks,
        COUNT(DISTINCT l.language_name) as languages,
        SUM(s.gross_rate) as revenue,
        COUNT(DISTINCT s.spot_id) as spots,
        ROUND(SUM(s.gross_rate) / COUNT(DISTINCT slb.block_id), 2) as avg_revenue_per_block
    FROM spots s
    LEFT JOIN markets m ON s.market_id = m.market_id
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN language_blocks lb ON slb.block_id = lb.block_id
    LEFT JOIN languages l ON lb.language_id = l.language_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND m.market_code IS NOT NULL
      {year_condition}
    GROUP BY m.market_name, m.market_code
    ORDER BY revenue DESC
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_time_slot_performance_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> List[Tuple]:
    """Retrieve time slot performance data"""
    cursor = conn.cursor()
    year_condition, _, _ = build_year_conditions(year_filter)

    query = f"""
    SELECT 
        lb.day_part,
        COUNT(DISTINCT slb.block_id) as blocks,
        SUM(s.gross_rate) as revenue,
        COUNT(DISTINCT s.spot_id) as spots,
        ROUND(SUM(s.gross_rate) / COUNT(DISTINCT slb.block_id), 2) as revenue_per_block,
        ROUND(AVG(s.gross_rate), 2) as avg_spot_revenue
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    JOIN language_blocks lb ON slb.block_id = lb.block_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND lb.day_part IS NOT NULL
      {year_condition}
    GROUP BY lb.day_part
    ORDER BY revenue DESC
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_comprehensive_block_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> List[Tuple]:
    """Retrieve comprehensive block performance data - FIXED month counting"""
    cursor = conn.cursor()
    year_condition, case_condition, month_case_condition = build_year_conditions(
        year_filter
    )

    query = f"""
    SELECT 
        lb.block_name,
        l.language_name,
        m.market_code,
        lb.day_of_week,
        lb.day_part,
        lb.time_start || '-' || lb.time_end as time_slot,
        
        -- Revenue Metrics
        COALESCE(SUM(s.gross_rate), 0) as total_revenue,
        COALESCE(COUNT(s.spot_id), 0) as total_spots,
        COALESCE(ROUND(SUM(s.gross_rate) / NULLIF(COUNT(s.spot_id), 0), 2), 0) as revenue_per_spot,
        
        -- Time & Efficiency Metrics - FIXED month counting
        COUNT(DISTINCT {month_case_condition}) as active_months_in_period,
        COALESCE(ROUND(COUNT(s.spot_id) / NULLIF(COUNT(DISTINCT {month_case_condition}), 0), 1), 0) as spots_per_month,
        
        -- Calculate time duration in hours
        ROUND((
            (CAST(SUBSTR(lb.time_end, 1, 2) AS INTEGER) * 60 + CAST(SUBSTR(lb.time_end, 4, 2) AS INTEGER)) -
            (CAST(SUBSTR(lb.time_start, 1, 2) AS INTEGER) * 60 + CAST(SUBSTR(lb.time_start, 4, 2) AS INTEGER))
        ) / 60.0, 1) as duration_hours,
        
        -- Customer Diversity
        COUNT(DISTINCT s.customer_id) as unique_customers,
        
        -- Performance Indicators
        CASE WHEN COUNT(s.spot_id) = 0 THEN 'UNUSED'
             WHEN COUNT(s.spot_id) < 5 THEN 'VERY_LOW' 
             WHEN COALESCE(SUM(s.gross_rate) / NULLIF(COUNT(s.spot_id), 0), 0) < 20 THEN 'LOW_VALUE'
             WHEN COUNT(DISTINCT s.customer_id) = 1 THEN 'SINGLE_CUSTOMER'
             ELSE 'PERFORMING' END as performance_flag
             
    FROM language_blocks lb
    LEFT JOIN languages l ON lb.language_id = l.language_id
    LEFT JOIN schedule_market_assignments sma ON lb.schedule_id = sma.schedule_id
    LEFT JOIN markets m ON sma.market_id = m.market_id
    LEFT JOIN spot_language_blocks slb ON lb.block_id = slb.block_id
    LEFT JOIN spots s ON slb.spot_id = s.spot_id 
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL) 
        AND s.gross_rate > 0
        {year_condition}
    WHERE lb.is_active = 1
    GROUP BY lb.block_id, lb.block_name, l.language_name, m.market_code, 
             lb.day_of_week, lb.day_part, lb.time_start, lb.time_end
    ORDER BY total_revenue ASC, revenue_per_spot ASC
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_bottom_daytime_blocks_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> List[Tuple]:
    """Retrieve bottom 15 daytime blocks data - FIXED month counting"""
    cursor = conn.cursor()
    year_condition, case_condition, month_case_condition = build_year_conditions(
        year_filter
    )

    query = f"""
    SELECT 
        lb.block_name,
        l.language_name,
        m.market_code,
        lb.day_of_week,
        lb.day_part,
        lb.time_start || '-' || lb.time_end as time_slot,
        
        -- Performance Metrics
        SUM(s.gross_rate) as total_revenue,
        COUNT(s.spot_id) as total_spots,
        ROUND(SUM(s.gross_rate) / COUNT(s.spot_id), 2) as revenue_per_spot,
        COUNT(DISTINCT s.customer_id) as unique_customers,
        COUNT(DISTINCT {month_case_condition}) as active_months_in_period,
        
        -- Efficiency Metrics - FIXED month counting
        ROUND(COUNT(s.spot_id) / NULLIF(COUNT(DISTINCT {month_case_condition}), 0), 1) as spots_per_month,
        ROUND(SUM(s.gross_rate) / NULLIF(COUNT(DISTINCT {month_case_condition}), 0), 0) as revenue_per_month,
        
        -- Calculate block duration in hours
        ROUND((
            (CAST(SUBSTR(lb.time_end, 1, 2) AS INTEGER) * 60 + CAST(SUBSTR(lb.time_end, 4, 2) AS INTEGER)) -
            (CAST(SUBSTR(lb.time_start, 1, 2) AS INTEGER) * 60 + CAST(SUBSTR(lb.time_start, 4, 2) AS INTEGER))
        ) / 60.0, 1) as duration_hours,
        
        -- Performance ranking score
        (SUM(s.gross_rate) * 0.7) + (ROUND(SUM(s.gross_rate) / COUNT(s.spot_id), 2) * COUNT(s.spot_id) * 0.3) as performance_score
        
    FROM language_blocks lb
    JOIN languages l ON lb.language_id = l.language_id
    JOIN schedule_market_assignments sma ON lb.schedule_id = sma.schedule_id
    JOIN markets m ON sma.market_id = m.market_id
    JOIN spot_language_blocks slb ON lb.block_id = slb.block_id
    JOIN spots s ON slb.spot_id = s.spot_id
    WHERE lb.is_active = 1
      AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      -- Daytime/Primetime filter: 6 AM to 11 PM
      AND (
        (CAST(SUBSTR(lb.time_start, 1, 2) AS INTEGER) >= 6 AND CAST(SUBSTR(lb.time_start, 1, 2) AS INTEGER) <= 22) OR
        (CAST(SUBSTR(lb.time_start, 1, 2) AS INTEGER) = 23 AND CAST(SUBSTR(lb.time_start, 4, 2) AS INTEGER) = 0)
      )
      {year_condition}
    GROUP BY lb.block_id, lb.block_name, l.language_name, m.market_code, 
             lb.day_of_week, lb.day_part, lb.time_start, lb.time_end
    HAVING SUM(s.gross_rate) > 0 AND COUNT(s.spot_id) > 0
    ORDER BY performance_score ASC
    LIMIT 15
    """

    cursor.execute(query)
    return cursor.fetchall()


def get_insights_data(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> Dict[str, Any]:
    """Retrieve data for key insights"""
    cursor = conn.cursor()
    year_condition, _, _ = build_year_conditions(year_filter)

    # Most Profitable Language
    profitable_lang_query = f"""
    SELECT 
        l.language_name,
        ROUND(SUM(s.gross_rate) / COUNT(s.spot_id), 2) as revenue_per_spot,
        COUNT(s.spot_id) as spot_count
    FROM spots s
    LEFT JOIN languages l ON s.language_id = l.language_id
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND l.language_name IS NOT NULL
      {year_condition}
    GROUP BY l.language_name
    HAVING COUNT(s.spot_id) >= 100
    ORDER BY revenue_per_spot DESC
    LIMIT 1
    """

    cursor.execute(profitable_lang_query)
    most_profitable = cursor.fetchone()

    # Busiest Time Slot
    busiest_slot_query = f"""
    SELECT 
        lb.day_part,
        COUNT(DISTINCT s.spot_id) as spot_count
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    JOIN language_blocks lb ON slb.block_id = lb.block_id
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.gross_rate > 0
      AND lb.day_part IS NOT NULL
      {year_condition}
    GROUP BY lb.day_part
    ORDER BY spot_count DESC
    LIMIT 1
    """

    cursor.execute(busiest_slot_query)
    busiest_slot = cursor.fetchone()

    # Analysis Period
    period_query = f"""
    SELECT 
        MIN(s.broadcast_month) as earliest,
        MAX(s.broadcast_month) as latest,
        COUNT(DISTINCT SUBSTR(s.broadcast_month, 1, 7)) as total_months
    FROM spots s
    WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
      AND s.broadcast_month IS NOT NULL
      {year_condition}
    """

    cursor.execute(period_query)
    period = cursor.fetchone()

    return {
        "most_profitable_language": most_profitable,
        "busiest_time_slot": busiest_slot,
        "analysis_period": period,
    }


# =============================================================================
# DATA PROCESSING FUNCTIONS (Issue #3 Fix - Separated Business Logic)
# =============================================================================


def process_comprehensive_block_data(blocks_data: List[Tuple]) -> Dict[str, Any]:
    """Process comprehensive block data to extract insights"""
    if not blocks_data:
        return {"blocks": [], "summary": {}}

    weak_blocks = []
    unused_blocks = []
    single_customer_blocks = []

    for block in blocks_data:
        flag = block[13]  # performance_flag column

        if flag == "UNUSED":
            unused_blocks.append(block)
        elif flag in ["VERY_LOW", "LOW_VALUE"]:
            weak_blocks.append(block)
        elif flag == "SINGLE_CUSTOMER":
            single_customer_blocks.append(block)

    total_blocks = len(blocks_data)
    summary = {
        "total_blocks": total_blocks,
        "unused_count": len(unused_blocks),
        "weak_count": len(weak_blocks),
        "single_customer_count": len(single_customer_blocks),
        "unused_percentage": (len(unused_blocks) / total_blocks * 100)
        if total_blocks > 0
        else 0,
        "weak_percentage": (len(weak_blocks) / total_blocks * 100)
        if total_blocks > 0
        else 0,
        "single_customer_percentage": (len(single_customer_blocks) / total_blocks * 100)
        if total_blocks > 0
        else 0,
    }

    return {
        "blocks": blocks_data,
        "weak_blocks": weak_blocks,
        "unused_blocks": unused_blocks,
        "single_customer_blocks": single_customer_blocks,
        "summary": summary,
    }


def process_bottom_blocks_data(bottom_blocks: List[Tuple]) -> Dict[str, Any]:
    """Process bottom blocks data to identify issues"""
    if not bottom_blocks:
        return {"blocks": [], "issues": {}, "summary": {}}

    issue_summary = {
        "Low Total Revenue": 0,
        "Poor Revenue/Spot": 0,
        "Single Customer": 0,
        "Low Utilization": 0,
        "Short Duration": 0,
    }

    processed_blocks = []

    for rank, block in enumerate(bottom_blocks, 1):
        revenue = block[6] or 0
        spots = block[7] or 0
        rev_per_spot = block[8] or 0
        customers = block[9] or 0
        spots_per_month = block[11] or 0
        duration = block[13] or 0

        # Identify primary issue
        primary_issue = "Multiple Issues"
        if revenue < 2000:
            primary_issue = "Low Total Rev"
            issue_summary["Low Total Revenue"] += 1
        elif rev_per_spot < 25:
            primary_issue = "Poor $/Spot"
            issue_summary["Poor Revenue/Spot"] += 1
        elif customers == 1:
            primary_issue = "Single Customer"
            issue_summary["Single Customer"] += 1
        elif spots_per_month < 3:
            primary_issue = "Low Utilization"
            issue_summary["Low Utilization"] += 1
        elif duration < 1:
            primary_issue = "Short Duration"
            issue_summary["Short Duration"] += 1

        processed_blocks.append(
            {"rank": rank, "data": block, "primary_issue": primary_issue}
        )

    # Calculate summary statistics
    total_revenue = sum(block[6] for block in bottom_blocks)
    total_spots = sum(block[7] for block in bottom_blocks)
    avg_rev_per_spot = total_revenue / total_spots if total_spots > 0 else 0

    summary = {
        "total_revenue": total_revenue,
        "total_spots": total_spots,
        "avg_revenue_per_spot": avg_rev_per_spot,
        "monthly_revenue": sum(block[12] for block in bottom_blocks),
    }

    return {"blocks": processed_blocks, "issues": issue_summary, "summary": summary}


# =============================================================================
# PRESENTATION FUNCTIONS (Issue #3 Fix - Separated Display Logic)
# =============================================================================


def display_performance_overview(data: Dict[str, Any]) -> None:
    """Display performance overview section"""
    print("📊 PERFORMANCE OVERVIEW")
    print("-" * 30)
    print(f"Total Blocks: {data['total_blocks']}")
    print(f"Languages: {data['total_languages']}")
    print(f"Markets: {data['total_markets']}")
    print(f"Total Revenue: ${data['total_revenue']:,.2f}")
    print(f"Total Spots: {data['total_spots']:,}")
    print(f"Avg Spot Revenue: ${data['avg_spot_revenue']:,.2f}")


def display_language_performance(languages_data: List[Tuple]) -> None:
    """Display language performance analysis"""
    print(f"\n🌐 LANGUAGE PERFORMANCE ANALYSIS")
    print("-" * 40)
    print(
        f"{'Language':15} {'Blocks':>7} {'Revenue':>12} {'Spots':>8} {'Revenue/Spot':>12} {'Avg Revenue':>12}"
    )
    print("-" * 85)

    for lang in languages_data:
        language_name = lang[0][:14]
        blocks = lang[1] or 0
        revenue = lang[2] or 0
        spots = lang[3] or 0
        rev_per_spot = lang[4] or 0
        avg_revenue = lang[5] or 0

        print(
            f"{language_name:15} {blocks:>7} ${revenue:>11,.0f} {spots:>8,} ${rev_per_spot:>11.2f} ${avg_revenue:>11.2f}"
        )


def display_top_blocks(blocks_data: List[Tuple], limit: int = 15) -> None:
    """Display top performing blocks"""
    print(f"\n🏆 TOP {limit} PERFORMING LANGUAGE BLOCKS")
    print("-" * 50)
    print(
        f"{'Block Name':25} {'Language':12} {'Market':8} {'Time Slot':15} {'Revenue':>12} {'Spots':>7} {'Months':>7}"
    )
    print("-" * 105)

    for block in blocks_data:
        block_name = block[0][:24] if block[0] else "Unknown"
        language = block[1][:11] if block[1] else "Unknown"
        market = block[2] if block[2] else "Unknown"
        time_slot = block[3][:14] if block[3] else "Unknown"
        revenue = block[4] or 0
        spots = block[5] or 0
        months = block[6] or 0

        print(
            f"{block_name:25} {language:12} {market:8} {time_slot:15} ${revenue:>11,.0f} {spots:>7} {months:>7}"
        )


def display_market_performance(markets_data: List[Tuple]) -> None:
    """Display market performance"""
    print(f"\n🏢 MARKET PERFORMANCE")
    print("-" * 25)
    print(
        f"{'Market':15} {'Code':6} {'Blocks':>7} {'Languages':>10} {'Revenue':>12} {'Spots':>8} {'Avg/Block':>10}"
    )
    print("-" * 85)

    for market in markets_data:
        market_name = market[0][:14] if market[0] else "Unknown"
        code = market[1] if market[1] else "N/A"
        blocks = market[2] or 0
        languages = market[3] or 0
        revenue = market[4] or 0
        spots = market[5] or 0
        avg_per_block = market[6] or 0

        print(
            f"{market_name:15} {code:6} {blocks:>7} {languages:>10} ${revenue:>11,.0f} {spots:>8,} ${avg_per_block:>9,.0f}"
        )


def display_time_slot_performance(timeslots_data: List[Tuple]) -> None:
    """Display time slot performance"""
    print(f"\n⏰ TIME SLOT PERFORMANCE")
    print("-" * 30)
    print(
        f"{'Time Slot':15} {'Blocks':>7} {'Revenue':>12} {'Spots':>8} {'Revenue/Block':>13} {'Avg Revenue':>12}"
    )
    print("-" * 85)

    for slot in timeslots_data:
        day_part = slot[0][:14] if slot[0] else "Unknown"
        blocks = slot[1] or 0
        revenue = slot[2] or 0
        spots = slot[3] or 0
        rev_per_block = slot[4] or 0
        avg_revenue = slot[5] or 0

        print(
            f"{day_part:15} {blocks:>7} ${revenue:>11,.0f} {spots:>8,} ${rev_per_block:>12,.0f} ${avg_revenue:>11.2f}"
        )


def display_comprehensive_blocks(processed_data: Dict[str, Any]) -> None:
    """Display comprehensive block performance"""
    print(f"\n📈 COMPREHENSIVE BLOCK PERFORMANCE ANALYSIS")
    print("-" * 55)

    if not processed_data["blocks"]:
        print("No language blocks found for analysis.")
        return

    print(
        f"{'Block Name':25} {'Language':10} {'Mkt':4} {'Day':3} {'Time':12} {'Revenue':>10} {'Spots':>6} {'$/Spot':>7} {'Cust':>4} {'Flag':>12}"
    )
    print("-" * 110)

    for block in processed_data["blocks"]:
        block_name = block[0][:24] if block[0] else "Unknown"
        language = block[1][:9] if block[1] else "Unknown"
        market = block[2][:3] if block[2] else "N/A"
        day = block[3][:3] if block[3] else "N/A"
        time_slot = block[5][:11] if block[5] else "Unknown"
        revenue = block[6] or 0
        spots = block[7] or 0
        rev_per_spot = block[8] or 0
        customers = block[12] or 0
        flag = block[13]

        # Color coding for CLI
        flag_display = {
            "UNUSED": "🔴 UNUSED",
            "VERY_LOW": "🟠 VERY_LOW",
            "LOW_VALUE": "🟡 LOW_VALUE",
            "SINGLE_CUSTOMER": "🟣 SINGLE_CUST",
            "PERFORMING": "🟢 PERFORMING",
        }.get(flag, flag)

        print(
            f"{block_name:25} {language:10} {market:4} {day:3} {time_slot:12} ${revenue:>9,.0f} {spots:>6} ${rev_per_spot:>6.0f} {customers:>4} {flag_display:>12}"
        )

    # Display summary
    summary = processed_data["summary"]
    print(f"\n📊 PERFORMANCE SUMMARY:")
    print(f"   • Total Language Blocks: {summary['total_blocks']}")
    print(
        f"   • Unused Blocks: {summary['unused_count']} ({summary['unused_percentage']:.1f}%)"
    )
    print(
        f"   • Weak Performing: {summary['weak_count']} ({summary['weak_percentage']:.1f}%)"
    )
    print(
        f"   • Single Customer Risk: {summary['single_customer_count']} ({summary['single_customer_percentage']:.1f}%)"
    )


def display_bottom_blocks(processed_data: Dict[str, Any]) -> None:
    """Display bottom performing blocks"""
    print(f"\n📉 BOTTOM 15 DAYTIME/PRIMETIME LANGUAGE BLOCKS")
    print("-" * 55)
    print(
        "Criteria: Day through primetime hours (6 AM - 11 PM) • Must have revenue > $0 • Must have spots assigned"
    )
    print()

    if not processed_data["blocks"]:
        print("No qualifying language blocks found for analysis.")
        return

    print(
        f"{'Rank':>4} {'Block Name':25} {'Language':10} {'Mkt':4} {'Day':3} {'Time':12} {'Revenue':>9} {'Spots':>6} {'$/Spot':>7} {'Cust':>4} {'Rev/Mo':>7} {'Issue':15}"
    )
    print("-" * 125)

    for processed_block in processed_data["blocks"]:
        rank = processed_block["rank"]
        block = processed_block["data"]
        primary_issue = processed_block["primary_issue"]

        block_name = block[0][:24] if block[0] else "Unknown"
        language = block[1][:9] if block[1] else "Unknown"
        market = block[2][:3] if block[2] else "N/A"
        day = block[3][:3] if block[3] else "N/A"
        time_slot = block[5][:11] if block[5] else "Unknown"
        revenue = block[6] or 0
        spots = block[7] or 0
        rev_per_spot = block[8] or 0
        customers = block[9] or 0
        revenue_per_month = block[12] or 0

        print(
            f"{rank:>4} {block_name:25} {language:10} {market:4} {day:3} {time_slot:12} ${revenue:>8,.0f} {spots:>6} ${rev_per_spot:>6.0f} {customers:>4} ${revenue_per_month:>6,.0f} {primary_issue:15}"
        )

    # Display summary
    summary = processed_data["summary"]
    print(f"\n📊 BOTTOM 15 SUMMARY:")
    print(f"   • Total Revenue: ${summary['total_revenue']:,.0f}")
    print(f"   • Total Spots: {summary['total_spots']:,}")
    print(f"   • Average Revenue/Spot: ${summary['avg_revenue_per_spot']:.2f}")
    print(f"   • Combined Monthly Revenue: ${summary['monthly_revenue']:,.0f}")

    print(f"\n🔍 PRIMARY ISSUES IDENTIFIED:")
    for issue, count in processed_data["issues"].items():
        if count > 0:
            percentage = (count / 15) * 100
            print(f"   • {issue}: {count} blocks ({percentage:.1f}%)")

    print(f"\n💡 IMMEDIATE ACTIONS FOR BOTTOM 15:")
    print(f"   🎯 ELIMINATE: Blocks with <$1,000 total revenue and <$20/spot")
    print(f"   🔄 CONSOLIDATE: Merge blocks in same time slots with similar issues")
    print(f"   📈 IMPROVE: Focus sales efforts on single-customer blocks")
    print(f"   ⏰ REALLOCATE: Move successful programming to these time slots")
    print(f"   📋 ANALYZE: Review programming content quality for poor $/spot blocks")


def display_key_insights(
    insights_data: Dict[str, Any], year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Display key insights"""
    print(f"\n💡 KEY INSIGHTS & RECOMMENDATIONS")
    print("-" * 40)

    most_profitable = insights_data["most_profitable_language"]
    busiest_slot = insights_data["busiest_time_slot"]
    period = insights_data["analysis_period"]

    if most_profitable:
        print(f"🔸 Most Profitable Language:")
        print(f"   {most_profitable[0]} generating ${most_profitable[1]:.2f} per spot")
        print(f"   ({most_profitable[2]:,} spots analyzed)")

    if busiest_slot:
        print(f"\n🔸 Busiest Time Slot:")
        print(f"   {busiest_slot[0]} with {busiest_slot[1]:,} spots")

    if period:
        earliest = period[0][:10] if period[0] else "Unknown"
        latest = period[1][:10] if period[1] else "Unknown"
        months = period[2] or 0
        year_display = f" ({year_filter})" if year_filter else ""
        print(f"\n🔸 Analysis Period{year_display}:")
        print(f"   {earliest} to {latest} ({months} months)")


# =============================================================================
# MAIN ANALYSIS FUNCTIONS (Issue #3 Fix - Orchestration Layer)
# =============================================================================


def generate_performance_overview(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate Performance Overview section"""
    data = get_performance_overview_data(conn, year_filter)
    display_performance_overview(data)


def generate_language_performance_analysis(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate Language Performance Analysis section"""
    data = get_language_performance_data(conn, year_filter)
    display_language_performance(data)


def generate_top_performing_blocks(
    conn: sqlite3.Connection,
    year_filter: Optional[Union[str, List[str]]],
    limit: int = 15,
) -> None:
    """Generate Top Performing Language Blocks section"""
    data = get_top_blocks_data(conn, year_filter, limit)
    display_top_blocks(data, limit)


def generate_market_performance(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate Market Performance section"""
    data = get_market_performance_data(conn, year_filter)
    display_market_performance(data)


def generate_time_slot_performance(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate Time Slot Performance section"""
    data = get_time_slot_performance_data(conn, year_filter)
    display_time_slot_performance(data)


def generate_comprehensive_block_performance(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate comprehensive language block performance analysis"""
    raw_data = get_comprehensive_block_data(conn, year_filter)
    processed_data = process_comprehensive_block_data(raw_data)
    display_comprehensive_blocks(processed_data)


def generate_bottom_15_daytime_blocks(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate bottom 15 performing language blocks"""
    raw_data = get_bottom_daytime_blocks_data(conn, year_filter)
    processed_data = process_bottom_blocks_data(raw_data)
    display_bottom_blocks(processed_data)


def generate_key_insights(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Generate Key Insights & Recommendations section"""
    data = get_insights_data(conn, year_filter)
    display_key_insights(data, year_filter)


def run_comprehensive_analysis(
    conn: sqlite3.Connection, year_filter: Optional[Union[str, List[str]]]
) -> None:
    """Run the complete comprehensive analysis"""

    # Header
    print("=" * 90)
    if year_filter:
        if isinstance(year_filter, list):
            year_str = " vs ".join(year_filter)
            print(f"COMPREHENSIVE LANGUAGE BLOCK ANALYSIS: {year_str}")
        else:
            print(f"COMPREHENSIVE LANGUAGE BLOCK ANALYSIS: {year_filter}")
    else:
        print("COMPREHENSIVE LANGUAGE BLOCK ANALYSIS: ALL DATA")
    print("=" * 90)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(
        "🔧 REFACTORED: Centralized year filters, separated concerns & FIXED month counting"
    )

    # Run all analysis sections
    generate_performance_overview(conn, year_filter)
    generate_language_performance_analysis(conn, year_filter)
    generate_top_performing_blocks(conn, year_filter)
    generate_bottom_15_daytime_blocks(conn, year_filter)
    generate_comprehensive_block_performance(conn, year_filter)
    generate_market_performance(conn, year_filter)
    generate_time_slot_performance(conn, year_filter)
    generate_key_insights(conn, year_filter)

    # Footer
    print(f"\n{'=' * 90}")
    print("✅ Comprehensive Language Block Analysis Complete!")
    print(
        "🔧 Code refactored: Centralized filters, separated concerns & FIXED month counting"
    )
    print("📊 Issues #2 & #3 resolved + month calculation corrected")
    print(f"{'=' * 90}")


def interactive_mode(conn: sqlite3.Connection) -> None:
    """Interactive mode for selecting analysis options"""
    available_years = get_available_years(conn)

    print("🎯 INTERACTIVE LANGUAGE BLOCK ANALYSIS")
    print("=" * 50)
    print(f"Available years: {', '.join(available_years)}")
    print("\nOptions:")
    print("1. Single year analysis")
    print("2. Compare two years")
    print("3. All data analysis")
    print("4. Custom year range")

    while True:
        choice = input("\nSelect option (1-4): ").strip()

        if choice == "1":
            print(f"\nAvailable years: {', '.join(available_years)}")
            year = input("Enter year: ").strip()
            if year in available_years:
                run_comprehensive_analysis(conn, year)
                break
            else:
                print(f"❌ Invalid year. Available: {', '.join(available_years)}")

        elif choice == "2":
            print(f"\nAvailable years: {', '.join(available_years)}")
            year1 = input("Enter first year: ").strip()
            year2 = input("Enter second year: ").strip()
            if year1 in available_years and year2 in available_years:
                run_comprehensive_analysis(conn, [year1, year2])
                break
            else:
                print(f"❌ Invalid year(s). Available: {', '.join(available_years)}")

        elif choice == "3":
            run_comprehensive_analysis(conn, None)
            break

        elif choice == "4":
            print(f"\nAvailable years: {', '.join(available_years)}")
            years_input = input("Enter years separated by commas: ").strip()
            years = [y.strip() for y in years_input.split(",")]
            if all(year in available_years for year in years):
                run_comprehensive_analysis(conn, years)
                break
            else:
                print(f"❌ Invalid year(s). Available: {', '.join(available_years)}")
        else:
            print("❌ Invalid option. Please select 1-4.")


def main() -> int:
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="REFACTORED: Enhanced Language Block Analysis Tool"
    )
    parser.add_argument(
        "--database", default="data/database/production.db", help="Database path"
    )

    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--year", help="Analyze specific year (e.g., 2024)")
    mode_group.add_argument(
        "--compare", nargs=2, metavar=("YEAR1", "YEAR2"), help="Compare two years"
    )
    mode_group.add_argument(
        "--all", action="store_true", help="Analyze all available data"
    )
    mode_group.add_argument(
        "--interactive", action="store_true", help="Interactive mode"
    )

    args = parser.parse_args()

    # Connect to database
    conn = get_database_connection(args.database)
    if not conn:
        return 1

    try:
        available_years = get_available_years(conn)

        if args.interactive:
            interactive_mode(conn)

        elif args.year:
            if args.year not in available_years:
                print(
                    f"❌ Year {args.year} not found. Available: {', '.join(available_years)}"
                )
                return 1
            run_comprehensive_analysis(conn, args.year)

        elif args.compare:
            year1, year2 = args.compare
            if year1 not in available_years or year2 not in available_years:
                print(f"❌ Invalid year(s). Available: {', '.join(available_years)}")
                return 1
            run_comprehensive_analysis(conn, [year1, year2])

        elif args.all:
            run_comprehensive_analysis(conn, None)

        return 0

    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        return 1

    finally:
        conn.close()


if __name__ == "__main__":
    exit(main())
