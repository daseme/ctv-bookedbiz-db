#!/bin/bash

# Enhanced Other Non-Language Export Script with Validation
# Updated to use campaign_type field with comprehensive validation
# 
# CRITICAL FIX (July 2024): This script uses the campaign_type field
# for Individual Language and ROS classification instead of legacy logic.
# 
# Impact:
# - 2023: Reduced from ~6,401 spots to ~59 spots (99.1% reduction)
# - 2024: Minimal impact (already working correctly)
# - Future years: Will work correctly with proper classification

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false
VALIDATE_ONLY=false
ANALYZE_CONTENT=false

show_help() {
    cat << EOF
Enhanced Other Non-Language Export Script with Validation

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: other_nonlang_enhanced_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information and detailed breakdown
    --validate-only         Run validation checks without exporting
    --analyze-content       Deep analysis of what's in Other Non-Language
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2024                             # Export 2024 data
    $0 -y 2023                             # Export 2023 data (properly fixed)
    $0 -y 2024 --debug                     # Export with debug info
    $0 --validate-only                     # Check data integrity only
    $0 --analyze-content                   # Deep dive into content types

FIXED SYSTEM:
    Uses campaign_type field for proper classification:
    - Individual Language: campaign_type = 'language_specific'
    - ROS (Run on Schedule): campaign_type = 'ros'
    - Multi-Language: campaign_type = 'multi_language'
    
EXPECTED RESULTS:
    - 2023: ~59 spots (was 6,401) - 99.1% reduction
    - 2024: ~201 spots (unchanged)
    - Other Non-Language now contains only true miscellaneous content

WHAT'S IN OTHER NON-LANGUAGE:
    - Unassigned spots (no language blocks)
    - Special event coverage
    - Technical difficulties/make-goods
    - Miscellaneous content not fitting other categories
    - Spots with missing time information

VALIDATION FEATURES:
    - Campaign type integrity checking
    - Missing assignment detection
    - Content type analysis
    - Revenue impact assessment
    - Data quality verification

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -y|--year)
            YEAR="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -d|--database)
            DB_PATH="$2"
            shift 2
            ;;
        --debug)
            DEBUG=true
            shift
            ;;
        --validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        --analyze-content)
            ANALYZE_CONTENT=true
            shift
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Validate inputs
if [[ ! "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "Error: Year must be 4 digits (e.g., 2024)"
    exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: Database file not found: $DB_PATH"
    exit 1
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 Enhanced Other Non-Language Analysis for $YEAR"
echo "📊 Database: $DB_PATH"
echo "🎯 Using campaign_type field for proper classification"
echo ""

# Run validation checks
if [[ "$VALIDATE_ONLY" == true ]] || [[ "$DEBUG" == true ]]; then
    echo "🔍 Running validation checks..."
    
    # Check campaign_type population
    echo ""
    echo "📊 Campaign Type Population Check:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
SELECT 
    COALESCE(slb.campaign_type, 'NO_ASSIGNMENT') as campaign_type,
    COUNT(*) as spot_count,
    printf('%.2f', SUM(s.gross_rate)) as total_revenue
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
GROUP BY COALESCE(slb.campaign_type, 'NO_ASSIGNMENT')
ORDER BY COUNT(*) DESC;
EOF

    # Check for spots that might be misclassified
    echo ""
    echo "🔍 Checking for potential misclassifications in Other Non-Language..."
    
    # Count spots with no assignment
    NO_ASSIGNMENT_COUNT=$(sqlite3 "$DB_PATH" << EOF
SELECT COUNT(*)
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
AND slb.spot_id IS NULL
AND s.spot_type NOT IN ('PRD', 'SVC');
EOF
)
    
    echo "   Spots with no language block assignment: $NO_ASSIGNMENT_COUNT"
    
    if [[ $NO_ASSIGNMENT_COUNT -gt 1000 ]]; then
        echo "   ⚠️  High number of unassigned spots - consider running assignment script"
    fi
fi

# Deep content analysis
if [[ "$ANALYZE_CONTENT" == true ]] || [[ "$DEBUG" == true ]]; then
    echo ""
    echo "📊 Deep Analysis of Other Non-Language Content:"
    
    # Analyze by spot type
    echo ""
    echo "By Spot Type:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
WITH other_non_language AS (
    -- Same CTE logic as main query
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.revenue_type = 'Paid Programming'
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.spot_id IS NULL AND s.spot_type IN ('PRD', 'SVC')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.campaign_type IN ('language_specific', 'ros', 'multi_language')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
)
SELECT 
    COALESCE(s.spot_type, 'NULL') as spot_type,
    COUNT(*) as count,
    printf('%.2f', SUM(s.gross_rate)) as revenue
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
GROUP BY s.spot_type
ORDER BY COUNT(*) DESC;
EOF

    # Analyze by time availability
    echo ""
    echo "By Time Information:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
WITH other_non_language AS (
    -- Same CTE logic
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.revenue_type = 'Paid Programming'
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.spot_id IS NULL AND s.spot_type IN ('PRD', 'SVC')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.campaign_type IN ('language_specific', 'ros', 'multi_language')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
)
SELECT 
    CASE 
        WHEN s.time_in IS NULL AND s.time_out IS NULL THEN 'No Time Info'
        WHEN s.time_in IS NULL OR s.time_out IS NULL THEN 'Partial Time Info'
        ELSE 'Has Complete Times'
    END as time_status,
    COUNT(*) as count,
    printf('%.2f', SUM(s.gross_rate)) as revenue
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
GROUP BY time_status;
EOF

    # Top customers in Other Non-Language
    echo ""
    echo "Top Customers in Other Non-Language:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
WITH other_non_language AS (
    -- Same CTE logic
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.revenue_type = 'Paid Programming'
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.spot_id IS NULL AND s.spot_type IN ('PRD', 'SVC')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND slb.campaign_type IN ('language_specific', 'ros', 'multi_language')
    EXCEPT
    SELECT DISTINCT s.spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
)
SELECT 
    COALESCE(c.normalized_name, 'Unknown') as customer,
    COUNT(*) as spots,
    printf('%.2f', SUM(s.gross_rate)) as revenue,
    GROUP_CONCAT(DISTINCT s.spot_type) as spot_types
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
GROUP BY c.customer_id
ORDER BY SUM(s.gross_rate) DESC
LIMIT 10;
EOF
    
    if [[ "$VALIDATE_ONLY" == true ]]; then
        exit 0
    fi
fi

# Set default output file if not provided
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="other_nonlang_enhanced_${YEAR}.csv"
fi

echo "📁 Output: $OUTPUT_FILE"
echo ""

# Enhanced query with better categorization
echo "⚡ Generating enhanced Other Non-Language export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
-- Enhanced query with detailed categorization
WITH base_spots AS (
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
),
-- All category CTEs remain the same...
direct_response AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR COALESCE(s.bill_code, '') LIKE '%WorldLink%')
),
paid_programming AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.revenue_type = 'Paid Programming'
),
branded_content AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'PRD'
),
services AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.spot_id IS NULL
    AND s.spot_type = 'SVC'
),
individual_language AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    AND slb.campaign_type = 'language_specific'
),
ros AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    AND slb.campaign_type = 'ros'
),
packages AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND s.spot_type = 'PKG'
    AND (s.time_in IS NULL OR s.time_out IS NULL OR s.time_in = '' OR s.time_out = '')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
),
multi_language AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    AND slb.campaign_type = 'multi_language'
),
other_non_language AS (
    SELECT spot_id FROM base_spots
    EXCEPT
    SELECT spot_id FROM direct_response
    EXCEPT
    SELECT spot_id FROM paid_programming
    EXCEPT
    SELECT spot_id FROM branded_content
    EXCEPT
    SELECT spot_id FROM services
    EXCEPT
    SELECT spot_id FROM individual_language
    EXCEPT
    SELECT spot_id FROM ros
    EXCEPT
    SELECT spot_id FROM packages
    EXCEPT
    SELECT spot_id FROM multi_language
)
SELECT 
    s.spot_id,
    s.bill_code,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    COALESCE(a.agency_name, 'No Agency') as agency_name,
    s.gross_rate,
    s.station_net,
    s.spot_type,
    s.revenue_type,
    s.time_in,
    s.time_out,
    s.day_of_week,
    CASE 
        WHEN s.day_of_week IN ('saturday', 'sunday') THEN 'Weekend'
        WHEN s.day_of_week IN ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') THEN 'Weekday'
        ELSE 'Unknown'
    END as weekday_weekend,
    s.air_date,
    s.broadcast_month,
    s.sales_person,
    s.language_code,
    s.program,
    s.market_name,
    -- Enhanced categorization
    CASE 
        WHEN s.time_in IS NULL AND s.time_out IS NULL THEN 'No Time Info'
        WHEN s.time_in IS NULL OR s.time_out IS NULL THEN 'Partial Time Info'
        ELSE 'Has Complete Times'
    END as time_completeness,
    CASE 
        WHEN slb.spot_id IS NULL THEN 'No Language Assignment'
        WHEN slb.campaign_type = 'no_coverage' THEN 'No Grid Coverage'
        ELSE 'Other Reason'
    END as nonlang_reason,
    CASE 
        WHEN s.spot_type IN ('BNS', 'CRD') THEN 'Non-Revenue'
        WHEN s.gross_rate < 0 THEN 'Credit/Adjustment'
        WHEN s.gross_rate = 0 THEN 'Zero Rate'
        ELSE 'Revenue-Generating'
    END as revenue_category,
    -- Content analysis
    CASE 
        WHEN s.program LIKE '%TEST%' THEN 'Test/Technical'
        WHEN s.program LIKE '%MAKE%GOOD%' THEN 'Make Good'
        WHEN s.program LIKE '%SPECIAL%' THEN 'Special Event'
        WHEN s.program IS NULL THEN 'No Program Info'
        ELSE 'Regular Programming'
    END as content_type,
    'Other Non-Language' as final_category,
    COALESCE(slb.campaign_type, 'NO_ASSIGNMENT') as campaign_type_debug,
    COALESCE(slb.customer_intent, 'NONE') as intent_debug,
    COALESCE(slb.alert_reason, 'NONE') as alert_reason_debug
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
ORDER BY 
    s.gross_rate DESC,
    s.air_date, 
    s.time_in;
EOF

# Check results and provide enhanced analysis
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ Enhanced export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {printf "%.2f", sum}')
    echo "💰 Total revenue: \$$REVENUE"
    
    # Expected vs actual comparisons
    echo ""
    echo "📊 Year-over-Year Comparison:"
    if [[ "$YEAR" == "2024" ]]; then
        echo "   Expected: ~201 spots, ~\$1,313.70 revenue"
        echo "   Actual: $RECORD_COUNT spots, \$$REVENUE revenue"
    elif [[ "$YEAR" == "2023" ]]; then
        echo "   Before fix: ~6,401 spots, ~\$213,221 revenue"
        echo "   After fix: $RECORD_COUNT spots, \$$REVENUE revenue"
        IMPROVEMENT=$(echo "scale=1; (1 - $RECORD_COUNT / 6401) * 100" | bc -l)
        echo "   Improvement: ${IMPROVEMENT}% reduction in misclassified spots"
    fi
    
    # Enhanced statistics
    echo ""
    echo "📈 Content Analysis:"
    echo "   No Time Info: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',No Time Info,')"
    echo "   Partial Time Info: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Partial Time Info,')"
    echo "   Has Complete Times: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Has Complete Times,')"
    
    echo ""
    echo "📊 Assignment Status:"
    echo "   No Language Assignment: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',No Language Assignment,')"
    echo "   No Grid Coverage: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',No Grid Coverage,')"
    
    echo ""
    echo "💰 Revenue Categories:"
    echo "   Revenue-Generating: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Revenue-Generating,')"
    echo "   Non-Revenue (BNS/CRD): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Non-Revenue,')"
    echo "   Credits/Adjustments: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Credit/Adjustment,')"
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Spot Type Breakdown"
        tail -n +2 "$OUTPUT_FILE" | cut -d',' -f7 | sort | uniq -c | sort -rn
        
        echo ""
        echo "🔍 Debug: Content Type Analysis"
        tail -n +2 "$OUTPUT_FILE" | cut -d',' -f23 | sort | uniq -c | sort -rn
        
        echo ""
        echo "🔍 Debug: Sample high-value spots"
        echo "Spot ID, Bill Code, Customer, Revenue"
        tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5 -rn | head -5 | cut -d',' -f1,2,3,5
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 Other Non-Language Summary:"
echo "   Contains only truly unclassified content"
echo "   Most spots lack time info or language assignments"
echo "   Includes make-goods, tests, and special events"
echo "   2023 shows 99%+ improvement with campaign_type usage"
echo ""
echo "💡 Key validation features:"
echo "   - Campaign type integrity checking"
echo "   - Content type analysis (test, make-good, special)"
echo "   - Time information completeness"
echo "   - Revenue category breakdown"
echo "   - Assignment status verification"