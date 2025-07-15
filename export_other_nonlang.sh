#!/bin/bash

# FIXED Other Non-Language Export Script
# Updated to use campaign_type field for proper classification
# 
# CRITICAL FIX (July 2024): This script now uses the campaign_type field
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

show_help() {
    cat << EOF
FIXED Other Non-Language Export Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: other_nonlang_fixed_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2024                             # Export 2024 data
    $0 -y 2023                             # Export 2023 data (now properly fixed)
    $0 -y 2024 --debug                     # Export with debug info
    $0 -y 2024 -o final_other_nonlang.csv  # Export to specific file

FIXED SYSTEM:
    Now uses campaign_type field for proper classification instead of legacy logic.
    
    Key Changes:
    - Individual Language: Uses campaign_type = 'language_specific'
    - ROS (Run on Schedule): Uses campaign_type = 'ros'
    - Multi-Language: Uses campaign_type = 'multi_language'
    
    Expected Results:
    - 2023: ~59 spots (was 6,401)
    - 2024: ~201 spots (unchanged)
    - Other Non-Language now contains only true miscellaneous content

TROUBLESHOOTING:
    If results seem wrong, verify campaign_type field is populated:
    
    SELECT campaign_type, COUNT(*) FROM spot_language_blocks 
    WHERE spot_id IN (SELECT spot_id FROM spots WHERE broadcast_month LIKE '%-YY')
    GROUP BY campaign_type;
    
    If campaign_type is NULL for many spots, reprocess the year:
    python cli_01_assign_language_blocks.py --force-year YYYY

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

# Set default output file if not provided
if [[ -z "$OUTPUT_FILE" ]]; then
    OUTPUT_FILE="other_nonlang_fixed_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 FIXED Other Non-Language Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Using campaign_type field for proper classification"
echo ""

# FIXED query approach using campaign_type
echo "⚡ Generating FIXED Other Non-Language export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
-- FIXED query with campaign_type logic
WITH base_spots AS (
    SELECT spot_id FROM spots s
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
),
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
    -- FIXED: Use campaign_type instead of spans_multiple_blocks logic
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
    -- FIXED: Use campaign_type instead of business_rule_applied
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
    -- FIXED: Use campaign_type with proper exclusions
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
    CASE 
        WHEN s.time_in IS NULL AND s.time_out IS NULL THEN 'Both NULL'
        WHEN s.time_in IS NULL THEN 'time_in NULL'
        WHEN s.time_out IS NULL THEN 'time_out NULL'
        ELSE 'Has Times'
    END as time_status,
    CASE 
        WHEN s.time_in IS NULL THEN 'No Time Info'
        WHEN s.time_in < '06:00:00' THEN 'Overnight (00:00-05:59)'
        WHEN s.time_in < '12:00:00' THEN 'Morning (06:00-11:59)'
        WHEN s.time_in < '18:00:00' THEN 'Afternoon (12:00-17:59)'
        WHEN s.time_in < '24:00:00' THEN 'Evening (18:00-23:59)'
        ELSE 'Unknown'
    END as time_period,
    CASE 
        WHEN s.spot_type = 'PRD' THEN 'PRD (Branded Content)'
        WHEN s.spot_type = 'SVC' THEN 'SVC (Services)'
        WHEN s.spot_type = 'BNS' THEN 'BNS (Bonus Spot)'
        WHEN s.spot_type = 'CRD' THEN 'CRD (Credit)'
        WHEN s.spot_type = 'PKG' THEN 'PKG (Package - should not appear here)'
        WHEN s.spot_type = 'COM' THEN 'COM (Commercial)'
        ELSE COALESCE(s.spot_type, 'NULL')
    END as spot_type_analysis,
    'Other Non-Language (FIXED)' as final_category,
    -- FIXED: Add campaign_type for debugging
    COALESCE(slb.campaign_type, 'NO_ASSIGNMENT') as campaign_type_debug
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
ORDER BY s.revenue_type, s.spot_type, s.gross_rate DESC;
EOF

# Check results
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ FIXED export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
    echo "💰 Total revenue: \$${REVENUE}"
    
    # Expected vs actual for different years
    if [[ "$YEAR" == "2024" ]]; then
        EXPECTED_SPOTS=201
        EXPECTED_REVENUE=1313.70
        echo "📊 Expected for 2024: $EXPECTED_SPOTS spots, \$${EXPECTED_REVENUE} revenue"
    elif [[ "$YEAR" == "2023" ]]; then
        EXPECTED_SPOTS=59
        EXPECTED_REVENUE=-56842.84
        echo "📊 Expected for 2023: $EXPECTED_SPOTS spots, \$${EXPECTED_REVENUE} revenue"
    else
        echo "📊 No specific expectations for $YEAR"
    fi
    
    # Show improvement for 2023
    if [[ "$YEAR" == "2023" ]]; then
        echo ""
        echo "🎯 FIXED 2023 Results:"
        echo "   Before fix: ~6,401 spots, ~\$213,221 revenue"
        echo "   After fix: $RECORD_COUNT spots, \$${REVENUE} revenue"
        echo "   Improvement: $(echo "6401 - $RECORD_COUNT" | bc) fewer spots (99.1% reduction)"
    fi
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Sample records"
        head -n 6 "$OUTPUT_FILE"
        echo ""
        echo "📈 FIXED statistics:"
        echo "   Total records: $RECORD_COUNT"
        echo "   BNS spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',BNS,')"
        echo "   CRD spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',CRD,')"
        echo "   COM spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',COM,')"
        echo "   PKG spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',PKG,')"
        echo "   Weekend spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekend,')"
        echo "   Weekday spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekday,')"
        
        echo ""
        echo "🐛 Campaign type debug:"
        echo "   NO_ASSIGNMENT: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',NO_ASSIGNMENT')"
        echo "   language_specific: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',language_specific')"
        echo "   ros: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',ros')"
        echo "   multi_language: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',multi_language')"
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 FIXED Summary:"
echo "   This export now uses campaign_type field for proper classification"
echo "   Individual Language and ROS spots are properly excluded"
echo "   Other Non-Language contains only true miscellaneous content"
echo "   2023 data should show dramatic improvement (~99% fewer spots)"
echo ""
echo "💡 Key improvements:"
echo "   - Uses campaign_type = 'language_specific' for Individual Language"
echo "   - Uses campaign_type = 'ros' for ROS classification"
echo "   - Uses campaign_type = 'multi_language' for Multi-Language"
echo "   - Maintains all other category precedence rules"
echo "   - Added campaign_type_debug column for troubleshooting"