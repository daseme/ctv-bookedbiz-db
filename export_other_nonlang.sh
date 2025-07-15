#!/bin/bash

# Working Other Non-Language Export Script
# Updated for 9-category system with Packages category

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false

show_help() {
    cat << EOF
Working Other Non-Language Export Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: other_nonlang_working_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2024                             # Export 2024 data
    $0 -y 2024 --debug                     # Export with debug info
    $0 -y 2024 -o final_other_nonlang.csv  # Export to specific file

UPDATED SYSTEM:
    Now works with 9-category system where package deals have been moved
    to a separate Packages category. Other Non-Language now contains:
    - 201 spots (down from 210)
    - $1,313.70 revenue (down from $42,160.35)
    - True miscellaneous content only

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
    OUTPUT_FILE="other_nonlang_working_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 Working Other Non-Language Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Using updated 9-category precedence logic"
echo ""

# Single query approach to avoid table name conflicts
echo "⚡ Generating Other Non-Language export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
-- Single query with all precedence logic
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
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
),
ros AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND slb.business_rule_applied IN ('ros_duration', 'ros_time')
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
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
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
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
    'Other Non-Language' as final_category
FROM other_non_language onl
JOIN spots s ON onl.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
ORDER BY s.revenue_type, s.spot_type, s.gross_rate DESC;
EOF

# Check results
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ Export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Validate against expected numbers
    if [[ "$RECORD_COUNT" == "201" ]]; then
        echo "✅ SUCCESS: Perfect match with updated unified analysis!"
        echo "✅ Record count: $RECORD_COUNT matches expected 201"
    else
        echo "❌ ISSUE: Record count $RECORD_COUNT doesn't match expected 201"
        echo "❌ Difference: $((RECORD_COUNT - 201))"
    fi
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Sample records"
        head -n 6 "$OUTPUT_FILE"
        echo ""
        echo "📈 Final statistics:"
        echo "   Total records: $RECORD_COUNT"
        echo "   BNS spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',BNS,')"
        echo "   CRD spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',CRD,')"
        echo "   COM spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',COM,')"
        echo "   PKG spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',PKG,')"
        echo "   Weekend spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekend,')"
        echo "   Weekday spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekday,')"
        
        # Check revenue
        REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
        echo "   Total revenue: \$${REVENUE}"
        
        if [[ $(echo "$REVENUE == 1313.70" | bc -l) == 1 ]]; then
            echo "   ✅ Revenue matches expected \$1,313.70"
        else
            echo "   ❌ Revenue doesn't match expected \$1,313.70"
        fi
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 Summary:"
echo "   Expected: 201 spots, \$1,313.70 revenue"
echo "   Actual: $RECORD_COUNT spots from export"
echo "   Package deals (~\$40K, 9 spots) now in separate Packages category"
echo ""
echo "💡 This export contains only true miscellaneous content:"
echo "   - Credit adjustments and billing corrections"
echo "   - Bonus spots and general commercials"
echo "   - Operational content without language targeting"
echo "   - NO package deals (moved to Packages category)"