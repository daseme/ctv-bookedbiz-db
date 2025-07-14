#!/bin/bash

# Other Non-Language Spots Export Script
# Exports spots that fall into the "Other Non-Language" category
# These are English-language general market spots outside specific language targeting
# Usage: ./export_other_nonlang.sh [OPTIONS]

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
CORE_FIELDS_ONLY=false

# Function to display help
show_help() {
    cat << EOF
Other Non-Language Spots Export Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: other_nonlang_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -c, --core-only         Export only core fields (bill_code, customer_name, gross_rate, spot_type, time_in, time_out, weekday_weekend)
    -h, --help              Show this help message

EXAMPLES:
    $0                                      # Export 2024 data with all fields
    $0 -y 2023                             # Export 2023 data
    $0 -y 2024 -o my_export.csv           # Export 2024 to specific file
    $0 -y 2023 -c                         # Export 2023 with core fields only
    $0 -y 2024 -d /path/to/other.db       # Use different database

DEFINITION:
    "Other Non-Language" spots are English-language general market advertising that:
    - Are Internal Ad Sales (not Direct Response/WorldLink)
    - Are NOT in specific language blocks
    - Are NOT multi-language (cross-audience)
    - Are NOT ROS (Run on Schedule) programming
    - Are NOT Paid Programming
    - Are NOT Branded Content (PRD) or Services (SVC)

TYPICAL CUSTOMERS:
    - Major brands (Aldi, Lexus, McDonald's)
    - Government agencies (CalTrans, DHCS, Cal Fire)
    - Public transit (Sound Transit)
    - General English-speaking market advertisers

EXCLUSIONS APPLIED:
    - WorldLink (Direct Response category)
    - Individual Language Blocks (language-specific targeting)
    - Multi-Language (cross-audience targeting)
    - ROS/Roadblocks (broadcast sponsorships)
    - Paid Programming (religious, shopping, etc.)
    - Branded Content (PRD) and Services (SVC)

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
        -c|--core-only)
            CORE_FIELDS_ONLY=true
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

# Validate year format
if [[ ! "$YEAR" =~ ^[0-9]{4}$ ]]; then
    echo "Error: Year must be 4 digits (e.g., 2024)"
    exit 1
fi

# Set default output file if not provided
if [[ -z "$OUTPUT_FILE" ]]; then
    if [[ "$CORE_FIELDS_ONLY" == true ]]; then
        OUTPUT_FILE="other_nonlang_spots_${YEAR}_core.csv"
    else
        OUTPUT_FILE="other_nonlang_spots_${YEAR}.csv"
    fi
fi

# Check if database exists
if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: Database file not found: $DB_PATH"
    exit 1
fi

# Get last 2 digits of year for broadcast_month matching
YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 Exporting Other Non-Language spots for $YEAR..."
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Core fields only: $CORE_FIELDS_ONLY"
echo ""

# Build the appropriate query based on options
if [[ "$CORE_FIELDS_ONLY" == true ]]; then
    QUERY="
SELECT 
    s.bill_code,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    s.gross_rate,
    s.spot_type,
    s.time_in,
    s.time_out,
    language_code,
    CASE 
        WHEN s.day_of_week IN ('saturday', 'sunday') THEN 'Weekend'
        WHEN s.day_of_week IN ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') THEN 'Weekday'
        ELSE 'Unknown'
    END as weekday_weekend
FROM spots s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
-- Must be Internal Ad Sales (not Direct Response)
AND s.revenue_type = 'Internal Ad Sales'
-- Exclude Direct Response (WorldLink)
AND NOT (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR 
         COALESCE(s.bill_code, '') LIKE '%WorldLink%')
-- Exclude Individual Language Blocks
AND NOT ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
-- Exclude Multi-Language (Cross-Audience)
AND NOT (slb.spans_multiple_blocks = 1)
-- Exclude Paid Programming
AND s.revenue_type != 'Paid Programming'
-- Exclude Branded Content (PRD) and Services (SVC) that are in other categories
AND NOT (s.spot_type = 'PRD' AND slb.spot_id IS NULL)
AND NOT (s.spot_type = 'SVC' AND slb.spot_id IS NULL)
ORDER BY s.bill_code, s.air_date;"
else
    QUERY="
SELECT 
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
    -- Language block status
    CASE 
        WHEN slb.spot_id IS NULL THEN 'Not in any language block'
        WHEN slb.spans_multiple_blocks = 1 THEN 'Multi-Language (should not appear)'
        WHEN slb.block_id IS NULL THEN 'In system but no block assigned'
        ELSE 'Has language block (should not appear)'
    END as language_block_status,
    slb.customer_intent,
    slb.assignment_method,
    -- Time period classification
    CASE 
        WHEN s.time_in < '06:00:00' THEN 'Overnight (00:00-05:59)'
        WHEN s.time_in < '12:00:00' THEN 'Morning (06:00-11:59)'
        WHEN s.time_in < '18:00:00' THEN 'Afternoon (12:00-17:59)'
        WHEN s.time_in < '24:00:00' THEN 'Evening (18:00-23:59)'
        ELSE 'Unknown'
    END as time_period
FROM spots s
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
-- Must be Internal Ad Sales (not Direct Response)
AND s.revenue_type = 'Internal Ad Sales'
-- Exclude Direct Response (WorldLink)
AND NOT (COALESCE(a.agency_name, '') LIKE '%WorldLink%' OR 
         COALESCE(s.bill_code, '') LIKE '%WorldLink%')
-- Exclude Individual Language Blocks
AND NOT ((slb.spans_multiple_blocks = 0 AND slb.block_id IS NOT NULL) OR 
         (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NOT NULL))
-- Exclude Multi-Language (Cross-Audience)
AND NOT (slb.spans_multiple_blocks = 1)
-- Exclude Paid Programming
AND s.revenue_type != 'Paid Programming'
-- Exclude Branded Content (PRD) and Services (SVC) that are in other categories
AND NOT (s.spot_type = 'PRD' AND slb.spot_id IS NULL)
AND NOT (s.spot_type = 'SVC' AND slb.spot_id IS NULL)
ORDER BY s.bill_code, s.air_date;"
fi

# Execute the query
echo "⚡ Running query..."
if sqlite3 -header -csv "$DB_PATH" "$QUERY" > "$OUTPUT_FILE"; then
    # Count the results
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ Export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    echo ""
    echo "🔍 Sample records:"
    head -n 6 "$OUTPUT_FILE"
    echo ""
    echo "📈 Quick stats:"
    echo "   Weekday spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekday')"
    echo "   Weekend spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekend')"
    if [[ "$CORE_FIELDS_ONLY" == false ]]; then
        echo "   COM spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',COM,')"
        echo "   BNS spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',BNS,')"
        echo "   Not in language blocks: $(tail -n +2 "$OUTPUT_FILE" | grep -c 'Not in any language block')"
    fi
    echo ""
    echo "🎯 Exclusions applied:"
    echo "   - WorldLink (Direct Response Sales)"
    echo "   - Individual Language Blocks (language-specific targeting)"
    echo "   - Multi-Language (cross-audience targeting)"
    echo "   - Paid Programming (religious, shopping, etc.)"
    echo "   - Branded Content (PRD) and Services (SVC) categories"
    echo ""
    echo "💡 These spots represent English-language general market advertising"
    echo "   that targets broad audiences outside specific language programming."
    echo ""
    echo "🔗 Related analysis:"
    echo "   - Run unified_analysis.py to see full revenue breakdown"
    echo "   - Check ROS analyzer for broadcast sponsorship categorization"
    echo "   - Compare with Multi-Language export for cross-audience targeting"
else
    echo "❌ Export failed!"
    exit 1
fi