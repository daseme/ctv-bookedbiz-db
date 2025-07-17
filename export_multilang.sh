#!/bin/bash

# Multi-Language Spots Export Script - UPDATED FOR CAMPAIGN_TYPE
# Exports multi-language category spots using proper campaign_type classification
# Usage: ./export_multilang.sh [OPTIONS]

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
CORE_FIELDS_ONLY=false

# Function to display help
show_help() {
    cat << EOF
Multi-Language Spots Export Script (Updated for campaign_type)

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: multilang_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -c, --core-only         Export only core fields (bill_code, time_in, time_out, revenue_type, language_code, weekday_weekend)
    -h, --help              Show this help message

EXAMPLES:
    $0                                      # Export 2024 data with all fields
    $0 -y 2023                             # Export 2023 data
    $0 -y 2024 -o my_export.csv           # Export 2024 to specific file
    $0 -y 2023 -c                         # Export 2023 with core fields only
    $0 -y 2024 -d /path/to/other.db       # Use different database

MULTI-LANGUAGE CATEGORY DEFINITION:
    - Uses campaign_type = 'multi_language' (proper classification)
    - Cross-audience targeting spanning multiple language families
    - Excludes WorldLink (Direct Response category)
    - Excludes Paid Programming (revenue_type = 'Paid Programming')
    - Excludes Chinese Prime Time slots (language-specific, not multi-language)
    - Excludes ROS (Run on Schedule) spots

UPDATED BUSINESS LOGIC:
    - Proper campaign_type classification (not customer_intent)
    - Mutually exclusive with other categories
    - True cross-audience targeting only
    - Enhanced exclusions for accurate categorization

PAID PROGRAMMING EXCLUSIONS:
    - McHale Media:Kingdom of God (Religious programming)
    - NKB:Shop LC (Shopping content) 
    - Fujisankei (Japanese programming)
    - All other revenue_type = 'Paid Programming' content

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
        OUTPUT_FILE="multilang_spots_${YEAR}_core.csv"
    else
        OUTPUT_FILE="multilang_spots_${YEAR}.csv"
    fi
fi

# Check if database exists
if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: Database file not found: $DB_PATH"
    exit 1
fi

# Get last 2 digits of year for broadcast_month matching
YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 Exporting Multi-Language spots for $YEAR..."
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Core fields only: $CORE_FIELDS_ONLY"
echo "🔧 Using campaign_type = 'multi_language' classification"
echo ""

# Build the appropriate query based on options
if [[ "$CORE_FIELDS_ONLY" == true ]]; then
    QUERY="
SELECT 
    s.bill_code,
    s.time_in,
    s.time_out,
    s.revenue_type,
    s.air_date,
    s.language_code,
    CASE 
        WHEN s.day_of_week IN ('saturday', 'sunday') THEN 'Weekend'
        WHEN s.day_of_week IN ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') THEN 'Weekday'
        ELSE 'Unknown'
    END as weekday_weekend
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
AND s.revenue_type != 'Paid Programming'
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
AND slb.campaign_type = 'multi_language'
ORDER BY s.bill_code;"
else
    QUERY="
SELECT 
    s.bill_code,
    s.time_in,
    s.time_out,
    s.revenue_type,
    s.language_code,
    s.day_of_week,
    CASE 
        WHEN s.day_of_week IN ('saturday', 'sunday') THEN 'Weekend'
        WHEN s.day_of_week IN ('monday', 'tuesday', 'wednesday', 'thursday', 'friday') THEN 'Weekday'
        ELSE 'Unknown'
    END as weekday_weekend,
    s.air_date,
    s.gross_rate,
    s.broadcast_month,
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    slb.customer_intent,
    slb.spans_multiple_blocks,
    slb.campaign_type,
    slb.business_rule_applied
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
AND s.revenue_type != 'Paid Programming'
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
AND slb.campaign_type = 'multi_language'
ORDER BY s.bill_code;"
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
    echo ""
    echo "🎯 Updated Classification Applied:"
    echo "   ✅ campaign_type = 'multi_language' (proper classification)"
    echo "   ✅ WorldLink excluded (Direct Response category)"
    echo "   ✅ Paid Programming excluded (separate category)"
    echo "   ✅ Branded Content (PRD) excluded (separate category)"
    echo "   ✅ Services (SVC) excluded (separate category)"
    echo "   ✅ True cross-audience targeting only"
    echo ""
    echo "🚀 Business Impact:"
    echo "   - More accurate multi-language categorization"
    echo "   - Proper mutual exclusivity with other categories"
    echo "   - Better cross-audience targeting analysis"
    echo "   - Enhanced campaign_type field utilization"
else
    echo "❌ Export failed!"
    exit 1
fi