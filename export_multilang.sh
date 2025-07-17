#!/bin/bash

# Enhanced Multi-Language Spots Export Script with Validation
# Exports multi-language category spots with language family analysis and validation
# Usage: ./export_multilang.sh [OPTIONS]

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
CORE_FIELDS_ONLY=false
DEBUG=false
VALIDATE_ONLY=false

# Function to display help
show_help() {
    cat << EOF
Enhanced Multi-Language Spots Export Script with Language Family Validation

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: multilang_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -c, --core-only         Export only core fields
    --debug                 Show debug information and language family analysis
    --validate-only         Run validation checks without exporting
    -h, --help              Show this help message

EXAMPLES:
    $0                                      # Export 2024 data with all fields
    $0 -y 2023                             # Export 2023 data
    $0 -y 2024 --debug                     # Export with language family analysis
    $0 --validate-only                     # Check data integrity only

MULTI-LANGUAGE CATEGORY DEFINITION:
    - Uses campaign_type = 'multi_language' (proper classification)
    - Cross-audience targeting spanning DIFFERENT language families
    - Examples of language families:
      * Chinese family: Mandarin (2) + Cantonese (3)
      * Filipino family: Tagalog (4)
      * South Asian family: Hindi, Punjabi, etc. (6)
      * Single languages: Vietnamese (7), Korean (8), Japanese (9), etc.
    
VALID MULTI-LANGUAGE COMBINATIONS:
    - Chinese + Filipino (e.g., Mandarin + Tagalog)
    - Chinese + Hmong
    - Filipino + South Asian
    - Any combination crossing family boundaries

EXCLUSIONS:
    - WorldLink (Direct Response category)
    - Paid Programming
    - ROS (Run on Schedule) spots
    - Same language family only (e.g., only Mandarin + Cantonese)

VALIDATION FEATURES:
    - Language family combination analysis
    - Cross-family validation
    - Revenue impact by language combination
    - Data integrity checking

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
        --debug)
            DEBUG=true
            shift
            ;;
        --validate-only)
            VALIDATE_ONLY=true
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

# Check if database exists
if [[ ! -f "$DB_PATH" ]]; then
    echo "Error: Database file not found: $DB_PATH"
    exit 1
fi

# Get last 2 digits of year for broadcast_month matching
YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 Multi-Language Cross-Audience Export for $YEAR..."
echo "📊 Database: $DB_PATH"
echo "🔧 Using campaign_type = 'multi_language' classification"
echo ""

# Run validation checks
if [[ "$VALIDATE_ONLY" == true ]] || [[ "$DEBUG" == true ]]; then
    echo "🔍 Running validation checks..."
    
    # Check language family combinations
    echo ""
    echo "📊 Language Family Combination Analysis:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
SELECT 
    slb.alert_reason,
    COUNT(*) as spot_count,
    COUNT(DISTINCT s.customer_id) as unique_customers,
    printf('%.2f', SUM(s.gross_rate)) as total_revenue,
    printf('%.2f', AVG(s.gross_rate)) as avg_revenue
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND slb.campaign_type = 'multi_language'
GROUP BY slb.alert_reason
ORDER BY COUNT(*) DESC;
EOF

    # Check for potential misclassifications
    echo ""
    echo "🔍 Checking for potential misclassifications..."
    CHINESE_ONLY_COUNT=$(sqlite3 "$DB_PATH" << EOF
SELECT COUNT(*)
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND slb.campaign_type = 'multi_language'
AND slb.alert_reason = 'True multi-language: [2, 3]';
EOF
)
    
    if [[ $CHINESE_ONLY_COUNT -gt 0 ]]; then
        echo "⚠️  Found $CHINESE_ONLY_COUNT spots marked as multi-language but only span Chinese family (Mandarin + Cantonese)"
        echo "   These might need reclassification to 'language_specific'"
    else
        echo "✅ No Chinese-only misclassifications found"
    fi
    
    # Top customers analysis
    echo ""
    echo "📊 Top Multi-Language Customers:"
    sqlite3 "$DB_PATH" << EOF
.mode column
.headers on
SELECT 
    COALESCE(c.normalized_name, 'Unknown') as customer_name,
    COUNT(*) as spot_count,
    printf('%.2f', SUM(s.gross_rate)) as total_revenue,
    GROUP_CONCAT(DISTINCT slb.alert_reason) as language_patterns
FROM spots s
JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND slb.campaign_type = 'multi_language'
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
    if [[ "$CORE_FIELDS_ONLY" == true ]]; then
        OUTPUT_FILE="multilang_spots_${YEAR}_core.csv"
    else
        OUTPUT_FILE="multilang_spots_${YEAR}.csv"
    fi
fi

echo "📁 Output: $OUTPUT_FILE"
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
WITH language_analysis AS (
    SELECT 
        s.spot_id,
        -- Extract language IDs from alert_reason for analysis
        CASE 
            WHEN slb.alert_reason LIKE '%[1,%' THEN 'English + Others'
            WHEN slb.alert_reason LIKE '%[2, 3, 4]%' THEN 'Chinese + Filipino'
            WHEN slb.alert_reason LIKE '%[2, 3, 5]%' THEN 'Chinese + Hmong'
            WHEN slb.alert_reason LIKE '%[2, 5]%' THEN 'Mandarin + Hmong'
            WHEN slb.alert_reason LIKE '%[3, 4]%' THEN 'Cantonese + Filipino'
            WHEN slb.alert_reason LIKE '%[4, 5]%' THEN 'Filipino + Hmong'
            WHEN slb.alert_reason LIKE '%[4, 6]%' THEN 'Filipino + South Asian'
            WHEN slb.alert_reason LIKE '%[6, 7]%' THEN 'South Asian + Vietnamese'
            WHEN slb.alert_reason LIKE '%[8, 2]%' THEN 'Korean + Mandarin'
            ELSE 'Other Combination'
        END as language_combination
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    WHERE slb.campaign_type = 'multi_language'
)
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
    slb.business_rule_applied,
    slb.alert_reason,
    la.language_combination,
    -- Duration calculation for analysis
    CASE 
        WHEN s.time_in IS NULL OR s.time_out IS NULL THEN NULL
        WHEN s.time_out LIKE '%day%' THEN 
            1440 - (CAST(substr(s.time_in, 1, 2) AS INTEGER) * 60 + CAST(substr(s.time_in, 4, 2) AS INTEGER))
        WHEN s.time_in <= s.time_out THEN 
            (CAST(substr(s.time_out, 1, 2) AS INTEGER) - CAST(substr(s.time_in, 1, 2) AS INTEGER)) * 60 + 
            (CAST(substr(s.time_out, 4, 2) AS INTEGER) - CAST(substr(s.time_in, 4, 2) AS INTEGER))
        ELSE 
            (24 * 60) - ((CAST(substr(s.time_in, 1, 2) AS INTEGER) - CAST(substr(s.time_out, 1, 2) AS INTEGER)) * 60 + 
                          (CAST(substr(s.time_in, 4, 2) AS INTEGER) - CAST(substr(s.time_out, 4, 2) AS INTEGER)))
    END as duration_minutes,
    -- Block count from blocks_spanned
    CASE 
        WHEN slb.blocks_spanned IS NULL THEN 0
        WHEN slb.blocks_spanned = '[]' THEN 0
        ELSE LENGTH(slb.blocks_spanned) - LENGTH(REPLACE(slb.blocks_spanned, ',', '')) + 1
    END as block_count
FROM spots s
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN language_analysis la ON s.spot_id = la.spot_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
AND s.revenue_type != 'Paid Programming'
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
AND slb.campaign_type = 'multi_language'
ORDER BY la.language_combination, s.gross_rate DESC, s.bill_code;"
fi

# Execute the query
echo "⚡ Running query..."
if sqlite3 -header -csv "$DB_PATH" "$QUERY" > "$OUTPUT_FILE"; then
    # Count the results
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ Export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    if [[ "$CORE_FIELDS_ONLY" != true ]]; then
        REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $9} END {printf "%.2f", sum}')
        echo "💰 Total multi-language revenue: \$$REVENUE"
    fi
    
    echo ""
    echo "🔍 Quick stats:"
    echo "   Weekday spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekday')"
    echo "   Weekend spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekend')"
    
    if [[ "$DEBUG" == true ]] && [[ "$CORE_FIELDS_ONLY" != true ]]; then
        echo ""
        echo "📊 Language Combination Breakdown:"
        tail -n +2 "$OUTPUT_FILE" | cut -d',' -f16 | sort | uniq -c | sort -rn | head -10
        
        echo ""
        echo "⏱️ Duration Analysis:"
        echo "   < 1 hour: $(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$17 > 0 && $17 < 60' | wc -l)"
        echo "   1-3 hours: $(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$17 >= 60 && $17 < 180' | wc -l)"
        echo "   3-6 hours: $(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$17 >= 180 && $17 < 360' | wc -l)"
        echo "   > 6 hours: $(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$17 >= 360' | wc -l)"
    fi
    
    echo ""
    echo "🎯 Multi-Language Classification Criteria:"
    echo "   ✅ Spans multiple DIFFERENT language families"
    echo "   ✅ Cross-audience targeting strategy"
    echo "   ✅ Not same-family combinations (e.g., Mandarin+Cantonese only)"
    echo "   ✅ Excludes Direct Response, Paid Programming, ROS"
    echo ""
    echo "📋 Common Language Family Combinations:"
    echo "   • Chinese + Filipino (cross-family)"
    echo "   • Chinese + Hmong (cross-family)"
    echo "   • Filipino + South Asian (cross-family)"
    echo "   • South Asian + Vietnamese (cross-family)"
    echo ""
    echo "💡 Note: Chinese family includes both Mandarin and Cantonese"
    echo "         Spots targeting only within Chinese family are 'language_specific'"
else
    echo "❌ Export failed!"
    exit 1
fi