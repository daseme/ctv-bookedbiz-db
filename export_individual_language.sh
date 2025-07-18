#!/bin/bash

# Export Individual Language Spots Script
# Exports all spots associated with a specific language's blocks
# 
# This script extracts spots that are targeted to specific language-speaking audiences,
# with support for language families (e.g., Chinese includes Mandarin/Cantonese)

# Default values
YEAR="2024"
LANGUAGE=""
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false

show_help() {
    cat << EOF
Individual Language Spots Export Script

USAGE:
    $0 -l LANGUAGE [OPTIONS]

REQUIRED:
    -l, --language LANGUAGE Language to export (chinese, filipino, vietnamese, korean, etc.)

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: LANGUAGE_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information
    -h, --help              Show this help message

SUPPORTED LANGUAGES:
    chinese     - Exports Mandarin and Cantonese spots
    filipino    - Exports Tagalog and Filipino spots
    vietnamese  - Exports Vietnamese spots
    korean      - Exports Korean spots
    japanese    - Exports Japanese spots
    hmong       - Exports Hmong spots
    southasian  - Exports Hindi, Punjabi, Bengali, Gujarati spots
    english     - Exports English spots

EXAMPLES:
    $0 -l chinese -y 2024                   # Export 2024 Chinese spots
    $0 -l filipino -y 2023                  # Export 2023 Filipino spots
    $0 -l vietnamese -o vietnam_ads.csv     # Export Vietnamese with custom filename
    $0 -l korean --debug                    # Export Korean spots with debug info

EXPORT INCLUDES:
    - Spots running in specified language blocks
    - Both individual language and multi-language campaigns
    - Complete spot details including customer, agency, revenue, and timing

EOF
}

# Function to get SQL language names based on language parameter
get_language_names() {
    case "$1" in
        chinese)
            echo "('Mandarin', 'Cantonese', 'Chinese')"
            ;;
        filipino)
            echo "('Tagalog', 'Filipino')"
            ;;
        southasian)
            echo "('Hindi', 'Punjabi', 'Bengali', 'Gujarati', 'South Asian')"
            ;;
        vietnamese)
            echo "('Vietnamese')"
            ;;
        korean)
            echo "('Korean')"
            ;;
        japanese)
            echo "('Japanese')"
            ;;
        hmong)
            echo "('Hmong')"
            ;;
        english)
            echo "('English')"
            ;;
        *)
            # For other languages, try exact match with capitalized first letter
            local cap_lang="$(echo ${1:0:1} | tr '[:lower:]' '[:upper:]')${1:1}"
            echo "('$cap_lang')"
            ;;
    esac
}

# Function to get display name for language
get_display_name() {
    case "$1" in
        chinese)
            echo "Chinese (Mandarin/Cantonese)"
            ;;
        filipino)
            echo "Filipino (Tagalog)"
            ;;
        southasian)
            echo "South Asian"
            ;;
        *)
            # Capitalize first letter
            echo "$(echo ${1:0:1} | tr '[:lower:]' '[:upper:]')${1:1}"
            ;;
    esac
}

# Function to get language emoji
get_language_emoji() {
    case "$1" in
        chinese)
            echo "🇨🇳"
            ;;
        filipino)
            echo "🇵🇭"
            ;;
        vietnamese)
            echo "🇻🇳"
            ;;
        korean)
            echo "🇰🇷"
            ;;
        japanese)
            echo "🇯🇵"
            ;;
        southasian)
            echo "🇮🇳"
            ;;
        english)
            echo "🇺🇸"
            ;;
        hmong)
            echo "🏔️"
            ;;
        *)
            echo "🌐"
            ;;
    esac
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -l|--language)
            LANGUAGE="$2"
            shift 2
            ;;
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

# Validate required parameter
if [[ -z "$LANGUAGE" ]]; then
    echo "Error: Language parameter is required"
    echo "Use -l or --language to specify the language (e.g., -l chinese)"
    echo ""
    show_help
    exit 1
fi

# Convert language to lowercase for consistency
LANGUAGE=$(echo "$LANGUAGE" | tr '[:upper:]' '[:lower:]')

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
    OUTPUT_FILE="${LANGUAGE}_spots_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"
LANGUAGE_NAMES=$(get_language_names "$LANGUAGE")
DISPLAY_NAME=$(get_display_name "$LANGUAGE")
EMOJI=$(get_language_emoji "$LANGUAGE")

echo "$EMOJI $DISPLAY_NAME Language Spots Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🔍 Language filter: $LANGUAGE_NAMES"
echo ""

# Main export query for language spots
echo "⚡ Generating $DISPLAY_NAME language spots export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
WITH language_spots AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    JOIN language_blocks lb ON COALESCE(slb.block_id, slb.primary_block_id) = lb.block_id
    JOIN languages l ON lb.language_id = l.language_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    AND l.language_name IN $LANGUAGE_NAMES
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
    -- Duration calculation
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
    s.day_of_week,
    s.air_date,
    s.broadcast_month,
    s.sales_person,
    s.language_code,
    s.program,
    s.market_name,
    -- Language block details
    GROUP_CONCAT(DISTINCT l.language_name) as languages,
    GROUP_CONCAT(DISTINCT lb.block_name) as block_names,
    slb.campaign_type,
    -- Categorization
    CASE 
        WHEN slb.campaign_type = 'language_specific' THEN 'Individual ' || '$DISPLAY_NAME'
        WHEN slb.campaign_type = 'multi_language' THEN 'Multi-Language (includes ' || '$DISPLAY_NAME' || ')'
        ELSE 'Other ' || '$DISPLAY_NAME' || ' Association'
    END as targeting_type,
    -- Language specifics
    l.language_name as primary_language_variant
FROM language_spots ls
JOIN spots s ON ls.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN language_blocks lb ON COALESCE(slb.block_id, slb.primary_block_id) = lb.block_id
LEFT JOIN languages l ON lb.language_id = l.language_id
WHERE l.language_name IN $LANGUAGE_NAMES
GROUP BY s.spot_id
ORDER BY 
    s.gross_rate DESC, 
    s.air_date, 
    s.time_in;
EOF

# Check results and provide summary
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ $DISPLAY_NAME language spots export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
    echo "💰 Total $DISPLAY_NAME spots revenue: \$${REVENUE}"
    
    # Language variant breakdown for multi-variant languages
    if [[ "$LANGUAGE" == "chinese" || "$LANGUAGE" == "filipino" || "$LANGUAGE" == "southasian" ]]; then
        echo ""
        echo "🔍 LANGUAGE VARIANT BREAKDOWN:"
        
        case "$LANGUAGE" in
            chinese)
                MANDARIN_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Mandarin')
                CANTONESE_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Cantonese')
                echo "   🈵 Mandarin spots: $MANDARIN_COUNT"
                echo "   🈲 Cantonese spots: $CANTONESE_COUNT"
                ;;
            filipino)
                TAGALOG_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Tagalog')
                FILIPINO_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Filipino')
                echo "   🇵🇭 Tagalog spots: $TAGALOG_COUNT"
                echo "   🇵🇭 Filipino spots: $FILIPINO_COUNT"
                ;;
            southasian)
                HINDI_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Hindi')
                PUNJABI_COUNT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Punjabi')
                echo "   🇮🇳 Hindi spots: $HINDI_COUNT"
                echo "   🇮🇳 Punjabi spots: $PUNJABI_COUNT"
                ;;
        esac
    fi
    
    # Campaign type breakdown
    LANGUAGE_SPECIFIC=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'language_specific')
    MULTI_LANGUAGE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'multi_language')
    
    echo ""
    echo "📋 CAMPAIGN TYPE BREAKDOWN:"
    echo "   📍 Language-specific campaigns: $LANGUAGE_SPECIFIC"
    echo "   🌐 Multi-language campaigns: $MULTI_LANGUAGE"
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Top 5 $DISPLAY_NAME advertisers by revenue"
        tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5 -nr | head -5 | while IFS=',' read -r spot_id bill_code customer rest; do
            echo "   Customer: $customer (Bill Code: $bill_code)"
        done
        
        echo ""
        echo "📅 Sample spots:"
        tail -n +2 "$OUTPUT_FILE" | head -3 | while IFS=',' read -r spot_id bill_code customer agency revenue rest; do
            echo "   Spot $spot_id: $customer - \$$revenue"
        done
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 $DISPLAY_NAME Language Export Summary:"
echo "   This export includes all spots targeted to $DISPLAY_NAME-speaking audiences"
echo "   Individual language and multi-language campaigns are captured"
echo "   Revenue includes both paid spots and bonus (BNS) spots"
echo ""
echo "💡 Notes:"
echo "   - 'Individual $DISPLAY_NAME' = Spots targeting only $DISPLAY_NAME audiences"
echo "   - 'Multi-Language' = Spots targeting $DISPLAY_NAME plus other languages"
echo "   - Use --debug flag to see top advertisers and sample spots"