#!/bin/bash

# Updated Export Individual Language Spots Script
# Uses the NEW language assignment system with spot_language_assignments table
# 
# This script extracts spots that have been assigned to specific languages
# through direct mapping (assignment_method = 'direct_mapping')

# Default values
YEAR="2024"
LANGUAGE=""
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false

show_help() {
    cat << EOF
Individual Language Spots Export Script (Updated System)

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
    english     - Exports English spots (direct mapping only)

EXAMPLES:
    $0 -l chinese -y 2024                   # Export 2024 Chinese spots
    $0 -l filipino -y 2023                  # Export 2023 Filipino spots
    $0 -l vietnamese -o vietnam_ads.csv     # Export Vietnamese with custom filename
    $0 -l korean --debug                    # Export Korean spots with debug info

NEW SYSTEM FEATURES:
    - Uses spot_language_assignments table (not time blocks)
    - Only exports spots with direct language mapping
    - Includes assignment confidence and method information
    - Shows business rule categorization
    - Excludes spots that default to English via business rules

EXPORT INCLUDES:
    - Only Internal Ad Sales + COM/BNS spots with direct language assignment
    - Assignment method, confidence, and review status
    - Complete spot details including customer, agency, revenue, and timing
    - Language assignment metadata and notes

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

# Function to get language code pattern for filtering
get_language_codes() {
    case "$1" in
        chinese)
            echo "('M', 'C')"  # Mandarin, Cantonese
            ;;
        filipino)
            echo "('T', 'F')"  # Tagalog, Filipino
            ;;
        vietnamese)
            echo "('V')"
            ;;
        korean)
            echo "('K')"
            ;;
        japanese)
            echo "('J')"
            ;;
        hmong)
            echo "('H')"
            ;;
        southasian)
            echo "('S', 'P', 'B', 'G')"  # South Asian, Punjabi, Bengali, Gujarati
            ;;
        english)
            echo "('E')"
            ;;
        *)
            # Try first letter uppercase
            local first_letter="$(echo ${1:0:1} | tr '[:lower:]' '[:upper:]')"
            echo "('$first_letter')"
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
LANGUAGE_CODES=$(get_language_codes "$LANGUAGE")
DISPLAY_NAME=$(get_display_name "$LANGUAGE")
EMOJI=$(get_language_emoji "$LANGUAGE")

echo "$EMOJI $DISPLAY_NAME Language Spots Export for $YEAR (Updated System)"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🔍 Language filter: $LANGUAGE_NAMES"
echo "🏷️  Language codes: $LANGUAGE_CODES"
echo ""

# Main export query using NEW language assignment system
echo "⚡ Generating $DISPLAY_NAME language spots export using NEW assignment system..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
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
    s.language_code as original_language_code,
    s.program,
    s.market_name,
    -- NEW SYSTEM: Language assignment details
    sla.language_code as assigned_language,
    l.language_name as assigned_language_name,
    sla.assignment_method,
    sla.language_status,
    sla.confidence,
    sla.requires_review,
    sla.notes as assignment_notes,
    sla.assigned_date,
    -- Business categorization
    s.spot_category,
    CASE 
        WHEN s.spot_category = 'language_assignment_required' THEN 'Language-Targeted'
        WHEN s.spot_category = 'default_english' THEN 'Default English'
        WHEN s.spot_category = 'review_category' THEN 'Review Required'
        ELSE 'Other: ' || COALESCE(s.spot_category, 'Unknown')
    END as business_category,
    -- Language targeting analysis
    CASE 
        WHEN sla.assignment_method = 'direct_mapping' THEN 'Direct Language Targeting'
        WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Business Rule Default'
        WHEN sla.assignment_method = 'business_review_required' THEN 'Needs Business Review'
        WHEN sla.assignment_method = 'undetermined_flagged' THEN 'Language Undetermined'
        ELSE 'Other Assignment Method'
    END as targeting_method
FROM spots s
JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
-- NEW SYSTEM: Filter for spots with actual language assignments
AND sla.assignment_method = 'direct_mapping'  -- Only direct language targeting
AND l.language_name IN $LANGUAGE_NAMES
-- Only include the spots that are actually language-targeted (not default English)
AND s.revenue_type = 'Internal Ad Sales'
AND s.spot_type IN ('COM', 'BNS')
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
    
    if [[ $RECORD_COUNT -eq 0 ]]; then
        echo ""
        echo "⚠️  No $DISPLAY_NAME language spots found for $YEAR"
        echo "💡 This could mean:"
        echo "   - No spots were directly assigned to $DISPLAY_NAME languages"
        echo "   - Language assignments haven't been processed yet"
        echo "   - All $DISPLAY_NAME content is categorized under business rules"
        echo ""
        echo "🔍 Try running the language assignment system first:"
        echo "   python cli_01_language_assignment.py --process-all-categories"
        exit 0
    fi
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {printf "%.2f", sum}')
    echo "💰 Total $DISPLAY_NAME spots revenue: \$${REVENUE}"
    
    # Assignment method breakdown
    echo ""
    echo "🔧 ASSIGNMENT METHOD BREAKDOWN:"
    DIRECT_MAPPING=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'direct_mapping')
    echo "   📍 Direct Language Mapping: $DIRECT_MAPPING (should be all records)"
    
    # Confidence analysis
    echo ""
    echo "📊 ASSIGNMENT QUALITY:"
    HIGH_CONFIDENCE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$15 >= 0.8 {count++} END {print count+0}')
    REVIEW_REQUIRED=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$16 == "1" {count++} END {print count+0}')
    echo "   🎯 High confidence (≥0.8): $HIGH_CONFIDENCE"
    echo "   ⚠️  Requiring review: $REVIEW_REQUIRED"
    
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
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Top 5 $DISPLAY_NAME advertisers by revenue"
        tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5 -nr | head -5 | while IFS=',' read -r spot_id bill_code customer rest; do
            echo "   Customer: $customer (Bill Code: $bill_code)"
        done
        
        echo ""
        echo "📅 Sample language assignments:"
        tail -n +2 "$OUTPUT_FILE" | head -3 | while IFS=',' read -r spot_id bill_code customer agency revenue station_net spot_type revenue_type time_in time_out duration dow air_date month sales original_code program market assigned_lang assigned_name method status confidence review notes date category business targeting; do
            echo "   Spot $spot_id: $customer -> $assigned_name ($method, confidence: $confidence)"
        done
        
        echo ""
        echo "🎯 Assignment method details:"
        echo "   Direct Mapping: Language assigned from spots.language_code"
        echo "   High Confidence: System confident in language assignment"
        echo "   Review Required: Spots flagged for manual review"
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 $DISPLAY_NAME Language Export Summary (Updated System):"
echo "   This export uses the NEW language assignment system"
echo "   Only includes spots with direct language mapping (assignment_method = 'direct_mapping')"
echo "   Excludes spots that default to English via business rules"
echo "   Includes assignment confidence and review status"
echo ""
echo "💡 New System Notes:"
echo "   - 'Direct Language Targeting' = Spots assigned via spots.language_code"
echo "   - 'High Confidence' = System confident in language assignment (≥0.8)"
echo "   - Only Internal Ad Sales + COM/BNS spots get individual language assignments"
echo "   - Other revenue types (Direct Response, Paid Programming) default to English"
echo ""
echo "🔧 Troubleshooting:"
echo "   - If no spots found, run language assignment processing first"
echo "   - Use --debug flag to see assignment method details"
echo "   - Check spot_language_assignments table for assignment status"