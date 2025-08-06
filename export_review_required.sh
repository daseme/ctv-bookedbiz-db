#!/bin/bash

# Export Review Required Spots Script
# Exports all spots that need manual review or fall into "Other" categories
# 
# This script extracts spots that require business review, have undetermined
# languages, or fall into unusual revenue/spot type combinations

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false
REVIEW_TYPE="all"

show_help() {
    cat << EOF
Review Required Spots Export Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: review_required_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -t, --type TYPE         Review type filter (default: all)
    --debug                 Show debug information
    -h, --help              Show this help message

REVIEW TYPES:
    all                 - All spots requiring review
    business            - Business review required (unusual revenue/spot combinations)
    undetermined        - Undetermined language (L code) spots
    invalid             - Invalid language codes
    high-value          - High-value spots requiring review (>$500)
    low-confidence      - Low confidence assignments (<0.5)

EXAMPLES:
    $0 -y 2024                              # All review required spots for 2024
    $0 -t undetermined -y 2023              # Only undetermined language spots
    $0 -t high-value -o priority_review.csv # High-value spots needing review
    $0 --debug                              # All review spots with debug info

EXPORT INCLUDES:
    - Spots flagged for business review
    - Spots with undetermined language codes (L)
    - Spots with invalid language codes
    - Low confidence language assignments
    - Assignment method and confidence information
    - Detailed notes about why review is required

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
        -t|--type)
            REVIEW_TYPE="$2"
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
    OUTPUT_FILE="review_required_${REVIEW_TYPE}_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

# Function to get filter conditions based on review type
get_review_filter() {
    case "$1" in
        business)
            echo "AND sla.assignment_method = 'business_review_required'"
            ;;
        undetermined)
            echo "AND sla.assignment_method = 'undetermined_flagged'"
            ;;
        invalid)
            echo "AND (sla.language_status = 'invalid' OR sla.assignment_method = 'invalid_code_flagged')"
            ;;
        high-value)
            echo "AND sla.requires_review = 1 AND s.gross_rate > 500"
            ;;
        low-confidence)
            echo "AND sla.confidence < 0.5"
            ;;
        all|*)
            echo "AND (sla.requires_review = 1 OR sla.confidence < 0.5 OR sla.assignment_method IN ('business_review_required', 'undetermined_flagged'))"
            ;;
    esac
}

REVIEW_FILTER=$(get_review_filter "$REVIEW_TYPE")

# Get display name for review type
get_review_display_name() {
    case "$1" in
        business)
            echo "Business Review Required"
            ;;
        undetermined)
            echo "Undetermined Language"
            ;;
        invalid)
            echo "Invalid Language Codes"
            ;;
        high-value)
            echo "High-Value Review Required"
            ;;
        low-confidence)
            echo "Low Confidence Assignments"
            ;;
        all|*)
            echo "All Review Required"
            ;;
    esac
}

DISPLAY_NAME=$(get_review_display_name "$REVIEW_TYPE")

echo "🚨 $DISPLAY_NAME Spots Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🔍 Review type: $REVIEW_TYPE"
echo ""

# Main export query for review required spots
echo "⚡ Generating $DISPLAY_NAME spots export..."

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
                          (CAST(substr(s.time_in, 4, 2) AS INTEGER) - CAST(substr(s.time_in, 4, 2) AS INTEGER)))
    END as duration_minutes,
    s.day_of_week,
    s.air_date,
    s.broadcast_month,
    s.sales_person,
    s.language_code as original_language_code,
    s.program,
    s.market_name,
    -- Language assignment details
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
    -- Review reason analysis
    CASE 
        WHEN sla.assignment_method = 'business_review_required' THEN 'Unusual Revenue/Spot Type Combination'
        WHEN sla.assignment_method = 'undetermined_flagged' THEN 'Language Code L - Needs Manual Determination'
        WHEN sla.assignment_method = 'invalid_code_flagged' THEN 'Invalid Language Code'
        WHEN sla.confidence < 0.5 THEN 'Low Confidence Assignment'
        WHEN sla.requires_review = 1 THEN 'System Flagged for Review'
        ELSE 'Other Review Required'
    END as review_reason,
    -- Priority scoring
    CASE 
        WHEN s.gross_rate > 1000 THEN 'High Priority ($1000+)'
        WHEN s.gross_rate > 500 THEN 'Medium Priority ($500-1000)'  
        WHEN s.gross_rate > 100 THEN 'Low Priority ($100-500)'
        ELSE 'Very Low Priority (<$100)'
    END as priority_level,
    -- Resolution guidance
    CASE 
        WHEN sla.assignment_method = 'business_review_required' AND s.revenue_type = 'Internal Ad Sales' AND s.spot_type IN ('PKG', 'CRD', 'AV', 'BB') 
            THEN 'Review if spot type is correct - may need reclassification'
        WHEN sla.assignment_method = 'business_review_required' AND s.revenue_type = 'Other'
            THEN 'Verify revenue type - may need to be reclassified'
        WHEN sla.assignment_method = 'undetermined_flagged'
            THEN 'Manual language determination needed - check spot content/context'
        WHEN sla.language_status = 'invalid'
            THEN 'Language code not found in languages table - verify code'
        WHEN sla.confidence < 0.5
            THEN 'Low confidence - verify assignment is correct'
        ELSE 'General review required - check assignment details'
    END as resolution_guidance
FROM spots s
LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
$REVIEW_FILTER
ORDER BY 
    CASE 
        WHEN s.gross_rate > 1000 THEN 1
        WHEN s.gross_rate > 500 THEN 2
        WHEN s.gross_rate > 100 THEN 3
        ELSE 4
    END,
    s.gross_rate DESC, 
    sla.assigned_date DESC;
EOF

# Check results and provide summary
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ $DISPLAY_NAME spots export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    if [[ $RECORD_COUNT -eq 0 ]]; then
        echo ""
        echo "🎉 No $DISPLAY_NAME spots found for $YEAR"
        echo "💡 This could mean:"
        echo "   - All spots have been successfully assigned"
        echo "   - Language assignment processing is complete"
        echo "   - All review items have been resolved"
        exit 0
    fi
    
    # Calculate revenue at risk
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {printf "%.2f", sum}')
    echo "💰 Total revenue requiring review: \$${REVENUE}"
    
    # Review reason breakdown
    echo ""
    echo "🔍 REVIEW REASON BREAKDOWN:"
    BUSINESS_REVIEW=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'business_review_required')
    UNDETERMINED=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'undetermined_flagged')
    INVALID_CODE=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'invalid_code_flagged')
    LOW_CONFIDENCE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$18 < 0.5 {count++} END {print count+0}')
    
    echo "   📋 Business review required: $BUSINESS_REVIEW"
    echo "   ❓ Undetermined language (L): $UNDETERMINED"
    echo "   ❌ Invalid language codes: $INVALID_CODE"
    echo "   📉 Low confidence (<0.5): $LOW_CONFIDENCE"
    
    # Priority breakdown
    echo ""
    echo "📊 PRIORITY BREAKDOWN:"
    HIGH_PRIORITY=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'High Priority')
    MEDIUM_PRIORITY=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Medium Priority')
    LOW_PRIORITY=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Low Priority')
    VERY_LOW_PRIORITY=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'Very Low Priority')
    
    echo "   🔴 High Priority (\$1000+): $HIGH_PRIORITY"
    echo "   🟡 Medium Priority (\$500-1000): $MEDIUM_PRIORITY"
    echo "   🟢 Low Priority (\$100-500): $LOW_PRIORITY"
    echo "   ⚪ Very Low Priority (<\$100): $VERY_LOW_PRIORITY"
    
    # Revenue type analysis for business review spots
    if [[ $BUSINESS_REVIEW -gt 0 ]]; then
        echo ""
        echo "🏢 BUSINESS REVIEW CATEGORIES:"
        INTERNAL_AD_SALES=$(tail -n +2 "$OUTPUT_FILE" | grep 'business_review_required' | grep -c 'Internal Ad Sales')
        OTHER_REVENUE=$(tail -n +2 "$OUTPUT_FILE" | grep 'business_review_required' | grep -c 'Other')
        LOCAL_REVENUE=$(tail -n +2 "$OUTPUT_FILE" | grep 'business_review_required' | grep -c 'Local')
        
        echo "   📺 Internal Ad Sales (unusual spot types): $INTERNAL_AD_SALES"
        echo "   ❓ Other revenue type: $OTHER_REVENUE"  
        echo "   🏘️  Local revenue type: $LOCAL_REVENUE"
    fi
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Top 5 highest value review required spots"
        tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5 -nr | head -5 | while IFS=',' read -r spot_id bill_code customer agency revenue rest; do
            echo "   Spot $spot_id: $customer - \$$revenue (Bill: $bill_code)"
        done
        
        echo ""
        echo "📅 Sample review reasons:"
        tail -n +2 "$OUTPUT_FILE" | head -3 | while IFS=',' read -r spot_id bill_code customer agency revenue station_net spot_type revenue_type time_in time_out duration dow air_date month sales original_code program market assigned_lang assigned_name method status confidence review notes date category business reason priority guidance; do
            echo "   Spot $spot_id: $reason"
        done
        
        echo ""
        echo "💡 Resolution guidance examples:"
        tail -n +2 "$OUTPUT_FILE" | head -3 | while IFS=',' read -r spot_id bill_code customer agency revenue station_net spot_type revenue_type time_in time_out duration dow air_date month sales original_code program market assigned_lang assigned_name method status confidence review notes date category business reason priority guidance; do
            echo "   Spot $spot_id: $guidance"
        done
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 $DISPLAY_NAME Export Summary:"
echo "   This export includes spots requiring manual attention"
echo "   Spots are prioritized by revenue value for efficient review"  
echo "   Resolution guidance provided for each spot type"
echo "   Assignment method and confidence levels included"
echo ""
echo "💡 Review Workflow:"
echo "   1. Start with High Priority spots (>$1000)"
echo "   2. Review business category assignments first"
echo "   3. Determine languages for undetermined (L) spots"
echo "   4. Verify invalid language codes"
echo "   5. Check low confidence assignments"
echo ""
echo "🔧 Next Steps:"
echo "   - Review and resolve flagged spots"
echo "   - Update language assignments as needed"
echo "   - Re-run language assignment processing after fixes"
echo "   - Verify assignments with: python cli_01_language_assignment.py --status"
echo ""
echo "📋 Filter Options:"
echo "   Use -t business, -t undetermined, -t high-value for focused exports"
echo "   Use --debug for detailed analysis and sample data"