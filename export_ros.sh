#!/bin/bash

# ROS (Run on Schedule) Export Script
# Export all ROS spots using campaign_type field for proper classification
# 
# This script extracts ROS spots which are broadcast sponsorships that run
# across multiple time periods or have broad reach targeting.

# Default values
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false

show_help() {
    cat << EOF
ROS (Run on Schedule) Export Script

USAGE:
    $0 [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: ros_spots_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    --debug                 Show debug information
    -h, --help              Show this help message

EXAMPLES:
    $0 -y 2023                              # Export 2023 ROS spots
    $0 -y 2024                              # Export 2024 ROS spots
    $0 -y 2023 --debug                      # Export with debug info
    $0 -y 2023 -o ros_analysis_2023.csv     # Export to specific file

ROS CLASSIFICATION:
    Uses campaign_type = 'ros' for proper identification of:
    - Broadcast sponsorships
    - Long-duration advertising (6+ hours)
    - All-day placements (1pm-midnight patterns)
    - General market targeting (not language-specific)

EXPECTED RESULTS:
    - 2023: Should show significant ROS activity
    - 2024: Baseline ROS patterns
    - Analysis focuses on duration, timing, and customer patterns

TROUBLESHOOTING:
    If no ROS spots found, check campaign_type field:
    
    SELECT campaign_type, COUNT(*) FROM spot_language_blocks 
    WHERE spot_id IN (SELECT spot_id FROM spots WHERE broadcast_month LIKE '%-YY')
    GROUP BY campaign_type;

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
    OUTPUT_FILE="ros_spots_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

echo "🔍 ROS (Run on Schedule) Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output: $OUTPUT_FILE"
echo "🎯 Using campaign_type = 'ros' for classification"
echo ""

# Main ROS export query
echo "⚡ Generating ROS spots export..."

sqlite3 -header -csv "$DB_PATH" << EOF > "$OUTPUT_FILE"
WITH ros_spots AS (
    SELECT DISTINCT s.spot_id
    FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN agencies a ON s.agency_id = a.agency_id
    WHERE s.broadcast_month LIKE '%-$YEAR_SUFFIX'
    AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
    AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
    -- Exclude higher precedence categories
    AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
    AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
    AND s.revenue_type != 'Paid Programming'
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'PRD')
    AND NOT (slb.spot_id IS NULL AND s.spot_type = 'SVC')
    -- ROS identification using campaign_type
    AND slb.campaign_type = 'ros'
),
time_analysis AS (
    SELECT 
        s.spot_id,
        s.time_in,
        s.time_out,
        CASE 
            WHEN s.time_in IS NULL OR s.time_out IS NULL THEN NULL
            WHEN s.time_in <= s.time_out THEN 
                CAST((strftime('%H', s.time_out) - strftime('%H', s.time_in)) * 60 + 
                     (strftime('%M', s.time_out) - strftime('%M', s.time_in)) AS INTEGER)
            ELSE 
                CAST((24 * 60) - ((strftime('%H', s.time_in) - strftime('%H', s.time_out)) * 60 + 
                                  (strftime('%M', s.time_in) - strftime('%M', s.time_out))) AS INTEGER)
        END as duration_minutes,
        CASE 
            WHEN s.time_in IS NULL OR s.time_out IS NULL THEN 'No Time Info'
            WHEN s.time_in <= s.time_out THEN 
                CASE 
                    WHEN ((strftime('%H', s.time_out) - strftime('%H', s.time_in)) * 60 + 
                          (strftime('%M', s.time_out) - strftime('%M', s.time_in))) >= 360 THEN 'Long Duration (6+ hours)'
                    WHEN ((strftime('%H', s.time_out) - strftime('%H', s.time_in)) * 60 + 
                          (strftime('%M', s.time_out) - strftime('%M', s.time_in))) >= 180 THEN 'Medium Duration (3-6 hours)'
                    ELSE 'Short Duration (<3 hours)'
                END
            ELSE 'Overnight Span'
        END as duration_category,
        CASE 
            WHEN s.time_in = '13:00:00' AND s.time_out = '00:00:00' THEN 'All-Day Pattern (1pm-midnight)'
            WHEN s.time_in = '06:00:00' AND s.time_out = '23:59:00' THEN 'Full Day Pattern (6am-11:59pm)'
            WHEN s.time_in = '06:00:00' AND s.time_out = '00:00:00' THEN 'Full Day Pattern (6am-midnight)'
            WHEN s.time_in <= '06:00:00' AND s.time_out >= '23:00:00' THEN 'Extended Day Pattern'
            ELSE 'Custom Pattern'
        END as time_pattern
    FROM spots s
    WHERE s.spot_id IN (SELECT spot_id FROM ros_spots)
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
    ta.duration_minutes,
    ta.duration_category,
    ta.time_pattern,
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
        WHEN s.time_in IS NULL THEN 'No Time Info'
        WHEN s.time_in < '06:00:00' THEN 'Overnight (00:00-05:59)'
        WHEN s.time_in < '12:00:00' THEN 'Morning (06:00-11:59)'
        WHEN s.time_in < '18:00:00' THEN 'Afternoon (12:00-17:59)'
        WHEN s.time_in < '24:00:00' THEN 'Evening (18:00-23:59)'
        ELSE 'Unknown'
    END as start_time_period,
    CASE 
        WHEN s.time_out IS NULL THEN 'No Time Info'
        WHEN s.time_out < '06:00:00' THEN 'Overnight (00:00-05:59)'
        WHEN s.time_out < '12:00:00' THEN 'Morning (06:00-11:59)'
        WHEN s.time_out < '18:00:00' THEN 'Afternoon (12:00-17:59)'
        WHEN s.time_out < '24:00:00' THEN 'Evening (18:00-23:59)'
        ELSE 'Unknown'
    END as end_time_period,
    CASE 
        WHEN s.spot_type = 'BNS' THEN 'BNS (Bonus Spot)'
        WHEN s.spot_type = 'COM' THEN 'COM (Commercial)'
        WHEN s.spot_type = 'CRD' THEN 'CRD (Credit)'
        WHEN s.spot_type = 'PKG' THEN 'PKG (Package)'
        ELSE COALESCE(s.spot_type, 'NULL')
    END as spot_type_analysis,
    'ROS (Run on Schedule)' as final_category,
    -- Additional ROS analysis fields
    COALESCE(slb.campaign_type, 'NO_ASSIGNMENT') as campaign_type_confirm,
    COALESCE(slb.business_rule_applied, 'NO_RULE') as business_rule_applied,
    CASE 
        WHEN ta.duration_minutes IS NULL THEN 'Unknown Duration'
        WHEN ta.duration_minutes >= 720 THEN 'Very Long (12+ hours)'
        WHEN ta.duration_minutes >= 360 THEN 'Long (6-12 hours)'
        WHEN ta.duration_minutes >= 180 THEN 'Medium (3-6 hours)'
        WHEN ta.duration_minutes >= 60 THEN 'Short (1-3 hours)'
        ELSE 'Very Short (<1 hour)'
    END as duration_bucket,
    CASE 
        WHEN s.time_in IS NOT NULL AND s.time_out IS NOT NULL THEN 
            CASE 
                WHEN s.time_in = '13:00:00' AND s.time_out = '00:00:00' THEN 'Standard ROS (1pm-midnight)'
                WHEN ta.duration_minutes >= 360 THEN 'Long-Duration ROS (6+ hours)'
                WHEN ta.duration_minutes >= 180 THEN 'Medium-Duration ROS (3-6 hours)'
                ELSE 'Short-Duration ROS (<3 hours)'
            END
        ELSE 'Undefined ROS Pattern'
    END as ros_type_analysis
FROM ros_spots rs
JOIN spots s ON rs.spot_id = s.spot_id
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies a ON s.agency_id = a.agency_id
LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
LEFT JOIN time_analysis ta ON s.spot_id = ta.spot_id
ORDER BY s.gross_rate DESC, s.air_date, s.time_in;
EOF

# Check results
if [[ -f "$OUTPUT_FILE" ]]; then
    RECORD_COUNT=$(tail -n +2 "$OUTPUT_FILE" | wc -l)
    echo "✅ ROS export completed successfully!"
    echo "📊 Records exported: $RECORD_COUNT"
    echo "📁 File saved: $OUTPUT_FILE"
    
    # Calculate revenue
    REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{sum += $5} END {print sum}')
    echo "💰 Total ROS revenue: \$${REVENUE}"
    
    # Calculate average per spot
    if [[ $RECORD_COUNT -gt 0 ]]; then
        AVG_PER_SPOT=$(echo "scale=2; $REVENUE / $RECORD_COUNT" | bc -l)
        echo "📈 Average per ROS spot: \$${AVG_PER_SPOT}"
    fi
    
    if [[ "$DEBUG" == true ]]; then
        echo ""
        echo "🔍 Debug: Sample records"
        head -n 6 "$OUTPUT_FILE"
        echo ""
        echo "📈 ROS statistics:"
        echo "   Total records: $RECORD_COUNT"
        echo "   BNS spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',BNS,')"
        echo "   COM spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',COM,')"
        echo "   Weekend spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekend,')"
        echo "   Weekday spots: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Weekday,')"
        
        echo ""
        echo "⏱️ Duration analysis:"
        echo "   Very Long (12+ hours): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Very Long (12+ hours),')"
        echo "   Long (6-12 hours): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Long (6-12 hours),')"
        echo "   Medium (3-6 hours): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Medium (3-6 hours),')"
        echo "   Short (1-3 hours): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Short (1-3 hours),')"
        echo "   Very Short (<1 hour): $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Very Short (<1 hour),')"
        
        echo ""
        echo "🎯 Time pattern analysis:"
        echo "   All-Day Pattern: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',All-Day Pattern')"
        echo "   Full Day Pattern: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Full Day Pattern')"
        echo "   Extended Day Pattern: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Extended Day Pattern')"
        echo "   Custom Pattern: $(tail -n +2 "$OUTPUT_FILE" | grep -c ',Custom Pattern')"
        
        echo ""
        echo "🏢 Top customers by revenue:"
        tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5 -rn | head -5 | while IFS=',' read -r spot_id bill_code customer_name agency_name revenue rest; do
            echo "   $customer_name: \$${revenue}"
        done
    fi
else
    echo "❌ Export failed!"
    exit 1
fi

echo ""
echo "🎯 ROS Analysis Summary:"
echo "   This export contains all ROS (Run on Schedule) spots for $YEAR"
echo "   ROS spots are broadcast sponsorships with broad reach targeting"
echo "   Common patterns: 1pm-midnight, 6am-11:59pm, 6+ hour durations"
echo "   Business purpose: General market reach, not language-specific"
echo ""
echo "💡 Key insights available:"
echo "   - Duration analysis (Very Long, Long, Medium, Short buckets)"
echo "   - Time pattern recognition (All-Day, Full Day, Extended Day)"
echo "   - Customer and agency breakdown"
echo "   - Revenue performance by ROS type"
echo "   - Weekday vs weekend distribution"