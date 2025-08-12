#!/bin/bash
set -euo pipefail

# Export Review Required Spots Script - UPDATED
# Exports spots that need manual review OR belong to "Other" business categories,
# aligned with the new language assignment rules.

# Defaults
YEAR="2024"
OUTPUT_FILE=""
DB_PATH="data/database/production.db"
DEBUG=false
REVIEW_TYPE="all"

show_help() {
    cat << 'EOF'
Review Required Spots Export Script - Updated for New Assignment System

USAGE:
    ./export_review_spots.sh [OPTIONS]

OPTIONS:
    -y, --year YEAR         Year to export (default: 2024)
    -o, --output FILE       Output CSV file (default: review_required_TYPE_YEAR.csv)
    -d, --database PATH     Database path (default: data/database/production.db)
    -t, --type TYPE         Review type filter (default: all)
    --debug                 Show debug information
    -h, --help              Show this help

REVIEW TYPES:
    all                 - All spots requiring review (or in "Other" business bucket)
    business            - Business Review Required
    undetermined        - Language code 'L'
    invalid             - Invalid language codes (not in languages)
    high-value          - Gross rate > $500 (and requires_review)
    low-confidence      - Confidence < 0.5
    fallback            - Default-English assignments (audit), includes:
                          default_english, business_rule_default_english, auto_default_com_bb

NOTES:
- BB is treated the same as COM.
- 'L' is always review.
- Invalid codes are always review.
- COM/BB with a valid code are NOT review.

EOF
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    -y|--year) YEAR="$2"; shift 2;;
    -o|--output) OUTPUT_FILE="$2"; shift 2;;
    -d|--database) DB_PATH="$2"; shift 2;;
    -t|--type) REVIEW_TYPE="$2"; shift 2;;
    --debug) DEBUG=true; shift;;
    -h|--help) show_help; exit 0;;
    *) echo "Unknown option: $1"; show_help; exit 1;;
  esac
done

# Validate
if [[ ! "$YEAR" =~ ^[0-9]{4}$ ]]; then
  echo "Error: Year must be 4 digits (e.g., 2024)"; exit 1
fi
if [[ ! -f "$DB_PATH" ]]; then
  echo "Error: Database not found: $DB_PATH"; exit 1
fi
if [[ -z "${OUTPUT_FILE}" ]]; then
  OUTPUT_FILE="review_required_${REVIEW_TYPE}_${YEAR}.csv"
fi

YEAR_SUFFIX="${YEAR: -2}"

# Build SQL filter (review type)
get_review_filter() {
  # robust to set -u
  local t="${1-}"
  case "${t:-all}" in
    business)
      echo "AND sla.assignment_method = 'business_review_required'"
      ;;
    undetermined)
      echo "AND sla.assignment_method = 'undetermined_flagged'"
      ;;
    invalid)
      echo "AND sla.language_status = 'invalid'"
      ;;
    high-value)
      echo "AND sla.requires_review = 1 AND s.gross_rate > 500"
      ;;
    low-confidence)
      echo "AND sla.confidence < 0.5"
      ;;
    fallback)
      echo "AND sla.assignment_method IN ('default_english','business_rule_default_english','auto_default_com_bb')"
      ;;
    all|*)
      echo "AND (
        sla.assignment_method IN ('business_review_required','undetermined_flagged','invalid_code_flagged')
        OR sla.requires_review = 1
        OR sla.confidence < 0.5
        OR sla.spot_id IS NULL
        OR s.revenue_type = 'Other'
        OR s.revenue_type NOT IN ('Direct Response Sales','Paid Programming','Branded Content','Internal Ad Sales')
        OR (s.revenue_type = 'Internal Ad Sales' AND s.spot_type NOT IN ('COM','BNS','BB'))
      )"
      ;;
  esac
}
REVIEW_FILTER="$(get_review_filter "${REVIEW_TYPE}")"

get_review_display_name() {
  local t="${1-}"
  case "${t:-all}" in
    business)      echo "Business Review Required" ;;
    undetermined)  echo "Undetermined Language" ;;
    invalid)       echo "Invalid Language Codes" ;;
    high-value)    echo "High-Value Review Required" ;;
    low-confidence)echo "Low Confidence Assignments" ;;
    fallback)      echo "Default-English Assignments (Audit)" ;;
    all|*)         echo "All Review Required" ;;
  esac
}

DISPLAY_NAME="$(get_review_display_name "${REVIEW_TYPE}")"

echo "🚨 $DISPLAY_NAME Spots Export for $YEAR"
echo "📊 Database: $DB_PATH"
echo "📁 Output:   $OUTPUT_FILE"
echo "🔍 Type:     $REVIEW_TYPE"
echo ""

# Generate CSV
sqlite3 -header -csv "$DB_PATH" > "$OUTPUT_FILE" <<SQL
SELECT 
  s.spot_id,
  s.bill_code,
  COALESCE(c.normalized_name,'Unknown') AS customer_name,
  COALESCE(a.agency_name,'No Agency')   AS agency_name,
  s.gross_rate,
  s.station_net,
  s.spot_type,
  s.revenue_type,
  s.time_in,
  s.time_out,

  -- duration in minutes (HH:MM), handle overnight
  CASE
    WHEN s.time_in IS NULL OR s.time_out IS NULL THEN NULL
    ELSE
      CASE
        WHEN s.time_in <= s.time_out THEN
          (CAST(substr(s.time_out,1,2) AS INT) * 60 + CAST(substr(s.time_out,4,2) AS INT))
          - (CAST(substr(s.time_in,1,2)  AS INT) * 60 + CAST(substr(s.time_in,4,2)  AS INT))
        ELSE
          (24*60 - (CAST(substr(s.time_in,1,2) AS INT) * 60 + CAST(substr(s.time_in,4,2) AS INT)))
          + (CAST(substr(s.time_out,1,2) AS INT) * 60 + CAST(substr(s.time_out,4,2) AS INT))
      END
  END AS duration_minutes,

  s.day_of_week,
  s.air_date,
  s.broadcast_month,
  s.sales_person,
  s.language_code AS original_language_code,
  s.comments      AS program_comments,
  s.market_name,

  -- assignment details
  sla.language_code      AS assigned_language,
  l.language_name        AS assigned_language_name,
  sla.assignment_method,
  sla.language_status,
  sla.confidence,
  sla.requires_review,
  sla.notes              AS assignment_notes,
  sla.assigned_date,

  -- Business category (BB treated like COM)
  CASE 
    WHEN s.revenue_type = 'Direct Response Sales' THEN 'Direct Response Sales'
    WHEN s.revenue_type = 'Paid Programming'     THEN 'Paid Programming'
    WHEN s.revenue_type = 'Branded Content'      THEN 'Branded Content'
    WHEN s.revenue_type = 'Internal Ad Sales' AND s.spot_type IN ('COM','BNS','BB') THEN 'Language-Targeted Advertising'
    ELSE 'Other/Review Required'
  END AS business_category,

  -- Review reason
  CASE
    WHEN sla.assignment_method = 'business_review_required' THEN 'Unusual Revenue/Spot Type Combination'
    WHEN sla.assignment_method = 'undetermined_flagged'     THEN 'Language Code L - Manual Determination'
    WHEN sla.assignment_method = 'default_english'          THEN 'Missing Language Code - Defaulted to English'
    WHEN sla.assignment_method = 'business_rule_default_english' THEN 'Business Rule Default English'
    WHEN sla.assignment_method = 'auto_default_com_bb'      THEN 'COM/BB Missing Code - Defaulted to English'
    WHEN s.revenue_type = 'Other'                           THEN 'Revenue Type: Other - Needs Classification'
    WHEN sla.language_status = 'invalid'                    THEN 'Invalid Language Code'
    WHEN sla.confidence < 0.5                               THEN 'Low Confidence Assignment'
    WHEN sla.requires_review = 1                            THEN 'System Flagged for Review'
    WHEN s.revenue_type NOT IN ('Direct Response Sales','Paid Programming','Branded Content','Internal Ad Sales') THEN 'Unusual Revenue Type'
    WHEN s.revenue_type = 'Internal Ad Sales' AND s.spot_type NOT IN ('COM','BNS','BB') THEN 'Internal Ad Sales + Unusual Spot Type'
    WHEN sla.spot_id IS NULL                                 THEN 'No Language Assignment Record'
    ELSE 'Other Review Required'
  END AS review_reason,

  -- Priority level
CASE 
  WHEN s.gross_rate > 1000 THEN 'High Priority (\$1000+)'
  WHEN s.gross_rate >  500 THEN 'Medium Priority (\$500-1000)'
  WHEN s.gross_rate >  100 THEN 'Low Priority (\$100-500)'
  ELSE 'Very Low Priority (<\$100)'
END AS priority_level,

  -- Resolution guidance (BB not unusual)
  CASE 
    WHEN sla.assignment_method = 'business_review_required'
         AND s.revenue_type = 'Internal Ad Sales'
         AND s.spot_type IN ('PKG','CRD','AV') THEN 'Review spot type: Internal Ad Sales usually uses COM/BNS/BB'
    WHEN sla.assignment_method = 'business_review_required'
         AND s.revenue_type NOT IN ('Direct Response Sales','Paid Programming','Branded Content','Internal Ad Sales')
         THEN 'Verify revenue type classification'
    WHEN s.revenue_type = 'Other' THEN 'Classify revenue type'
    WHEN sla.assignment_method = 'undetermined_flagged' OR sla.language_status = 'undetermined'
         THEN 'Manual language determination needed'
    WHEN sla.assignment_method IN ('default_english','business_rule_default_english','auto_default_com_bb')
         THEN 'Language missing - confirm English or set correct language'
    WHEN sla.language_status = 'invalid' THEN 'Language code not in languages table - verify'
    WHEN sla.confidence < 0.5 THEN 'Low confidence - verify assignment'
    WHEN sla.spot_id IS NULL THEN 'No language assignment record - run processing'
    ELSE 'General review required - inspect details'
  END AS resolution_guidance

FROM spots s
LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
LEFT JOIN languages l ON UPPER(sla.language_code) = UPPER(l.language_code)
LEFT JOIN customers c ON s.customer_id = c.customer_id
LEFT JOIN agencies  a ON s.agency_id  = a.agency_id
WHERE s.broadcast_month LIKE '%-${YEAR_SUFFIX}'
  AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
  AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
  ${REVIEW_FILTER}
ORDER BY 
  CASE WHEN s.gross_rate > 1000 THEN 1
       WHEN s.gross_rate >  500 THEN 2
       WHEN s.gross_rate >  100 THEN 3
       ELSE 4 END,
  s.gross_rate DESC, 
  COALESCE(sla.assigned_date,'')
;
SQL

# Summary
if [[ -f "$OUTPUT_FILE" ]]; then
  RECORD_COUNT=$(($(wc -l < "$OUTPUT_FILE") - 1))
  [[ $RECORD_COUNT -lt 0 ]] && RECORD_COUNT=0
  echo "✅ $DISPLAY_NAME export complete"
  echo "📊 Records: $RECORD_COUNT"
  echo "📁 File:    $OUTPUT_FILE"

  if [[ $RECORD_COUNT -eq 0 ]]; then
    echo "🎉 No $DISPLAY_NAME spots found for $YEAR"
    exit 0
  fi

  # Revenue at risk (col 5)
  REVENUE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '{x=$5+0; s+=x} END {printf "%.2f", s+0}')
  echo "💰 Total revenue requiring attention: \$${REVENUE}"

  echo ""
  echo "🔍 REVIEW REASON BREAKDOWN (string match):"
  BUSINESS_REVIEW=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'business_review_required' || true)
  UNDETERMINED=$(tail -n +2 "$OUTPUT_FILE"   | grep -c 'undetermined_flagged'     || true)
  FALLBACK=$(tail -n +2 "$OUTPUT_FILE"       | egrep -c 'default_english|business_rule_default_english|auto_default_com_bb' || true)

  # Column-aware counts:
  # 21=assignment_method, 22=language_status, 23=confidence, 24=requires_review, 28=review_reason
  INVALID_CODE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' 'tolower($22)=="invalid"{c++} END{print c+0}')
  LOW_CONFIDENCE=$(tail -n +2 "$OUTPUT_FILE" | awk -F',' '$23+0 < 0.5{c++} END{print c+0}')
  NO_ASSIGNMENT=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'No Language Assignment Record' || true)

  echo "   📋 Business review required: $BUSINESS_REVIEW"
  echo "   ❓ Undetermined flagged:     $UNDETERMINED"
  echo "   🔄 Default English fallback: $FALLBACK"
  echo "   ❌ Invalid language codes:   $INVALID_CODE"
  echo "   📉 Low confidence (<0.5):    $LOW_CONFIDENCE"
  echo "   ❌ No assignment record:     $NO_ASSIGNMENT"

  echo ""
  echo "📊 PRIORITY BREAKDOWN:"
  HIGH_PRIORITY=$(tail -n +2 "$OUTPUT_FILE" | grep -c 'High Priority'    || true)
  MEDIUM_PRIORITY=$(tail -n +2 "$OUTPUT_FILE"| grep -c 'Medium Priority'  || true)
  LOW_PRIORITY=$(tail -n +2 "$OUTPUT_FILE"   | grep -c 'Low Priority'     || true)
  VERY_LOW_PRIORITY=$(tail -n +2 "$OUTPUT_FILE"| grep -c 'Very Low Priority' || true)
  echo "   🔴 High (\$1000+):    $HIGH_PRIORITY"
  echo "   🟡 Medium (\$500-1k): $MEDIUM_PRIORITY"
  echo "   🟢 Low (\$100-500):   $LOW_PRIORITY"
  echo "   ⚪ Very Low (<\$100): $VERY_LOW_PRIORITY"

  if [[ "$DEBUG" == true ]]; then
    echo ""
    echo "🔍 Top 5 highest value review spots"
    tail -n +2 "$OUTPUT_FILE" | sort -t',' -k5,5nr | head -5 | \
      awk -F',' '{printf "   Spot %s: %s - $%s (Bill: %s)\n",$1,$3,$5,$2}'

    echo ""
    echo "📅 Sample review reasons:"
    tail -n +2 "$OUTPUT_FILE" | head -3 | \
      awk -F',' '{printf "   Spot %s: %s\n",$1,$28}'
    echo ""
    echo "💡 Resolution guidance examples:"
    tail -n +2 "$OUTPUT_FILE" | head -3 | \
      awk -F',' '{printf "   Spot %s: %s\n",$1,$30}'
  fi
else
  echo "❌ Export failed!"; exit 1
fi

echo ""
echo "🎯 $DISPLAY_NAME Export Summary:"
echo "   - Prioritize High Priority spots"
echo "   - Resolve business category issues next"
echo "   - Determine languages for 'L' spots"
echo "   - Validate invalid codes"
echo "   - Audit default-English fallbacks (if any)"
