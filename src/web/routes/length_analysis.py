"""
Length Analysis Dashboard
Rate intelligence by spot length bucket with industry-standard categorization.
"""

from flask import Blueprint, render_template, request
from src.services.container import get_container

length_analysis_bp = Blueprint('length_analysis', __name__, url_prefix='/length-analysis')


@length_analysis_bp.route('/')
def dashboard():
    """Main length analysis dashboard."""
    container = get_container()
    db = container.get("database_connection")
    
    # Get filter parameters
    revenue_type = request.args.get('revenue_type', 'Internal Ad Sales')
    year = request.args.get('year', '')
    
    # Build year filter for SQL
    year_filter = ""
    year_filter_v = ""
    year_param = []
    if year:
        year_filter = "AND broadcast_month LIKE ?"
        year_filter_v = "AND v.broadcast_month LIKE ?"
        year_param = [f"%-{year[-2:]}"]
    
    with db.connection() as conn:
        def rows_to_dicts(rows):
            return [dict(row) for row in rows]
        
        # Get available years for filter
        years = conn.execute("""
            SELECT DISTINCT '20' || SUBSTR(broadcast_month, 5, 2) AS year
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            ORDER BY year DESC
        """, [revenue_type]).fetchall()
        available_years = [row['year'] for row in years]
        
        # Summary by length bucket
        bucket_summary_raw = conn.execute(f"""
            SELECT 
                length_bucket,
                bucket_sort_order,
                COUNT(*) AS spot_count,
                ROUND(SUM(gross_rate), 2) AS total_revenue,
                ROUND(AVG(gross_rate), 2) AS avg_rate,
                ROUND(AVG(station_net), 2) AS avg_net,
                ROUND(AVG(rate_per_second), 4) AS avg_rate_per_second,
                ROUND(AVG(margin_pct), 2) AS avg_margin_pct,
                MIN(gross_rate) AS min_rate,
                MAX(gross_rate) AS max_rate
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            {year_filter}
            GROUP BY length_bucket, bucket_sort_order
            ORDER BY bucket_sort_order
        """, [revenue_type] + year_param).fetchall()
        bucket_summary = rows_to_dicts(bucket_summary_raw)
        
        # Trend by month (last 12 months, :30 benchmark)
        monthly_trend_raw = conn.execute("""
            SELECT 
                broadcast_month,
                length_bucket,
                COUNT(*) AS spot_count,
                ROUND(AVG(gross_rate), 2) AS avg_rate
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
              AND length_bucket IN ('Billboard', ':15', ':30', ':60')
              AND broadcast_month IN (
                  SELECT DISTINCT broadcast_month 
                  FROM month_closures 
                  ORDER BY closed_date DESC 
                  LIMIT 12
              )
            GROUP BY broadcast_month, length_bucket
            ORDER BY broadcast_month, bucket_sort_order
        """, [revenue_type]).fetchall()
        monthly_trend = rows_to_dicts(monthly_trend_raw)
        
        # Revenue type options for filter
        revenue_types = conn.execute("""
            SELECT DISTINCT revenue_type 
            FROM v_spot_length_analysis 
            WHERE revenue_type IS NOT NULL
            ORDER BY revenue_type
        """).fetchall()
        
        # Totals for context
        totals = conn.execute(f"""
            SELECT 
                COUNT(*) AS total_spots,
                ROUND(SUM(gross_rate), 2) AS total_revenue,
                ROUND(AVG(gross_rate), 2) AS overall_avg_rate,
                ROUND(AVG(margin_pct), 2) AS overall_margin
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            {year_filter}
        """, [revenue_type] + year_param).fetchone()
        
        # Rate ladder analysis (relative to :30)
        thirty_sec_rate = next(
            (row['avg_rate'] for row in bucket_summary if row['length_bucket'] == ':30'), 
            None
        )
        
        rate_ladder = []
        if thirty_sec_rate:
            for row in bucket_summary:
                rate_ladder.append({
                    'bucket': row['length_bucket'],
                    'avg_rate': row['avg_rate'],
                    'ratio_to_30': round(row['avg_rate'] / thirty_sec_rate, 2) if thirty_sec_rate else None,
                    'spot_count': row['spot_count'],
                    'margin_pct': row['avg_margin_pct']
                })

        # Top rate combinations (language + market)
        top_rate_combos = conn.execute(f"""
            SELECT 
                l.language_group,
                m.market_code,
                COUNT(*) AS spots,
                ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                ROUND(AVG(v.margin_pct), 2) AS avg_margin,
                ROUND(SUM(v.gross_rate), 0) AS total_revenue
            FROM spots s
            JOIN v_spot_length_analysis v ON s.spot_id = v.spot_id
            JOIN languages l ON s.language_code = l.language_code
            JOIN markets m ON s.market_id = m.market_id
            WHERE v.revenue_type = ?
              AND l.language_group NOT IN ('Review Required', 'Multi-Language')
              {year_filter_v}
            GROUP BY l.language_group, m.market_code
            HAVING COUNT(*) >= 50
            ORDER BY avg_rate DESC
            LIMIT 15
        """, [revenue_type] + year_param).fetchall()
        
        # Bottom rate combos (opportunity gaps)
        bottom_rate_combos = conn.execute(f"""
            SELECT 
                l.language_group,
                m.market_code,
                COUNT(*) AS spots,
                ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                ROUND(AVG(v.margin_pct), 2) AS avg_margin,
                ROUND(SUM(v.gross_rate), 0) AS total_revenue
            FROM spots s
            JOIN v_spot_length_analysis v ON s.spot_id = v.spot_id
            JOIN languages l ON s.language_code = l.language_code
            JOIN markets m ON s.market_id = m.market_id
            WHERE v.revenue_type = ?
              AND l.language_group NOT IN ('Review Required', 'Multi-Language')
              {year_filter_v}
            GROUP BY l.language_group, m.market_code
            HAVING COUNT(*) >= 50
            ORDER BY avg_rate ASC
            LIMIT 10
        """, [revenue_type] + year_param).fetchall()
    
    return render_template(
        'length_analysis/dashboard.html',
        bucket_summary=bucket_summary,
        monthly_trend=monthly_trend,
        revenue_types=[r['revenue_type'] for r in revenue_types],
        selected_revenue_type=revenue_type,
        available_years=available_years,
        selected_year=year,
        totals=totals,
        rate_ladder=rate_ladder,
        thirty_sec_rate=thirty_sec_rate,
        top_rate_combos=top_rate_combos,
        bottom_rate_combos=bottom_rate_combos
    )


@length_analysis_bp.route('/by-language')
def by_language():
    """Length mix analysis by language group."""
    container = get_container()
    db = container.get("database_connection")
    revenue_type = request.args.get('revenue_type', 'Internal Ad Sales')
    year = request.args.get('year', '')
    
    year_filter = ""
    year_param = []
    if year:
        year_filter = "AND v.broadcast_month LIKE ?"
        year_param = [f"%-{year[-2:]}"]
    
    with db.connection() as conn:
        years = conn.execute("""
            SELECT DISTINCT '20' || SUBSTR(broadcast_month, 5, 2) AS year
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            ORDER BY year DESC
        """, [revenue_type]).fetchall()
        available_years = [row['year'] for row in years]
        
        # Aggregate by language_group, not individual language_code
        language_mix = conn.execute(f"""
            SELECT 
                l.language_group AS language_name,
                l.language_group AS language_code,
                l.language_group,
                COUNT(*) AS total_spots,
                ROUND(SUM(v.gross_rate), 2) AS total_revenue,
                ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                ROUND(AVG(v.margin_pct), 2) AS avg_margin,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = 'Billboard' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_billboard,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':15' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_15,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':30' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_30,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':45' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_45,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':60' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_60,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket IN ('Extended', ':120', 'Long-form', 'Program-length') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_long
            FROM spots s
            JOIN v_spot_length_analysis v ON s.spot_id = v.spot_id
            JOIN languages l ON s.language_code = l.language_code
            WHERE v.revenue_type = ?
              AND s.language_code IS NOT NULL
              AND s.language_code != ''
              {year_filter}
            GROUP BY l.language_group
            ORDER BY total_revenue DESC
        """, [revenue_type] + year_param).fetchall()
        
        # Rate breakdown by length for each language GROUP
        language_groups = [row['language_group'] for row in language_mix]
        language_length_rates = {}
        if language_groups:
            placeholders = ','.join(['?' for _ in language_groups])
            length_breakdown = conn.execute(f"""
                SELECT 
                    l.language_group AS language_code,
                    v.length_bucket,
                    v.bucket_sort_order,
                    COUNT(*) AS spots,
                    ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                    ROUND(AVG(v.margin_pct), 2) AS avg_margin
                FROM spots s
                JOIN v_spot_length_analysis v ON s.spot_id = v.spot_id
                JOIN languages l ON s.language_code = l.language_code
                WHERE v.revenue_type = ?
                  AND l.language_group IN ({placeholders})
                  {year_filter}
                GROUP BY l.language_group, v.length_bucket, v.bucket_sort_order
                HAVING COUNT(*) >= 5
                ORDER BY l.language_group, v.bucket_sort_order
            """, [revenue_type] + language_groups + year_param).fetchall()
            
            for row in length_breakdown:
                code = row['language_code']
                if code not in language_length_rates:
                    language_length_rates[code] = []
                language_length_rates[code].append({
                    'bucket': row['length_bucket'],
                    'spots': row['spots'],
                    'avg_rate': row['avg_rate'],
                    'avg_margin': row['avg_margin']
                })
    
    return render_template(
        'length_analysis/by_language.html',
        language_mix=language_mix,
        language_length_rates=language_length_rates,
        selected_revenue_type=revenue_type,
        available_years=available_years,
        selected_year=year
    )


@length_analysis_bp.route('/by-market')
def by_market():
    """Length mix analysis by market."""
    container = get_container()
    db = container.get("database_connection")
    revenue_type = request.args.get('revenue_type', 'Internal Ad Sales')
    year = request.args.get('year', '')
    
    # Build year filter for SQL
    year_filter = ""
    year_param = []
    if year:
        year_filter = "AND v.broadcast_month LIKE ?"
        year_param = [f"%-{year[-2:]}"]
    
    with db.connection() as conn:
        # Get available years for filter
        years = conn.execute("""
            SELECT DISTINCT '20' || SUBSTR(broadcast_month, 5, 2) AS year
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            ORDER BY year DESC
        """, [revenue_type]).fetchall()
        available_years = [row['year'] for row in years]
        
        # Summary by market
        market_mix = conn.execute(f"""
            SELECT 
                v.market_code,
                COUNT(*) AS total_spots,
                ROUND(SUM(v.gross_rate), 2) AS total_revenue,
                ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                ROUND(AVG(v.margin_pct), 2) AS avg_margin,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = 'Billboard' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_billboard,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':15' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_15,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':30' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_30,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':45' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_45,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket = ':60' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_60,
                ROUND(100.0 * SUM(CASE WHEN v.length_bucket IN ('Extended', ':120', 'Long-form', 'Program-length') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_long
            FROM v_spot_length_analysis v
            WHERE v.revenue_type = ?
              AND v.market_code IS NOT NULL
              {year_filter}
            GROUP BY v.market_code
            ORDER BY total_revenue DESC
        """, [revenue_type] + year_param).fetchall()
        
        # Get rate breakdown by length for each market
        market_codes = [row['market_code'] for row in market_mix]
        market_length_rates = {}
        if market_codes:
            placeholders = ','.join(['?' for _ in market_codes])
            length_breakdown = conn.execute(f"""
                SELECT 
                    v.market_code,
                    v.length_bucket,
                    v.bucket_sort_order,
                    COUNT(*) AS spots,
                    ROUND(AVG(v.gross_rate), 2) AS avg_rate,
                    ROUND(AVG(v.margin_pct), 2) AS avg_margin
                FROM v_spot_length_analysis v
                WHERE v.revenue_type = ?
                  AND v.market_code IN ({placeholders})
                  {year_filter}
                GROUP BY v.market_code, v.length_bucket, v.bucket_sort_order
                HAVING COUNT(*) >= 5
                ORDER BY v.market_code, v.bucket_sort_order
            """, [revenue_type] + market_codes + year_param).fetchall()
            
            for row in length_breakdown:
                code = row['market_code']
                if code not in market_length_rates:
                    market_length_rates[code] = []
                market_length_rates[code].append({
                    'bucket': row['length_bucket'],
                    'spots': row['spots'],
                    'avg_rate': row['avg_rate'],
                    'avg_margin': row['avg_margin']
                })
    
    return render_template(
        'length_analysis/by_market.html',
        market_mix=market_mix,
        market_length_rates=market_length_rates,
        selected_revenue_type=revenue_type,
        available_years=available_years,
        selected_year=year
    )

@length_analysis_bp.route('/by-customer')
def by_customer():
    """Length mix analysis by customer."""
    container = get_container()
    db = container.get("database_connection")
    revenue_type = request.args.get('revenue_type', 'Internal Ad Sales')
    year = request.args.get('year', '')
    min_spots = request.args.get('min_spots', 100, type=int)
    
    # Build year filter for SQL
    year_filter = ""
    year_param = []
    if year:
        year_filter = "AND broadcast_month LIKE ?"
        year_param = [f"%-{year[-2:]}"]
    
    with db.connection() as conn:
        # Get available years for filter
        years = conn.execute("""
            SELECT DISTINCT '20' || SUBSTR(broadcast_month, 5, 2) AS year
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            ORDER BY year DESC
        """, [revenue_type]).fetchall()
        available_years = [row['year'] for row in years]
        
        customer_mix = conn.execute(f"""
            SELECT 
                customer_name,
                MIN(customer_id) AS customer_id,
                COUNT(*) AS total_spots,
                ROUND(SUM(gross_rate), 2) AS total_revenue,
                ROUND(AVG(gross_rate), 2) AS avg_rate,
                ROUND(AVG(margin_pct), 2) AS avg_margin,
                ROUND(100.0 * SUM(CASE WHEN length_bucket = 'Billboard' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_billboard,
                ROUND(100.0 * SUM(CASE WHEN length_bucket = ':15' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_15,
                ROUND(100.0 * SUM(CASE WHEN length_bucket = ':30' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_30,
                ROUND(100.0 * SUM(CASE WHEN length_bucket = ':45' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_45,
                ROUND(100.0 * SUM(CASE WHEN length_bucket = ':60' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_60,
                ROUND(100.0 * SUM(CASE WHEN length_bucket IN ('Extended', ':120', 'Long-form', 'Program-length') THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_long
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
              AND customer_name IS NOT NULL
              {year_filter}
            GROUP BY customer_name
            HAVING COUNT(*) >= ?
            ORDER BY total_revenue DESC
            LIMIT 50
        """, [revenue_type] + year_param + [min_spots]).fetchall()
        
        # Get rate breakdown by length for each customer
        customer_names = [row['customer_name'] for row in customer_mix]
        customer_length_rates = {}
        if customer_names:
            placeholders = ','.join(['?' for _ in customer_names])
            length_breakdown = conn.execute(f"""
                SELECT 
                    customer_name,
                    length_bucket,
                    bucket_sort_order,
                    COUNT(*) AS spots,
                    ROUND(AVG(gross_rate), 2) AS avg_rate,
                    ROUND(AVG(margin_pct), 2) AS avg_margin
                FROM v_spot_length_analysis
                WHERE revenue_type = ?
                  AND customer_name IN ({placeholders})
                  {year_filter}
                GROUP BY customer_name, length_bucket, bucket_sort_order
                HAVING COUNT(*) >= 5
                ORDER BY customer_name, bucket_sort_order
            """, [revenue_type] + customer_names + year_param).fetchall()
            
            for row in length_breakdown:
                cust = row['customer_name']
                if cust not in customer_length_rates:
                    customer_length_rates[cust] = []
                customer_length_rates[cust].append({
                    'bucket': row['length_bucket'],
                    'spots': row['spots'],
                    'avg_rate': row['avg_rate'],
                    'avg_margin': row['avg_margin']
                })
    
    return render_template(
        'length_analysis/by_customer.html',
        customer_mix=customer_mix,
        customer_length_rates=customer_length_rates,
        selected_revenue_type=revenue_type,
        available_years=available_years,
        selected_year=year,
        min_spots=min_spots
    )


@length_analysis_bp.route('/margin-analysis')
def margin_analysis():
    """Deep dive on margin by length."""
    container = get_container()
    db = container.get("database_connection")
    revenue_type = request.args.get('revenue_type', 'Internal Ad Sales')
    year = request.args.get('year', '')
    
    # Build year filter for SQL
    year_filter = ""
    year_param = []
    if year:
        year_filter = "AND broadcast_month LIKE ?"
        year_param = [f"%-{year[-2:]}"]
    
    with db.connection() as conn:
        def rows_to_dicts(rows):
            return [dict(row) for row in rows]
        
        # Get available years for filter
        years = conn.execute("""
            SELECT DISTINCT '20' || SUBSTR(broadcast_month, 5, 2) AS year
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            ORDER BY year DESC
        """, [revenue_type]).fetchall()
        available_years = [row['year'] for row in years]
        
        # Margin distribution by bucket
        margin_by_bucket_raw = conn.execute(f"""
            SELECT 
                length_bucket,
                bucket_sort_order,
                COUNT(*) AS spot_count,
                ROUND(SUM(gross_rate), 2) AS gross_revenue,
                ROUND(SUM(station_net), 2) AS net_revenue,
                ROUND(SUM(gross_rate) - SUM(station_net), 2) AS margin_dollars,
                ROUND(AVG(margin_pct), 2) AS avg_margin_pct,
                ROUND(MIN(margin_pct), 2) AS min_margin,
                ROUND(MAX(margin_pct), 2) AS max_margin
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
            {year_filter}
            GROUP BY length_bucket, bucket_sort_order
            ORDER BY bucket_sort_order
        """, [revenue_type] + year_param).fetchall()
        margin_by_bucket = rows_to_dicts(margin_by_bucket_raw)
        
        # Low margin spots (investigation list)
        low_margin_spots = conn.execute(f"""
            SELECT 
                customer_name,
                length_bucket,
                COUNT(*) AS spot_count,
                ROUND(AVG(gross_rate), 2) AS avg_rate,
                ROUND(AVG(margin_pct), 2) AS avg_margin
            FROM v_spot_length_analysis
            WHERE revenue_type = ?
              AND margin_pct < 5
              AND customer_name IS NOT NULL
              {year_filter}
            GROUP BY customer_name, length_bucket
            HAVING COUNT(*) >= 10
            ORDER BY spot_count DESC
            LIMIT 20
        """, [revenue_type] + year_param).fetchall()
    
    return render_template(
        'length_analysis/margin_analysis.html',
        margin_by_bucket=margin_by_bucket,
        low_margin_spots=low_margin_spots,
        selected_revenue_type=revenue_type,
        available_years=available_years,
        selected_year=year
    )