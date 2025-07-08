"""
Multi-Language (Cross-Audience) Deep Analysis Framework
======================================================

Advanced analysis tools for understanding the $407,960.30 Multi-Language category.
This provides systematic methodology for uncovering cross-audience opportunities.

Key Questions to Answer:
1. WHO are the cross-audience advertisers?
2. WHEN do cross-audience spots perform best?
3. WHERE (which dayparts) offer the most opportunity?
4. WHY do some languages cross over better than others?
5. HOW can we optimize cross-audience inventory pricing?
"""

import sqlite3
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from query_builders import MultiLanguageQueryBuilder, BaseQueryBuilder


@dataclass
class CrossAudienceInsight:
    """Container for cross-audience analysis results"""
    category: str
    revenue: float
    spots: int
    percentage: float
    details: Dict[str, Any]


class MultiLanguageDeepAnalyzer:
    """
    Advanced analyzer for Multi-Language (Cross-Audience) revenue category
    
    This provides systematic tools for understanding:
    - Filipino leadership patterns (60.3% dominance)
    - Transition time opportunities (16:00-19:00)
    - Cross-audience customer behavior
    - Pricing optimization opportunities
    """
    
    def __init__(self, db_path: str = "data/database/production.db"):
        self.db_path = db_path
        self.db_connection = None
    
    def __enter__(self):
        self.db_connection = sqlite3.connect(self.db_path)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_connection:
            self.db_connection.close()
    
    def analyze_complete_cross_audience_patterns(self, year: str = "2024") -> Dict[str, Any]:
        """
        Complete multi-dimensional analysis of cross-audience patterns
        
        Returns comprehensive insights across all key dimensions.
        """
        
        print(f"🔍 Deep Analysis: Multi-Language (Cross-Audience) for {year}")
        print("=" * 60)
        
        results = {
            'year': year,
            'total_category_revenue': self._get_base_category_revenue(year),
            'language_leadership': self._analyze_language_leadership(year),
            'time_opportunity_analysis': self._analyze_time_opportunities(year),
            'customer_cross_audience_strategy': self._analyze_customer_strategies(year),
            'daypart_performance': self._analyze_daypart_performance(year),
            'transition_time_goldmine': self._analyze_transition_times(year),
            'weekend_vs_weekday': self._analyze_weekend_patterns(year),
            'agency_cross_audience_expertise': self._analyze_agency_patterns(year),
            'pricing_optimization_opportunities': self._analyze_pricing_opportunities(year),
            'seasonal_patterns': self._analyze_seasonal_patterns(year)
        }
        
        return results
    
    def _get_base_category_revenue(self, year: str) -> Dict[str, Any]:
        """Get base Multi-Language category metrics"""
        builder = MultiLanguageQueryBuilder(year)
        builder.add_multi_language_conditions().exclude_chinese_prime_time().exclude_nkb_overnight_shopping()
        result = builder.execute_revenue_query(self.db_connection)
        
        return {
            'revenue': result.revenue,
            'spots': result.spot_count,
            'avg_rate': result.revenue / result.spot_count if result.spot_count > 0 else 0
        }
    
    def _analyze_language_leadership(self, year: str) -> Dict[str, Any]:
        """
        Deep dive into language code patterns
        
        Filipino (T) dominance: 60.3%, but what drives the other 39.7%?
        """
        
        query = f"""
        SELECT 
            COALESCE(s.language_code, 'Unknown') as language_code,
            -- Language name mapping
            CASE 
                WHEN s.language_code = 'T' THEN 'Filipino (Tagalog)'
                WHEN s.language_code = 'M' THEN 'Mandarin'
                WHEN s.language_code = 'C' THEN 'Cantonese'
                WHEN s.language_code = 'V' THEN 'Vietnamese'
                WHEN s.language_code = 'H' THEN 'Hindi'
                WHEN s.language_code = 'K' THEN 'Korean'
                WHEN s.language_code = 'SA' THEN 'South Asian'
                WHEN s.language_code = 'E' THEN 'English'
                WHEN s.language_code = 'Hm' THEN 'Hmong'
                WHEN s.language_code = 'M/C' THEN 'Mandarin/Cantonese Mix'
                ELSE 'Unknown (' || COALESCE(s.language_code, 'NULL') || ')'
            END as language_name,
            
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            
            -- Market penetration
            COUNT(DISTINCT s.customer_id) as unique_customers,
            COUNT(DISTINCT s.agency_id) as unique_agencies,
            COUNT(DISTINCT DATE(s.air_date)) as unique_days,
            
            -- Time patterns
            AVG(
                CASE 
                    WHEN s.time_in LIKE '__:__:__' THEN 
                        CAST(SUBSTR(s.time_in, 1, 2) AS INTEGER) * 60 + 
                        CAST(SUBSTR(s.time_in, 4, 2) AS INTEGER)
                    ELSE NULL 
                END
            ) as avg_start_minute,
            
            -- Revenue percentages
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM spots s2
                JOIN spot_language_blocks slb2 ON s2.spot_id = slb2.spot_id
                LEFT JOIN agencies a2 ON s2.agency_id = a2.agency_id
                LEFT JOIN customers c2 ON s2.customer_id = c2.customer_id
                WHERE s2.broadcast_month LIKE '%-{year[-2:]}'
                AND (s2.revenue_type != 'Trade' OR s2.revenue_type IS NULL)
                AND (s2.gross_rate IS NOT NULL OR s2.station_net IS NOT NULL OR s2.spot_type = 'BNS')
                AND (slb2.spans_multiple_blocks = 1 OR 
                     (slb2.spans_multiple_blocks = 0 AND slb2.block_id IS NULL) OR 
                     (slb2.spans_multiple_blocks IS NULL AND slb2.block_id IS NULL))
                AND COALESCE(a2.agency_name, '') NOT LIKE '%WorldLink%'
                AND COALESCE(s2.bill_code, '') NOT LIKE '%WorldLink%'
                AND NOT (
                    (s2.time_in >= '19:00:00' AND s2.time_out <= '23:59:59' 
                     AND s2.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                    OR
                    (s2.time_in >= '20:00:00' AND s2.time_out <= '23:59:59'
                     AND s2.day_of_week IN ('Saturday', 'Sunday'))
                )
                AND COALESCE(c2.normalized_name, '') NOT LIKE '%NKB%'
                AND COALESCE(s2.bill_code, '') NOT LIKE '%NKB%'
                AND COALESCE(a2.agency_name, '') NOT LIKE '%NKB%'
            ), 2) as spot_percentage,
            
            ROUND(SUM(COALESCE(s.gross_rate, 0)) * 100.0 / (
                SELECT SUM(COALESCE(s3.gross_rate, 0)) FROM spots s3
                JOIN spot_language_blocks slb3 ON s3.spot_id = slb3.spot_id
                LEFT JOIN agencies a3 ON s3.agency_id = a3.agency_id
                LEFT JOIN customers c3 ON s3.customer_id = c3.customer_id
                WHERE s3.broadcast_month LIKE '%-{year[-2:]}'
                AND (s3.revenue_type != 'Trade' OR s3.revenue_type IS NULL)
                AND (s3.gross_rate IS NOT NULL OR s3.station_net IS NOT NULL OR s3.spot_type = 'BNS')
                AND (slb3.spans_multiple_blocks = 1 OR 
                     (slb3.spans_multiple_blocks = 0 AND slb3.block_id IS NULL) OR 
                     (slb3.spans_multiple_blocks IS NULL AND slb3.block_id IS NULL))
                AND COALESCE(a3.agency_name, '') NOT LIKE '%WorldLink%'
                AND COALESCE(s3.bill_code, '') NOT LIKE '%WorldLink%'
                AND NOT (
                    (s3.time_in >= '19:00:00' AND s3.time_out <= '23:59:59' 
                     AND s3.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
                    OR
                    (s3.time_in >= '20:00:00' AND s3.time_out <= '23:59:59'
                     AND s3.day_of_week IN ('Saturday', 'Sunday'))
                )
                AND COALESCE(c3.normalized_name, '') NOT LIKE '%NKB%'
                AND COALESCE(s3.bill_code, '') NOT LIKE '%NKB%'
                AND COALESCE(a3.agency_name, '') NOT LIKE '%NKB%'
            ), 2) as revenue_percentage
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY s.language_code
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        language_analysis = []
        for row in results:
            language_analysis.append({
                'language_code': row[0],
                'language_name': row[1],
                'spots': row[2],
                'revenue': row[3],
                'avg_rate': row[4],
                'unique_customers': row[5],
                'unique_agencies': row[6],
                'unique_days': row[7],
                'avg_start_time': f"{int(row[8] // 60):02d}:{int(row[8] % 60):02d}" if row[8] else "Unknown",
                'spot_percentage': row[9],
                'revenue_percentage': row[10]
            })
        
        return {
            'language_breakdown': language_analysis,
            'filipino_dominance': next((lang for lang in language_analysis if lang['language_code'] == 'T'), None),
            'top_cross_languages': language_analysis[:5]
        }
    
    def _analyze_time_opportunities(self, year: str) -> Dict[str, Any]:
        """
        Analyze time-based patterns for cross-audience opportunities
        
        Focus on the transition time opportunity (16:00-19:00) mentioned in your guide.
        """
        
        query = f"""
        SELECT 
            -- Time buckets
            CASE 
                WHEN s.time_in >= '06:00:00' AND s.time_in < '09:00:00' THEN 'Early Morning (6am-9am)'
                WHEN s.time_in >= '09:00:00' AND s.time_in < '12:00:00' THEN 'Morning (9am-12pm)'
                WHEN s.time_in >= '12:00:00' AND s.time_in < '15:00:00' THEN 'Midday (12pm-3pm)'
                WHEN s.time_in >= '15:00:00' AND s.time_in < '16:00:00' THEN 'Afternoon Lead-in (3pm-4pm)'
                WHEN s.time_in >= '16:00:00' AND s.time_in < '19:00:00' THEN '🎯 TRANSITION GOLDMINE (4pm-7pm)'
                WHEN s.time_in >= '19:00:00' AND s.time_in < '22:00:00' THEN 'Prime Time (7pm-10pm)'
                WHEN s.time_in >= '22:00:00' AND s.time_in < '24:00:00' THEN 'Late Night (10pm-12am)'
                ELSE 'Overnight (12am-6am)'
            END as time_bucket,
            
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            COUNT(DISTINCT s.language_code) as language_diversity,
            
            -- Day type breakdown
            COUNT(CASE WHEN s.day_of_week IN ('Saturday', 'Sunday') THEN 1 END) as weekend_spots,
            COUNT(CASE WHEN s.day_of_week NOT IN ('Saturday', 'Sunday') THEN 1 END) as weekday_spots,
            
            -- Language code breakdown for this time
            GROUP_CONCAT(DISTINCT s.language_code) as languages_in_timeframe,
            
            -- Premium vs standard rates
            COUNT(CASE WHEN s.gross_rate > 50 THEN 1 END) as premium_spots,
            COUNT(CASE WHEN s.gross_rate <= 50 THEN 1 END) as standard_spots
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY time_bucket
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        time_analysis = []
        transition_goldmine = None
        
        for row in results:
            time_data = {
                'time_bucket': row[0],
                'spots': row[1],
                'revenue': row[2],
                'avg_rate': row[3],
                'unique_customers': row[4],
                'language_diversity': row[5],
                'weekend_spots': row[6],
                'weekday_spots': row[7],
                'languages': row[8].split(',') if row[8] else [],
                'premium_spots': row[9],
                'standard_spots': row[10]
            }
            time_analysis.append(time_data)
            
            if '🎯 TRANSITION GOLDMINE' in row[0]:
                transition_goldmine = time_data
        
        return {
            'time_bucket_analysis': time_analysis,
            'transition_goldmine': transition_goldmine,
            'top_time_buckets': sorted(time_analysis, key=lambda x: x['revenue'], reverse=True)[:3]
        }
    
    def _analyze_customer_strategies(self, year: str) -> Dict[str, Any]:
        """
        Analyze which customers are using cross-audience strategies effectively
        
        This reveals WHO understands cross-audience value.
        """
        
        query = f"""
        SELECT 
            COALESCE(c.normalized_name, 'Unknown Customer') as customer_name,
            COUNT(*) as total_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            
            -- Language diversity strategy
            COUNT(DISTINCT s.language_code) as languages_used,
            GROUP_CONCAT(DISTINCT s.language_code) as language_codes,
            
            -- Time diversity strategy  
            COUNT(DISTINCT 
                CASE 
                    WHEN s.time_in >= '06:00:00' AND s.time_in < '12:00:00' THEN 'Morning'
                    WHEN s.time_in >= '12:00:00' AND s.time_in < '18:00:00' THEN 'Afternoon'
                    WHEN s.time_in >= '18:00:00' AND s.time_in < '24:00:00' THEN 'Evening'
                    ELSE 'Overnight'
                END
            ) as dayparts_used,
            
            -- Weekend vs weekday strategy
            COUNT(CASE WHEN s.day_of_week IN ('Saturday', 'Sunday') THEN 1 END) as weekend_spots,
            COUNT(CASE WHEN s.day_of_week NOT IN ('Saturday', 'Sunday') THEN 1 END) as weekday_spots,
            
            -- Consistency indicators
            COUNT(DISTINCT DATE(s.air_date)) as unique_days,
            COUNT(DISTINCT s.agency_id) as agencies_used,
            
            -- Sector classification
            COALESCE(sect.sector_name, 'Unknown Sector') as sector,
            
            -- Performance indicators
            MIN(s.gross_rate) as min_rate,
            MAX(s.gross_rate) as max_rate,
            
            -- Filipino focus indicator
            COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) as filipino_spots,
            ROUND(COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) * 100.0 / COUNT(*), 1) as filipino_percentage
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY c.normalized_name, sect.sector_name
        HAVING COUNT(*) >= 5  -- Focus on customers with meaningful cross-audience strategy
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        customer_analysis = []
        for row in results:
            customer_analysis.append({
                'customer_name': row[0],
                'total_spots': row[1],
                'total_revenue': row[2],
                'avg_rate': row[3],
                'languages_used': row[4],
                'language_codes': row[5].split(',') if row[5] else [],
                'dayparts_used': row[6],
                'weekend_spots': row[7],
                'weekday_spots': row[8],
                'unique_days': row[9],
                'agencies_used': row[10],
                'sector': row[11],
                'min_rate': row[12],
                'max_rate': row[13],
                'filipino_spots': row[14],
                'filipino_percentage': row[15]
            })
        
        return {
            'top_cross_audience_customers': customer_analysis[:10],
            'government_advertisers': [c for c in customer_analysis if 'government' in c['sector'].lower() or 'public' in c['sector'].lower()],
            'filipino_specialists': [c for c in customer_analysis if c['filipino_percentage'] >= 70],
            'multi_language_strategists': [c for c in customer_analysis if c['languages_used'] >= 3]
        }
    
    def _analyze_daypart_performance(self, year: str) -> Dict[str, Any]:
        """Analyze performance by day of week and time combinations"""
        
        query = f"""
        SELECT 
            s.day_of_week,
            CASE 
                WHEN s.time_in >= '06:00:00' AND s.time_in < '12:00:00' THEN 'Morning'
                WHEN s.time_in >= '12:00:00' AND s.time_in < '18:00:00' THEN 'Afternoon'  
                WHEN s.time_in >= '18:00:00' AND s.time_in < '24:00:00' THEN 'Evening'
                ELSE 'Overnight'
            END as daypart,
            
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            
            -- Language breakdown for this daypart
            COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) as filipino_spots,
            COUNT(CASE WHEN s.language_code IN ('M', 'C', 'M/C') THEN 1 END) as chinese_spots,
            COUNT(CASE WHEN s.language_code = 'V' THEN 1 END) as vietnamese_spots
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY s.day_of_week, daypart
        ORDER BY s.day_of_week, daypart
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        daypart_matrix = []
        for row in results:
            daypart_matrix.append({
                'day_of_week': row[0],
                'daypart': row[1],
                'spots': row[2],
                'revenue': row[3],
                'avg_rate': row[4],
                'unique_customers': row[5],
                'filipino_spots': row[6],
                'chinese_spots': row[7],
                'vietnamese_spots': row[8]
            })
        
        return {
            'daypart_matrix': daypart_matrix,
            'weekend_performance': [d for d in daypart_matrix if d['day_of_week'] in ['Saturday', 'Sunday']],
            'weekday_performance': [d for d in daypart_matrix if d['day_of_week'] not in ['Saturday', 'Sunday']]
        }
    
    def _analyze_transition_times(self, year: str) -> Dict[str, Any]:
        """Deep dive into the 16:00-19:00 transition time opportunity"""
        
        query = f"""
        SELECT 
            s.time_in,
            s.time_out,
            s.day_of_week,
            s.language_code,
            COALESCE(c.normalized_name, 'Unknown') as customer_name,
            s.gross_rate,
            
            -- Time classification
            CASE 
                WHEN s.time_in >= '16:00:00' AND s.time_in < '17:00:00' THEN '4pm Hour'
                WHEN s.time_in >= '17:00:00' AND s.time_in < '18:00:00' THEN '5pm Hour'
                WHEN s.time_in >= '18:00:00' AND s.time_in < '19:00:00' THEN '6pm Hour'
                ELSE 'Other'
            END as transition_hour
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        -- Focus on transition time
        AND s.time_in >= '16:00:00' AND s.time_in < '19:00:00'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        ORDER BY s.time_in, s.gross_rate DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        transition_analysis = []
        for row in results:
            transition_analysis.append({
                'time_in': row[0],
                'time_out': row[1],
                'day_of_week': row[2],
                'language_code': row[3],
                'customer_name': row[4],
                'gross_rate': row[5],
                'transition_hour': row[6]
            })
        
        # Aggregate by hour
        hour_summary = {}
        for spot in transition_analysis:
            hour = spot['transition_hour']
            if hour not in hour_summary:
                hour_summary[hour] = {'spots': 0, 'revenue': 0, 'customers': set(), 'languages': set()}
            
            hour_summary[hour]['spots'] += 1
            hour_summary[hour]['revenue'] += spot['gross_rate'] or 0
            hour_summary[hour]['customers'].add(spot['customer_name'])
            hour_summary[hour]['languages'].add(spot['language_code'])
        
        # Convert sets to counts
        for hour in hour_summary:
            hour_summary[hour]['unique_customers'] = len(hour_summary[hour]['customers'])
            hour_summary[hour]['unique_languages'] = len(hour_summary[hour]['languages'])
            hour_summary[hour]['avg_rate'] = hour_summary[hour]['revenue'] / hour_summary[hour]['spots'] if hour_summary[hour]['spots'] > 0 else 0
            del hour_summary[hour]['customers']  # Remove sets for JSON serialization
            del hour_summary[hour]['languages']
        
        return {
            'transition_spots': transition_analysis,
            'hourly_summary': hour_summary,
            'total_transition_revenue': sum(spot['gross_rate'] or 0 for spot in transition_analysis),
            'transition_opportunity_size': len(transition_analysis)
        }
    
    def _analyze_weekend_patterns(self, year: str) -> Dict[str, Any]:
        """Weekend vs weekday cross-audience patterns"""
        
        query = f"""
        SELECT 
            CASE 
                WHEN s.day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
                ELSE 'Weekday'
            END as day_type,
            
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            COUNT(DISTINCT s.language_code) as language_diversity,
            
            -- Top languages for this day type
            COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) as filipino_spots,
            COUNT(CASE WHEN s.language_code IN ('M', 'C', 'M/C') THEN 1 END) as chinese_spots,
            COUNT(CASE WHEN s.language_code = 'V' THEN 1 END) as vietnamese_spots,
            COUNT(CASE WHEN s.language_code = 'Hm' THEN 1 END) as hmong_spots
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY day_type
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        weekend_analysis = {}
        for row in results:
            weekend_analysis[row[0]] = {
                'spots': row[1],
                'revenue': row[2],
                'avg_rate': row[3],
                'unique_customers': row[4],
                'language_diversity': row[5],
                'filipino_spots': row[6],
                'chinese_spots': row[7],
                'vietnamese_spots': row[8],
                'hmong_spots': row[9]
            }
        
        return weekend_analysis
    
    def _analyze_agency_patterns(self, year: str) -> Dict[str, Any]:
        """Which agencies understand cross-audience buying?"""
        
        query = f"""
        SELECT 
            COALESCE(a.agency_name, 'Direct/No Agency') as agency_name,
            COUNT(*) as total_spots,
            SUM(COALESCE(s.gross_rate, 0)) as total_revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            COUNT(DISTINCT s.language_code) as languages_used,
            
            -- Cross-audience strategy indicators
            COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) as filipino_spots,
            ROUND(COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) * 100.0 / COUNT(*), 1) as filipino_percentage,
            
            -- Time diversity
            COUNT(DISTINCT 
                CASE 
                    WHEN s.time_in >= '06:00:00' AND s.time_in < '12:00:00' THEN 'Morning'
                    WHEN s.time_in >= '12:00:00' AND s.time_in < '18:00:00' THEN 'Afternoon'
                    WHEN s.time_in >= '18:00:00' AND s.time_in < '24:00:00' THEN 'Evening'
                    ELSE 'Overnight'
                END
            ) as dayparts_used
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY a.agency_name
        HAVING COUNT(*) >= 10  -- Focus on agencies with meaningful activity
        ORDER BY SUM(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        agency_analysis = []
        for row in results:
            agency_analysis.append({
                'agency_name': row[0],
                'total_spots': row[1],
                'total_revenue': row[2],
                'avg_rate': row[3],
                'unique_customers': row[4],
                'languages_used': row[5],
                'filipino_spots': row[6],
                'filipino_percentage': row[7],
                'dayparts_used': row[8]
            })
        
        return {
            'top_cross_audience_agencies': agency_analysis[:10],
            'filipino_specialists': [a for a in agency_analysis if a['filipino_percentage'] >= 50],
            'multi_language_experts': [a for a in agency_analysis if a['languages_used'] >= 4]
        }
    
    def _analyze_pricing_opportunities(self, year: str) -> Dict[str, Any]:
        """Identify pricing optimization opportunities"""
        
        # This analysis identifies underpriced vs overpriced cross-audience inventory
        query = f"""
        SELECT 
            s.language_code,
            CASE 
                WHEN s.time_in >= '16:00:00' AND s.time_in < '19:00:00' THEN 'Transition Time'
                WHEN s.day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
                ELSE 'Standard'
            END as inventory_type,
            
            COUNT(*) as spots,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            MIN(s.gross_rate) as min_rate,
            MAX(s.gross_rate) as max_rate,
            0 as rate_stddev
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND s.gross_rate > 0  -- Focus on paid inventory
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY s.language_code, inventory_type
        HAVING COUNT(*) >= 5
        ORDER BY AVG(COALESCE(s.gross_rate, 0)) DESC
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        pricing_analysis = []
        for row in results:
            pricing_analysis.append({
                'language_code': row[0],
                'inventory_type': row[1],
                'spots': row[2],
                'avg_rate': row[3],
                'min_rate': row[4],
                'max_rate': row[5],
                'rate_stddev': row[6] or 0,
                'rate_range': (row[5] or 0) - (row[4] or 0)
            })
        
        return {
            'pricing_by_language_and_type': pricing_analysis,
            'premium_opportunities': [p for p in pricing_analysis if p['inventory_type'] == 'Transition Time'],
            'weekend_pricing': [p for p in pricing_analysis if p['inventory_type'] == 'Weekend']
        }
    
    def _analyze_seasonal_patterns(self, year: str) -> Dict[str, Any]:
        """Monthly patterns in cross-audience advertising"""
        
        query = f"""
        SELECT 
            s.broadcast_month,
            COUNT(*) as spots,
            SUM(COALESCE(s.gross_rate, 0)) as revenue,
            AVG(COALESCE(s.gross_rate, 0)) as avg_rate,
            COUNT(DISTINCT s.customer_id) as unique_customers,
            
            -- Language breakdown by month
            COUNT(CASE WHEN s.language_code = 'T' THEN 1 END) as filipino_spots,
            COUNT(CASE WHEN s.language_code IN ('M', 'C', 'M/C') THEN 1 END) as chinese_spots,
            COUNT(CASE WHEN s.language_code = 'V' THEN 1 END) as vietnamese_spots
            
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        WHERE s.broadcast_month LIKE '%-{year[-2:]}'
        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        AND (s.gross_rate IS NOT NULL OR s.station_net IS NOT NULL OR s.spot_type = 'BNS')
        AND (slb.spans_multiple_blocks = 1 OR 
             (slb.spans_multiple_blocks = 0 AND slb.block_id IS NULL) OR 
             (slb.spans_multiple_blocks IS NULL AND slb.block_id IS NULL))
        AND COALESCE(a.agency_name, '') NOT LIKE '%WorldLink%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%WorldLink%'
        AND NOT (
            (s.time_in >= '19:00:00' AND s.time_out <= '23:59:59' 
             AND s.day_of_week IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'))
            OR
            (s.time_in >= '20:00:00' AND s.time_out <= '23:59:59'
             AND s.day_of_week IN ('Saturday', 'Sunday'))
        )
        AND COALESCE(c.normalized_name, '') NOT LIKE '%NKB%'
        AND COALESCE(s.bill_code, '') NOT LIKE '%NKB%'
        AND COALESCE(a.agency_name, '') NOT LIKE '%NKB%'
        GROUP BY s.broadcast_month
        ORDER BY s.broadcast_month
        """
        
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        seasonal_analysis = []
        for row in results:
            seasonal_analysis.append({
                'month': row[0],
                'spots': row[1],
                'revenue': row[2],
                'avg_rate': row[3],
                'unique_customers': row[4],
                'filipino_spots': row[5],
                'chinese_spots': row[6],
                'vietnamese_spots': row[7]
            })
        
        return {
            'monthly_breakdown': seasonal_analysis,
            'peak_months': sorted(seasonal_analysis, key=lambda x: x['revenue'], reverse=True)[:3],
            'growth_months': seasonal_analysis  # Could add month-over-month growth calculation
        }


def generate_multi_language_report(analysis_results: Dict[str, Any]) -> str:
    """Generate comprehensive markdown report for Multi-Language analysis"""
    
    report = f"""# Multi-Language (Cross-Audience) Deep Analysis Report - {analysis_results['year']}

*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*

## 🎯 Executive Summary

**Category Revenue**: ${analysis_results['total_category_revenue']['revenue']:,.2f}  
**Total Spots**: {analysis_results['total_category_revenue']['spots']:,}  
**Average Rate**: ${analysis_results['total_category_revenue']['avg_rate']:,.2f}  
**Strategic Finding**: Filipino programming drives cross-audience advertising

## 🏆 Key Discoveries

### 1. Filipino Cross-Audience Dominance
"""
    
    filipino_data = analysis_results['language_leadership']['filipino_dominance']
    if filipino_data:
        report += f"""
- **Filipino Revenue Share**: {filipino_data['revenue_percentage']}% (${filipino_data['revenue']:,.2f})
- **Filipino Spot Share**: {filipino_data['spot_percentage']}% ({filipino_data['spots']:,} spots)
- **Unique Customers**: {filipino_data['unique_customers']} using Filipino cross-audience strategy
- **Average Rate**: ${filipino_data['avg_rate']:,.2f}
"""

    # Language breakdown
    report += f"""

### 2. Cross-Audience Language Portfolio
"""
    
    for lang in analysis_results['language_leadership']['top_cross_languages']:
        report += f"""
#### {lang['language_name']}
- Revenue: ${lang['revenue']:,.2f} ({lang['revenue_percentage']}%)
- Spots: {lang['spots']:,} ({lang['spot_percentage']}%)
- Customers: {lang['unique_customers']} unique advertisers
- Avg Rate: ${lang['avg_rate']:,.2f}
"""

    # Transition time analysis
    transition = analysis_results['transition_time_goldmine']
    if transition['transition_spots']:
        report += f"""

### 3. 🎯 Transition Time Goldmine (4pm-7pm)
- **Total Revenue**: ${transition['total_transition_revenue']:,.2f}
- **Opportunity Size**: {transition['transition_opportunity_size']} spots
- **Strategic Value**: Cross-cultural reach during programming transitions

#### Hourly Breakdown:
"""
        for hour, data in transition['hourly_summary'].items():
            if data['spots'] > 0:
                report += f"""
- **{hour}**: ${data['revenue']:,.2f} ({data['spots']} spots, ${data['avg_rate']:,.2f} avg)
  - Customers: {data['unique_customers']} unique
  - Languages: {data['unique_languages']} different codes
"""

    # Customer strategy analysis
    report += f"""

### 4. Cross-Audience Customer Strategies

#### Top Cross-Audience Advertisers:
"""
    
    for i, customer in enumerate(analysis_results['customer_cross_audience_strategy']['top_cross_audience_customers'][:5], 1):
        report += f"""
{i}. **{customer['customer_name']}** ({customer['sector']})
   - Revenue: ${customer['total_revenue']:,.2f} ({customer['total_spots']} spots)
   - Languages Used: {customer['languages_used']} different codes
   - Filipino Focus: {customer['filipino_percentage']}% of spots
   - Strategy: {'Weekend + Weekday' if customer['weekend_spots'] > 0 and customer['weekday_spots'] > 0 else 'Weekday Focus' if customer['weekday_spots'] > customer['weekend_spots'] else 'Weekend Focus'}
"""

    # Weekend analysis
    weekend_data = analysis_results['weekend_vs_weekday']
    report += f"""

### 5. Weekend vs Weekday Performance
"""
    
    for day_type, data in weekend_data.items():
        report += f"""
#### {day_type}
- Revenue: ${data['revenue']:,.2f} ({data['spots']} spots)
- Average Rate: ${data['avg_rate']:,.2f}
- Language Diversity: {data['language_diversity']} different codes
- Filipino Dominance: {data['filipino_spots']} spots
"""

    # Actionable recommendations
    report += f"""

## 🚀 Strategic Recommendations

### 1. Filipino Programming Premium
- Recognize Filipino time slots as **premium cross-audience inventory**
- Consider Filipino-specific cross-audience packages
- Target government and public service advertisers (proven success)

### 2. Transition Time Monetization
- **4pm-7pm opportunity**: Underutilized cross-audience goldmine
- Create "Transition Time" advertising packages
- Target advertisers seeking cross-cultural reach

### 3. Weekend Cross-Audience Strategy
- Weekend inventory functions as general audience programming
- Premium pricing justified for weekend cross-audience slots
- Focus on entertainment, gaming, and lifestyle advertisers

### 4. Agency Education Program
- Train agencies on cross-audience value proposition
- Share success stories from top cross-audience customers
- Develop cross-audience buying guides

## 📊 Pricing Optimization Opportunities
"""

    pricing_data = analysis_results['pricing_optimization_opportunities']
    for opportunity in pricing_data['premium_opportunities']:
        report += f"""
- **{opportunity['language_code']} {opportunity['inventory_type']}**: ${opportunity['avg_rate']:,.2f} avg (range: ${opportunity['min_rate']:,.2f} - ${opportunity['max_rate']:,.2f})
"""

    report += f"""

---

*This analysis reveals significant untapped value in cross-audience advertising. Filipino programming leadership suggests a unique market position that could drive premium inventory pricing and expanded advertiser engagement.*
"""
    
    return report


# CLI interface for deep analysis
def main():
    """CLI interface for Multi-Language deep analysis"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Multi-Language Deep Analysis")
    parser.add_argument("--year", default="2024", help="Year to analyze")
    parser.add_argument("--output", help="Output file for report")
    
    args = parser.parse_args()
    
    # Run deep analysis
    with MultiLanguageDeepAnalyzer() as analyzer:
        results = analyzer.analyze_complete_cross_audience_patterns(args.year)
    
    # Generate report
    report = generate_multi_language_report(results)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Deep analysis report saved to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()