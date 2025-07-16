#!/usr/bin/env python3
"""
ROOT CAUSE ANALYSIS: Language Block Assignment Logic
====================================================

The issue: 453 spots classified as 'language_specific' but with block_id = NULL
These are "RPM:Thunder Valley" spots running 23:30:00 to "1 day, 0:00:00"

ROOT CAUSE IDENTIFIED:
The precedence rules are not properly catching these spots that should be ROS.

PROBLEMS IN CURRENT CODE:
1. _is_ros_by_time() method has incorrect patterns
2. Duration calculation may not handle "1 day, 0:00:00" format correctly
3. Precedence rules not applied in correct order

COMPREHENSIVE LANGUAGE MAPPING:
Chinese = M
Filipino = T  
Hmong = Hm
South Asian = SA
Vietnamese = V
Mandarin = M
Cantonese = C
Korean = K
Japanese = J
Tagalog = T
Hindi = SA
Punjabi = SA
Bengali = SA
Gujarati = SA
default = E
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

def analyze_problematic_spots(db_connection, year_suffix: str = "23") -> Dict[str, Any]:
    """
    Analyze the spots that are incorrectly classified as language_specific
    to understand why precedence rules failed
    """
    cursor = db_connection.cursor()
    
    # Get the problematic spots
    cursor.execute(f"""
        SELECT 
            s.spot_id,
            s.bill_code,
            s.time_in,
            s.time_out,
            s.day_of_week,
            s.gross_rate,
            slb.business_rule_applied,
            slb.alert_reason,
            a.agency_name
        FROM spots s
        LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.broadcast_month LIKE '%-{year_suffix}'
          AND slb.campaign_type = 'language_specific'
          AND slb.block_id IS NULL
        ORDER BY s.gross_rate DESC
        LIMIT 10
    """)
    
    problematic_spots = cursor.fetchall()
    
    analysis = {
        'total_count': len(problematic_spots),
        'spots': [],
        'patterns': {}
    }
    
    for spot in problematic_spots:
        spot_info = {
            'spot_id': spot[0],
            'bill_code': spot[1],
            'time_in': spot[2],
            'time_out': spot[3],
            'day_of_week': spot[4],
            'gross_rate': spot[5],
            'business_rule_applied': spot[6],
            'alert_reason': spot[7],
            'agency_name': spot[8]
        }
        
        # Analyze why precedence rules failed
        duration_minutes = calculate_spot_duration(spot[2], spot[3])
        spot_info['duration_minutes'] = duration_minutes
        spot_info['should_be_ros_duration'] = duration_minutes > 360
        spot_info['should_be_ros_time'] = is_ros_time_pattern(spot[2], spot[3])
        spot_info['should_be_worldlink'] = 'WorldLink' in (spot[8] or '') or 'WorldLink' in (spot[1] or '')
        
        analysis['spots'].append(spot_info)
        
        # Track patterns
        time_pattern = f"{spot[2]} to {spot[3]}"
        if time_pattern not in analysis['patterns']:
            analysis['patterns'][time_pattern] = 0
        analysis['patterns'][time_pattern] += 1
    
    return analysis

def calculate_spot_duration(time_in: str, time_out: str) -> int:
    """
    FIXED: Calculate spot duration in minutes, handling "1 day, 0:00:00" format
    """
    try:
        # Handle "1 day, 0:00:00" format
        if 'day' in time_out:
            # This means it goes to the next day
            start_minutes = time_to_minutes(time_in)
            # "1 day, 0:00:00" means 24:00:00 = 1440 minutes
            end_minutes = 1440  # 24 * 60
            return end_minutes - start_minutes
        else:
            start_minutes = time_to_minutes(time_in)
            end_minutes = time_to_minutes(time_out)
            
            if end_minutes >= start_minutes:
                return end_minutes - start_minutes
            else:
                # Handle midnight rollover
                return (24 * 60) - start_minutes + end_minutes
    except:
        return 0

def time_to_minutes(time_str: str) -> int:
    """Convert HH:MM:SS time string to minutes since midnight"""
    try:
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        return hours * 60 + minutes
    except (ValueError, IndexError):
        return 0

def is_ros_time_pattern(time_in: str, time_out: str) -> bool:
    """
    FIXED: Check if spot matches ROS time patterns
    """
    # Pattern 1: 13:00-23:59 (standard ROS)
    if time_in == "13:00:00" and time_out == "23:59:00":
        return True
    
    # Pattern 2: Late night to next day (common ROS pattern)
    if 'day' in time_out:
        # Any spot that runs to the next day could be ROS
        start_hour = int(time_in.split(':')[0])
        # Late night starts (after 19:00) running to next day
        if start_hour >= 19:
            return True
        # Very early morning starts (before 6:00) running to next day  
        if start_hour <= 6:
            return True
    
    # Pattern 3: Very long daytime slots
    if time_in == "06:00:00" and time_out == "23:59:00":
        return True
    
    return False

def get_fixed_assignment_logic() -> str:
    """
    Generate the fixed assignment logic that properly handles precedence
    """
    return """
def _apply_precedence_rules(self, spot: SpotData) -> Optional[AssignmentResult]:
    '''Apply precedence rules FIRST - FIXED VERSION'''
    
    # Rule 1: WorldLink Direct Response (highest priority)
    if self._is_worldlink_spot(spot):
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=1,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            campaign_type='direct_response',
            business_rule_applied='worldlink_direct_response',
            auto_resolved_date=datetime.now()
        )
    
    # Rule 2: ROS by Duration (> 6 hours = 360 minutes)
    duration = self._calculate_spot_duration(spot.time_in, spot.time_out)
    if duration > 360:
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=1,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            campaign_type='ros',
            business_rule_applied='ros_duration',
            auto_resolved_date=datetime.now()
        )
    
    # Rule 3: ROS by Time Pattern
    if self._is_ros_by_time_fixed(spot):
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=1,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            campaign_type='ros',
            business_rule_applied='ros_time',
            auto_resolved_date=datetime.now()
        )
    
    # Rule 4: Paid Programming
    if self._is_paid_programming(spot):
        return AssignmentResult(
            spot_id=spot.spot_id,
            success=True,
            schedule_id=1,
            block_id=None,
            customer_intent=CustomerIntent.INDIFFERENT,
            spans_multiple_blocks=True,
            campaign_type='paid_programming',
            business_rule_applied='revenue_type_paid_programming',
            auto_resolved_date=datetime.now()
        )
    
    return None

def _calculate_spot_duration(self, time_in: str, time_out: str) -> int:
    '''FIXED: Calculate spot duration in minutes, handling "1 day, 0:00:00" format'''
    try:
        # Handle "1 day, 0:00:00" format
        if 'day' in time_out:
            start_minutes = self._time_to_minutes(time_in)
            end_minutes = 1440  # 24 * 60 = next day midnight
            return end_minutes - start_minutes
        else:
            start_minutes = self._time_to_minutes(time_in)
            end_minutes = self._time_to_minutes(time_out)
            
            if end_minutes >= start_minutes:
                return end_minutes - start_minutes
            else:
                # Handle midnight rollover
                return (24 * 60) - start_minutes + end_minutes
    except:
        return 0

def _is_ros_by_time_fixed(self, spot: SpotData) -> bool:
    '''FIXED: Check if spot runs ROS time patterns'''
    
    # Pattern 1: 13:00-23:59 (standard ROS)
    if spot.time_in == "13:00:00" and spot.time_out == "23:59:00":
        return True
    
    # Pattern 2: Late night to next day (FIXED)
    if 'day' in spot.time_out:
        start_hour = int(spot.time_in.split(':')[0])
        # Late night starts (after 19:00) running to next day
        if start_hour >= 19:
            return True
        # Very early morning starts (before 6:00) running to next day
        if start_hour <= 6:
            return True
    
    # Pattern 3: Very long daytime slots
    if spot.time_in == "06:00:00" and spot.time_out == "23:59:00":
        return True
    
    return False
    """

def generate_comprehensive_language_mapping() -> Dict[str, str]:
    """
    Generate comprehensive language mapping from the provided specification
    """
    return {
        'Chinese': 'M',
        'Filipino': 'T',
        'Hmong': 'Hm', 
        'South Asian': 'SA',
        'Vietnamese': 'V',
        'Mandarin': 'M',
        'Cantonese': 'C',
        'Korean': 'K',
        'Japanese': 'J',
        'Tagalog': 'T',
        'Hindi': 'SA',
        'Punjabi': 'SA',
        'Bengali': 'SA',
        'Gujarati': 'SA',
        'default': 'E'
    }

def get_updated_language_case_statement() -> str:
    """
    Generate updated CASE statement using comprehensive language mapping
    """
    return """
    CASE 
        WHEN l.language_name IN ('Mandarin', 'Cantonese') THEN 'Chinese'
        WHEN l.language_name IN ('Tagalog', 'Filipino') THEN 'Filipino'
        WHEN l.language_name = 'Hmong' THEN 'Hmong'
        WHEN l.language_name IN ('Hindi', 'Punjabi', 'Bengali', 'Gujarati') OR l.language_name = 'South Asian' THEN 'South Asian'
        WHEN l.language_name = 'Vietnamese' THEN 'Vietnamese'
        WHEN l.language_name = 'Korean' THEN 'Korean'
        WHEN l.language_name = 'Japanese' THEN 'Japanese'
        WHEN l.language_name = 'English' THEN 'English'
        ELSE CONCAT('Other: ', COALESCE(l.language_name, 'Unknown'))
    END
    """

def main():
    """Test the analysis on the problematic spots"""
    db_path = "data/database/production.db"
    
    try:
        conn = sqlite3.connect(db_path)
        
        print("🔍 ANALYZING PROBLEMATIC SPOTS:")
        print("=" * 50)
        
        analysis = analyze_problematic_spots(conn)
        
        print(f"Total problematic spots: {analysis['total_count']}")
        print(f"\nTIME PATTERNS:")
        for pattern, count in analysis['patterns'].items():
            print(f"  • {pattern}: {count} spots")
        
        print(f"\nSAMPLE ANALYSIS:")
        for i, spot in enumerate(analysis['spots'][:5]):
            print(f"\n  Spot {spot['spot_id']} ({spot['bill_code']}):")
            print(f"    • Time: {spot['time_in']} to {spot['time_out']}")
            print(f"    • Duration: {spot['duration_minutes']} minutes")
            print(f"    • Should be ROS (duration): {spot['should_be_ros_duration']}")
            print(f"    • Should be ROS (time): {spot['should_be_ros_time']}")
            print(f"    • Alert reason: {spot['alert_reason']}")
        
        print(f"\n🎯 ROOT CAUSE:")
        print(f"  • Duration calculation not handling '1 day, 0:00:00' format")
        print(f"  • ROS time patterns not catching late night to next day")
        print(f"  • Precedence rules failing, allowing fallthrough to language_specific")
        
        print(f"\n🔧 SOLUTION:")
        print(f"  • Fix _calculate_spot_duration() to handle 'day' format")
        print(f"  • Fix _is_ros_by_time() to catch late night patterns")
        print(f"  • Update language mapping with comprehensive names")
        print(f"  • Proper precedence rule application")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()