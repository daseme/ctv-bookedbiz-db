#!/usr/bin/env python3
"""
Enhanced Business Rules Assignment Script with Better Progress Tracking
======================================================================

Your existing file with improved progress tracking, timing, and monitoring.
"""

import sqlite3
import sys
import logging
import time
import psutil  # ADD THIS IMPORT
from tqdm import tqdm
sys.path.append('src')
from services.business_rules_service import BusinessRulesService
from datetime import datetime

# Setup logging - ENHANCED
logging.basicConfig(level=logging.INFO, format='%(asctime)s - 🔧 Stage 2: %(message)s')
logger = logging.getLogger(__name__)

def assign_business_rules_spots(limit=None):
    """Assign unassigned spots using business rules - ENHANCED WITH PROGRESS TRACKING"""
    # ADD THIS - Start timing
    start_time = time.time()
    logger.info("🚀 Starting business rules assignment")
    
    conn = sqlite3.connect('./data/database/production.db')
    service = BusinessRulesService(conn)
    
    # Get unassigned spots that match business rules
    cursor = conn.cursor()
    query = '''
    SELECT s.spot_id FROM spots s
    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
    LEFT JOIN customers c ON s.customer_id = c.customer_id
    LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
    WHERE slb.spot_id IS NULL
      AND s.market_id IS NOT NULL
      AND s.time_in IS NOT NULL
      AND s.time_out IS NOT NULL
      AND (
        sec.sector_code = 'MEDIA' OR
        sec.sector_code = 'GOV' OR
        sec.sector_code = 'POLITICAL' OR
        (sec.sector_code = 'NPO' AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300) OR
        CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720
      )
    '''
    
    if limit:
        query += f' LIMIT {limit}'
    
    cursor.execute(query)
    spot_ids = [row[0] for row in cursor.fetchall()]
    
    # ENHANCED LOGGING
    total_spots = len(spot_ids)
    logger.info(f"📊 Found {total_spots:,} spots to process")
    
    if total_spots == 0:
        logger.info("✅ No spots need business rules assignment")
        return {'assigned': 0, 'errors': 0}
    
    # ADD THIS - Progress bar setup
    progress_bar = tqdm(
        total=total_spots,
        desc="🔧 Business Rules",
        unit="spots",
        unit_scale=True,
        dynamic_ncols=True,
        bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} spots [{elapsed}<{remaining}, {rate_fmt}]'
    )
    
    stats = {'assigned': 0, 'errors': 0}
    
    # ENHANCED PROCESSING LOOP
    for i, spot_id in enumerate(spot_ids):
        try:
            spot_data = service.get_spot_data_from_db(spot_id)
            if spot_data:
                result = service.evaluate_spot(spot_data)
                if result.auto_resolved:
                    # Get all language blocks for this market to populate blocks_spanned
                    blocks_spanned_value = get_blocks_spanned_for_spot(cursor, spot_data)
                    
                    # Insert assignment record with proper constraint handling
                    cursor.execute("""
                        INSERT INTO spot_language_blocks (
                            spot_id, schedule_id, block_id, customer_intent, 
                            intent_confidence, spans_multiple_blocks, blocks_spanned,
                            assignment_method, assigned_date, assigned_by,
                            requires_attention, alert_reason, notes,
                            business_rule_applied, auto_resolved_date
                        ) VALUES (?, 1, NULL, ?, 1.0, 1, ?, 'auto_computed', ?, 'business_rules', 0, ?, ?, ?, ?)
                    """, (
                        spot_id, 
                        result.customer_intent.value,
                        blocks_spanned_value,  # This fixes the CHECK constraint
                        datetime.now().isoformat(),
                        result.alert_reason,
                        result.notes,
                        result.rule_applied.rule_type.value,
                        datetime.now().isoformat()
                    ))
                    stats['assigned'] += 1
            
            # UPDATE PROGRESS BAR
            progress_bar.update(1)
            
            # ENHANCED LOGGING - Every 1000 spots
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed
                
                # Get memory usage
                memory_info = get_memory_usage()
                
                logger.info(
                    f"📦 Processed {i + 1:,}/{total_spots:,} spots "
                    f"({(i + 1)/total_spots*100:.1f}%) in {elapsed:.1f}s "
                    f"({rate:.1f} spots/s) "
                    f"[Assigned: {stats['assigned']:,}, Errors: {stats['errors']:,}] "
                    f"[Memory: {memory_info['process_mb']:.1f}MB, System: {memory_info['system_pct']:.1f}%]"
                )
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Error processing spot {spot_id}: {e}")
            stats['errors'] += 1
    
    # CLOSE PROGRESS BAR
    progress_bar.close()
    
    conn.commit()
    conn.close()
    
    # ENHANCED COMPLETION LOGGING
    total_time = time.time() - start_time
    logger.info(f"✅ Assignment complete in {total_time:.1f}s!")
    logger.info(f"📊 Final stats: Assigned {stats['assigned']:,}, Errors {stats['errors']:,}")
    logger.info(f"⚡ Average rate: {total_spots/total_time:.1f} spots/second")
    
    return stats

def get_memory_usage():
    """Get current memory usage statistics"""
    try:
        process = psutil.Process()
        memory = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        return {
            'process_mb': memory.rss / 1024 / 1024,
            'system_pct': system_memory.percent,
            'system_available_gb': system_memory.available / 1024 / 1024 / 1024
        }
    except:
        return {'process_mb': 0, 'system_pct': 0, 'system_available_gb': 0}

def get_blocks_spanned_for_spot(cursor, spot_data):
    """
    Get a representation of language blocks spanned for the spot.
    For business rules, we'll use a generic representation since they span multiple blocks.
    """
    try:
        # Try to get actual blocks for this market/schedule
        cursor.execute("""
            SELECT lb.block_id 
            FROM language_blocks lb
            JOIN programming_schedules ps ON lb.schedule_id = ps.schedule_id
            JOIN schedule_market_assignments sma ON ps.schedule_id = sma.schedule_id
            WHERE sma.market_id = ?
            AND lb.is_active = 1
            AND ps.is_active = 1
            ORDER BY lb.block_id
            LIMIT 10
        """, (spot_data.market_id,))
        
        blocks = [str(row[0]) for row in cursor.fetchall()]
        
        if blocks:
            # Return comma-separated list of block IDs
            return ','.join(blocks)
        else:
            # Fallback: return a generic representation
            return 'multiple_blocks'
            
    except Exception as e:
        logger.warning(f"Could not determine blocks for market {spot_data.market_id}: {e}")
        # Return a safe default that satisfies the constraint
        return 'multiple_blocks'

def get_assignment_stats():
    """Get current assignment statistics"""
    logger.info("📊 Gathering assignment statistics...")
    
    conn = sqlite3.connect('./data/database/production.db')
    cursor = conn.cursor()
    
    # Total spots
    cursor.execute("SELECT COUNT(*) FROM spots")
    total_spots = cursor.fetchone()[0]
    
    # Assigned spots
    cursor.execute("SELECT COUNT(*) FROM spot_language_blocks")
    assigned_spots = cursor.fetchone()[0]
    
    # Business rule assignments
    cursor.execute("SELECT COUNT(*) FROM spot_language_blocks WHERE business_rule_applied IS NOT NULL")
    business_rule_spots = cursor.fetchone()[0]
    
    # Business rule breakdown
    cursor.execute("""
        SELECT business_rule_applied, COUNT(*) 
        FROM spot_language_blocks 
        WHERE business_rule_applied IS NOT NULL
        GROUP BY business_rule_applied
    """)
    rule_breakdown = dict(cursor.fetchall())
    
    conn.close()
    
    logger.info(f"📊 ASSIGNMENT STATISTICS:")
    logger.info(f"   • Total spots: {total_spots:,}")
    logger.info(f"   • Assigned spots: {assigned_spots:,} ({assigned_spots/total_spots*100:.1f}%)")
    logger.info(f"   • Business rule assignments: {business_rule_spots:,} ({business_rule_spots/total_spots*100:.1f}%)")
    logger.info(f"   • Unassigned spots: {total_spots - assigned_spots:,}")
    
    if rule_breakdown:
        logger.info(f"🎯 BUSINESS RULE BREAKDOWN:")
        for rule_type, count in rule_breakdown.items():
            logger.info(f"   • {rule_type}: {count:,}")
    
    return {
        'total_spots': total_spots,
        'assigned_spots': assigned_spots,
        'business_rule_spots': business_rule_spots,
        'unassigned_spots': total_spots - assigned_spots,
        'rule_breakdown': rule_breakdown
    }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Assign unassigned spots using business rules")
    parser.add_argument("--limit", type=int, help="Limit number of spots to assign (for testing)")
    parser.add_argument("--test", action="store_true", help="Test mode - assign first 100 spots")
    parser.add_argument("--stats", action="store_true", help="Show assignment statistics")
    
    args = parser.parse_args()
    
    if args.stats:
        get_assignment_stats()
    elif args.test:
        assign_business_rules_spots(100)
    elif args.limit:
        assign_business_rules_spots(args.limit)
    else:
        confirm = input(f"Assign ALL unassigned spots using business rules? (yes/no): ")
        if confirm.lower() in ['yes', 'y']:
            assign_business_rules_spots()
        else:
            print("Assignment cancelled")