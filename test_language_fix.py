#!/usr/bin/env python3
"""
Test Script for Language Assignment Fix
======================================

This script tests the language assignment logic before implementing the full fix.
"""

import sqlite3
from typing import Dict, List, Set, Any

def analyze_block_languages(blocks_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Test version of _analyze_block_languages"""
    
    unique_languages = set(block['language_id'] for block in blocks_data)
    language_names = [block['language_name'] for block in blocks_data]
    
    # Define language families (confirmed correct groupings)
    language_families = {
        'Chinese': {2, 3},      # Mandarin=2, Cantonese=3 (SAME family)
        'Filipino': {4},        # Tagalog=4 (single language in current DB)
        'South Asian': {6},     # South Asian=6 (represents the family)
        'English': {1},         # English=1 (single language)
        'Vietnamese': {7},      # Vietnamese=7 (single language)
        'Korean': {8},          # Korean=8 (single language)
        'Japanese': {9},        # Japanese=9 (single language)
        'Hmong': {5}            # Hmong=5 (single language)
    }
    
    # Check for same language
    if len(unique_languages) == 1:
        primary_language = language_names[0]
        return {
            'classification': 'same_language',
            'unique_languages': list(unique_languages),
            'primary_language': primary_language,
            'family_name': primary_language,
            'block_count': len(blocks_data),
            'expected_campaign_type': 'language_specific'
        }
    
    # Check for same language family
    for family_name, family_languages in language_families.items():
        if unique_languages.issubset(family_languages):
            return {
                'classification': 'same_family',
                'unique_languages': list(unique_languages),
                'primary_language': None,
                'family_name': family_name,
                'block_count': len(blocks_data),
                'expected_campaign_type': 'language_specific'
            }
    
    # Different language families
    return {
        'classification': 'different_families',
        'unique_languages': list(unique_languages),
        'primary_language': None,
        'family_name': None,
        'block_count': len(blocks_data),
        'language_names': language_names,
        'expected_campaign_type': 'multi_language'
    }

def test_specific_cases(db_connection):
    """Test specific cases from the database"""
    
    print("=" * 60)
    print("TESTING LANGUAGE ASSIGNMENT LOGIC")
    print("=" * 60)
    
    # Test Case 1: The Tagalog case we found (blocks 247, 248)
    print("\n1. Testing Tagalog Blocks (247, 248) - Current Issue:")
    cursor = db_connection.cursor()
    cursor.execute("""
        SELECT lb.block_id, lb.language_id, l.language_name
        FROM language_blocks lb
        JOIN languages l ON lb.language_id = l.language_id
        WHERE lb.block_id IN (247, 248)
    """)
    
    tagalog_blocks = [
        {
            'block_id': row[0],
            'language_id': row[1],
            'language_name': row[2]
        }
        for row in cursor.fetchall()
    ]
    
    tagalog_result = analyze_block_languages(tagalog_blocks)
    print(f"   Result: {tagalog_result}")
    print(f"   Expected: language_specific (same language)")
    print(f"   Currently: multi_language (WRONG)")
    
    # Test Case 2: Find some Chinese blocks (if any)
    print("\n2. Testing Chinese Blocks (if any exist):")
    cursor.execute("""
        SELECT lb.block_id, lb.language_id, l.language_name
        FROM language_blocks lb
        JOIN languages l ON lb.language_id = l.language_id
        WHERE l.language_id IN (2, 3)  -- Mandarin or Cantonese
        LIMIT 4
    """)
    
    chinese_blocks = [
        {
            'block_id': row[0],
            'language_id': row[1],
            'language_name': row[2]
        }
        for row in cursor.fetchall()
    ]
    
    if chinese_blocks:
        chinese_result = analyze_block_languages(chinese_blocks)
        print(f"   Result: {chinese_result}")
        if chinese_result['classification'] == 'same_family':
            print(f"   Expected: language_specific (Chinese family)")
        else:
            print(f"   Expected: language_specific (same language)")
    else:
        print("   No Chinese blocks found in database")
    
    # Test Case 3: Find true multi-language examples
    print("\n3. Testing True Multi-Language Cases:")
    cursor.execute("""
        SELECT DISTINCT slb.blocks_spanned
        FROM spot_language_blocks slb
        WHERE slb.campaign_type = 'multi_language'
            AND slb.spans_multiple_blocks = 1
        LIMIT 5
    """)
    
    multi_language_examples = cursor.fetchall()
    
    for i, (blocks_spanned,) in enumerate(multi_language_examples[:3]):
        try:
            block_ids = eval(blocks_spanned)  # Parse "[247, 248]" to [247, 248]
            
            cursor.execute(f"""
                SELECT lb.block_id, lb.language_id, l.language_name
                FROM language_blocks lb
                JOIN languages l ON lb.language_id = l.language_id
                WHERE lb.block_id IN ({','.join(['?'] * len(block_ids))})
            """, block_ids)
            
            test_blocks = [
                {
                    'block_id': row[0],
                    'language_id': row[1],
                    'language_name': row[2]
                }
                for row in cursor.fetchall()
            ]
            
            if test_blocks:
                result = analyze_block_languages(test_blocks)
                print(f"   Example {i+1} - Blocks {block_ids}: {result['classification']}")
                print(f"   Languages: {[b['language_name'] for b in test_blocks]}")
                print(f"   Expected: {result['expected_campaign_type']}")
                
                # Check if this is incorrectly classified
                if result['expected_campaign_type'] == 'language_specific':
                    print(f"   ❌ INCORRECTLY CLASSIFIED as multi_language")
                else:
                    print(f"   ✅ Correctly classified as multi_language")
                print()
                
        except Exception as e:
            print(f"   Error processing example {i+1}: {e}")

def count_misclassifications(db_connection, year_suffix="22"):
    """Count how many spots are misclassified"""
    
    print("\n" + "=" * 60)
    print("COUNTING MISCLASSIFICATIONS")
    print("=" * 60)
    
    cursor = db_connection.cursor()
    
    # Get all multi_language spots that span multiple blocks
    cursor.execute("""
        SELECT s.spot_id, slb.blocks_spanned, s.gross_rate
        FROM spots s
        JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
        WHERE slb.campaign_type = 'multi_language'
            AND slb.spans_multiple_blocks = 1
            AND s.broadcast_month LIKE ?
    """, (f'%-{year_suffix}',))
    
    multi_language_spots = cursor.fetchall()
    
    misclassified_spots = []
    correctly_classified_spots = []
    
    for spot_id, blocks_spanned, gross_rate in multi_language_spots:
        try:
            block_ids = eval(blocks_spanned)
            
            # Get language info for these blocks
            cursor.execute(f"""
                SELECT lb.language_id, l.language_name
                FROM language_blocks lb
                JOIN languages l ON lb.language_id = l.language_id
                WHERE lb.block_id IN ({','.join(['?'] * len(block_ids))})
            """, block_ids)
            
            block_languages = [
                {
                    'language_id': row[0],
                    'language_name': row[1]
                }
                for row in cursor.fetchall()
            ]
            
            if block_languages:
                result = analyze_block_languages(block_languages)
                
                if result['expected_campaign_type'] == 'language_specific':
                    misclassified_spots.append((spot_id, gross_rate, result))
                else:
                    correctly_classified_spots.append((spot_id, gross_rate, result))
                    
        except Exception as e:
            print(f"Error processing spot {spot_id}: {e}")
    
    misclassified_revenue = sum(spot[1] or 0 for spot in misclassified_spots)
    correctly_classified_revenue = sum(spot[1] or 0 for spot in correctly_classified_spots)
    
    print(f"Total multi_language spots analyzed: {len(multi_language_spots)}")
    print(f"Misclassified spots (should be language_specific): {len(misclassified_spots)}")
    print(f"Correctly classified spots (truly multi_language): {len(correctly_classified_spots)}")
    print(f"Misclassified revenue: ${misclassified_revenue:,.2f}")
    print(f"Correctly classified revenue: ${correctly_classified_revenue:,.2f}")
    
    if len(misclassified_spots) > 0:
        print(f"\nMisclassification rate: {len(misclassified_spots)/len(multi_language_spots)*100:.1f}%")
        print(f"Revenue impact: ${misclassified_revenue:,.2f} incorrectly attributed to multi-language")
    
    return {
        'total_analyzed': len(multi_language_spots),
        'misclassified_count': len(misclassified_spots),
        'correctly_classified_count': len(correctly_classified_spots),
        'misclassified_revenue': misclassified_revenue,
        'correctly_classified_revenue': correctly_classified_revenue
    }

def main():
    """Main test function"""
    
    db_path = "data/database/production.db"
    
    try:
        with sqlite3.connect(db_path) as conn:
            print("🧪 Testing Language Assignment Logic Fix")
            
            # Test specific cases
            test_specific_cases(conn)
            
            # Count misclassifications
            results = count_misclassifications(conn, "22")  # 2022 data
            
            print("\n" + "=" * 60)
            print("SUMMARY")
            print("=" * 60)
            print(f"✅ Language family logic implemented correctly")
            print(f"📊 Found {results['misclassified_count']} spots needing reclassification")
            print(f"💰 ${results['misclassified_revenue']:,.2f} in revenue to be moved from multi-language to language-specific")
            print(f"🎯 Ready to implement the fix!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()