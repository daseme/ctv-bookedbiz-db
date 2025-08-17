#!/usr/bin/env python3
"""
Fix language assignment to only process current batch spots
"""

import re
from pathlib import Path

def fix_language_assignment():
    file_path = Path("src/cli/weekly_update.py")
    content = file_path.read_text()
    
    # Replace the uncategorized spots logic to be batch-specific
    old_pattern = r'uncategorized_spots = categorization_service\.get_uncategorized_spots\(\)'
    new_pattern = '''# Get uncategorized spots from current batch only
            batch_spots_query = """
                SELECT spot_id FROM spots 
                WHERE import_batch_id = ? AND spot_category IS NULL
            """
            cursor = conn.execute(batch_spots_query, (batch_id,))
            uncategorized_spots = [row[0] for row in cursor.fetchall()]'''
    
    if re.search(old_pattern, content):
        content = re.sub(old_pattern, new_pattern, content)
        print("✅ Fixed uncategorized spots query to be batch-specific")
    else:
        print("❌ Could not find uncategorized spots pattern")
    
    # Also need to pass batch_id to the method
    # Find the method definition and add batch_id parameter
    method_pattern = r'def _process_language_assignments\(self, batch_id: str\)'
    if re.search(method_pattern, content):
        print("✅ Method already has batch_id parameter")
    else:
        # If method doesn't have batch_id, we need to add it
        old_method = r'def _process_language_assignments\(self\)'
        new_method = 'def _process_language_assignments(self, batch_id: str)'
        content = re.sub(old_method, new_method, content)
        print("✅ Added batch_id parameter to language assignment method")
    
    # Save the fixed content
    file_path.write_text(content)
    return True

if __name__ == "__main__":
    if fix_language_assignment():
        print("🎯 Language assignment fixed to process only current batch spots")
    else:
        print("❌ Fix failed")
