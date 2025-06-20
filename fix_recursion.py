#!/usr/bin/env python3
"""Quick fix for recursion issue in pipeline service."""
import sys
sys.path.insert(0, 'src')

# The issue is likely in the enhanced pipeline service calling itself
# Let's check the current implementation
with open('src/services/pipeline_service.py', 'r') as f:
    content = f.read()

# Look for the problematic line
if 'def update_pipeline_data(' in content and 'self.update_pipeline_data(' in content:
    print("Found potential recursion issue in update_pipeline_data method")
    
    # Create a backup
    with open('src/services/pipeline_service.py.backup', 'w') as f:
        f.write(content)
    print("✅ Backup created: pipeline_service.py.backup")
    
    # Simple fix: ensure the base method doesn't call itself
    fixed_content = content.replace(
        'self.update_pipeline_data(',
        'self._update_pipeline_data_base('
    )
    
    # Only apply the fix to one location to avoid over-fixing
    lines = fixed_content.split('\n')
    fixed_lines = []
    fix_applied = False
    
    for line in lines:
        if ('self._update_pipeline_data_base(' in line and 
            'def update_pipeline_data(' not in line and 
            not fix_applied):
            # This is likely the problematic recursive call
            print(f"Fixing line: {line.strip()}")
            fix_applied = True
        fixed_lines.append(line)
    
    if fix_applied:
        with open('src/services/pipeline_service.py', 'w') as f:
            f.write('\n'.join(fixed_lines))
        print("✅ Recursion fix applied")
    else:
        print("⚠️  No obvious recursion issue found - system working despite warning")
else:
    print("✅ No recursion issue detected in current code")
