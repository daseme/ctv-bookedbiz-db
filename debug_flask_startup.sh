#!/bin/bash

echo "=== Flask Service Debugging ==="
echo "Current time: $(date)"
echo

# Check current Flask service status
echo "1. Flask Service Status:"
echo "========================"
sudo systemctl status flaskapp -l --no-pager
echo

# Check recent logs
echo "2. Recent Flask Service Logs:"
echo "============================"
sudo journalctl -u flaskapp -n 20 --no-pager
echo

# Check if we can import the problematic module
echo "3. Testing Python Module Import:"
echo "==============================="
cd /opt/apps/ctv-bookedbiz-db
source .venv/bin/activate

python3 -c "
try:
    print('Testing basic imports...')
    from decimal import Decimal
    from dataclasses import dataclass
    from typing import List, Dict
    print('✓ Basic imports successful')
    
    print('Testing report_data import...')
    from src.models.report_data import MonthlyRevenueReportData, CustomerMonthlyRow
    print('✓ report_data import successful')
    
    print('Testing other model imports...')
    from src.models import *
    print('✓ All models imported successfully')
    
    print('Testing web app imports...')
    from src.web.app import app
    print('✓ Web app import successful')
    
    print('All imports completed successfully!')
    
except Exception as e:
    print(f'✗ Import failed: {e}')
    import traceback
    traceback.print_exc()
"
echo

# Check syntax of Python files
echo "4. Python Syntax Check:"
echo "======================"
echo "Checking report_data.py syntax..."
python3 -m py_compile src/models/report_data.py
if [ $? -eq 0 ]; then
    echo "✓ report_data.py syntax is valid"
else
    echo "✗ report_data.py has syntax errors"
fi

echo "Checking web app syntax..."
python3 -m py_compile src/web/app.py
if [ $? -eq 0 ]; then
    echo "✓ app.py syntax is valid"
else
    echo "✗ app.py has syntax errors"
fi
echo

# Try manual Flask startup
echo "5. Manual Flask Startup Test:"
echo "============================="
echo "Attempting to start Flask manually..."
timeout 10s python3 -c "
import sys
sys.path.insert(0, '.')
try:
    from src.web.app import app
    print('Flask app object created successfully')
    print(f'App name: {app.name}')
    print('Starting test server...')
    app.run(host='0.0.0.0', port=8000, debug=True)
except Exception as e:
    print(f'Error starting Flask: {e}')
    import traceback
    traceback.print_exc()
" 2>&1
echo
echo "Manual test completed (timed out after 10 seconds if successful)"
echo

# Check for port conflicts
echo "6. Port Usage Check:"
echo "==================="
echo "Checking if port 8000 is in use:"
netstat -tulpn | grep :8000 || echo "Port 8000 is free"
echo

# Check recent system logs for related errors
echo "7. Recent System Logs:"
echo "===================="
echo "Checking for Python/Flask related errors in system logs..."
journalctl --since "10 minutes ago" | grep -i "python\|flask\|error" | tail -10
echo

echo "Debugging script completed."
echo "Next steps: Fix any import errors found above, then restart Flask service with:"
echo "sudo systemctl restart flaskapp"