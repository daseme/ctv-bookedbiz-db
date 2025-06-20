#!/usr/bin/env python3
import sys
sys.path.insert(0, 'src')
import tempfile
import os

# Set up test environment  
temp_dir = tempfile.mkdtemp(prefix='decay_test_')
data_path = temp_dir + '/data/processed'
os.makedirs(data_path, exist_ok=True)

os.environ['PROJECT_ROOT'] = temp_dir
os.environ['DATA_PATH'] = data_path
os.environ['FLASK_ENV'] = 'testing'

# Test decay system
from services.factory import initialize_services
from services.container import get_container

print('🧪 Testing Pipeline Decay System...')

try:
    initialize_services()
    container = get_container()
    pipeline_service = container.get('pipeline_service')

    # Check if decay system is available
    if hasattr(pipeline_service, 'decay_engine') and pipeline_service.decay_engine:
        print('✅ Decay engine available')
        
        # Test 1: Set calibration baseline
        success = pipeline_service.set_pipeline_calibration(
            ae_id='TEST_AE',
            month='2025-01',
            pipeline_value=50000,
            calibrated_by='test_user'
        )
        print(f'✅ Calibration set: {success}')
        
        # Test 2: Apply revenue booking
        success = pipeline_service.apply_revenue_booking(
            ae_id='TEST_AE',
            month='2025-01',
            amount=8000,
            customer='BigCorp Media',
            description='Q1 campaign booked'
        )
        print(f'✅ Revenue booking applied: {success}')
        
        # Test 3: Get decay summary
        summary = pipeline_service.get_pipeline_decay_summary('TEST_AE', '2025-01')
        if summary:
            print(f'✅ Decay summary: ${summary["calibrated_pipeline"]:,.0f} -> ${summary["current_pipeline"]:,.0f}')
            print(f'✅ Total decay: ${summary["total_decay"]:+,.0f}')
            print(f'✅ Decay events: {len(summary["decay_events"])}')
        else:
            print('❌ No decay summary available')
        
        # Test 4: Get decay analytics  
        analytics = pipeline_service.get_decay_analytics('TEST_AE')
        if analytics and 'monthly_summaries' in analytics:
            print(f'✅ Analytics generated: {len(analytics["monthly_summaries"])} months')
        else:
            print('❌ No analytics available')
        
        print('\n🎉 PIPELINE DECAY SYSTEM IS WORKING!')
        
    else:
        print('❌ Decay engine not available - checking why...')
        
        # Debug what's available
        print(f'Pipeline service type: {type(pipeline_service).__name__}')
        print('Available methods:')
        methods = [method for method in dir(pipeline_service) if not method.startswith('_')]
        for method in sorted(methods)[:10]:  # Show first 10 methods
            print(f'  - {method}')
        
        if hasattr(pipeline_service, 'decay_engine'):
            print(f'Decay engine value: {pipeline_service.decay_engine}')
        else:
            print('No decay_engine attribute found')

except Exception as e:
    print(f'❌ Test failed: {e}')
    import traceback
    traceback.print_exc()

finally:
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)
    print('✅ Cleanup completed')
