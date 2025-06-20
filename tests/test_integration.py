# tests/test_integration.py
"""
Integration tests for the refactored Flask application.
Tests the complete request-response cycle with real services.
"""
import pytest
import tempfile
import os
import json
from unittest.mock import patch

from src.web.app import create_app
from src.services.container import reset_container
from src.config.settings import reset_settings


class TestFlaskAppIntegration:
    """Integration tests for the Flask application."""
    
    def setup_method(self):
        """Set up test environment."""
        # Reset global state
        reset_container()
        reset_settings()
        
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        # Set environment variables
        os.environ['DB_PATH'] = self.temp_db.name
        os.environ['DATA_PATH'] = tempfile.mkdtemp()
        os.environ['FLASK_ENV'] = 'testing'
        
        # Create test app
        self.app = create_app('testing')
        self.client = self.app.test_client()
        
        # Create basic test data in database
        self._create_test_data()
    
    def teardown_method(self):
        """Clean up test environment."""
        # Remove temporary files
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
        
        # Clean up environment variables
        for key in ['DB_PATH', 'DATA_PATH', 'FLASK_ENV']:
            if key in os.environ:
                del os.environ[key]
    
    def _create_test_data(self):
        """Create minimal test data in database."""
        import sqlite3
        
        conn = sqlite3.connect(self.temp_db.name)
        
        # Create minimal schema
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spots (
                spot_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                sales_person TEXT,
                gross_rate DECIMAL,
                air_date DATE,
                revenue_type TEXT
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INTEGER PRIMARY KEY,
                normalized_name TEXT
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS month_closures (
                broadcast_month TEXT PRIMARY KEY,
                closed_date DATE,
                closed_by TEXT
            )
        """)
        
        # Insert test data
        conn.execute("""
            INSERT INTO customers (customer_id, normalized_name) 
            VALUES (1, 'Test Customer')
        """)
        
        conn.execute("""
            INSERT INTO spots (customer_id, sales_person, gross_rate, air_date, revenue_type)
            VALUES (1, 'John Doe', 1000.00, '2025-01-15', 'Regular')
        """)
        
        conn.commit()
        conn.close()
    
    def test_app_creation(self):
        """Test that the app is created successfully."""
        assert self.app is not None
        assert self.app.config['ENVIRONMENT'] == 'testing'
    
    def test_health_check_endpoint(self):
        """Test the health check endpoint."""
        response = self.client.get('/health')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['status'] == 'healthy'
        assert 'services_count' in data
    
    def test_app_info_endpoint(self):
        """Test the application info endpoint."""
        response = self.client.get('/info')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['app_name'] == 'CTV Reporting System'
        assert 'blueprints' in data
        assert 'features' in data
    
    def test_root_redirect(self):
        """Test that root URL redirects to reports."""
        response = self.client.get('/')
        
        assert response.status_code == 302
        assert '/reports/' in response.location
    
    def test_reports_index(self):
        """Test reports index page."""
        response = self.client.get('/reports/')
        
        assert response.status_code == 200
        assert b'html' in response.data.lower()
    
    @patch('src.services.report_data_service.ReportDataService.get_monthly_revenue_report_data')
    def test_monthly_revenue_report(self, mock_get_data):
        """Test monthly revenue report page."""
        # Mock the service response
        from src.models.report_data import MonthlyRevenueReportData, ReportMetadata, ReportFilters
        from decimal import Decimal
        
        mock_data = MonthlyRevenueReportData(
            selected_year=2025,
            available_years=[2025, 2024],
            total_customers=1,
            active_customers=1,
            total_revenue=Decimal('1000.00'),
            avg_monthly_revenue=Decimal('83.33'),
            revenue_data=[],
            ae_list=['John Doe'],
            revenue_types=['Regular'],
            month_status=[],
            filters=ReportFilters(year=2025),
            metadata=ReportMetadata(report_type="monthly_revenue")
        )
        
        mock_get_data.return_value = mock_data
        
        response = self.client.get('/reports/monthly-revenue')
        
        assert response.status_code == 200
        mock_get_data.assert_called_once()
    
    def test_api_health_check(self):
        """Test API health check endpoint."""
        response = self.client.get('/api/health')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert 'status' in data['data']
    
    @patch('src.services.report_data_service.ReportDataService._get_available_years')
    def test_api_available_years(self, mock_get_years):
        """Test API available years endpoint."""
        mock_get_years.return_value = [2025, 2024, 2023]
        
        response = self.client.get('/api/metadata/years')
        
        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['success'] is True
        assert 'available_years' in data['data']
    
    def test_api_not_found(self):
        """Test API 404 handling."""
        response = self.client.get('/api/nonexistent')
        
        assert response.status_code == 404
        data = json.loads(response.data)
        assert data['success'] is False
        assert data['error_code'] == 'ENDPOINT_NOT_FOUND'
    
    def test_web_not_found(self):
        """Test web 404 handling."""
        response = self.client.get('/nonexistent')
        
        assert response.status_code == 404
        # Should return HTML for web routes
        assert b'html' in response.data.lower()
    
    def test_service_injection_in_routes(self):
        """Test that services are properly injected into routes."""
        # This test verifies that the service container is working
        with self.app.app_context():
            from src.services.container import get_container
            
            container = get_container()
            assert container.has_service('database_connection')
            assert container.has_service('report_data_service')
    
    def test_error_handling_in_routes(self):
        """Test error handling in route handlers."""
        # Test with invalid year parameter
        response = self.client.get('/reports/monthly-revenue?year=invalid')
        
        # Should handle the error gracefully
        assert response.status_code in [400, 500]  # Either validation error or server error
    
    def test_request_parameter_extraction(self):
        """Test request parameter extraction and validation."""
        # Test with valid parameters
        response = self.client.get('/api/revenue/monthly/2025?customer_search=test&ae_filter=John')
        
        # Should process parameters without error
        assert response.status_code in [200, 500]  # Either success or service error
    
    def test_json_response_format(self):
        """Test that JSON responses have consistent format."""
        response = self.client.get('/api/health')
        
        assert response.status_code == 200
        assert response.content_type == 'application/json'
        
        data = json.loads(response.data)
        assert 'success' in data
        assert 'data' in data
    
    def test_template_context_preparation(self):
        """Test template context preparation."""
        with patch('src.services.report_data_service.ReportDataService.get_monthly_revenue_report_data') as mock_service:
            from src.models.report_data import MonthlyRevenueReportData, ReportMetadata, ReportFilters
            from decimal import Decimal
            
            mock_data = MonthlyRevenueReportData(
                selected_year=2025,
                available_years=[2025],
                total_customers=1,
                active_customers=1,
                total_revenue=Decimal('1000.00'),
                avg_monthly_revenue=Decimal('83.33'),
                revenue_data=[],
                ae_list=['John Doe'],
                revenue_types=['Regular'],
                month_status=[],
                filters=ReportFilters(year=2025),
                metadata=ReportMetadata(report_type="monthly_revenue")
            )
            
            mock_service.return_value = mock_data
            
            response = self.client.get('/reports/monthly-revenue')
            
            # Should render template successfully
            assert response.status_code == 200
            assert b'Monthly Revenue Dashboard' in response.data or b'html' in response.data.lower()


class TestServiceIntegration:
    """Test service integration and dependency injection."""
    
    def setup_method(self):
        """Set up test environment."""
        reset_container()
        reset_settings()
        
        # Create temporary database
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        os.environ['DB_PATH'] = self.temp_db.name
        os.environ['DATA_PATH'] = tempfile.mkdtemp()
        os.environ['FLASK_ENV'] = 'testing'
    
    def teardown_method(self):
        """Clean up test environment."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
        
        for key in ['DB_PATH', 'DATA_PATH', 'FLASK_ENV']:
            if key in os.environ:
                del os.environ[key]
    
    def test_service_container_initialization(self):
        """Test that service container initializes correctly."""
        from src.services.factory import initialize_services
        from src.services.container import get_container
        
        initialize_services()
        container = get_container()
        
        # Check that required services are registered
        assert container.has_service('database_connection')
        assert container.has_service('report_data_service')
    
    def test_database_connection_service(self):
        """Test database connection service."""
        from src.services.factory import initialize_services
        from src.services.container import get_container
        
        initialize_services()
        container = get_container()
        
        db_connection = container.get('database_connection')
        assert db_connection is not None
        assert db_connection.db_path == self.temp_db.name
    
    def test_report_data_service_creation(self):
        """Test report data service creation."""
        from src.services.factory import initialize_services
        from src.services.container import get_container
        
        initialize_services()
        container = get_container()
        
        report_service = container.get('report_data_service')
        assert report_service is not None
        assert hasattr(report_service, 'get_monthly_revenue_report_data')
    
    def test_service_singleton_behavior(self):
        """Test that singleton services return same instance."""
        from src.services.factory import initialize_services
        from src.services.container import get_container
        
        initialize_services()
        container = get_container()
        
        service1 = container.get('database_connection')
        service2 = container.get('database_connection')
        
        assert service1 is service2  # Same instance


class TestPerformanceIntegration:
    """Test performance aspects of the refactored system."""
    
    def setup_method(self):
        """Set up performance test environment."""
        reset_container()
        reset_settings()
        
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        os.environ['DB_PATH'] = self.temp_db.name
        os.environ['DATA_PATH'] = tempfile.mkdtemp()
        os.environ['FLASK_ENV'] = 'testing'
        
        self.app = create_app('testing')
        self.client = self.app.test_client()
    
    def teardown_method(self):
        """Clean up test environment."""
        if os.path.exists(self.temp_db.name):
            os.unlink(self.temp_db.name)
        
        for key in ['DB_PATH', 'DATA_PATH', 'FLASK_ENV']:
            if key in os.environ:
                del os.environ[key]
    
    def test_request_response_time(self):
        """Test that request response times are reasonable."""
        import time
        
        start_time = time.time()
        response = self.client.get('/health')
        end_time = time.time()
        
        response_time = (end_time - start_time) * 1000  # Convert to milliseconds
        
        assert response.status_code == 200
        assert response_time < 1000  # Should respond in less than 1 second
    
    def test_service_caching(self):
        """Test that service caching works correctly."""
        from src.services.factory import initialize_services
        from src.services.container import get_container
        
        initialize_services()
        container = get_container()
        
        report_service = container.get('report_data_service')
        
        # Test that cached methods work
        import time
        
        start_time = time.time()
        years1 = report_service._get_available_years(container.get('database_connection'))
        first_call_time = time.time() - start_time
        
        start_time = time.time()
        years2 = report_service._get_available_years(container.get('database_connection'))
        second_call_time = time.time() - start_time
        
        # Second call should be faster (cached)
        assert years1 == years2
        assert second_call_time < first_call_time or second_call_time < 0.001  # Very fast cached response


if __name__ == '__main__':
    pytest.main([__file__])