"""Tests for Flask web application routes."""

import pytest
import json
import os
import sys
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from web.app import app, get_monthly_revenue_summary, get_quarterly_performance_data, get_ae_performance_data, get_sector_performance_data


@pytest.fixture
def client():
    """Create test client for Flask app."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


@pytest.fixture
def mock_monthly_data():
    """Mock monthly revenue data."""
    return [
        {
            'month_name': 'January',
            'sort_order': '01',
            'spot_count': 150,
            'total_revenue': 75000.00,
            'avg_rate': 500.00,
            'min_rate': 100.00,
            'max_rate': 1500.00
        },
        {
            'month_name': 'February',
            'sort_order': '02',
            'spot_count': 120,
            'total_revenue': 60000.00,
            'avg_rate': 500.00,
            'min_rate': 200.00,
            'max_rate': 1200.00
        },
        {
            'month_name': '*** TOTAL ***',
            'sort_order': '99',
            'spot_count': 270,
            'total_revenue': 135000.00,
            'avg_rate': 500.00,
            'min_rate': 100.00,
            'max_rate': 1500.00
        }
    ]


@pytest.fixture
def mock_quarterly_data():
    """Mock quarterly performance data."""
    return {
        'current_year': 2024,
        'quarterly_data': [
            {
                'quarter': 'Q1',
                'year': '2024',
                'spot_count': 450,
                'total_revenue': 225000.00,
                'avg_rate': 500.00
            },
            {
                'quarter': 'Q2',
                'year': '2024',
                'spot_count': 360,
                'total_revenue': 180000.00,
                'avg_rate': 500.00
            },
            {
                'quarter': 'Q1',
                'year': '2023',
                'spot_count': 400,
                'total_revenue': 200000.00,
                'avg_rate': 500.00
            }
        ]
    }


@pytest.fixture
def mock_ae_data():
    """Mock AE performance data."""
    return [
        {
            'ae_name': 'John Doe',
            'spot_count': 200,
            'total_revenue': 1200000.00,
            'avg_rate': 600.00,
            'first_spot_date': '2024-01-15',
            'last_spot_date': '2024-06-30'
        },
        {
            'ae_name': 'Jane Smith',
            'spot_count': 150,
            'total_revenue': 750000.00,
            'avg_rate': 500.00,
            'first_spot_date': '2024-02-01',
            'last_spot_date': '2024-06-15'
        }
    ]


@pytest.fixture
def mock_sector_data():
    """Mock sector performance data."""
    return {
        'sectors': [
            {
                'sector_name': 'Automotive',
                'sector_code': 'AUTO',
                'spot_count': 100,
                'total_revenue': 500000.00,
                'avg_rate': 500.00
            },
            {
                'sector_name': 'Technology',
                'sector_code': 'TECH',
                'spot_count': 80,
                'total_revenue': 400000.00,
                'avg_rate': 500.00
            }
        ],
        'top_customers_by_sector': [
            {
                'sector_name': 'Automotive',
                'customer_name': 'Ford Motors',
                'spot_count': 50,
                'total_revenue': 250000.00
            }
        ]
    }


class TestRoutes:
    """Test class for Flask route endpoints."""

    def test_index_route(self, client):
        """Test the index/home page route."""
        response = client.get('/')
        assert response.status_code == 200
        assert b'CTV Booked Biz Reports' in response.data
        assert b'Available Reports' in response.data
        assert b'Monthly Revenue Summary' in response.data
        assert b'Expectation Tracking' in response.data
        assert b'Performance Story' in response.data
        assert b'Sector Analysis' in response.data

    @patch('web.app.get_monthly_revenue_summary')
    def test_report1_route_success(self, mock_get_data, client, mock_monthly_data):
        """Test the monthly revenue report route with successful data."""
        mock_get_data.return_value = mock_monthly_data
        
        response = client.get('/report1')
        assert response.status_code == 200
        assert b'Monthly Revenue Summary' in response.data
        assert b'January' in response.data
        assert b'75,000.00' in response.data

    @patch('web.app.get_monthly_revenue_summary')
    def test_report1_route_error(self, mock_get_data, client):
        """Test the monthly revenue report route with error."""
        mock_get_data.side_effect = Exception("Database error")
        
        response = client.get('/report1')
        assert response.status_code == 500
        assert b'Error generating report' in response.data

    @patch('web.app.get_quarterly_performance_data')
    @patch('web.app.get_ae_performance_data')
    def test_report2_route_success(self, mock_get_ae, mock_get_quarterly, client, mock_quarterly_data, mock_ae_data):
        """Test the expectation tracking report route with successful data."""
        mock_get_quarterly.return_value = mock_quarterly_data
        mock_get_ae.return_value = mock_ae_data
        
        response = client.get('/report2')
        assert response.status_code == 200
        assert b'Management Expectation Tracking' in response.data
        assert b'Quarterly Performance Analysis' in response.data
        assert b'Account Executive Performance' in response.data

    @patch('web.app.get_quarterly_performance_data')
    @patch('web.app.get_ae_performance_data')
    def test_report3_route_success(self, mock_get_ae, mock_get_quarterly, client, mock_quarterly_data, mock_ae_data):
        """Test the performance story report route with successful data."""
        mock_get_quarterly.return_value = mock_quarterly_data
        mock_get_ae.return_value = mock_ae_data
        
        response = client.get('/report3')
        assert response.status_code == 200
        assert b'Quarterly Performance Story' in response.data
        assert b'Current Year at a Glance' in response.data
        assert b'Top Performing Account Executives' in response.data

    @patch('web.app.get_quarterly_performance_data')
    @patch('web.app.get_sector_performance_data')
    @patch('web.app.get_ae_performance_data')
    def test_report4_route_success(self, mock_get_ae, mock_get_sector, mock_get_quarterly, client, mock_quarterly_data, mock_sector_data, mock_ae_data):
        """Test the sector analysis report route with successful data."""
        mock_get_quarterly.return_value = mock_quarterly_data
        mock_get_sector.return_value = mock_sector_data
        mock_get_ae.return_value = mock_ae_data
        
        response = client.get('/report4')
        assert response.status_code == 200
        assert b'Quarterly Performance with Sector Analysis' in response.data
        assert b'Sector Performance Analysis' in response.data
        assert b'Concentration Risk Assessment' in response.data

    @patch('web.app.get_monthly_revenue_summary')
    def test_api_monthly_data(self, mock_get_data, client, mock_monthly_data):
        """Test the API endpoint for monthly data."""
        mock_get_data.return_value = mock_monthly_data
        
        response = client.get('/api/data/monthly')
        assert response.status_code == 200
        assert response.content_type == 'application/json'
        
        data = json.loads(response.data)
        assert len(data) == 3
        assert data[0]['month_name'] == 'January'
        assert data[0]['total_revenue'] == 75000.00

    @patch('web.app.get_quarterly_performance_data')
    def test_api_quarterly_data(self, mock_get_data, client, mock_quarterly_data):
        """Test the API endpoint for quarterly data."""
        mock_get_data.return_value = mock_quarterly_data
        
        response = client.get('/api/data/quarterly')
        assert response.status_code == 200
        assert response.content_type == 'application/json'
        
        data = json.loads(response.data)
        assert data['current_year'] == 2024
        assert len(data['quarterly_data']) == 3

    @patch('web.app.get_ae_performance_data')
    def test_api_ae_data(self, mock_get_data, client, mock_ae_data):
        """Test the API endpoint for AE data."""
        mock_get_data.return_value = mock_ae_data
        
        response = client.get('/api/data/ae')
        assert response.status_code == 200
        assert response.content_type == 'application/json'
        
        data = json.loads(response.data)
        assert len(data) == 2
        assert data[0]['ae_name'] == 'John Doe'
        assert data[0]['total_revenue'] == 1200000.00

    @patch('web.app.get_sector_performance_data')
    def test_api_sectors_data(self, mock_get_data, client, mock_sector_data):
        """Test the API endpoint for sector data."""
        mock_get_data.return_value = mock_sector_data
        
        response = client.get('/api/data/sectors')
        assert response.status_code == 200
        assert response.content_type == 'application/json'
        
        data = json.loads(response.data)
        assert len(data['sectors']) == 2
        assert data['sectors'][0]['sector_name'] == 'Automotive'

    def test_api_invalid_report_type(self, client):
        """Test the API endpoint with invalid report type."""
        response = client.get('/api/data/invalid')
        assert response.status_code == 400
        
        data = json.loads(response.data)
        assert 'error' in data
        assert data['error'] == 'Invalid report type'

    @patch('web.app.get_monthly_revenue_summary')
    def test_api_error_handling(self, mock_get_data, client):
        """Test the API endpoint error handling."""
        mock_get_data.side_effect = Exception("Database connection failed")
        
        response = client.get('/api/data/monthly')
        assert response.status_code == 500
        
        data = json.loads(response.data)
        assert 'error' in data
        assert 'Database connection failed' in data['error']


class TestDataFunctions:
    """Test class for data retrieval functions."""

    @patch('web.app.db_connection.connect')
    def test_get_monthly_revenue_summary(self, mock_connect):
        """Test the monthly revenue summary data function."""
        # Mock database connection and response
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the cursor and results
        mock_cursor = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'month_name': 'January', 'spot_count': 100, 'total_revenue': 50000.00}
        ]
        
        # Mock file reading
        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value.read.return_value = "SELECT * FROM spots"
            
            result = get_monthly_revenue_summary()
            
            assert len(result) == 1
            assert result[0]['month_name'] == 'January'
            assert result[0]['spot_count'] == 100

    @patch('web.app.db_connection.connect')
    def test_get_quarterly_performance_data(self, mock_connect):
        """Test the quarterly performance data function."""
        # Mock database connection and response
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the cursor and results
        mock_cursor = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'quarter': 'Q1', 'year': '2024', 'spot_count': 100, 'total_revenue': 50000.00, 'avg_rate': 500.00}
        ]
        
        result = get_quarterly_performance_data()
        
        assert 'current_year' in result
        assert 'quarterly_data' in result
        assert len(result['quarterly_data']) == 1

    @patch('web.app.db_connection.connect')
    def test_get_ae_performance_data(self, mock_connect):
        """Test the AE performance data function."""
        # Mock database connection and response
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the cursor and results
        mock_cursor = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [
            {'ae_name': 'John Doe', 'spot_count': 100, 'total_revenue': 50000.00}
        ]
        
        result = get_ae_performance_data()
        
        assert len(result) == 1
        assert result[0]['ae_name'] == 'John Doe'

    @patch('web.app.db_connection.connect')
    def test_get_sector_performance_data(self, mock_connect):
        """Test the sector performance data function."""
        # Mock database connection and response
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock the cursor and results
        mock_cursor = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = [
            [{'sector_name': 'Automotive', 'spot_count': 100, 'total_revenue': 50000.00}],
            [{'sector_name': 'Automotive', 'customer_name': 'Ford', 'spot_count': 50, 'total_revenue': 25000.00}]
        ]
        
        result = get_sector_performance_data()
        
        assert 'sectors' in result
        assert 'top_customers_by_sector' in result
        assert len(result['sectors']) == 1
        assert result['sectors'][0]['sector_name'] == 'Automotive'


if __name__ == '__main__':
    pytest.main([__file__]) 