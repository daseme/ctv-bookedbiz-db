# tests/test_report_data_service.py
"""
Unit tests for the ReportDataService.
"""

import pytest
from unittest.mock import Mock, patch
from decimal import Decimal
from datetime import date

from src.services.report_data_service import ReportDataService
from src.models.report_data import (
    ReportFilters,
    MonthlyRevenueReportData,
    CustomerMonthlyRow,
    AEPerformanceReportData,
    QuarterlyPerformanceReportData,
)
from src.services.container import ServiceContainer


class TestReportDataService:
    """Test the ReportDataService class."""

    def setup_method(self):
        """Set up test fixtures."""
        # Create mock container
        self.mock_container = Mock(spec=ServiceContainer)
        self.mock_container.get_config.return_value = True

        # Create mock database connection
        self.mock_db_connection = Mock()
        self.mock_container.get.return_value = self.mock_db_connection

        # Create service instance
        self.service = ReportDataService(self.mock_container)

        # Set up mock database connection behavior
        self.mock_conn = Mock()
        self.mock_db_connection.connect.return_value = self.mock_conn

    def test_init_with_container(self):
        """Test service initialization with container."""
        service = ReportDataService(self.mock_container)
        assert service.container is self.mock_container

    def test_init_without_container(self):
        """Test service initialization without container (uses global)."""
        with patch("src.services.report_data_service.get_container") as mock_get:
            mock_get.return_value = self.mock_container
            service = ReportDataService()
            assert service.container is self.mock_container

    def test_get_monthly_revenue_report_data_basic(self):
        """Test basic monthly revenue report generation."""
        # Mock database responses
        self._setup_monthly_revenue_mocks()

        # Call service method
        result = self.service.get_monthly_revenue_report_data(2025)

        # Verify result structure
        assert isinstance(result, MonthlyRevenueReportData)
        assert result.selected_year == 2025
        assert len(result.available_years) > 0
        assert result.total_customers >= 0
        assert isinstance(result.total_revenue, Decimal)
        assert len(result.revenue_data) >= 0
        assert result.metadata.report_type == "monthly_revenue"

    def test_get_monthly_revenue_report_data_with_filters(self):
        """Test monthly revenue report with filters."""
        filters = ReportFilters(
            year=2025,
            customer_search="test",
            ae_filter="John Doe",
            revenue_type="Regular",
        )

        self._setup_monthly_revenue_mocks()

        result = self.service.get_monthly_revenue_report_data(2025, filters)

        assert result.filters == filters
        assert result.selected_year == 2025

    def test_get_ae_performance_report_data(self):
        """Test AE performance report generation."""
        # Mock database response for AE performance
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            ("John Doe", 100, 50000.00, 500.00, "2025-01-01", "2025-12-31"),
            ("Jane Smith", 75, 37500.00, 500.00, "2025-02-01", "2025-11-30"),
        ]
        self.mock_conn.execute.return_value = mock_cursor

        result = self.service.get_ae_performance_report_data()

        assert isinstance(result, AEPerformanceReportData)
        assert len(result.ae_performance) == 2
        assert result.ae_performance[0].ae_name == "John Doe"
        assert result.ae_performance[0].spot_count == 100
        assert result.total_revenue == Decimal("87500.00")
        assert result.top_performer == "John Doe"

    def test_get_quarterly_performance_data(self):
        """Test quarterly performance report generation."""
        # Mock quarterly data
        quarterly_cursor = Mock()
        quarterly_cursor.fetchall.return_value = [
            ("Q1", "2025", 250, 125000.00, 500.00),
            ("Q2", "2025", 200, 100000.00, 500.00),
        ]

        # Mock AE data
        ae_cursor = Mock()
        ae_cursor.fetchall.return_value = [
            ("John Doe", 100, 50000.00, 500.00, "2025-01-01", "2025-12-31")
        ]

        # Set up execute to return different cursors for different queries
        self.mock_conn.execute.side_effect = [quarterly_cursor, ae_cursor]

        result = self.service.get_quarterly_performance_data()

        assert isinstance(result, QuarterlyPerformanceReportData)
        assert result.current_year == date.today().year
        assert len(result.quarterly_data) == 2
        assert result.quarterly_data[0].quarter == "Q1"
        assert result.quarterly_data[0].year == 2025

    def test_customer_monthly_revenue_query_building(self):
        """Test that customer monthly revenue queries are built correctly."""
        filters = ReportFilters(
            year=2025,
            customer_search="Acme Corp",
            ae_filter="John Doe",
            revenue_type="Regular",
        )

        # Mock empty result to focus on query building
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        self.mock_conn.execute.return_value = mock_cursor

        # Mock other required methods
        self._setup_monthly_revenue_mocks()

        self.service._get_customer_monthly_revenue(
            self.mock_db_connection, 2025, filters
        )

        # Verify that execute was called with parameters including filters
        self.mock_conn.execute.assert_called()
        call_args = self.mock_conn.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        # Check that filters are applied in query
        assert "strftime('%Y', s.air_date) = ?" in query
        assert "LOWER(c.normalized_name) LIKE LOWER(?)" in query
        assert "s.sales_person = ?" in query
        assert "s.revenue_type = ?" in query

        # Check parameters
        assert "2025" in params
        assert "%Acme Corp%" in params
        assert "John Doe" in params
        assert "Regular" in params

    def test_error_handling(self):
        """Test error handling in service methods."""
        # Mock database connection to raise exception
        self.mock_db_connection.connect.side_effect = Exception("Database error")

        with pytest.raises(Exception):
            self.service.get_monthly_revenue_report_data(2025)

    def test_caching_behavior(self):
        """Test that caching works for expensive operations."""
        # This is more of an integration test, but we can verify cache usage
        self._setup_monthly_revenue_mocks()

        # Call method twice
        result1 = self.service._get_available_years(self.mock_db_connection)
        result2 = self.service._get_available_years(self.mock_db_connection)

        # Should get same result (from cache)
        assert result1 == result2

        # Should only call database once due to caching
        assert self.mock_conn.execute.call_count == 1

    def test_customer_monthly_row_creation(self):
        """Test creation of CustomerMonthlyRow objects from database data."""
        # Mock database response with monthly data
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [
            (1, "Acme Corp", "John Doe", "Regular", "Technology", "01", 5000.00),
            (1, "Acme Corp", "John Doe", "Regular", "Technology", "02", 6000.00),
            (2, "Beta Inc", "Jane Smith", "Regular", "Healthcare", "01", 3000.00),
        ]
        self.mock_conn.execute.return_value = mock_cursor

        filters = ReportFilters(year=2025)
        result = self.service._get_customer_monthly_revenue(
            self.mock_db_connection, 2025, filters
        )

        assert len(result) == 2  # Two unique customers

        # Check first customer (Acme Corp)
        acme = next(row for row in result if row.customer == "Acme Corp")
        assert acme.customer_id == 1
        assert acme.ae == "John Doe"
        assert acme.month_1 == Decimal("5000.00")
        assert acme.month_2 == Decimal("6000.00")
        assert acme.month_3 == Decimal("0")  # No data for March
        assert acme.total == Decimal("11000.00")

        # Check second customer (Beta Inc)
        beta = next(row for row in result if row.customer == "Beta Inc")
        assert beta.customer_id == 2
        assert beta.ae == "Jane Smith"
        assert beta.month_1 == Decimal("3000.00")
        assert beta.total == Decimal("3000.00")

    def test_month_status_creation(self):
        """Test month status creation from closure data."""
        # Mock month closures query
        closure_cursor = Mock()
        closure_cursor.fetchall.return_value = [
            ("Jan-25", "2025-02-01", "admin"),
            ("Feb-25", "2025-03-01", "admin"),
        ]

        # Mock other queries for complete test
        self._setup_monthly_revenue_mocks()

        result = self.service.get_monthly_revenue_report_data(2025)

        # Should have 12 months of status
        assert len(result.month_status) == 12

        # Check January status (should be closed)
        jan_status = next(m for m in result.month_status if m.month == "2025-01")
        assert jan_status.status == "CLOSED"
        assert jan_status.closed_by == "admin"

        # Check a future month (should be unknown)
        future_month = next(m for m in result.month_status if m.month == "2025-12")
        if date.today() < date(2025, 12, 1):
            assert future_month.status == "UNKNOWN"

    def _setup_monthly_revenue_mocks(self):
        """Set up mock responses for monthly revenue report."""
        # Mock customer monthly revenue data
        revenue_cursor = Mock()
        revenue_cursor.fetchall.return_value = [
            (1, "Acme Corp", "John Doe", "Regular", "Technology", "01", 5000.00)
        ]

        # Mock available years
        years_cursor = Mock()
        years_cursor.fetchall.return_value = [("2025",), ("2024",), ("2023",)]

        # Mock AE list
        ae_cursor = Mock()
        ae_cursor.fetchall.return_value = [("John Doe",), ("Jane Smith",)]

        # Mock revenue types
        types_cursor = Mock()
        types_cursor.fetchall.return_value = [("Regular",), ("Premium",)]

        # Mock month closures
        closure_cursor = Mock()
        closure_cursor.fetchall.return_value = [("Jan-25", "2025-02-01", "admin")]

        # Set up execute to return different cursors for different queries
        cursors = [
            revenue_cursor,
            years_cursor,
            ae_cursor,
            types_cursor,
            closure_cursor,
        ]
        self.mock_conn.execute.side_effect = cursors


class TestReportFilters:
    """Test the ReportFilters model."""

    def test_valid_filters(self):
        """Test creation of valid filters."""
        filters = ReportFilters(
            year=2025,
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            customer_search="test",
            ae_filter="John Doe",
        )

        assert filters.year == 2025
        assert filters.customer_search == "test"
        assert filters.ae_filter == "John Doe"

    def test_invalid_date_range(self):
        """Test validation of invalid date ranges."""
        with pytest.raises(ValueError):
            ReportFilters(start_date=date(2025, 12, 31), end_date=date(2025, 1, 1))

    def test_invalid_year(self):
        """Test validation of invalid years."""
        with pytest.raises(ValueError):
            ReportFilters(year=1999)

        with pytest.raises(ValueError):
            ReportFilters(year=2101)

    def test_is_empty(self):
        """Test empty filter detection."""
        empty_filters = ReportFilters()
        assert empty_filters.is_empty()

        non_empty_filters = ReportFilters(year=2025)
        assert not non_empty_filters.is_empty()

    def test_to_dict(self):
        """Test filter serialization to dictionary."""
        filters = ReportFilters(year=2025, customer_search="test")

        result = filters.to_dict()

        assert result["year"] == 2025
        assert result["customer_search"] == "test"
        assert result["start_date"] is None


class TestCustomerMonthlyRow:
    """Test the CustomerMonthlyRow model."""

    def test_creation(self):
        """Test creation of CustomerMonthlyRow."""
        row = CustomerMonthlyRow(customer_id=1, customer="Test Corp", ae="John Doe")

        assert row.customer_id == 1
        assert row.customer == "Test Corp"
        assert row.ae == "John Doe"
        assert row.total == Decimal("0")

    def test_month_value_operations(self):
        """Test getting and setting month values."""
        row = CustomerMonthlyRow(customer_id=1, customer="Test Corp", ae="John Doe")

        # Test setting values
        row.set_month_value(1, 1000)
        row.set_month_value(2, 2000.50)

        # Test getting values
        assert row.get_month_value(1) == Decimal("1000")
        assert row.get_month_value(2) == Decimal("2000.50")

        # Test total calculation
        assert row.total == Decimal("3000.50")

    def test_invalid_month_operations(self):
        """Test invalid month number handling."""
        row = CustomerMonthlyRow(customer_id=1, customer="Test Corp", ae="John Doe")

        with pytest.raises(ValueError):
            row.get_month_value(0)

        with pytest.raises(ValueError):
            row.get_month_value(13)

        with pytest.raises(ValueError):
            row.set_month_value(0, 1000)

    def test_to_dict(self):
        """Test conversion to dictionary."""
        row = CustomerMonthlyRow(customer_id=1, customer="Test Corp", ae="John Doe")
        row.set_month_value(1, 1000)

        result = row.to_dict()

        assert result["customer_id"] == 1
        assert result["customer"] == "Test Corp"
        assert result["ae"] == "John Doe"
        assert result["month_1"] == 1000.0
        assert result["total"] == 1000.0


if __name__ == "__main__":
    pytest.main([__file__])
