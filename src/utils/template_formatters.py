# src/utils/template_formatters.py
"""
Template data formatters for consistent data presentation.
Provides utilities for currency, date, and percentage formatting.
"""

from typing import Union, Optional, List, Dict, Any
from decimal import Decimal
from datetime import date, datetime
import calendar
import json


def format_currency(
    amount: Union[Decimal, float, int, None],
    include_cents: bool = False,
    null_display: str = "-",
) -> str:
    """
    Format currency values for display.

    Args:
        amount: Amount to format
        include_cents: Whether to include cents in display
        null_display: What to display for null/zero values

    Returns:
        Formatted currency string
    """
    if amount is None or amount == 0:
        return null_display

    # Convert to float for formatting
    if isinstance(amount, Decimal):
        amount = float(amount)

    # Handle negative values
    is_negative = amount < 0
    amount = abs(amount)

    if include_cents:
        formatted = f"${amount:,.2f}"
    else:
        formatted = f"${amount:,,.0f}"

    if is_negative:
        formatted = f"-{formatted}"

    return formatted


def format_percentage(
    value: Union[float, Decimal, None], decimal_places: int = 1, null_display: str = "-"
) -> str:
    """
    Format percentage values for display.

    Args:
        value: Percentage value (0.15 = 15%)
        decimal_places: Number of decimal places
        null_display: What to display for null values

    Returns:
        Formatted percentage string
    """
    if value is None:
        return null_display

    if isinstance(value, Decimal):
        value = float(value)

    formatted = f"{value * 100:.{decimal_places}f}%"
    return formatted


def format_date_display(
    date_value: Union[date, datetime, str, None], format_type: str = "short"
) -> str:
    """
    Format dates for display.

    Args:
        date_value: Date to format
        format_type: "short" (Jan 2025), "long" (January 2025), "iso" (2025-01-15)

    Returns:
        Formatted date string
    """
    if date_value is None:
        return "-"

    # Convert string to date if needed
    if isinstance(date_value, str):
        try:
            if "T" in date_value:  # ISO datetime
                date_value = datetime.fromisoformat(
                    date_value.replace("Z", "+00:00")
                ).date()
            else:  # ISO date
                date_value = datetime.fromisoformat(date_value).date()
        except ValueError:
            return str(date_value)  # Return as-is if can't parse

    if isinstance(date_value, datetime):
        date_value = date_value.date()

    if format_type == "short":
        return date_value.strftime("%b %Y")
    elif format_type == "long":
        return date_value.strftime("%B %Y")
    elif format_type == "iso":
        return date_value.isoformat()
    elif format_type == "display":
        return date_value.strftime("%m/%d/%Y")
    else:
        return str(date_value)


def format_number(
    number: Union[int, float, Decimal, None],
    decimal_places: int = 0,
    null_display: str = "-",
) -> str:
    """
    Format numbers with thousands separators.

    Args:
        number: Number to format
        decimal_places: Number of decimal places
        null_display: What to display for null values

    Returns:
        Formatted number string
    """
    if number is None:
        return null_display

    if isinstance(number, Decimal):
        number = float(number)

    if decimal_places == 0:
        return f"{number:,,.0f}"
    else:
        return f"{number:,.{decimal_places}f}"


def format_month_name(month_number: int, year: int, format_type: str = "short") -> str:
    """
    Format month names for display.

    Args:
        month_number: Month number (1-12)
        year: Year
        format_type: "short" (Jan), "long" (January), "full" (January 2025)

    Returns:
        Formatted month string
    """
    if not 1 <= month_number <= 12:
        return f"Month {month_number}"

    if format_type == "short":
        return calendar.month_abbr[month_number]
    elif format_type == "long":
        return calendar.month_name[month_number]
    elif format_type == "full":
        return f"{calendar.month_name[month_number]} {year}"
    else:
        return calendar.month_name[month_number]


def create_chart_data(
    revenue_data: List[Dict[str, Any]], chart_type: str = "monthly_trend"
) -> Dict[str, Any]:
    """
    Prepare data for JavaScript charts.

    Args:
        revenue_data: List of revenue records
        chart_type: Type of chart data to create

    Returns:
        Chart data structure ready for frontend consumption
    """
    if chart_type == "monthly_trend":
        return _create_monthly_trend_data(revenue_data)
    elif chart_type == "ae_comparison":
        return _create_ae_comparison_data(revenue_data)
    elif chart_type == "customer_breakdown":
        return _create_customer_breakdown_data(revenue_data)
    else:
        return {"error": f"Unknown chart type: {chart_type}"}


def _create_monthly_trend_data(revenue_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create monthly trend chart data."""
    # Aggregate by month
    monthly_totals = {}
    for row in revenue_data:
        for month in range(1, 13):
            month_key = f"month_{month}"
            if month_key in row:
                month_name = format_month_name(month, 2025, "short")
                if month_name not in monthly_totals:
                    monthly_totals[month_name] = 0
                monthly_totals[month_name] += float(row[month_key] or 0)

    return {
        "type": "line",
        "labels": list(monthly_totals.keys()),
        "datasets": [
            {
                "label": "Monthly Revenue",
                "data": list(monthly_totals.values()),
                "borderColor": "#4299e1",
                "backgroundColor": "rgba(66, 153, 225, 0.1)",
                "tension": 0.1,
            }
        ],
    }


def _create_ae_comparison_data(revenue_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Create AE comparison chart data."""
    # Aggregate by AE
    ae_totals = {}
    for row in revenue_data:
        ae = row.get("ae", "Unknown")
        total = float(row.get("total", 0))
        if ae not in ae_totals:
            ae_totals[ae] = 0
        ae_totals[ae] += total

    # Sort by total and take top 10
    sorted_aes = sorted(ae_totals.items(), key=lambda x: x[1], reverse=True)[:10]

    return {
        "type": "bar",
        "labels": [ae for ae, _ in sorted_aes],
        "datasets": [
            {
                "label": "Total Revenue",
                "data": [total for _, total in sorted_aes],
                "backgroundColor": "#48bb78",
                "borderColor": "#38a169",
                "borderWidth": 1,
            }
        ],
    }


def _create_customer_breakdown_data(
    revenue_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Create customer breakdown pie chart data."""
    # Get top 10 customers by revenue
    customer_totals = {}
    for row in revenue_data:
        customer = row.get("customer", "Unknown")
        total = float(row.get("total", 0))
        if customer not in customer_totals:
            customer_totals[customer] = 0
        customer_totals[customer] += total

    sorted_customers = sorted(
        customer_totals.items(), key=lambda x: x[1], reverse=True
    )[:10]

    # Color palette for pie chart
    colors = [
        "#ff6384",
        "#36a2eb",
        "#ffce56",
        "#4bc0c0",
        "#9966ff",
        "#ff9f40",
        "#ff6384",
        "#c9cbcf",
        "#4bc0c0",
        "#ff6384",
    ]

    return {
        "type": "pie",
        "labels": [customer for customer, _ in sorted_customers],
        "datasets": [
            {
                "data": [total for _, total in sorted_customers],
                "backgroundColor": colors[: len(sorted_customers)],
                "borderWidth": 1,
            }
        ],
    }


def prepare_template_context(
    report_data: Dict[str, Any],
    additional_formatters: Optional[Dict[str, callable]] = None,
) -> Dict[str, Any]:
    """
    Prepare template context with formatting functions.

    Args:
        report_data: Raw report data
        additional_formatters: Additional formatting functions

    Returns:
        Template context with data and formatters
    """
    context = {
        # Data
        "data": report_data,
        # Formatting functions
        "format_currency": format_currency,
        "format_percentage": format_percentage,
        "format_date": format_date_display,
        "format_number": format_number,
        "format_month": format_month_name,
        # Utility functions
        "create_chart_data": create_chart_data,
    }

    # Add any additional formatters
    if additional_formatters:
        context.update(additional_formatters)

    return context


def serialize_for_javascript(data: Any) -> str:
    """
    Serialize data for JavaScript consumption.
    Handles Decimal objects and other Python-specific types.

    Args:
        data: Data to serialize

    Returns:
        JSON string safe for JavaScript
    """

    def decimal_handler(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (date, datetime)):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    return json.dumps(data, default=decimal_handler, ensure_ascii=False)


def calculate_statistics(revenue_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate summary statistics for revenue data.

    Args:
        revenue_data: List of revenue records

    Returns:
        Dictionary of calculated statistics
    """
    if not revenue_data:
        return {
            "total_customers": 0,
            "active_customers": 0,
            "total_revenue": 0,
            "avg_monthly_revenue": 0,
            "max_customer_revenue": 0,
            "min_customer_revenue": 0,
        }

    total_revenue = sum(float(row.get("total", 0)) for row in revenue_data)
    active_customers = sum(1 for row in revenue_data if float(row.get("total", 0)) > 0)
    customer_revenues = [float(row.get("total", 0)) for row in revenue_data]

    return {
        "total_customers": len(revenue_data),
        "active_customers": active_customers,
        "total_revenue": total_revenue,
        "avg_monthly_revenue": total_revenue / 12 if total_revenue > 0 else 0,
        "max_customer_revenue": max(customer_revenues) if customer_revenues else 0,
        "min_customer_revenue": min(customer_revenues) if customer_revenues else 0,
        "avg_customer_revenue": total_revenue / len(revenue_data)
        if revenue_data
        else 0,
    }


# Jinja2 filters for template use
def register_template_filters(app):
    """Register custom Jinja2 filters with Flask app."""

    @app.template_filter("currency")
    def currency_filter(amount, include_cents=False):
        return format_currency(amount, include_cents)

    @app.template_filter("percentage")
    def percentage_filter(value, decimal_places=1):
        return format_percentage(value, decimal_places)

    @app.template_filter("number")
    def number_filter(number, decimal_places=0):
        return format_number(number, decimal_places)

    @app.template_filter("date_display")
    def date_filter(date_value, format_type="short"):
        return format_date_display(date_value, format_type)

    @app.template_filter("month_name")
    def month_filter(month_number, year=2025, format_type="short"):
        return format_month_name(month_number, year, format_type)

    @app.template_filter("js_safe")
    def js_safe_filter(data):
        return serialize_for_javascript(data)
