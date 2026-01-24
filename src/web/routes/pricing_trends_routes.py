"""
Flask routes for pricing trends and intelligence endpoints.
"""
from flask import Blueprint, render_template, request, jsonify
from decimal import Decimal

from src.services.container import get_container
from src.models.pricing_intelligence import (
    TrendPoint,
    MarginTrendPoint,
    RateVolatility
)


# ============================================================================
# Blueprint Setup
# ============================================================================

pricing_trends_bp = Blueprint(
    'pricing_trends',
    __name__,
    url_prefix='/pricing/trends'
)


def get_pricing_trends_service():
    """Dependency injection helper - matches your existing pattern"""
    container = get_container()
    return container.get("pricing_trends_service")

# ============================================================================
# Routes
# ============================================================================

@pricing_trends_bp.route('/')
def trends_dashboard():
    """Main trends dashboard - landing page"""
    return render_template('pricing/trends_dashboard.html')


@pricing_trends_bp.route('/rate-trends')
def rate_trends():
    """
    Rate trends analysis page.
    Shows how average rates change over time by dimension.
    """
    dimension = request.args.get('dimension', 'sales_person')
    months_back = int(request.args.get('months_back', 12))
    
    service = get_pricing_trends_service()
    trends = service.get_rate_trends(dimension, months_back)
    
    # Convert to JSON-friendly format for charts
    chart_data = _prepare_trends_for_chart(trends)
    
    return render_template(
        'pricing/rate_trends.html',
        dimension=dimension,
        months_back=months_back,
        trends=trends,
        chart_data=chart_data,
        available_dimensions=['sales_person', 'sector', 'language', 'market']
    )


@pricing_trends_bp.route('/margin-trends')
def margin_trends():
    """
    Margin analysis page.
    Shows gross margin percentage trends over time.
    """
    groupby = request.args.get('groupby', 'sales_person')
    months_back = int(request.args.get('months_back', 12))
    
    service = get_pricing_trends_service()
    margins = service.get_margin_trends(groupby, months_back)
    
    chart_data = _prepare_margins_for_chart(margins)
    
    return render_template(
        'pricing/margin_trends.html',
        groupby=groupby,
        months_back=months_back,
        margins=margins,
        chart_data=chart_data,
        available_dimensions=['sales_person', 'sector', 'language', 'market']
    )


@pricing_trends_bp.route('/pricing-consistency')
def pricing_consistency():
    """
    Pricing consistency analysis.
    Shows coefficient of variation to identify pricing discipline issues.
    """
    dimension = request.args.get('dimension', 'sales_person')
    timeframe = request.args.get('timeframe', '2025')
    
    service = get_pricing_trends_service()
    volatility = service.get_pricing_consistency(dimension, timeframe)
    
    return render_template(
        'pricing/pricing_consistency.html',
        dimension=dimension,
        timeframe=timeframe,
        volatility=volatility,
        available_dimensions=['sales_person', 'sector', 'language', 'market']
    )


@pricing_trends_bp.route('/yoy-comparison')
def yoy_comparison():
    """
    Year-over-year comparison page.
    Shows which dimensions are improving vs declining.
    """
    dimension = request.args.get('dimension', 'sales_person')
    current_year = request.args.get('current', '2025')
    previous_year = request.args.get('previous', '2024')
    
    service = get_pricing_trends_service()
    comparison = service.get_dimension_comparison_summary(
        dimension,
        current_year,
        previous_year
    )
    
    return render_template(
        'pricing/yoy_comparison.html',
        dimension=dimension,
        current_year=current_year,
        previous_year=previous_year,
        comparison=comparison,
        available_dimensions=['sales_person', 'sector', 'language', 'market']
    )


@pricing_trends_bp.route('/momentum/<dimension>/<dimension_value>')
def rate_momentum(dimension: str, dimension_value: str):
    """
    Individual momentum analysis.
    Shows trend direction for a specific dimension value.
    """
    months_back = int(request.args.get('months_back', 6))
    
    service = get_pricing_trends_service()
    momentum = service.get_rate_momentum(dimension, dimension_value, months_back)
    
    return render_template(
        'pricing/rate_momentum.html',
        dimension=dimension,
        dimension_value=dimension_value,
        months_back=months_back,
        momentum=momentum
    )

@pricing_trends_bp.route('/concentration')
def concentration_analysis():
    """
    Revenue concentration analysis page.
    Shows HHI, top N metrics, and customer concentration risk.
    """
    year = request.args.get('year', '2025')
    
    service = get_pricing_trends_service()
    
    # Get current year metrics
    current_metrics = service.get_concentration_metrics(year)
    
    print(f"DEBUG: Year={year}, Metrics={current_metrics}")  # ADD THIS LINE
    
    # Get top customers
    top_customers = service.get_top_customers(year, limit=50)
    
    print(f"DEBUG: Top customers count={len(top_customers)}")  # ADD THIS LINE
    
    # Get trend over last 3 years
    years = [str(int(year) - 2), str(int(year) - 1), year]
    trend = service.get_concentration_trend(years)
    
    print(f"DEBUG: Trend count={len(trend)}")  # ADD THIS LINE
    
    return render_template(
        'pricing/concentration_analysis.html',
        year=year,
        metrics=current_metrics,
        top_customers=top_customers,
        trend=trend,
        available_years=['2025', '2024', '2023', '2022', '2021']
    )
# ============================================================================
# API Endpoints (for AJAX/Chart.js)
# ============================================================================

@pricing_trends_bp.route('/api/rate-trends-data')
def api_rate_trends_data():
    """JSON endpoint for chart data"""
    dimension = request.args.get('dimension', 'sales_person')
    months_back = int(request.args.get('months_back', 12))
    
    service = get_pricing_trends_service()
    trends = service.get_rate_trends(dimension, months_back)
    
    return jsonify(_prepare_trends_for_chart(trends))


@pricing_trends_bp.route('/api/margin-trends-data')
def api_margin_trends_data():
    """JSON endpoint for margin chart data"""
    groupby = request.args.get('groupby', 'sales_person')
    months_back = int(request.args.get('months_back', 12))
    
    service = get_pricing_trends_service()
    margins = service.get_margin_trends(groupby, months_back)
    
    return jsonify(_prepare_margins_for_chart(margins))


# ============================================================================
# Helper Functions
# ============================================================================

def _prepare_trends_for_chart(trends: dict) -> dict:
    """
    Convert trends data to Chart.js format.
    
    Returns:
        {
            labels: ['Jan-24', 'Feb-24', ...],
            datasets: [
                {label: 'Lisa Chen', data: [100, 105, 110, ...]},
                {label: 'John Smith', data: [95, 98, 102, ...]}
            ]
        }
    """
    if not trends:
        return {'labels': [], 'datasets': []}
    
    # Get all unique periods across all dimension values
    all_periods = set()
    for trend_points in trends.values():
        all_periods.update(tp.period for tp in trend_points)
    
    labels = sorted(all_periods)
    
    datasets = []
    for dim_val, trend_points in trends.items():
        # Create lookup for this dimension value
        period_lookup = {tp.period: float(tp.average_rate) for tp in trend_points}
        
        # Build data array aligned with labels
        data = [period_lookup.get(period, None) for period in labels]
        
        datasets.append({
            'label': dim_val,
            'data': data,
            'borderWidth': 2,
            'fill': False
        })
    
    return {
        'labels': labels,
        'datasets': datasets
    }


def _prepare_margins_for_chart(margins: dict) -> dict:
    """Convert margin trends to Chart.js format"""
    if not margins:
        return {'labels': [], 'datasets': []}
    
    all_periods = set()
    for margin_points in margins.values():
        all_periods.update(mp.period for mp in margin_points)
    
    labels = sorted(all_periods)
    
    datasets = []
    for dim_val, margin_points in margins.items():
        period_lookup = {mp.period: mp.margin_percentage for mp in margin_points}
        data = [period_lookup.get(period, None) for period in labels]
        
        datasets.append({
            'label': dim_val,
            'data': data,
            'borderWidth': 2,
            'fill': False
        })
    
    return {
        'labels': labels,
        'datasets': datasets
    }


def _decimal_to_float(obj):
    """Helper for JSON serialization of Decimal objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")