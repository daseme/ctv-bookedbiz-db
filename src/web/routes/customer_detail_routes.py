"""Customer detail report routes."""

from flask import Blueprint, render_template, abort
from src.services.customer_detail_service import CustomerDetailService
from src.services.container import get_container

customer_detail_bp = Blueprint(
    'customer_detail', 
    __name__,
    template_folder='templates'
)


@customer_detail_bp.route('/customer/<int:customer_id>')
def customer_detail(customer_id: int):
    """Display customer detail report page."""
    try:
        container = get_container()
        db = container.get('database_connection')
    except Exception as e:
        abort(500, f"Database connection not available: {e}")
    
    with db.connection() as conn:
        service = CustomerDetailService(conn)
        report = service.get_customer_detail(customer_id)
    
    if not report:
        abort(404, f"Customer {customer_id} not found")
    
    return render_template(
        'customer_detail.html',
        report=report,
        page_title=f"Customer: {report.summary.normalized_name}"
    )


@customer_detail_bp.route('/api/customer/<int:customer_id>/monthly-trend')
def customer_monthly_trend_api(customer_id: int):
    """API endpoint for monthly trend chart data."""
    from flask import jsonify
    
    db = current_app.extensions.get('db')
    if not db:
        return jsonify({"error": "Database not available"}), 500
    
    with db.connection() as conn:
        service = CustomerDetailService(conn)
        trend = service._get_monthly_trend(customer_id, months=36)
    
    return jsonify({
        "labels": [m.broadcast_month for m in trend],
        "gross": [float(m.gross_revenue) for m in trend],
        "net": [float(m.net_revenue) for m in trend],
        "spots": [m.spot_count for m in trend]
    })