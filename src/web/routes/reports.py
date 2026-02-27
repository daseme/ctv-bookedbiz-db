# src/web/routes/reports.py
"""
Reports blueprint with clean, focused route handlers.
Uses dependency injection and delegates to service layer.
"""

import logging
from flask import Blueprint, render_template, request, jsonify, Response
from flask_login import current_user
import io
import csv
import traceback
from datetime import date, datetime
from decimal import Decimal
from src.services.container import get_container
from src.models.report_data import ReportFilters
from src.web.utils.request_helpers import (
    get_year_parameter,
    safe_get_service,
    log_requests,
    handle_request_errors,
)
from src.utils.template_formatters import prepare_template_context
from src.web.placement_confirmation_parser import (
    contracts_highlight_7_days,
    contracts_by_client_15_days,
)
from src.models.planning import PlanningPeriod

logger = logging.getLogger(__name__)

# Create blueprint
reports_bp = Blueprint("reports", __name__, url_prefix="/reports")


@reports_bp.route("/")
@log_requests
def reports_index():
    """Reports index page with links to all reports."""
    return render_template("index.html")


@reports_bp.route("/revenue-dashboard-customer")
@log_requests
@handle_request_errors
def revenue_dashboard_customer():
    """Customer Revenue Dashboard - Interactive monthly revenue analysis."""
    try:
        # Get services
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        # Extract parameters with proper defaults
        year = get_year_parameter(default_year=date.today().year)

        # Build filters from request parameters
        customer_search = request.args.get("customer_search", "").strip()
        ae_filter = request.args.get("ae_filter", "").strip()
        revenue_type = request.args.get("revenue_type", "").strip()
        revenue_field = request.args.get("revenue_field", "gross").strip()
        sector = request.args.get("sector", "").strip()
        market = request.args.get("market", "").strip()

        # Convert empty strings and 'all' to None
        filters = ReportFilters(
            year=year,
            customer_search=customer_search if customer_search else None,
            ae_filter=ae_filter if ae_filter and ae_filter != "all" else None,
            revenue_type=revenue_type
            if revenue_type and revenue_type != "all"
            else None,
            revenue_field=revenue_field
            if revenue_field in ["gross", "net"]
            else "gross",
            sector=sector if sector and sector != "all" else None,
            market=market if market and market != "all" else None,
        )

        # Get report data
        logger.info(
            f"Generating customer revenue dashboard for year {year} with filters: {filters.to_dict()}"
        )
        report_data = report_service.get_monthly_revenue_report_data(year, filters)

        # Prepare template context
        template_data = {
            "title": "Customer Revenue Dashboard",
            "data": report_data.to_dict(),
        }

        logger.info(
            f"Dashboard generated successfully: {report_data.total_customers} customers, "
            f"{report_data.active_customers} active, ${report_data.total_revenue:,.0f} total revenue"
        )

        return render_template("revenue-dashboard-customer.html", **template_data)

    except Exception as e:
        logger.error(f"Error generating customer revenue dashboard: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating customer revenue dashboard: {str(e)}",
        ), 500


@reports_bp.route("/customer-sector-manager")
def customer_sector_manager():
    """Customer and Sector Management Tool"""
    try:
        return render_template("customer_sector_manager.html")
    except Exception as e:
        logger.error(f"Error rendering customer sector manager: {e}")
        return render_template("error_500.html"), 500


# Add this to src/web/routes/reports.py


@reports_bp.route("/ae-dashboard")
@log_requests
@handle_request_errors
def ae_dashboard():
    """AE Account Management Dashboard - YoY Performance Analysis."""
    try:
        # Get services
        container = get_container()
        ae_service = safe_get_service(container, "ae_dashboard_service")

        # Extract parameters
        year = get_year_parameter(default_year=date.today().year)
        ae_filter = request.args.get("ae_filter", "").strip()

        # Convert "everyone" to None for service
        if ae_filter == "everyone" or not ae_filter:
            ae_filter = None

        # Get dashboard data
        logger.info(
            f"Generating AE dashboard for year {year}, AE filter: {ae_filter or 'Everyone'}"
        )
        dashboard_data = ae_service.get_dashboard_data(year, ae_filter)

        # Prepare template context
        template_data = {
            "title": "Account Management Dashboard",
            "data": dashboard_data.to_dict(),
        }

        logger.info(
            f"AE Dashboard generated: {len(dashboard_data.customers)} customers, "
            f"${dashboard_data.total_ytd_2024:,.0f} total revenue"
        )

        return render_template("ae-dashboard.html", **template_data)

    except Exception as e:
        logger.error(f"Error generating AE dashboard: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating AE dashboard: {str(e)}",
        ), 500


# ============================================================================
# AE Personal Dashboard (for logged-in AE users)
# ============================================================================


@reports_bp.route("/ae-dashboard-personal")
@handle_request_errors
@log_requests
def ae_dashboard_personal():
    """AE Personal Dashboard - Shows monthly booked revenue, forecast, and budget for logged-in AE.

    For admins/management: allows selecting any AE via ?ae= parameter.
    For AE users: shows their own dashboard.
    """
    if not current_user.is_authenticated:
        from flask import redirect, url_for

        return redirect(url_for("user_management.login"))

    try:
        container = get_container()
        planning_service = safe_get_service(container, "planning_service")
        budget_service = safe_get_service(container, "budget_service")

        if not planning_service or not hasattr(planning_service, "repository"):
            raise ValueError("Planning service not available")

        # Get year from query parameter or default to current year
        current_year = request.args.get("year", type=int) or date.today().year

        # Get account revenue date range filters
        account_start_date = request.args.get("account_start_date", "").strip()
        account_end_date = request.args.get("account_end_date", "").strip()
        
        # If no date filters provided, default to first and last day of selected year
        if not account_start_date and not account_end_date:
            account_start_date = date(current_year, 1, 1).isoformat()
            account_end_date = date(current_year, 12, 31).isoformat()

        # Get available years from database
        available_years = []
        try:
            db_connection = container.get("database_connection")
            conn = db_connection.connect()
            try:
                cursor = conn.execute("""
                    SELECT DISTINCT CAST('20' || SUBSTR(broadcast_month, -2) AS INTEGER) as year
                    FROM spots
                    WHERE broadcast_month IS NOT NULL 
                      AND broadcast_month <> ''
                      AND SUBSTR(broadcast_month, -2) IS NOT NULL
                    ORDER BY year DESC
                """)
                available_years = [
                    row[0] for row in cursor.fetchall() if row[0] is not None
                ]
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not get available years: {e}")
            # Fallback to default range
            available_years = list(range(2021, 2027))

        # Determine which AE to show
        # Admins/management can select an AE, AE users see their own
        is_admin_or_management = current_user.role.value in ["admin", "management"]
        selected_ae = request.args.get("ae", "").strip()

        # Determine which AE to show
        ae_name = (
            selected_ae
            if (is_admin_or_management and selected_ae)
            else current_user.full_name
        )
        if not ae_name or not ae_name.strip():
            raise ValueError(f"Cannot determine AE name for user {current_user.email}")

        logger.info(
            f"Loading dashboard for AE: {ae_name}, User: {current_user.email}, Role: {current_user.role.value}"
        )

        # Get revenue entities list for dropdown (admins/management only)
        ae_list = []
        if is_admin_or_management:
            try:
                entities = planning_service.get_revenue_entities()
                ae_list = [entity.entity_name for entity in entities]
                # Add "All" option at the beginning
                ae_list.insert(0, "All")
            except Exception as e:
                logger.warning(f"Could not get revenue entities list: {e}")
        
        # Check if showing "All" revenue
        show_all_revenue = (is_admin_or_management and selected_ae == "All")

        # Helper function to calculate percentage
        def calc_pct(numerator, denominator):
            return float((numerator / denominator) * 100) if denominator > 0 else 0.0

        # Get monthly data for all 12 months
        monthly_data = []
        total_booked = Decimal("0")
        total_forecast = Decimal("0")
        total_budget = Decimal("0")
        repo = planning_service.repository

        for month_num in range(1, 13):
            # Get booked revenue
            if show_all_revenue:
                # Sum all booked revenue across all AEs
                try:
                    period = PlanningPeriod(year=current_year, month=month_num)
                    db_connection = container.get("database_connection")
                    conn = db_connection.connect()
                    try:
                        cursor = conn.execute(
                            """
                            SELECT COALESCE(SUM(gross_rate), 0) AS booked
                            FROM spots
                            WHERE broadcast_month = ?
                              AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                        """,
                            (period.broadcast_month,),
                        )
                        row = cursor.fetchone()
                        booked_revenue = Decimal(str(row["booked"])) if row else Decimal("0")
                    finally:
                        conn.close()
                except Exception as e:
                    logger.warning(
                        f"Could not get all booked revenue for {current_year}-{month_num:02d}: {e}"
                    )
                    booked_revenue = Decimal("0")
            else:
                try:
                    booked_revenue = repo.get_booked_revenue(
                        ae_name, current_year, month_num
                    ) or Decimal("0")
                except Exception as e:
                    logger.warning(
                        f"Could not get booked revenue for {ae_name} {current_year}-{month_num:02d}: {e}"
                    )
                    booked_revenue = Decimal("0")

            # Get budget
            if show_all_revenue:
                # Sum all budgets across all AEs
                try:
                    db_connection = container.get("database_connection")
                    conn = db_connection.connect()
                    try:
                        cursor = conn.execute(
                            """
                            SELECT COALESCE(SUM(budget_amount), 0) AS total_budget
                            FROM budget
                            WHERE year = ? AND month = ?
                        """,
                            (current_year, month_num),
                        )
                        row = cursor.fetchone()
                        budget = Decimal(str(row["total_budget"])) if row else Decimal("0")
                    finally:
                        conn.close()
                except Exception as e:
                    logger.warning(
                        f"Could not get all budget for {current_year}-{month_num:02d}: {e}"
                    )
                    budget = Decimal("0")
            else:
                try:
                    budget = repo.get_budget(ae_name, current_year, month_num) or Decimal(
                        "0"
                    )
                except Exception:
                    try:
                        budget = Decimal(
                            str(
                                budget_service.get_monthly_budget(
                                    ae_name, f"{current_year}-{month_num:02d}"
                                )
                            )
                        )
                    except:
                        budget = Decimal("0")

            # Get forecast_entered (defaults to budget if not set)
            if show_all_revenue:
                # Calculate effective forecast per AE, then sum
                # For each AE: forecast = max(forecast_entered (or budget), booked_revenue)
                try:
                    period = PlanningPeriod(year=current_year, month=month_num)
                    total_effective_forecast = Decimal("0")
                    
                    # Get all revenue entities
                    entities = planning_service.get_revenue_entities()
                    
                    for entity in entities:
                        entity_name = entity.entity_name
                        
                        # Get booked revenue for this entity
                        try:
                            entity_booked = repo.get_booked_revenue(
                                entity_name, current_year, month_num
                            ) or Decimal("0")
                        except Exception:
                            entity_booked = Decimal("0")
                        
                        # Get forecast_entered (defaults to budget if not set)
                        try:
                            entity_forecast_data = repo.get_forecast_with_metadata(
                                entity_name, current_year, month_num
                            )
                            entity_forecast_entered = (
                                entity_forecast_data["amount"]
                                if entity_forecast_data
                                else None
                            )
                        except Exception:
                            try:
                                entity_forecast_entered = repo.get_forecast(
                                    entity_name, current_year, month_num
                                )
                            except:
                                entity_forecast_entered = None
                        
                        # Get budget for this entity (for defaulting forecast)
                        try:
                            entity_budget = repo.get_budget(
                                entity_name, current_year, month_num
                            ) or Decimal("0")
                        except Exception:
                            entity_budget = Decimal("0")
                        
                        # Forecast entered defaults to budget if not set
                        if entity_forecast_entered is None:
                            entity_forecast_entered = entity_budget
                        
                        # Effective forecast = max(forecast_entered, booked)
                        entity_effective_forecast = max(entity_forecast_entered, entity_booked)
                        total_effective_forecast += entity_effective_forecast
                    
                    forecast_entered = total_effective_forecast
                except Exception as e:
                    logger.warning(
                        f"Could not get all forecast for {current_year}-{month_num:02d}: {e}"
                    )
                    # Fallback: use budget as forecast_entered
                    forecast_entered = budget
            else:
                try:
                    forecast_data = repo.get_forecast_with_metadata(
                        ae_name, current_year, month_num
                    )
                    forecast_entered = (
                        forecast_data["amount"]
                        if forecast_data
                        else (budget if budget > 0 else Decimal("0"))
                    )
                except Exception:
                    try:
                        forecast_entered = repo.get_forecast(
                            ae_name, current_year, month_num
                        ) or (budget if budget > 0 else Decimal("0"))
                    except:
                        forecast_entered = budget if budget > 0 else Decimal("0")

            # Calculate effective forecast = max(forecast_entered, booked) to match planning tool
            # For "All", forecast_entered already includes the per-AE max calculation, so use it directly
            if show_all_revenue:
                forecast = forecast_entered
            else:
                forecast = max(forecast_entered, booked_revenue)

            monthly_data.append(
                {
                    "month": month_num,
                    "month_name": datetime(current_year, month_num, 1).strftime("%B"),
                    "month_str": f"{current_year}-{month_num:02d}",
                    "booked_revenue": float(booked_revenue),
                    "forecast": float(forecast),
                    "budget": float(budget),
                    "forecast_vs_budget_pct": calc_pct(forecast, budget),
                    "booked_vs_budget_pct": calc_pct(booked_revenue, budget),
                    "booked_vs_forecast_pct": calc_pct(booked_revenue, forecast),
                }
            )

            total_booked += booked_revenue
            total_forecast += forecast
            total_budget += budget

        # Get account revenue data grouped by sector
        account_revenue_by_sector = {}
        total_account_revenue = Decimal("0")
        total_account_count = 0
        account_revenue_sectors = []
        try:
            db_connection = container.get("database_connection")
            conn = db_connection.connect()
            try:
                # Build date filter for account revenue
                date_filter = ""
                query_params = []

                if account_start_date and account_end_date:
                    date_filter = "AND s.air_date >= ? AND s.air_date <= ?"
                    query_params.extend([account_start_date, account_end_date])
                elif account_start_date:
                    date_filter = "AND s.air_date >= ?"
                    query_params.append(account_start_date)
                elif account_end_date:
                    date_filter = "AND s.air_date <= ?"
                    query_params.append(account_end_date)
                else:
                    # Default to full year if no date range specified
                    date_filter = "AND SUBSTR(s.broadcast_month, -2) = ?"
                    query_params.append(str(current_year)[-2:])

                # Build entity filter - special handling for WorldLink, House, and All
                entity_filter = ""
                if show_all_revenue:
                    # Show all revenue - no entity filter
                    entity_filter = "WHERE 1=1"
                elif ae_name == "WorldLink":
                    # WorldLink revenue is identified by bill_code prefix, not sales_person
                    entity_filter = "WHERE s.bill_code LIKE 'WorldLink:%'"
                elif ae_name == "House":
                    # House revenue excludes WorldLink bill_codes
                    entity_filter = "WHERE UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?)) AND s.bill_code NOT LIKE 'WorldLink:%'"
                    query_params.insert(0, ae_name)
                else:
                    # Standard AE lookup by sales_person
                    entity_filter = "WHERE UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?))"
                    query_params.insert(0, ae_name)

                cursor = conn.execute(
                    f"""
                    SELECT
                        COALESCE(c.normalized_name, s.bill_code, 'Unknown') AS customer_name,
                        COALESCE(sec.sector_name, 'Unknown') AS sector,
                        ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total_revenue,
                        c.customer_id
                    FROM spots s
                    LEFT JOIN customers c ON s.customer_id = c.customer_id
                    LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
                    {entity_filter}
                        {date_filter}
                        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                        AND COALESCE(s.gross_rate, 0) > 0
                    GROUP BY
                        COALESCE(c.normalized_name, s.bill_code, 'Unknown'),
                        COALESCE(sec.sector_name, 'Unknown'),
                        c.customer_id
                    ORDER BY sector, total_revenue DESC
                """,
                    query_params,
                )

                for row in cursor.fetchall():
                    sector = row[1]
                    revenue = float(row[2])

                    if sector not in account_revenue_by_sector:
                        account_revenue_by_sector[sector] = {
                            "sector": sector,
                            "accounts": [],
                            "total_revenue": 0.0,
                        }

                    account_revenue_by_sector[sector]["accounts"].append(
                        {
                            "customer_name": row[0],
                            "total_revenue": revenue,
                            "customer_id": row[3],
                        }
                    )
                    account_revenue_by_sector[sector]["total_revenue"] += revenue
                    total_account_revenue += Decimal(str(revenue))
                    total_account_count += 1

                # Sort accounts within each sector by revenue (already done in SQL, but ensure it)
                for sector_data in account_revenue_by_sector.values():
                    sector_data["accounts"].sort(
                        key=lambda x: x["total_revenue"], reverse=True
                    )
                    # total_revenue is already float from accumulation

            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not get account revenue data: {e}")

        # Convert to sorted list (sectors alphabetically)
        account_revenue_sectors = sorted(
            account_revenue_by_sector.values(), key=lambda x: x["sector"]
        )

        # Contracts added in the last 30 days: one line per contract, grouped by client with totals
        contracts_by_client = []
        try:
            db_connection = container.get("database_connection")
            conn = db_connection.connect()
            try:
                entity_filter = ""
                params = []
                if show_all_revenue:
                    # Show all contracts - no entity filter
                    entity_filter = "WHERE 1=1"
                elif ae_name == "WorldLink":
                    entity_filter = "WHERE s.bill_code LIKE 'WorldLink:%'"
                elif ae_name == "House":
                    entity_filter = "WHERE UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?)) AND s.bill_code NOT LIKE 'WorldLink:%'"
                    params.append(ae_name)
                else:
                    entity_filter = "WHERE UPPER(TRIM(s.sales_person)) = UPPER(TRIM(?))"
                    params.append(ae_name)
                cursor = conn.execute(
                    f"""
                    SELECT
                        COALESCE(c.normalized_name, s.bill_code, 'Unknown') AS customer_name,
                        COALESCE(NULLIF(TRIM(s.contract), ''), '—') AS contract,
                        ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total,
                        MIN(s.load_date) AS added_date,
                        c.customer_id
                    FROM spots s
                    LEFT JOIN customers c ON s.customer_id = c.customer_id
                    {entity_filter}
                        AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
                        AND s.load_date IS NOT NULL
                        AND datetime(s.load_date) >= datetime('now', '-30 days')
                    GROUP BY customer_name, contract, c.customer_id
                    ORDER BY customer_name, total DESC
                    """,
                    params,
                )
                client_map = {}
                for row in cursor.fetchall():
                    customer_name = row[0]
                    contract = row[1] or "—"
                    total = float(row[2])
                    added_date = row[3]
                    customer_id = row[4]
                    if customer_name not in client_map:
                        client_map[customer_name] = {
                            "total": 0.0,
                            "contracts": [],
                            "customer_id": customer_id,
                        }
                    client_map[customer_name]["total"] += total
                    client_map[customer_name]["contracts"].append({
                        "contract": contract,
                        "total": total,
                        "added_date": added_date,
                    })
                contracts_by_client = [
                    {
                        "client": client,
                        "total": data["total"],
                        "contracts": data["contracts"],
                        "customer_id": data["customer_id"],
                    }
                    for client, data in sorted(
                        client_map.items(),
                        key=lambda x: x[1]["total"],
                        reverse=True,
                    )
                ]
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not get contracts last 30 days: {e}")

        # Highlight reel: contracts added in the last 7 days from placement confirmation files
        contracts_highlight = []
        try:
            # Pass None for ae_name when showing all revenue
            highlight_ae_name = None if show_all_revenue else ae_name
            contracts_highlight = contracts_highlight_7_days(ae_name=highlight_ae_name)
            contracts_highlight.sort(key=lambda x: x.get("added_date") or "", reverse=True)
        except Exception as e:
            logger.warning(f"Could not get contracts highlight from placement files: {e}")

        # Pending insertion orders
        pending_orders = []
        pending_orders_count = 0
        pending_orders_scanned_at = None
        try:
            pending_svc = safe_get_service(container, "pending_order_service")
            if pending_svc:
                po_ae = None if show_all_revenue else ae_name
                po_data = pending_svc.get_pending_orders(ae_name=po_ae)
                pending_orders = po_data.get("orders", [])
                pending_orders_count = po_data.get("order_count", 0)
                pending_orders_scanned_at = po_data.get("scanned_at")
        except Exception as e:
            logger.warning(f"Could not load pending insertion orders: {e}")

        # Check if this is an AJAX request for account revenue data only
        if (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            and request.args.get("ajax") == "account_revenue"
        ):
            return jsonify(
                {
                    "account_revenue_sectors": account_revenue_sectors,
                    "total_account_revenue": float(total_account_revenue),
                    "total_account_count": total_account_count,
                    "totals": {"budget": float(total_budget)},
                    "account_start_date": account_start_date,
                    "account_end_date": account_end_date,
                }
            )

        return render_template(
            "ae-dashboard-personal.html",
            **{
                "title": "My Dashboard"
                if not is_admin_or_management
                else "AE Dashboard",
                "ae_name": "All AEs" if show_all_revenue else ae_name,
                "year": current_year,
                "monthly_data": monthly_data,
                "is_admin_or_management": is_admin_or_management,
                "ae_list": ae_list,
                "selected_ae": selected_ae
                if (is_admin_or_management and selected_ae)
                else "",
                "totals": {
                    "booked": float(total_booked),
                    "forecast": float(total_forecast),
                    "budget": float(total_budget),
                    "forecast_vs_budget_pct": calc_pct(total_forecast, total_budget),
                    "booked_vs_budget_pct": calc_pct(total_booked, total_budget),
                    "booked_vs_forecast_pct": calc_pct(total_booked, total_forecast),
                },
                "account_revenue_sectors": account_revenue_sectors,
                "total_account_revenue": float(total_account_revenue),
                "total_account_count": total_account_count,
                "available_years": available_years,
                "account_start_date": account_start_date,
                "account_end_date": account_end_date,
                "contracts_highlight": contracts_highlight,
                "today": date.today().isoformat(),
                "pending_orders": pending_orders,
                "pending_orders_count": pending_orders_count,
                "pending_orders_scanned_at": pending_orders_scanned_at,
            },
        )

    except Exception as e:
        error_trace = traceback.format_exc()
        logger.error(f"Error generating AE personal dashboard: {e}\n{error_trace}")
        try:
            return render_template(
                "error_500.html", message=f"Error generating dashboard: {str(e)}"
            ), 500
        except Exception:
            from flask import current_app

            debug_mode = current_app.config.get("DEBUG", False)
            return jsonify(
                {
                    "error": "Internal server error",
                    "message": str(e),
                    "traceback": error_trace if debug_mode else None,
                }
            ), 500


@reports_bp.route("/contracts-added")
@handle_request_errors
@log_requests
def contracts_added_page():
    """Full list of contracts added in the last 15 days, by client with expandable contract rows.
    For AEs: only their contracts. For admin/management: dropdown to pick AE; WorldLink shown separately.
    """
    if not current_user.is_authenticated:
        from flask import redirect, url_for
        return redirect(url_for("user_management.login"))

    try:
        container = get_container()
        is_admin_or_management = current_user.role.value in ["admin", "management"]
        selected_ae = request.args.get("ae", "").strip()
        ae_name = (
            selected_ae
            if (is_admin_or_management and selected_ae)
            else (current_user.full_name if not is_admin_or_management else None)
        )
        if not is_admin_or_management and (not ae_name or not ae_name.strip()):
            raise ValueError("Cannot determine AE name")

        # AE list for dropdown (admin/management): revenue entities + WorldLink as separate option
        ae_list = []
        if is_admin_or_management:
            try:
                planning_service = safe_get_service(container, "planning_service")
                if planning_service:
                    entities = planning_service.get_revenue_entities()
                    ae_list = [e.entity_name for e in entities]
                if "WorldLink" not in ae_list:
                    ae_list.append("WorldLink")
                ae_list = sorted(ae_list, key=lambda x: (1 if x == "WorldLink" else 0, x))
                # Add "All" option at the beginning
                ae_list.insert(0, "All")
            except Exception as e:
                logger.warning(f"Could not get AE list for contracts-added: {e}")
                if "WorldLink" not in ae_list:
                    ae_list = ["WorldLink"]
                ae_list.insert(0, "All")
        
        # Check if showing "All" revenue
        show_all_revenue = (is_admin_or_management and selected_ae == "All")

        contracts_by_client = []
        if show_all_revenue:
            # Show all contracts across all AEs
            try:
                contracts_by_client = contracts_by_client_15_days(ae_name=None)
            except Exception as e:
                logger.warning(f"Could not get all contracts from placement confirmation files: {e}")
        elif ae_name and ae_name.strip():
            try:
                contracts_by_client = contracts_by_client_15_days(ae_name=ae_name)
            except Exception as e:
                logger.warning(f"Could not get contracts from placement confirmation files: {e}")

        sort = request.args.get("sort", "total_desc").strip() or "total_desc"
        if sort not in ("total_asc", "total_desc"):
            sort = "total_desc"
        contract_sort = request.args.get("contract_sort", "amount_desc").strip() or "amount_desc"
        if contract_sort not in ("amount_asc", "amount_desc"):
            contract_sort = "amount_desc"
        client_reverse = sort == "total_desc"
        contract_reverse = contract_sort == "amount_desc"
        if contracts_by_client:
            contracts_by_client = sorted(
                contracts_by_client,
                key=lambda c: c["total"],
                reverse=client_reverse,
            )
            for c in contracts_by_client:
                c["contracts"] = sorted(
                    c["contracts"],
                    key=lambda x: x["total"],
                    reverse=contract_reverse,
                )

        total_contracts_15d = sum(c["total"] for c in contracts_by_client) if contracts_by_client else 0
        
        # Check if this is an AJAX request for contracts data only
        if (
            request.headers.get("X-Requested-With") == "XMLHttpRequest"
            and request.args.get("ajax") == "contracts_table"
        ):
            return jsonify(
                {
                    "contracts_by_client": contracts_by_client,
                    "total_contracts_15d": total_contracts_15d,
                    "sort": sort,
                    "contract_sort": contract_sort,
                }
            )
        
        return render_template(
            "contracts_added.html",
            ae_name="All AEs" if show_all_revenue else (ae_name or ""),
            contracts_by_client=contracts_by_client,
            total_contracts_15d=total_contracts_15d,
            is_admin_or_management=is_admin_or_management,
            selected_ae=selected_ae,
            ae_list=ae_list,
            sort=sort,
            contract_sort=contract_sort,
        )
    except Exception as e:
        logger.error(f"Error loading contracts added page: {e}", exc_info=True)
        return render_template("error_500.html", message=str(e)), 500


# ============================================================================
# Management Performance Report
# ============================================================================


@reports_bp.route("/management-performance")
@reports_bp.route("/management-performance/<int:year>")
def management_performance(year: int = None):
    """
    Management Performance Report - Company and entity quarterly performance.
    """
    from datetime import date
    from src.services.container import get_container

    container = get_container()
    service = container.get("management_performance_service")

    if year is None:
        year = date.today().year

    # Get pacing mode from query param (default to budget)
    pacing_mode = request.args.get("pacing", "budget")
    if pacing_mode not in ("budget", "forecast"):
        pacing_mode = "budget"

    report_data = service.get_management_report(year, pacing_mode)

    return render_template("management-performance.html", report=report_data)


@reports_bp.route("/management-performance/csv/<int:year>")
def management_performance_csv(year: int):
    """Export management performance data as CSV."""
    import csv
    import io
    from src.services.container import get_container

    container = get_container()
    service = container.get("management_performance_service")

    report_data = service.get_management_report(year)

    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(
        ["Entity", "Quarter", "Booked", "Budget", "Pacing", "Budget %", "YoY %"]
    )

    # Company totals
    for q in report_data.company.quarterly:
        yoy = f"{q.yoy_change_pct:.1f}%" if q.yoy_change_pct is not None else "New"
        writer.writerow(
            [
                "COMPANY TOTAL",
                q.quarter_label,
                float(q.booked),
                float(q.budget),
                float(q.pacing),
                f"{q.budget_pacing_pct:.1f}%",
                yoy,
            ]
        )

    # Entity data
    for entity in report_data.entities:
        for q in entity.quarterly:
            yoy = f"{q.yoy_change_pct:.1f}%" if q.yoy_change_pct is not None else "New"
            writer.writerow(
                [
                    entity.entity_name,
                    q.quarter_label,
                    float(q.booked),
                    float(q.budget),
                    float(q.pacing),
                    f"{q.budget_pacing_pct:.1f}%",
                    yoy,
                ]
            )

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=management_performance_{year}.csv"
        },
    )

@reports_bp.route('/planning/api/booked-detail')
def api_booked_detail():
    """Get detailed breakdown of booked revenue by customer."""
    entity = request.args.get("entity")
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)
    limit = request.args.get("limit", default=50, type=int)

    # Validate required params
    if not entity:
        return jsonify(
            {"success": False, "error": "Missing required parameter: entity"}
        ), 400
    if not year:
        return jsonify(
            {"success": False, "error": "Missing required parameter: year"}
        ), 400
    if month is None or month < 0 or month > 12:
        return jsonify(
            {"success": False, "error": "Invalid month parameter (0-12)"}
        ), 400

    try:
        # Get service from container (same pattern as your other routes)
        container = get_container()
        service = container.get("planning_service")

        if month == 0:
            result = service.get_booked_detail_annual(entity, year, limit)
        else:
            result = service.get_booked_detail(entity, year, month, limit)

        if result is None:
            return jsonify(
                {"success": False, "error": f"Entity not found: {entity}"}
            ), 404

        return jsonify({"success": True, "data": result})

    except Exception as e:
        logger.error(f"Error fetching booked detail: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@reports_bp.route("/monthly/revenue-summary")
@log_requests
@handle_request_errors
def monthly_revenue_summary():
    """Monthly Revenue Summary Report."""
    try:
        container = get_container()
        db = container.get("database_connection")
        
        # Get filter parameters
        from_month = request.args.get("from_month", "")
        from_year = request.args.get("from_year", "")
        to_month = request.args.get("to_month", "")
        to_year = request.args.get("to_year", "")

        # Default month to Jan/Dec when year selected but month is "All"
        if from_year and not from_month:
            from_month = "01"
        if to_year and not to_month:
            to_month = "12"

        logger.warning(f"FILTER PARAMS: from={from_month}/{from_year}, to={to_month}/{to_year}")
        
        with db.connection() as conn:
            cursor = conn.cursor()
            
            # Build date filter conditions using numeric YYMM comparison
            date_conditions = []
            params = []

            if from_year and from_month:
                date_conditions.append("""
                    (CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER) * 100 + 
                    CASE SUBSTR(broadcast_month, 1, 3)
                        WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                        WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                        WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                        WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                    END) >= ?
                """)
                yy = int(from_year) - 2000
                mm = int(from_month)
                params.append(yy * 100 + mm)

            if to_year and to_month:
                date_conditions.append("""
                    (CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER) * 100 + 
                    CASE SUBSTR(broadcast_month, 1, 3)
                        WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                        WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                        WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                        WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                    END) <= ?
                """)
                yy = int(to_year) - 2000
                mm = int(to_month)
                params.append(yy * 100 + mm)

            # Get quarter filter
            quarter_filter = request.args.get("quarter", "").strip()
            
            # Add quarter filter if specified
            if quarter_filter:
                quarter_months_sql = {
                    'Q1': "('Jan','Feb','Mar')",
                    'Q2': "('Apr','May','Jun')",
                    'Q3': "('Jul','Aug','Sep')",
                    'Q4': "('Oct','Nov','Dec')"
                }
                if quarter_filter in quarter_months_sql:
                    date_conditions.append(f"SUBSTR(broadcast_month, 1, 3) IN {quarter_months_sql[quarter_filter]}")

            where_clause = " AND ".join(date_conditions) if date_conditions else "1=1"

            logger.warning(f"WHERE CLAUSE: {where_clause}")
            logger.warning(f"PARAMS: {params}")
            
            # Get available years
            cursor.execute("""
                SELECT DISTINCT 2000 + CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER) as year
                FROM spots
                WHERE broadcast_month IS NOT NULL
                ORDER BY year DESC
            """)
            available_years = [row[0] for row in cursor.fetchall()]
            
            # Get monthly data
            cursor.execute(f"""
                SELECT 
                    broadcast_month,
                    COUNT(*) as spot_count,
                    SUM(gross_rate) as total_revenue,
                    AVG(gross_rate) as avg_rate,
                    MIN(gross_rate) as min_rate,
                    MAX(gross_rate) as max_rate
                FROM spots
                WHERE revenue_type = 'Internal Ad Sales'
                  AND gross_rate > 0
                  AND {where_clause}
                GROUP BY broadcast_month
                ORDER BY 
                    CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER),
                    CASE SUBSTR(broadcast_month, 1, 3)
                        WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                        WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                        WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                        WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                    END
            """, params)
            
            # Get closed months
            cursor.execute("SELECT broadcast_month FROM month_closures")
            closed_months = {row[0] for row in cursor.fetchall()}
            
            monthly_rows = cursor.execute(f"""
                SELECT 
                    broadcast_month,
                    COUNT(*) as spot_count,
                    SUM(gross_rate) as total_revenue,
                    AVG(gross_rate) as avg_rate,
                    MIN(gross_rate) as min_rate,
                    MAX(gross_rate) as max_rate
                FROM spots
                WHERE revenue_type = 'Internal Ad Sales'
                  AND gross_rate > 0
                  AND {where_clause}
                GROUP BY broadcast_month
                ORDER BY 
                    CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER),
                    CASE SUBSTR(broadcast_month, 1, 3)
                        WHEN 'Jan' THEN 1 WHEN 'Feb' THEN 2 WHEN 'Mar' THEN 3
                        WHEN 'Apr' THEN 4 WHEN 'May' THEN 5 WHEN 'Jun' THEN 6
                        WHEN 'Jul' THEN 7 WHEN 'Aug' THEN 8 WHEN 'Sep' THEN 9
                        WHEN 'Oct' THEN 10 WHEN 'Nov' THEN 11 WHEN 'Dec' THEN 12
                    END
            """, params).fetchall()
            
            monthly_data = []
            total_spots = 0
            total_revenue = 0
            
            for row in monthly_rows:
                bm, spots, rev, avg_r, min_r, max_r = row
                total_spots += spots or 0
                total_revenue += rev or 0
                monthly_data.append({
                    "formatted_month": bm,
                    "spot_count": spots,
                    "total_revenue": rev,
                    "avg_rate": avg_r,
                    "min_rate": min_r,
                    "max_rate": max_r,
                    "status": "CLOSED" if bm in closed_months else "OPEN"
                })
            
            # Add total row
            if monthly_data:
                monthly_data.append({
                    "formatted_month": "*** TOTAL ***",
                    "spot_count": total_spots,
                    "total_revenue": total_revenue,
                    "avg_rate": total_revenue / total_spots if total_spots else 0,
                    "min_rate": None,
                    "max_rate": None,
                    "status": ""
                })
            
            # Get quarterly data
            quarterly_rows = cursor.execute(f"""
                SELECT 
                    CASE 
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jan','Feb','Mar') THEN 'Q1'
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Apr','May','Jun') THEN 'Q2'
                        WHEN SUBSTR(broadcast_month, 1, 3) IN ('Jul','Aug','Sep') THEN 'Q3'
                        ELSE 'Q4'
                    END as quarter,
                    2000 + CAST(SUBSTR(broadcast_month, 5, 2) AS INTEGER) as year,
                    COUNT(*) as spot_count,
                    SUM(gross_rate) as total_revenue,
                    AVG(gross_rate) as avg_rate
                FROM spots
                WHERE revenue_type = 'Internal Ad Sales'
                AND gross_rate > 0
                AND {where_clause}
                GROUP BY quarter, year
                ORDER BY year, quarter
            """, params).fetchall()

            # Define which months belong to each quarter
            quarter_months = {
                'Q1': ['Jan', 'Feb', 'Mar'],
                'Q2': ['Apr', 'May', 'Jun'],
                'Q3': ['Jul', 'Aug', 'Sep'],
                'Q4': ['Oct', 'Nov', 'Dec']
            }

            quarterly_data = []
            for r in quarterly_rows:
                quarter, year, spot_count, total_revenue, avg_rate = r
                yy = str(year)[-2:]  # e.g., "25" from 2025
                
                # Check if all months in this quarter are closed
                months_in_quarter = [f"{m}-{yy}" for m in quarter_months[quarter]]
                all_closed = all(m in closed_months for m in months_in_quarter)
                
                quarterly_data.append({
                    "quarter": quarter,
                    "year": year,
                    "spot_count": spot_count,
                    "total_revenue": total_revenue,
                    "avg_rate": avg_rate,
                    "status": "CLOSED" if all_closed else "OPEN"
                })
            
            # Client summary
            client_row = cursor.execute(f"""
                SELECT 
                    COUNT(DISTINCT customer_id) as total_clients,
                    AVG(client_revenue) as avg_revenue,
                    MAX(client_revenue) as max_revenue
                FROM (
                    SELECT customer_id, SUM(gross_rate) as client_revenue
                    FROM spots
                    WHERE revenue_type = 'Internal Ad Sales'
                      AND gross_rate > 0
                      AND customer_id IS NOT NULL
                      AND {where_clause}
                    GROUP BY customer_id
                )
            """, params).fetchone()
            
            client_summary = {
                "total_clients": client_row[0] or 0,
                "avg_revenue_per_client": client_row[1] or 0,
                "max_client_revenue": client_row[2] or 0
            }
            
            # Client monthly data
            client_monthly_rows = cursor.execute(f"""
                SELECT 
                    broadcast_month,
                    COUNT(DISTINCT customer_id) as client_count,
                    SUM(gross_rate) as total_revenue,
                    AVG(gross_rate) as avg_spot_rate
                FROM spots
                WHERE revenue_type = 'Internal Ad Sales'
                  AND gross_rate > 0
                  AND customer_id IS NOT NULL
                  AND {where_clause}
                GROUP BY broadcast_month
                ORDER BY broadcast_month
            """, params).fetchall()
            
            client_monthly_data = []
            for row in client_monthly_rows:
                client_monthly_data.append({
                    "formatted_month": row[0],
                    "client_count": row[1],
                    "total_revenue": row[2],
                    "avg_revenue_per_client": (row[2] / row[1]) if row[1] else 0,
                    "avg_spot_rate": row[3]
                })
            
            # Top clients
            top_client_rows = cursor.execute(f"""
                SELECT 
                    COALESCE(c.normalized_name, s.bill_code) as client_name,
                    SUM(s.gross_rate) as total_revenue,
                    COUNT(*) as spot_count,
                    AVG(s.gross_rate) as avg_rate
                FROM spots s
                LEFT JOIN customers c ON s.customer_id = c.customer_id
                WHERE s.revenue_type = 'Internal Ad Sales'
                  AND s.gross_rate > 0
                  AND {where_clause.replace('broadcast_month', 's.broadcast_month')}
                GROUP BY COALESCE(c.normalized_name, s.bill_code)
                ORDER BY total_revenue DESC
                LIMIT 20
            """, params).fetchall()
            
            top_clients = [
                {"client_name": r[0], "total_revenue": r[1], "spot_count": r[2], "avg_rate": r[3]}
                for r in top_client_rows
            ]
        
        data = {
            "available_years": available_years,
            "monthly_data": monthly_data,
            "quarterly_data": quarterly_data,
            "client_summary": client_summary,
            "client_monthly_data": client_monthly_data,
            "top_clients": top_clients
        }
        
        return render_template(
            "monthly_revenue_summary.html", 
            title="Monthly Inhouse Ad Sales Revenue Summary", 
            data=data
)
        
    except Exception as e:
        logger.error(f"Error generating monthly revenue summary: {e}", exc_info=True)
        raise


@reports_bp.route("/report2")
@log_requests
@handle_request_errors
def expectation_tracking_report():
    """Management Expectation Tracking Report (report2.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get quarterly and AE performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)

        expectation_data = {
            "current_year": quarterly_data.current_year,
            "quarterly_data": [q.to_dict() for q in quarterly_data.quarterly_data],
            "ae_performance": [ae.to_dict() for ae in ae_data.ae_performance],
        }

        context = prepare_template_context(
            expectation_data, {"title": "Management Expectation Tracking"}
        )

        return render_template("report2.html", **context)

    except Exception as e:
        logger.error(f"Error generating report2: {e}")
        return render_template("error_500.html", message="Error generating report"), 500


@reports_bp.route("/report3")
@log_requests
@handle_request_errors
def performance_story_report():
    """Quarterly Performance Story Report (report3.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")

        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get performance data
        quarterly_data = report_service.get_quarterly_performance_data(filters)
        ae_data = report_service.get_ae_performance_report_data(filters)

        performance_data = {
            **quarterly_data.to_dict(),
            "ae_performance": [ae.to_dict() for ae in ae_data.ae_performance],
        }

        context = prepare_template_context(
            performance_data, {"title": "Quarterly Performance Story"}
        )

        return render_template("report3.html", **context)

    except Exception as e:
        logger.error(f"Error generating report3: {e}")
        return render_template("error_500.html", message="Error generating report"), 500


@reports_bp.route("/sector-analysis")
@log_requests
@handle_request_errors
def sector_analysis_report():
    """Sector analysis with market segmentation (sector-analysis.html)."""
    try:
        container = get_container()
        report_service = safe_get_service(container, "report_data_service")
        year = get_year_parameter(default_year=date.today().year)
        filters = ReportFilters(year=year)

        # Get sector data
        sector_data = report_service.get_sector_performance_data(filters)

        # Get available years for dropdown
        available_years = report_service.repository.get_available_years()

        return render_template(
            "sector-analysis.html",
            title="Sector Analysis",
            data=sector_data,
            selected_year=year,
            available_years=available_years,
        )

    except Exception as e:
        logger.error(f"Error generating sector-analysis: {e}", exc_info=True)
        return render_template(
            "error_500.html", message=f"Error generating report: {str(e)}"
        ), 500


@reports_bp.route("/market-analysis")
@log_requests
@handle_request_errors
def market_analysis_report():
    """Market Analysis Report - Language performance by market."""
    try:
        container = get_container()
        service = safe_get_service(container, "market_analysis_service")

        year = request.args.get("year", str(date.today().year))

        logger.info(f"Generating market analysis report for year {year}")
        data = service.get_market_analysis_data(year)

        return render_template(
            "market-analysis.html",
            title="Market Analysis",
            data=data.to_dict(),
        )

    except Exception as e:
        logger.error(f"Error generating market analysis: {e}", exc_info=True)
        return render_template(
            "error_500.html",
            message=f"Error generating market analysis: {str(e)}",
        ), 500


@reports_bp.route("/market-analysis/export/<report_type>")
@log_requests
@handle_request_errors
def market_analysis_export(report_type: str):
    """Export market analysis data to CSV."""
    try:
        container = get_container()
        service = safe_get_service(container, "market_analysis_service")

        year = request.args.get("year", str(date.today().year))

        logger.info(f"Exporting market analysis {report_type} for year {year}")

        data = service.get_csv_data(year, report_type)

        if not data:
            return "No data available", 404

        # Create CSV
        output = io.StringIO()
        if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        response = Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=market_analysis_{report_type}_{year}.csv"
            },
        )
        return response

    except Exception as e:
        logger.error(f"Error exporting market analysis: {e}", exc_info=True)
        return f"Export error: {str(e)}", 500


@reports_bp.route("/language-blocks")
@log_requests
@handle_request_errors
def language_blocks_report():
    """Language Block Performance Report with Nordic design."""
    try:
        template_data = {
            "title": "Language Block Performance Report",
            "description": "Comprehensive analysis of language-specific advertising blocks",
        }

        logger.info("Rendering language blocks report template")
        return render_template("language_blocks_report.html", **template_data)

    except Exception as e:
        logger.error(f"Error rendering language blocks report: {e}")
        return render_template(
            "error_500.html", message="Error loading language blocks report"
        ), 500


# Error handlers for this blueprint
@reports_bp.errorhandler(404)
def not_found_error(error):
    """Handle 404 errors in reports blueprint."""
    return render_template("error_404.html"), 404


@reports_bp.errorhandler(500)
def internal_error(error):
    """Handle 500 errors in reports blueprint."""
    logger.error(f"Internal error in reports blueprint: {error}")
    return render_template("error_500.html"), 500
