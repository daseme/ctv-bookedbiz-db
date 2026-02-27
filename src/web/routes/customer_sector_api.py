# REPLACE YOUR ENTIRE src/web/routes/customer_sector_api.py file with this:

from flask import Blueprint, request, jsonify
from src.services.container import get_container
import logging
from src.utils.query_builders import CustomerNormalizationQueryBuilder

customer_sector_bp = Blueprint(
    "customer_sector_api", __name__, url_prefix="/api/customer-sector"
)
logger = logging.getLogger(__name__)


@customer_sector_bp.before_request
def _require_admin_for_writes():
    if request.method in ('POST', 'PUT', 'DELETE'):
        from flask_login import current_user
        if not hasattr(current_user, 'role') or current_user.role.value != 'admin':
            return jsonify({"error": "Admin access required"}), 403


@customer_sector_bp.route("/customers", methods=["GET"])
def get_customers():
    """Get customers - FILTER WORLDLINK CLIENTS VERSION"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Get all active customers, excluding WorldLink-related names
        customer_query = """
        SELECT 
            c.customer_id,
            c.normalized_name,
            COALESCE(s.sector_name, 'Unassigned') AS sector,
            c.updated_date
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE c.is_active = 1
          -- Filter out WorldLink clients by name patterns
          AND c.normalized_name NOT LIKE '%WorldLink%'
          AND c.normalized_name NOT LIKE '%Worldlink%' 
          AND c.normalized_name NOT LIKE 'Direct Donor%'
          AND c.normalized_name NOT LIKE 'Marketing Architects%'
          AND c.normalized_name NOT LIKE '%FinanceBuzz%'
          AND c.normalized_name NOT LIKE '%Marketing Arch%'
        ORDER BY c.normalized_name
        """

        cursor.execute(customer_query)
        customer_rows = cursor.fetchall()

        # Get revenue data for all customers in one efficient query
        revenue_query = """
        SELECT 
            audit.customer_id,
            ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total_revenue,
            COUNT(s.spot_id) AS spot_count
        FROM v_customer_normalization_audit audit
        LEFT JOIN spots s ON audit.raw_text = s.bill_code
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.revenue_type = 'Internal Ad Sales'
          AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
          AND s.gross_rate > 0
          AND audit.customer_id IN ({})
        GROUP BY audit.customer_id
        """.format(",".join(["?"] * len(customer_rows)))

        customer_ids = [row[0] for row in customer_rows]
        if customer_ids:  # Only run revenue query if we have customers
            # cursor.execute(revenue_query, customer_ids)  # TEMP: Disabled for performance
            revenue_rows = []  # TEMP: Return empty revenue data
        else:
            revenue_rows = []

        # Create revenue lookup dict
        revenue_lookup = {}
        for rev_row in revenue_rows:
            revenue_lookup[rev_row[0]] = {
                "total_revenue": float(rev_row[1]) if rev_row[1] else 0.0,
                "spot_count": rev_row[2] if rev_row[2] else 0,
            }

        # Build final customer list - include all customers (even $0 revenue)
        customers = []
        for row in customer_rows:
            customer_id = row[0]
            revenue_data = revenue_lookup.get(
                customer_id, {"total_revenue": 0.0, "spot_count": 0}
            )

            customers.append(
                {
                    "id": customer_id,
                    "name": row[1],
                    "sector": row[2] if row[2] != "Unassigned" else None,
                    "lastUpdated": str(row[3])[:10] if row[3] else "2025-01-01",
                    "totalRevenue": revenue_data["total_revenue"],
                    "spotCount": revenue_data["spot_count"],
                    "resolutionStatus": "resolved",
                    "isUnresolved": False,
                }
            )

        conn.close()

        logger.info(f"Fetched {len(customers)} customers (filtered WorldLink clients)")
        return jsonify({"success": True, "data": customers})

    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        import traceback

        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


# ADD this endpoint to debug California Black Media's revenue issue


@customer_sector_bp.route("/debug/revenue-issue/<customer_name>", methods=["GET"])
def debug_revenue_issue(customer_name):
    """Debug why a customer isn't getting revenue calculated"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Step 1: Find the customer record
        cursor.execute(
            "SELECT customer_id, normalized_name FROM customers WHERE normalized_name LIKE ?",
            (f"%{customer_name}%",),
        )
        customer_record = cursor.fetchone()
        customer_data = list(customer_record) if customer_record else None

        if not customer_record:
            return jsonify({"error": f"Customer '{customer_name}' not found"})

        customer_id = customer_record[0]

        # Step 2: Check spots with matching bill_code
        cursor.execute(
            """
            SELECT 
                bill_code,
                COUNT(*) as spot_count,
                SUM(gross_rate) as total_revenue,
                revenue_type,
                MIN(air_date) as earliest_date,
                MAX(air_date) as latest_date
            FROM spots 
            WHERE bill_code LIKE ?
              AND revenue_type = 'Internal Ad Sales'
              AND gross_rate > 0
            GROUP BY bill_code, revenue_type
            ORDER BY total_revenue DESC
            """,
            (f"%{customer_name}%",),
        )
        spots_data = [list(row) for row in cursor.fetchall()]

        # Step 3: Check normalization audit entries
        cursor.execute(
            """
            SELECT 
                raw_text,
                normalized_name,
                customer_id as audit_customer_id,
                exists_in_customers
            FROM v_customer_normalization_audit 
            WHERE normalized_name LIKE ? OR raw_text LIKE ?
            """,
            (f"%{customer_name}%", f"%{customer_name}%"),
        )
        audit_entries = [list(row) for row in cursor.fetchall()]

        # Step 4: Check if bill_codes match between spots and audit
        if spots_data:
            bill_codes = [spot[0] for spot in spots_data]
            placeholders = ",".join(["?"] * len(bill_codes))

            cursor.execute(
                f"""
                SELECT 
                    raw_text,
                    normalized_name,
                    customer_id,
                    exists_in_customers
                FROM v_customer_normalization_audit 
                WHERE raw_text IN ({placeholders})
                """,
                bill_codes,
            )
            matching_audit_entries = [list(row) for row in cursor.fetchall()]
        else:
            matching_audit_entries = []

        # Step 5: Test the exact revenue query being used
        cursor.execute(
            """
            SELECT 
                audit.customer_id,
                audit.raw_text,
                audit.normalized_name,
                s.bill_code,
                s.gross_rate,
                s.revenue_type,
                a.agency_name
            FROM v_customer_normalization_audit audit
            LEFT JOIN spots s ON audit.raw_text = s.bill_code
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.revenue_type = 'Internal Ad Sales'
              AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
              AND s.gross_rate > 0
              AND audit.customer_id = ?
            LIMIT 10
            """,
            (customer_id,),
        )
        revenue_query_results = [list(row) for row in cursor.fetchall()]

        # Step 6: Check if customer_id is properly set in audit
        cursor.execute(
            """
            SELECT COUNT(*) as total_entries,
                   COUNT(customer_id) as entries_with_customer_id,
                   COUNT(CASE WHEN customer_id = ? THEN 1 END) as entries_for_this_customer
            FROM v_customer_normalization_audit 
            WHERE raw_text LIKE ?
            """,
            (customer_id, f"%{customer_name}%"),
        )
        audit_stats = list(cursor.fetchone())

        conn.close()

        return jsonify(
            {
                "success": True,
                "customer_searched": customer_name,
                "diagnostics": {
                    "customer_record": customer_data,
                    "spots_with_revenue": spots_data,
                    "normalization_audit_entries": audit_entries,
                    "matching_audit_entries": matching_audit_entries,
                    "revenue_query_results": revenue_query_results,
                    "audit_statistics": {
                        "total_entries": audit_stats[0],
                        "entries_with_customer_id": audit_stats[1],
                        "entries_for_this_customer": audit_stats[2],
                    },
                    "analysis": {
                        "customer_exists": customer_data is not None,
                        "has_spots_with_revenue": len(spots_data) > 0,
                        "has_audit_entries": len(audit_entries) > 0,
                        "audit_properly_linked": len(matching_audit_entries) > 0,
                        "revenue_query_returns_data": len(revenue_query_results) > 0,
                    },
                },
            }
        )

    except Exception as e:
        logger.error(f"Error debugging revenue issue: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


# ADD this endpoint to your customer_sector_api.py file (in the DEBUG ENDPOINTS section)


@customer_sector_bp.route("/debug/normalization-view/<customer_name>", methods=["GET"])
def debug_normalization_view(customer_name):
    """Debug why a customer doesn't appear in v_customer_normalization_audit"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Step 1: Check what feeds the normalization view
        # Based on your schema, it uses raw_customer_inputs and spots data

        # Check raw_customer_inputs
        cursor.execute(
            "SELECT raw_text, created_at FROM raw_customer_inputs WHERE raw_text LIKE ?",
            (f"%{customer_name}%",),
        )
        raw_inputs = [list(row) for row in cursor.fetchall()]

        # Check customer_canonical_map
        cursor.execute(
            "SELECT alias_name, canonical_name, updated_date FROM customer_canonical_map WHERE alias_name LIKE ? OR canonical_name LIKE ?",
            (f"%{customer_name}%", f"%{customer_name}%"),
        )
        canonical_map = [list(row) for row in cursor.fetchall()]

        # Check spots with this bill_code
        cursor.execute(
            """
            SELECT DISTINCT bill_code, revenue_type, COUNT(*) as count, SUM(gross_rate) as revenue
            FROM spots 
            WHERE bill_code LIKE ?
            GROUP BY bill_code, revenue_type
            ORDER BY revenue DESC
            """,
            (f"%{customer_name}%",),
        )
        spots_data = [list(row) for row in cursor.fetchall()]

        # Check text_strips (things that get cleaned from names)
        cursor.execute("SELECT needle FROM text_strips")
        text_strips = [row[0] for row in cursor.fetchall()]

        # Test the v_raw_clean view logic manually
        test_name = customer_name
        for needle in text_strips:
            if needle in test_name:
                test_name = test_name.replace(needle, "")
        cleaned_name = " ".join(test_name.split())  # Collapse spaces

        # Check if the cleaned name would match anything
        cursor.execute(
            "SELECT * FROM v_raw_clean WHERE raw_text LIKE ?", (f"%{customer_name}%",)
        )
        raw_clean_entries = [list(row) for row in cursor.fetchall()]

        # Check v_normalized_candidates
        cursor.execute(
            "SELECT * FROM v_normalized_candidates WHERE raw_text LIKE ? OR customer LIKE ?",
            (f"%{customer_name}%", f"%{customer_name}%"),
        )
        normalized_candidates = [list(row) for row in cursor.fetchall()]

        # The key insight: Check what SHOULD trigger the view to include this customer
        # The view likely starts from raw_customer_inputs or spots
        cursor.execute(
            """
            SELECT DISTINCT bill_code 
            FROM spots 
            WHERE bill_code LIKE ?
              AND revenue_type != 'Trade'
            """,
            (f"%{customer_name}%",),
        )
        qualifying_bill_codes = [row[0] for row in cursor.fetchall()]

        conn.close()

        return jsonify(
            {
                "success": True,
                "customer_searched": customer_name,
                "normalization_system_check": {
                    "raw_customer_inputs": raw_inputs,
                    "customer_canonical_map": canonical_map,
                    "spots_data": spots_data,
                    "text_strips_applied": {
                        "original": customer_name,
                        "cleaned": cleaned_name,
                        "strips_found": [
                            needle for needle in text_strips if needle in customer_name
                        ],
                    },
                    "raw_clean_entries": raw_clean_entries,
                    "normalized_candidates": normalized_candidates,
                    "qualifying_bill_codes": qualifying_bill_codes,
                },
                "analysis": {
                    "in_raw_inputs": len(raw_inputs) > 0,
                    "in_canonical_map": len(canonical_map) > 0,
                    "has_qualifying_spots": len(qualifying_bill_codes) > 0,
                    "in_raw_clean": len(raw_clean_entries) > 0,
                    "in_candidates": len(normalized_candidates) > 0,
                },
                "recommendation": "Customer needs to be added to raw_customer_inputs table to appear in normalization view",
            }
        )

    except Exception as e:
        logger.error(f"Error debugging normalization view: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc}")
        return jsonify({"success": False, "error": str(e)}), 500


# ADD this endpoint to analyze how customers should enter normalization pipeline


@customer_sector_bp.route("/debug/analyze-normalization-gaps", methods=["GET"])
def analyze_normalization_gaps():
    """Analyze gaps in the normalization pipeline to understand missing customers"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # 1. Find all customers that exist but aren't in normalization audit
        cursor.execute(
            """
            SELECT 
                c.customer_id,
                c.normalized_name,
                c.created_date,
                CASE WHEN audit.customer_id IS NOT NULL THEN 1 ELSE 0 END as in_audit
            FROM customers c
            LEFT JOIN v_customer_normalization_audit audit ON c.customer_id = audit.customer_id
            WHERE c.is_active = 1
            ORDER BY c.created_date DESC
            """
        )
        customer_audit_status = cursor.fetchall()

        customers_missing_from_audit = []
        customers_in_audit = []

        for row in customer_audit_status:
            customer_data = {
                "customer_id": row[0],
                "normalized_name": row[1],
                "created_date": row[2],
                "in_audit": bool(row[3]),
            }

            if row[3]:  # in audit
                customers_in_audit.append(customer_data)
            else:  # missing from audit
                customers_missing_from_audit.append(customer_data)

        # 2. For missing customers, check if they have spots with revenue
        missing_with_revenue = []
        for customer in customers_missing_from_audit[:20]:  # Limit to avoid timeout
            cursor.execute(
                """
                SELECT 
                    COUNT(*) as spot_count,
                    SUM(CASE WHEN revenue_type = 'Internal Ad Sales' AND gross_rate > 0 THEN gross_rate ELSE 0 END) as ias_revenue,
                    SUM(CASE WHEN revenue_type != 'Trade' AND gross_rate > 0 THEN gross_rate ELSE 0 END) as total_revenue,
                    GROUP_CONCAT(DISTINCT bill_code) as bill_codes_used
                FROM spots s
                WHERE s.bill_code LIKE ?
                """,
                (f"%{customer['normalized_name']}%",),
            )
            revenue_row = cursor.fetchone()

            if revenue_row and revenue_row[0] > 0:  # Has spots
                customer_with_revenue = customer.copy()
                customer_with_revenue.update(
                    {
                        "spot_count": revenue_row[0],
                        "ias_revenue": float(revenue_row[1]) if revenue_row[1] else 0.0,
                        "total_revenue": float(revenue_row[2])
                        if revenue_row[2]
                        else 0.0,
                        "bill_codes_used": revenue_row[3],
                    }
                )
                missing_with_revenue.append(customer_with_revenue)

        # 3. Check what's in raw_customer_inputs vs what should be
        cursor.execute("SELECT COUNT(*) FROM raw_customer_inputs")
        total_raw_inputs = cursor.fetchone()[0]

        cursor.execute(
            "SELECT raw_text FROM raw_customer_inputs ORDER BY created_at DESC LIMIT 10"
        )
        recent_raw_inputs = [row[0] for row in cursor.fetchall()]

        # 4. Check recent spots data that might be missing from normalization
        cursor.execute(
            """
            SELECT DISTINCT 
                bill_code,
                COUNT(*) as spots,
                SUM(gross_rate) as revenue,
                MIN(air_date) as first_date,
                MAX(air_date) as last_date
            FROM spots 
            WHERE revenue_type = 'Internal Ad Sales' 
              AND gross_rate > 0
              AND air_date >= date('now', '-30 days')  -- Recent spots
            GROUP BY bill_code
            HAVING revenue > 1000  -- Significant revenue
            ORDER BY revenue DESC
            LIMIT 20
            """
        )
        recent_significant_spots = []
        for row in cursor.fetchall():
            # Check if this bill_code is in normalization audit
            cursor.execute(
                "SELECT COUNT(*) FROM v_customer_normalization_audit WHERE raw_text = ?",
                (row[0],),
            )
            in_audit = cursor.fetchone()[0] > 0

            recent_significant_spots.append(
                {
                    "bill_code": row[0],
                    "spots": row[1],
                    "revenue": float(row[2]),
                    "first_date": row[3],
                    "last_date": row[4],
                    "in_normalization_audit": in_audit,
                }
            )

        # 5. Check import batch history to understand data flow
        cursor.execute(
            """
            SELECT 
                import_mode,
                COUNT(*) as batch_count,
                MAX(import_date) as most_recent,
                SUM(records_imported) as total_records
            FROM import_batches 
            WHERE status = 'COMPLETED'
            GROUP BY import_mode
            ORDER BY most_recent DESC
            """
        )
        import_history = [list(row) for row in cursor.fetchall()]

        conn.close()

        return jsonify(
            {
                "success": True,
                "analysis": {
                    "customers_summary": {
                        "total_customers": len(customers_in_audit)
                        + len(customers_missing_from_audit),
                        "in_normalization_audit": len(customers_in_audit),
                        "missing_from_audit": len(customers_missing_from_audit),
                        "missing_with_revenue": len(missing_with_revenue),
                    },
                    "raw_inputs_summary": {
                        "total_raw_inputs": total_raw_inputs,
                        "recent_examples": recent_raw_inputs,
                    },
                    "missing_customers_with_revenue": missing_with_revenue,
                    "recent_spots_not_in_audit": [
                        spot
                        for spot in recent_significant_spots
                        if not spot["in_normalization_audit"]
                    ],
                    "import_batch_history": import_history,
                },
                "recommendations": {
                    "immediate_action": f"Add {len(missing_with_revenue)} high-revenue customers to raw_customer_inputs",
                    "process_fix": "Review data import process to ensure new bill_codes are added to raw_customer_inputs automatically",
                    "monitoring": "Set up alerts when customers exist but aren't in normalization audit",
                },
            }
        )

    except Exception as e:
        logger.error(f"Error analyzing normalization gaps: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/debug/check-agencies/<customer_name>", methods=["GET"])
def check_agency_filtering(customer_name):
    """Check agency connections and filtering for a customer"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Check all agency connections for this customer
        cursor.execute(
            """
            SELECT DISTINCT 
                s.bill_code,
                s.agency_id,
                a.agency_name,
                s.revenue_type,
                COUNT(*) as spot_count,
                SUM(s.gross_rate) as total_revenue,
                -- Show what the filter condition evaluates to
                CASE 
                    WHEN a.agency_name = 'WorldLink' THEN 'FILTERED OUT (WorldLink)'
                    WHEN a.agency_name IS NULL THEN 'NO AGENCY (INCLUDED)'
                    ELSE 'INCLUDED (Not WorldLink)'
                END as filter_status
            FROM spots s
            LEFT JOIN agencies a ON s.agency_id = a.agency_id
            WHERE s.bill_code LIKE ?
              AND s.revenue_type = 'Internal Ad Sales'
              AND s.gross_rate > 0
            GROUP BY s.bill_code, s.agency_id, a.agency_name, s.revenue_type
            ORDER BY total_revenue DESC
            """,
            (f"%{customer_name}%",),
        )
        agency_data = cursor.fetchall()

        agency_connections = []
        for row in agency_data:
            agency_connections.append(
                {
                    "bill_code": row[0],
                    "agency_id": row[1],
                    "agency_name": row[2],
                    "revenue_type": row[3],
                    "spot_count": row[4],
                    "total_revenue": float(row[5]) if row[5] else 0.0,
                    "filter_status": row[6],
                }
            )

        # Check if this customer appears in normalization audit
        cursor.execute(
            """
            SELECT 
                raw_text,
                normalized_name, 
                customer_id,
                exists_in_customers
            FROM v_customer_normalization_audit 
            WHERE raw_text LIKE ? OR normalized_name LIKE ?
            """,
            (f"%{customer_name}%", f"%{customer_name}%"),
        )
        audit_data = [list(row) for row in cursor.fetchall()]

        # Show current WorldLink filtering logic result
        test_revenue_query = """
        SELECT 
            audit.customer_id,
            ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total_revenue,
            COUNT(s.spot_id) AS spot_count,
            GROUP_CONCAT(DISTINCT a.agency_name) as agencies_involved
        FROM v_customer_normalization_audit audit
        LEFT JOIN spots s ON audit.raw_text = s.bill_code
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        WHERE s.revenue_type = 'Internal Ad Sales'
          AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)  -- Current filter
          AND s.gross_rate > 0
          AND (audit.raw_text LIKE ? OR audit.normalized_name LIKE ?)
        GROUP BY audit.customer_id
        """

        cursor.execute(test_revenue_query, (f"%{customer_name}%", f"%{customer_name}%"))
        current_filter_result = cursor.fetchall()

        filter_results = []
        for row in current_filter_result:
            filter_results.append(
                {
                    "customer_id": row[0],
                    "total_revenue": float(row[1]) if row[1] else 0.0,
                    "spot_count": row[2],
                    "agencies_involved": row[3],
                }
            )

        conn.close()

        return jsonify(
            {
                "success": True,
                "customer_searched": customer_name,
                "diagnostics": {
                    "agency_connections": agency_connections,
                    "normalization_audit": audit_data,
                    "current_filter_results": filter_results,
                    "summary": {
                        "has_worldlink_connection": any(
                            conn.get("agency_name") == "WorldLink"
                            for conn in agency_connections
                        ),
                        "should_be_filtered": any(
                            conn.get("agency_name") == "WorldLink"
                            for conn in agency_connections
                        ),
                        "appears_in_current_results": len(filter_results) > 0,
                    },
                },
            }
        )

    except Exception as e:
        logger.error(f"Error checking agency filtering: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/sectors", methods=["GET"])
def get_sectors():
    """Get all sectors"""
    try:
        container = get_container()
        db = container.get("database_connection")

        query = """
        SELECT
            sector_id,
            sector_name as name,
            COALESCE(sector_group, 'Other') as sector_group,
            COALESCE(sector_group, 'Other') as description
        FROM sectors
        WHERE is_active = 1
        ORDER BY CASE sector_group
            WHEN 'Commercial' THEN 1 WHEN 'Financial' THEN 2
            WHEN 'Healthcare' THEN 3 WHEN 'Outreach' THEN 4
            WHEN 'Political' THEN 5 WHEN 'Other' THEN 6 ELSE 7
        END, sector_name
        """

        conn = db.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        sectors = []
        colors = [
            "#88c0d0",
            "#81a1c1",
            "#5e81ac",
            "#bf616a",
            "#d08770",
            "#ebcb8b",
            "#a3be8c",
            "#b48ead",
        ]

        for i, row in enumerate(rows):
            sectors.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "sector_group": row[2],
                    "description": row[3],
                    "color": colors[i % len(colors)],
                }
            )

        conn.close()
        return jsonify({"success": True, "data": sectors})

    except Exception as e:
        logger.error(f"Error fetching sectors: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/customers/<int:customer_id>/sector", methods=["PUT"])
def update_customer_sector(customer_id):
    """Update customer sector assignment - COMPLETE DEBUG VERSION"""
    try:
        data = request.get_json()
        sector_name = data.get("sector")

        logger.info("=== SECTOR UPDATE DEBUG START ===")
        logger.info(f"Customer ID: {customer_id}, Sector: {sector_name}")
        logger.info(f"Request data: {data}")

        container = get_container()
        db = container.get("database_connection")

        # DEBUG: Database connection details
        logger.info(f"Database connection type: {type(db)}")
        if hasattr(db, "database_path"):
            logger.info(f"Database path: {db.database_path}")
        if hasattr(db, "db_path"):
            logger.info(f"DB path: {db.db_path}")

        conn = db.connect()
        cursor = conn.cursor()

        # DEBUG: Which database file are we actually using?
        try:
            cursor.execute("PRAGMA database_list")
            db_info = cursor.fetchall()
            # Convert rows to tuples for logging
            db_info_serializable = [tuple(row) for row in db_info]
            logger.info(f"Database list: {db_info_serializable}")

            for db_entry in db_info:
                if db_entry[1] == "main":  # Main database
                    db_file_path = db_entry[2]
                    logger.info(f"Main database file: {db_file_path}")

                    import os

                    if db_file_path and os.path.exists(db_file_path):
                        stat_info = os.stat(db_file_path)
                        logger.info(f"Database file size: {stat_info.st_size} bytes")
                        logger.info(
                            f"Database file permissions: {oct(stat_info.st_mode)[-3:]}"
                        )
                        logger.info(
                            f"Database file writable: {os.access(db_file_path, os.W_OK)}"
                        )
                    else:
                        logger.error(f"Database file does not exist: {db_file_path}")
        except Exception as db_debug_error:
            logger.error(f"Database debug failed: {db_debug_error}")

        # CRITICAL FIX: Handle unresolved customers (customer_id = 0)
        if customer_id == 0:
            customer_name = data.get("customer_name")
            if not customer_name:
                logger.error("Missing customer_name for unresolved customer")
                return jsonify(
                    {
                        "success": False,
                        "error": "Customer name is required for unresolved customers",
                    }
                ), 400

            logger.info(f"Processing unresolved customer: {customer_name}")

            # Check if customer already exists first
            cursor.execute(
                "SELECT customer_id FROM customers WHERE normalized_name = ? AND is_active = 1",
                (customer_name,),
            )
            existing_customer = cursor.fetchone()

            if existing_customer:
                # Customer already exists, use existing ID
                customer_id = existing_customer[0]
                logger.info(
                    f"Found existing customer record: {customer_name} (ID: {customer_id})"
                )
            else:
                # Create new customer record
                logger.info(f"Creating new customer record: {customer_name}")
                cursor.execute(
                    """
                    INSERT INTO customers (normalized_name, is_active, created_date, updated_date)
                    VALUES (?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (customer_name,),
                )
                customer_id = cursor.lastrowid
                logger.info(
                    f"Created new customer record: {customer_name} (ID: {customer_id})"
                )

                # Verify the insertion
                cursor.execute(
                    "SELECT customer_id, normalized_name FROM customers WHERE customer_id = ?",
                    (customer_id,),
                )
                verify_insert = cursor.fetchone()
                verify_insert_data = tuple(verify_insert) if verify_insert else None
                logger.info(f"Verified new customer insertion: {verify_insert_data}")

        # Get sector_id from sector_name
        sector_id = None
        if sector_name:
            cursor.execute(
                "SELECT sector_id FROM sectors WHERE sector_name = ? AND is_active = 1",
                (sector_name,),
            )
            result = cursor.fetchone()
            if result:
                sector_id = result[0]
                logger.info(f"Found sector_id {sector_id} for sector '{sector_name}'")
            else:
                logger.warning(f"Sector '{sector_name}' not found in database")
        else:
            logger.info("Setting sector to NULL (unassigned)")

        # VERIFY CUSTOMER EXISTS BEFORE UPDATE
        cursor.execute(
            "SELECT customer_id, normalized_name, sector_id FROM customers WHERE customer_id = ?",
            (customer_id,),
        )
        customer_before = cursor.fetchone()
        customer_before_data = tuple(customer_before) if customer_before else None
        logger.info(f"Customer before update: {customer_before_data}")

        if not customer_before:
            logger.error(f"Customer {customer_id} not found in database!")
            return jsonify(
                {"success": False, "error": f"Customer with ID {customer_id} not found"}
            ), 404

        # UPDATE CUSTOMER SECTOR via junction table (trigger syncs customers.sector_id)
        logger.info(f"Updating customer {customer_id} to sector_id {sector_id}")
        cursor.execute(
            """
            INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
            VALUES (?, ?, 1, 'web_user')
            ON CONFLICT(customer_id, sector_id) DO UPDATE SET is_primary = 1
            """,
            (customer_id, sector_id),
        )
        # Also update timestamp on customer
        cursor.execute(
            "UPDATE customers SET updated_date = CURRENT_TIMESTAMP WHERE customer_id = ?",
            (customer_id,),
        )

        rows_affected = cursor.rowcount
        logger.info(f"UPDATE affected {rows_affected} rows")

        if rows_affected == 0:
            logger.error(f"UPDATE affected 0 rows for customer {customer_id}")
            return jsonify(
                {
                    "success": False,
                    "error": f"Customer with ID {customer_id} could not be updated",
                }
            ), 404

        # VERIFY THE CHANGE WAS MADE (before commit)
        cursor.execute(
            """
            SELECT c.customer_id, c.normalized_name, c.sector_id, s.sector_name
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE c.customer_id = ?
            """,
            (customer_id,),
        )
        verification_before_commit = cursor.fetchone()
        verification_before_data = (
            tuple(verification_before_commit) if verification_before_commit else None
        )
        logger.info(
            f"Customer after UPDATE (before commit): {verification_before_data}"
        )

        # Log the sector assignment change for audit trail
        try:
            cursor.execute(
                """
                INSERT INTO sector_assignment_audit 
                (customer_id, new_sector_id, assignment_method, assigned_by)
                VALUES (?, ?, 'manual_direct', 'web_interface')
                """,
                (customer_id, sector_id),
            )
            audit_rows = cursor.rowcount
            logger.info(f"Audit log inserted: {audit_rows} rows")
        except Exception as audit_error:
            logger.warning(f"Audit logging failed: {audit_error}")

        # COMMIT THE TRANSACTION
        logger.info("=== COMMITTING TRANSACTION ===")
        try:
            conn.commit()
            logger.info("Transaction committed successfully")
        except Exception as commit_error:
            logger.error(f"COMMIT FAILED: {commit_error}")
            conn.rollback()
            logger.info("Transaction rolled back")
            return jsonify(
                {"success": False, "error": f"Failed to commit changes: {commit_error}"}
            ), 500

        # VERIFY AFTER COMMIT - Open new connection to be sure
        final_verification_data = None
        try:
            conn.close()
            conn = db.connect()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT c.customer_id, c.normalized_name, c.sector_id, s.sector_name, c.updated_date
                FROM customers c
                LEFT JOIN sectors s ON c.sector_id = s.sector_id
                WHERE c.customer_id = ?
                """,
                (customer_id,),
            )
            final_verification = cursor.fetchone()
            final_verification_data = (
                tuple(final_verification) if final_verification else None
            )
            logger.info(
                f"FINAL VERIFICATION (new connection): {final_verification_data}"
            )

            conn.close()

        except Exception as verify_error:
            logger.error(f"Final verification failed: {verify_error}")

        logger.info("=== SECTOR UPDATE DEBUG END ===")

        return jsonify(
            {
                "success": True,
                "message": "Customer sector updated successfully",
                "customer_id": customer_id,
                "debug_info": {
                    "rows_affected": rows_affected,
                    "sector_id": sector_id,
                    "before_commit": list(verification_before_data)
                    if verification_before_data
                    else None,
                    "final_state": list(final_verification_data)
                    if final_verification_data
                    else None,
                },
            }
        )

    except Exception as e:
        logger.error(f"ERROR in update_customer_sector: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback

        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/customers/bulk-sector", methods=["PUT"])
def bulk_update_sectors():
    """Bulk update customer sectors - FIXED to handle unresolved customers"""
    try:
        data = request.get_json()
        customer_updates = data.get(
            "customer_updates", []
        )  # Array of {id, name, sector}
        sector_name = data.get("sector")

        if not customer_updates:
            return jsonify({"success": False, "error": "No customers selected"}), 400

        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Get sector_id from sector_name
        sector_id = None
        if sector_name:
            cursor.execute(
                "SELECT sector_id FROM sectors WHERE sector_name = ? AND is_active = 1",
                (sector_name,),
            )
            result = cursor.fetchone()
            if result:
                sector_id = result[0]

        updated_count = 0
        created_count = 0

        for customer_update in customer_updates:
            customer_id = customer_update.get("id")
            customer_name = customer_update.get("name")

            # CRITICAL FIX: Handle unresolved customers (customer_id = 0)
            if customer_id == 0:
                if not customer_name:
                    logger.warning("Skipping unresolved customer without name")
                    continue

                # Create the customer record
                cursor.execute(
                    """
                    INSERT INTO customers (normalized_name, is_active, created_date, updated_date)
                    VALUES (?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (customer_name,),
                )
                customer_id = cursor.lastrowid
                created_count += 1
                logger.info(
                    f"Created new customer record: {customer_name} (ID: {customer_id})"
                )

            # Update customer sector via junction table (trigger syncs cache)
            cursor.execute(
                """
                INSERT INTO customer_sectors (customer_id, sector_id, is_primary, assigned_by)
                VALUES (?, ?, 1, 'bulk_update')
                ON CONFLICT(customer_id, sector_id) DO UPDATE SET is_primary = 1
            """,
                (customer_id, sector_id),
            )
            cursor.execute(
                "UPDATE customers SET updated_date = CURRENT_TIMESTAMP WHERE customer_id = ?",
                (customer_id,),
            )

            if cursor.rowcount > 0:
                updated_count += 1
                # Log the sector assignment change for audit trail
                try:
                    cursor.execute(
                        """
                        INSERT INTO sector_assignment_audit
                        (customer_id, new_sector_id, assignment_method, assigned_by)
                        VALUES (?, ?, 'bulk_update', 'web_interface')
                    """,
                        (customer_id, sector_id),
                    )
                except Exception as audit_error:
                    logger.warning(
                        f"Audit logging failed for customer {customer_id}: {audit_error}"
                    )

        conn.commit()
        conn.close()

        return jsonify(
            {
                "success": True,
                "message": f"{updated_count} customers updated, {created_count} customers created",
                "updated_count": updated_count,
                "created_count": created_count,
            }
        )

    except Exception as e:
        logger.error(f"Error bulk updating sectors: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ADD this test endpoint to your customer_sector_api.py file


@customer_sector_bp.route("/debug/test-customer/<customer_name>", methods=["GET"])
def test_specific_customer(customer_name):
    """Test a specific customer to see why they might not appear"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        logger.info(f"=== TESTING CUSTOMER: {customer_name} ===")

        # Test 1: Check if customer exists in customers table
        cursor.execute(
            "SELECT customer_id, normalized_name, sector_id, is_active FROM customers WHERE normalized_name LIKE ? OR normalized_name LIKE ?",
            (f"%{customer_name}%", f"%{customer_name.upper()}%"),
        )
        customer_matches = cursor.fetchall()
        customer_data = [list(row) for row in customer_matches]

        # Test 2: Check if customer appears in normalization audit
        cursor.execute(
            "SELECT raw_text, normalized_name, customer_id, exists_in_customers FROM v_customer_normalization_audit WHERE normalized_name LIKE ? OR raw_text LIKE ?",
            (f"%{customer_name}%", f"%{customer_name}%"),
        )
        audit_matches = cursor.fetchall()
        audit_data = [list(row) for row in audit_matches]

        # Test 3: Check raw spots data
        cursor.execute(
            "SELECT DISTINCT bill_code FROM spots WHERE bill_code LIKE ? AND revenue_type = 'Internal Ad Sales' LIMIT 10",
            (f"%{customer_name}%",),
        )
        bill_code_matches = cursor.fetchall()
        bill_code_data = [row[0] for row in bill_code_matches]

        # Test 4: Check revenue data if customer exists
        revenue_data = []
        if customer_matches:
            customer_id = customer_matches[0][0]
            cursor.execute(
                """
                SELECT 
                    SUM(COALESCE(s.gross_rate, 0)) AS total_revenue,
                    COUNT(s.spot_id) AS spot_count,
                    COUNT(DISTINCT s.bill_code) AS unique_bill_codes
                FROM v_customer_normalization_audit audit
                LEFT JOIN spots s ON audit.raw_text = s.bill_code
                LEFT JOIN agencies a ON s.agency_id = a.agency_id
                WHERE s.revenue_type = 'Internal Ad Sales'
                  AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
                  AND s.gross_rate > 0
                  AND audit.customer_id = ?
                """,
                (customer_id,),
            )
            revenue_row = cursor.fetchone()
            if revenue_row:
                revenue_data = {
                    "total_revenue": float(revenue_row[0]) if revenue_row[0] else 0.0,
                    "spot_count": revenue_row[1] if revenue_row[1] else 0,
                    "unique_bill_codes": revenue_row[2] if revenue_row[2] else 0,
                }

        # Test 5: Check if similar names exist
        cursor.execute(
            "SELECT DISTINCT bill_code FROM spots WHERE bill_code LIKE '%BLACK%' OR bill_code LIKE '%MEDIA%' LIMIT 10"
        )
        similar_matches = cursor.fetchall()
        similar_data = [row[0] for row in similar_matches]

        conn.close()

        return jsonify(
            {
                "success": True,
                "customer_searched": customer_name,
                "diagnostics": {
                    "customers_table_matches": customer_data,
                    "normalization_audit_matches": audit_data,
                    "bill_code_matches": bill_code_data,
                    "revenue_data": revenue_data,
                    "similar_names": similar_data,
                    "summary": {
                        "exists_in_customers": len(customer_data) > 0,
                        "exists_in_audit": len(audit_data) > 0,
                        "exists_in_spots": len(bill_code_data) > 0,
                        "has_revenue": revenue_data
                        and revenue_data.get("total_revenue", 0) > 0,
                    },
                },
            }
        )

    except Exception as e:
        logger.error(f"Error testing customer: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/sectors", methods=["POST"])
def add_sector():
    """Add a new sector"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400

        name = (data.get("name") or "").strip()
        description = (data.get("description") or "").strip()

        if not name:
            return jsonify({"success": False, "error": "Sector name is required"}), 400

        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Check if sector already exists
        cursor.execute("SELECT sector_id FROM sectors WHERE sector_name = ?", (name,))
        if cursor.fetchone():
            conn.close()
            return jsonify({"success": False, "error": "Sector already exists"}), 400

        # Insert new sector
        cursor.execute(
            """
            INSERT INTO sectors (sector_code, sector_name, sector_group, is_active)
            VALUES (?, ?, ?, 1)
        """,
            (name.upper().replace(" ", "_"), name, description),
        )

        conn.commit()
        sector_id = cursor.lastrowid
        conn.close()

        logger.info(f"Successfully created sector '{name}' with ID {sector_id}")

        return jsonify(
            {
                "success": True,
                "message": "Sector added successfully",
                "data": {"id": sector_id, "name": name, "description": description},
            }
        )

    except Exception as e:
        logger.error(f"Error adding sector: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/sectors/<int:sector_id>", methods=["PUT"])
def update_sector(sector_id):
    """Update an existing sector"""
    try:
        data = request.get_json()
        name = data.get("name", "").strip()
        description = data.get("description", "").strip()

        if not name:
            return jsonify({"success": False, "error": "Sector name is required"}), 400

        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Check if another sector already has this name
        cursor.execute(
            "SELECT sector_id FROM sectors WHERE sector_name = ? AND sector_id != ? AND is_active = 1",
            (name, sector_id),
        )
        if cursor.fetchone():
            return jsonify(
                {"success": False, "error": "A sector with this name already exists"}
            ), 400

        # Update sector
        cursor.execute(
            """
            UPDATE sectors 
            SET sector_name = ?, 
                sector_group = ?, 
                sector_code = UPPER(REPLACE(?, ' ', '_')),
                updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ? AND is_active = 1
        """,
            (name, description, name, sector_id),
        )

        if cursor.rowcount == 0:
            return jsonify(
                {"success": False, "error": "Sector not found or inactive"}
            ), 404

        conn.commit()
        conn.close()

        return jsonify(
            {
                "success": True,
                "message": f'Sector updated to "{name}"',
                "data": {"id": sector_id, "name": name, "description": description},
            }
        )

    except Exception as e:
        logger.error(f"Error updating sector: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/sectors/<int:sector_id>", methods=["DELETE"])
def delete_sector(sector_id):
    """Delete a sector (unassigns customers first)"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # First, remove all junction table entries for this sector
        # (delete triggers will auto-promote or NULL the customers.sector_id cache)
        cursor.execute(
            "DELETE FROM customer_sectors WHERE sector_id = ?",
            (sector_id,),
        )

        # Then deactivate the sector (don't hard delete)
        cursor.execute(
            """
            UPDATE sectors
            SET is_active = 0, updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ?
        """,
            (sector_id,),
        )

        conn.commit()
        conn.close()

        return jsonify({"success": True, "message": "Sector deleted successfully"})

    except Exception as e:
        logger.error(f"Error deleting sector: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# =============================================================================
# DEBUG ENDPOINTS
# =============================================================================


@customer_sector_bp.route("/debug/full-flow/<int:customer_id>", methods=["GET"])
def debug_customer_full_flow(customer_id):
    """Comprehensive diagnostic for a specific customer"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Get customer's current state from all relevant tables
        customer_query = """
        SELECT 
            c.customer_id,
            c.normalized_name,
            c.sector_id,
            s.sector_name,
            c.created_date,
            c.updated_date,
            c.is_active
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        WHERE c.customer_id = ?
        """

        cursor.execute(customer_query, (customer_id,))
        customer_raw = cursor.fetchone()

        if not customer_raw:
            return jsonify({"error": f"Customer {customer_id} not found"})

        customer_data = {
            "customer_id": customer_raw[0],
            "normalized_name": customer_raw[1],
            "sector_id": customer_raw[2],
            "sector_name": customer_raw[3],
            "created_date": customer_raw[4],
            "updated_date": customer_raw[5],
            "is_active": bool(customer_raw[6]),
        }

        # Get audit trail for this customer
        audit_query = """
        SELECT 
            audit_id,
            old_sector_id,
            new_sector_id,
            assignment_method,
            assigned_by,
            assigned_at,
            notes
        FROM sector_assignment_audit
        WHERE customer_id = ?
        ORDER BY assigned_at DESC
        LIMIT 10
        """

        cursor.execute(audit_query, (customer_id,))
        audit_raw = cursor.fetchall()

        audit_data = []
        for audit_row in audit_raw:
            audit_data.append(
                {
                    "audit_id": audit_row[0],
                    "old_sector_id": audit_row[1],
                    "new_sector_id": audit_row[2],
                    "assignment_method": audit_row[3],
                    "assigned_by": audit_row[4],
                    "assigned_at": audit_row[5],
                    "notes": audit_row[6],
                }
            )

        # Check how this customer appears in the main query used by the frontend
        frontend_query = """
        SELECT
            COALESCE(audit.customer_id, 0) AS customer_id,
            COALESCE(audit.normalized_name, s.bill_code, 'Unknown') AS name,
            COALESCE(sect.sector_name, 'Unassigned') AS sector,
            COALESCE(c.updated_date, '2025-01-01') AS lastUpdated,
            ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total_revenue,
            COUNT(s.spot_id) AS spot_count,
            CASE 
                WHEN audit.customer_id IS NOT NULL THEN 'resolved'
                ELSE 'unresolved'
            END AS resolution_status
        FROM spots s
        {CustomerNormalizationQueryBuilder.build_customer_join()}
        LEFT JOIN customers c ON audit.customer_id = c.customer_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
        WHERE s.revenue_type = 'Internal Ad Sales'
          AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
          AND s.gross_rate > 0
          AND (COALESCE(c.is_active, 1) = 1 OR audit.customer_id IS NULL)
          AND COALESCE(audit.customer_id, 0) = ?
        GROUP BY 
            COALESCE(audit.customer_id, 0),
            COALESCE(audit.normalized_name, s.bill_code),
            sect.sector_name,
            c.updated_date
        """

        cursor.execute(frontend_query, (customer_id,))
        frontend_raw = cursor.fetchone()

        frontend_data = None
        if frontend_raw:
            frontend_data = {
                "customer_id": frontend_raw[0],
                "name": frontend_raw[1],
                "sector": frontend_raw[2],
                "lastUpdated": frontend_raw[3],
                "total_revenue": float(frontend_raw[4]) if frontend_raw[4] else 0.0,
                "spot_count": frontend_raw[5],
                "resolution_status": frontend_raw[6],
            }

        conn.close()

        return jsonify(
            {
                "success": True,
                "customer_id": customer_id,
                "diagnostics": {
                    "customer_table": customer_data,
                    "audit_trail": audit_data,
                    "frontend_query_result": frontend_data,
                    "summary": {
                        "customer_exists": customer_data is not None,
                        "has_sector": customer_data["sector_id"] is not None,
                        "appears_in_frontend": frontend_data is not None,
                        "audit_entries": len(audit_data),
                    },
                },
            }
        )

    except Exception as e:
        logger.error(f"Full flow diagnostic failed: {e}")
        import traceback

        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/debug/test-db-write", methods=["GET"])
def test_database_write():
    """Test endpoint to verify database write capability - FIXED JSON SERIALIZATION"""
    try:
        logger.info("=== DATABASE WRITE TEST START ===")

        container = get_container()
        db = container.get("database_connection")

        # Debug connection details
        logger.info(f"Database connection type: {type(db)}")
        if hasattr(db, "database_path"):
            logger.info(f"Database path: {db.database_path}")
        if hasattr(db, "db_path"):
            logger.info(f"DB path: {db.db_path}")

        conn = db.connect()
        cursor = conn.cursor()

        # Database file info - FIXED: Convert rows to serializable format
        cursor.execute("PRAGMA database_list")
        db_info_raw = cursor.fetchall()
        db_info = [list(row) for row in db_info_raw]  # Convert to list of lists
        logger.info(f"Database list: {db_info}")

        db_file_path = None
        file_info = {}

        for db_entry in db_info:
            if db_entry[1] == "main":
                db_file_path = db_entry[2]
                logger.info(f"Main database file: {db_file_path}")

                import os

                if db_file_path and os.path.exists(db_file_path):
                    stat_info = os.stat(db_file_path)
                    file_info = {
                        "path": db_file_path,
                        "size": stat_info.st_size,
                        "permissions": oct(stat_info.st_mode)[-3:],
                        "writable": os.access(db_file_path, os.W_OK),
                        "exists": True,
                    }
                    logger.info(f"File size: {file_info['size']} bytes")
                    logger.info(f"File permissions: {file_info['permissions']}")
                    logger.info(f"File writable: {file_info['writable']}")
                else:
                    file_info = {
                        "path": db_file_path,
                        "exists": False,
                        "error": "File does not exist",
                    }
                    logger.error("Database file does not exist!")

        # Get current timestamp for testing
        import time

        test_time = f"test-{int(time.time())}"

        # Find a customer to test with - FIXED: Convert row to serializable
        cursor.execute(
            "SELECT customer_id, normalized_name, sector_id FROM customers LIMIT 1"
        )
        test_customer_raw = cursor.fetchone()

        if not test_customer_raw:
            return jsonify({"error": "No customers found for testing"})

        test_customer = list(test_customer_raw)  # Convert to list
        customer_id = test_customer[0]
        customer_name = test_customer[1]
        original_sector = test_customer[2]

        logger.info(
            f"Testing with customer {customer_id} ('{customer_name}'), original sector: {original_sector}"
        )

        # Test 1: Simple update
        logger.info("=== TEST 1: Simple update ===")
        cursor.execute(
            "UPDATE customers SET updated_date = ? WHERE customer_id = ?",
            (test_time, customer_id),
        )
        rows_affected_1 = cursor.rowcount
        logger.info(f"Update affected {rows_affected_1} rows")

        # Verify before commit
        cursor.execute(
            "SELECT updated_date FROM customers WHERE customer_id = ?", (customer_id,)
        )
        date_before_commit_raw = cursor.fetchone()
        date_before_commit = (
            date_before_commit_raw[0] if date_before_commit_raw else None
        )
        logger.info(f"Date before commit: {date_before_commit}")

        # Commit
        conn.commit()
        logger.info("Committed test update")

        # Verify after commit
        cursor.execute(
            "SELECT updated_date FROM customers WHERE customer_id = ?", (customer_id,)
        )
        date_after_commit_raw = cursor.fetchone()
        date_after_commit = date_after_commit_raw[0] if date_after_commit_raw else None
        logger.info(f"Date after commit: {date_after_commit}")

        test1_passed = date_after_commit == test_time
        logger.info(f"Test 1 passed: {test1_passed}")

        # Test 2: Sector assignment
        logger.info("=== TEST 2: Sector assignment ===")

        # Get a sector to test with - FIXED: Convert row to serializable
        cursor.execute(
            "SELECT sector_id, sector_name FROM sectors WHERE is_active = 1 LIMIT 1"
        )
        test_sector_raw = cursor.fetchone()

        test2_passed = False
        sector_test_info = {}

        if test_sector_raw:
            test_sector = list(test_sector_raw)  # Convert to list
            test_sector_id = test_sector[0]
            test_sector_name = test_sector[1]
            logger.info(f"Testing with sector {test_sector_id} ('{test_sector_name}')")

            # Update sector
            cursor.execute(
                "UPDATE customers SET sector_id = ? WHERE customer_id = ?",
                (test_sector_id, customer_id),
            )
            rows_affected_2 = cursor.rowcount
            logger.info(f"Sector update affected {rows_affected_2} rows")

            # Commit
            conn.commit()
            logger.info("Committed sector update")

            # Verify - FIXED: Convert row to serializable
            cursor.execute(
                """
                SELECT c.sector_id, s.sector_name 
                FROM customers c 
                LEFT JOIN sectors s ON c.sector_id = s.sector_id 
                WHERE c.customer_id = ?
                """,
                (customer_id,),
            )
            final_sector_raw = cursor.fetchone()
            final_sector = list(final_sector_raw) if final_sector_raw else None
            logger.info(f"Final sector: {final_sector}")

            test2_passed = final_sector and final_sector[0] == test_sector_id
            logger.info(f"Test 2 passed: {test2_passed}")

            sector_test_info = {
                "test_sector_id": test_sector_id,
                "test_sector_name": test_sector_name,
                "rows_affected": rows_affected_2,
                "final_sector": final_sector,
            }

            # Restore original sector
            cursor.execute(
                "UPDATE customers SET sector_id = ? WHERE customer_id = ?",
                (original_sector, customer_id),
            )
            conn.commit()
            logger.info("Restored original sector")
        else:
            logger.warning("No sectors found for testing")
            sector_test_info = {"error": "No sectors available for testing"}

        # Restore original timestamp
        cursor.execute(
            "UPDATE customers SET updated_date = CURRENT_TIMESTAMP WHERE customer_id = ?",
            (customer_id,),
        )
        conn.commit()

        conn.close()

        logger.info("=== DATABASE WRITE TEST END ===")

        return jsonify(
            {
                "success": True,
                "message": "Database write test completed",
                "test_results": {
                    "test_customer": {
                        "id": customer_id,
                        "name": customer_name,
                        "original_sector": original_sector,
                    },
                    "test_timestamp": test_time,
                    "test1_simple_update": {
                        "passed": test1_passed,
                        "rows_affected": rows_affected_1,
                        "date_before_commit": date_before_commit,
                        "date_after_commit": date_after_commit,
                    },
                    "test2_sector_assignment": {
                        "passed": test2_passed,
                        **sector_test_info,
                    },
                    "overall_passed": test1_passed and test2_passed,
                },
                "database_info": {
                    "connection_type": str(type(db)),
                    "database_list": db_info,
                    "file_info": file_info,
                },
            }
        )

    except Exception as e:
        logger.error(f"Database write test failed: {e}")
        import traceback

        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/debug/db-info", methods=["GET"])
def get_database_info():
    """Quick database info endpoint"""
    try:
        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # Get database file path
        cursor.execute("PRAGMA database_list")
        db_list = cursor.fetchall()

        # Convert to serializable format
        db_info = []
        for row in db_list:
            db_info.append({"seq": row[0], "name": row[1], "file": row[2]})

        # Get basic stats
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM sectors")
        sector_count = cursor.fetchone()[0]

        conn.close()

        return jsonify(
            {
                "success": True,
                "database_info": db_info,
                "stats": {"customers": customer_count, "sectors": sector_count},
            }
        )

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
