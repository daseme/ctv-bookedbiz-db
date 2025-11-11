from flask import Blueprint, request, jsonify, current_app
from src.services.container import get_container
import logging

customer_sector_bp = Blueprint(
    "customer_sector_api", __name__, url_prefix="/api/customer-sector"
)
logger = logging.getLogger(__name__)


@customer_sector_bp.route("/customers", methods=["GET"])
def get_customers():
    """Get customers with Internal Ad Sales revenue, INCLUDING unresolved ones"""
    try:
        container = get_container()
        db = container.get("database_connection")

        # FIXED: Include both resolved AND unresolved customers
        query = """
        SELECT
            COALESCE(audit.customer_id, 0) AS customer_id,
            COALESCE(audit.normalized_name, s.bill_code, 'Unknown') AS name,
            COALESCE(sect.sector_name, 'Unassigned') AS sector,
            COALESCE(c.updated_date, '2025-01-01') AS lastUpdated,
            ROUND(SUM(COALESCE(s.gross_rate, 0)), 2) AS total_revenue,
            COUNT(s.spot_id) AS spot_count,
            -- Add flag to distinguish resolved vs unresolved
            CASE 
                WHEN audit.customer_id IS NOT NULL THEN 'resolved'
                ELSE 'unresolved'
            END AS resolution_status
        FROM spots s
        LEFT JOIN v_customer_normalization_audit audit ON audit.raw_text = s.bill_code
        LEFT JOIN customers c ON audit.customer_id = c.customer_id
        LEFT JOIN agencies a ON s.agency_id = a.agency_id
        LEFT JOIN sectors sect ON c.sector_id = sect.sector_id
        WHERE s.revenue_type = 'Internal Ad Sales'
          AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
          AND s.gross_rate > 0
          -- CRITICAL FIX: Remove this line that was excluding unresolved customers
          -- AND COALESCE(audit.customer_id, 0) != 0  <-- This was the problem!
          AND (COALESCE(c.is_active, 1) = 1 OR audit.customer_id IS NULL)  -- Include unresolved
        GROUP BY 
            COALESCE(audit.customer_id, 0),
            COALESCE(audit.normalized_name, s.bill_code),
            sect.sector_name,
            c.updated_date
        HAVING COUNT(s.spot_id) > 0
        ORDER BY 
            CASE WHEN audit.customer_id IS NULL THEN 0 ELSE 1 END,  -- Unresolved first
            name
        """

        conn = db.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        customers = []
        for row in rows:
            customers.append(
                {
                    "id": row[0],
                    "name": row[1],
                    "sector": row[2] if row[2] != "Unassigned" else None,
                    "lastUpdated": str(row[3])[:10] if row[3] else "2025-01-01",
                    "totalRevenue": float(row[4]) if row[4] else 0.0,
                    "spotCount": row[5] if row[5] else 0,
                    "resolutionStatus": row[6],  
                    "isUnresolved": row[6] == 'unresolved'  
                }
            )

        conn.close()
        
        resolved_count = len([c for c in customers if not c['isUnresolved']])
        unresolved_count = len([c for c in customers if c['isUnresolved']])
        
        logger.info(
            f"Fetched {len(customers)} total customers: "
            f"{resolved_count} resolved, {unresolved_count} unresolved"
        )
        return jsonify({"success": True, "data": customers})

    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
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
            COALESCE(sector_group, 'Business sector') as description
        FROM sectors
        WHERE is_active = 1
        ORDER BY sector_name
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
                    "description": row[2],
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
    """Update customer sector assignment - FIXED to handle existing customers"""
    try:
        data = request.get_json()
        sector_name = data.get("sector")

        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # CRITICAL FIX: Handle unresolved customers (customer_id = 0)
        if customer_id == 0:
            customer_name = data.get("customer_name")
            if not customer_name:
                return jsonify({
                    "success": False, 
                    "error": "Customer name is required for unresolved customers"
                }), 400

            # FIXED: Check if customer already exists first
            cursor.execute(
                "SELECT customer_id FROM customers WHERE normalized_name = ? AND is_active = 1",
                (customer_name,)
            )
            existing_customer = cursor.fetchone()
            
            if existing_customer:
                # Customer already exists, use existing ID
                customer_id = existing_customer[0]
                logger.info(f"Found existing customer record: {customer_name} (ID: {customer_id})")
            else:
                # Create new customer record
                cursor.execute(
                    """
                    INSERT INTO customers (normalized_name, is_active, created_date, updated_date)
                    VALUES (?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (customer_name,)
                )
                customer_id = cursor.lastrowid
                logger.info(f"Created new customer record: {customer_name} (ID: {customer_id})")

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

        # Update customer
        cursor.execute(
            """
            UPDATE customers 
            SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
            WHERE customer_id = ?
        """,
            (sector_id, customer_id),
        )

        if cursor.rowcount == 0:
            return jsonify({
                "success": False, 
                "error": f"Customer with ID {customer_id} not found"
            }), 404

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
        except Exception as audit_error:
            logger.warning(f"Audit logging failed: {audit_error}")

        conn.commit()
        conn.close()

        return jsonify({
            "success": True, 
            "message": "Customer sector updated successfully",
            "customer_id": customer_id  # Return the real customer_id for frontend update
        })

    except Exception as e:
        logger.error(f"Error updating customer sector: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/customers/bulk-sector", methods=["PUT"])
def bulk_update_sectors():
    """Bulk update customer sectors - FIXED to handle unresolved customers"""
    try:
        data = request.get_json()
        customer_updates = data.get("customer_updates", [])  # Array of {id, name, sector}
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
                    logger.warning(f"Skipping unresolved customer without name")
                    continue

                # Create the customer record
                cursor.execute(
                    """
                    INSERT INTO customers (normalized_name, is_active, created_date, updated_date)
                    VALUES (?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (customer_name,)
                )
                customer_id = cursor.lastrowid
                created_count += 1
                logger.info(f"Created new customer record: {customer_name} (ID: {customer_id})")

            # Update customer sector
            cursor.execute(
                """
                UPDATE customers 
                SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """,
                (sector_id, customer_id),
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

        return jsonify({
            "success": True,
            "message": f"{updated_count} customers updated, {created_count} customers created",
            "updated_count": updated_count,
            "created_count": created_count
        })

    except Exception as e:
        logger.error(f"Error bulk updating sectors: {e}")
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

        # First, unassign all customers from this sector
        cursor.execute(
            """
            UPDATE customers 
            SET sector_id = NULL, updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ?
        """,
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