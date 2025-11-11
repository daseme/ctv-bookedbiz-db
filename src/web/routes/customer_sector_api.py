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
    """Get customers with Internal Ad Sales revenue, INCLUDING unresolved ones"""
    try:
        container = get_container()
        db = container.get("database_connection")

        # FIXED: Include both resolved AND unresolved customers
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
                    "resolutionStatus": row[6],  
                    "isUnresolved": row[6] == 'unresolved'  
                }
            )

        conn.close()
        
        resolved_count = len([c for c in customers if not c['isUnresolved']])
        unresolved_count = len([c for c in customers if c['isUnresolved']])
        
        
        resolved_count = len([c for c in customers if not c['isUnresolved']])
        unresolved_count = len([c for c in customers if c['isUnresolved']])
        
        logger.info(
            f"Fetched {len(customers)} total customers: "
            f"{resolved_count} resolved, {unresolved_count} unresolved"
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
    """Update customer sector assignment - FIXED VERSION"""
    try:
        data = request.get_json()
        sector_name = data.get("sector")
        
        logger.info(f"=== SECTOR UPDATE DEBUG START ===")
        logger.info(f"Customer ID: {customer_id}, Sector: {sector_name}")
        logger.info(f"Request data: {data}")

        container = get_container()
        db = container.get("database_connection")
        
        # DEBUG: Database connection details
        logger.info(f"Database connection type: {type(db)}")
        if hasattr(db, 'database_path'):
            logger.info(f"Database path: {db.database_path}")
        if hasattr(db, 'db_path'):
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
                if db_entry[1] == 'main':  # Main database
                    db_file_path = db_entry[2]
                    logger.info(f"Main database file: {db_file_path}")
                    
                    import os
                    if db_file_path and os.path.exists(db_file_path):
                        stat_info = os.stat(db_file_path)
                        logger.info(f"Database file size: {stat_info.st_size} bytes")
                        logger.info(f"Database file permissions: {oct(stat_info.st_mode)[-3:]}")
                        logger.info(f"Database file writable: {os.access(db_file_path, os.W_OK)}")
                    else:
                        logger.error(f"Database file does not exist: {db_file_path}")
        except Exception as db_debug_error:
            logger.error(f"Database debug failed: {db_debug_error}")

        # CRITICAL FIX: Handle unresolved customers (customer_id = 0)
        if customer_id == 0:
            customer_name = data.get("customer_name")
            if not customer_name:
                logger.error("Missing customer_name for unresolved customer")
                return jsonify({
                    "success": False, 
                    "error": "Customer name is required for unresolved customers"
                }), 400

            logger.info(f"Processing unresolved customer: {customer_name}")

            # Check if customer already exists first
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
                logger.info(f"Creating new customer record: {customer_name}")
                cursor.execute(
                    """
                    INSERT INTO customers (normalized_name, is_active, created_date, updated_date)
                    VALUES (?, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (customer_name,)
                )
                customer_id = cursor.lastrowid
                logger.info(f"Created new customer record: {customer_name} (ID: {customer_id})")
                
                # Verify the insertion
                cursor.execute("SELECT customer_id, normalized_name FROM customers WHERE customer_id = ?", (customer_id,))
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
        cursor.execute("SELECT customer_id, normalized_name, sector_id FROM customers WHERE customer_id = ?", (customer_id,))
        customer_before = cursor.fetchone()
        customer_before_data = tuple(customer_before) if customer_before else None
        logger.info(f"Customer before update: {customer_before_data}")
        
        if not customer_before:
            logger.error(f"Customer {customer_id} not found in database!")
            return jsonify({
                "success": False, 
                "error": f"Customer with ID {customer_id} not found"
            }), 404

        # UPDATE CUSTOMER SECTOR
        logger.info(f"Updating customer {customer_id} to sector_id {sector_id}")
        cursor.execute(
            """
            UPDATE customers 
            SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
            WHERE customer_id = ?
            """,
            (sector_id, customer_id),
        )

        rows_affected = cursor.rowcount
        logger.info(f"UPDATE affected {rows_affected} rows")

        if rows_affected == 0:
            logger.error(f"UPDATE affected 0 rows for customer {customer_id}")
            return jsonify({
                "success": False, 
                "error": f"Customer with ID {customer_id} could not be updated"
            }), 404

        # VERIFY THE CHANGE WAS MADE (before commit)
        cursor.execute(
            """
            SELECT c.customer_id, c.normalized_name, c.sector_id, s.sector_name
            FROM customers c
            LEFT JOIN sectors s ON c.sector_id = s.sector_id
            WHERE c.customer_id = ?
            """,
            (customer_id,)
        )
        verification_before_commit = cursor.fetchone()
        verification_before_data = tuple(verification_before_commit) if verification_before_commit else None
        logger.info(f"Customer after UPDATE (before commit): {verification_before_data}")

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
            return jsonify({
                "success": False, 
                "error": f"Failed to commit changes: {commit_error}"
            }), 500
        
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
                (customer_id,)
            )
            final_verification = cursor.fetchone()
            final_verification_data = tuple(final_verification) if final_verification else None
            logger.info(f"FINAL VERIFICATION (new connection): {final_verification_data}")
            
            conn.close()
            
        except Exception as verify_error:
            logger.error(f"Final verification failed: {verify_error}")

        logger.info(f"=== SECTOR UPDATE DEBUG END ===")

        return jsonify({
            "success": True, 
            "message": "Customer sector updated successfully",
            "customer_id": customer_id,
            "debug_info": {
                "rows_affected": rows_affected,
                "sector_id": sector_id,
                "before_commit": list(verification_before_data) if verification_before_data else None,
                "final_state": list(final_verification_data) if final_verification_data else None
            }
        })

    except Exception as e:
        logger.error(f"ERROR in update_customer_sector: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/customers/bulk-sector", methods=["PUT"])
def bulk_update_sectors():
    """Bulk update customer sectors - COMPLETE DEBUG VERSION"""
    try:
        data = request.get_json()
        customer_updates = data.get("customer_updates", [])  
        sector_name = data.get("sector")

        logger.info(f"=== BULK UPDATE DEBUG START ===")
        logger.info(f"Sector: {sector_name}")
        logger.info(f"Customer updates: {len(customer_updates)} customers")
        logger.info(f"Full request data: {data}")

        if not customer_updates:
            return jsonify({"success": False, "error": "No customers selected"}), 400

        container = get_container()
        db = container.get("database_connection")
        conn = db.connect()
        cursor = conn.cursor()

        # DEBUG: Database info
        cursor.execute("PRAGMA database_list")
        db_info = cursor.fetchall()
        logger.info(f"Database list: {db_info}")

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
                logger.warning(f"Sector '{sector_name}' not found")
        else:
            logger.info("Setting sector to NULL (unassigned)")

        updated_count = 0
        created_count = 0

        for i, customer_update in enumerate(customer_updates):
            customer_id = customer_update.get("id")
            customer_name = customer_update.get("name")
            
            logger.info(f"Processing customer {i+1}/{len(customer_updates)}: ID={customer_id}, Name='{customer_name}'")

            # Handle unresolved customers (customer_id = 0)
            if customer_id == 0:
                if not customer_name:
                    logger.warning(f"Skipping unresolved customer without name")
                    continue

                # Check if customer already exists
                cursor.execute(
                    "SELECT customer_id FROM customers WHERE normalized_name = ? AND is_active = 1",
                    (customer_name,)
                )
                existing = cursor.fetchone()
                
                if existing:
                    customer_id = existing[0]
                    logger.info(f"Found existing customer: {customer_name} (ID: {customer_id})")
                else:
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
                    
                    # Verify creation
                    cursor.execute("SELECT customer_id, normalized_name FROM customers WHERE customer_id = ?", (customer_id,))
                    verify = cursor.fetchone()
                    logger.info(f"Verified creation: {verify}")

            # Update customer sector
            logger.info(f"Updating customer {customer_id} to sector_id {sector_id}")
            
            # Check current state
            cursor.execute("SELECT customer_id, normalized_name, sector_id FROM customers WHERE customer_id = ?", (customer_id,))
            before_update = cursor.fetchone()
            logger.info(f"Before update: {before_update}")
            
            cursor.execute(
                """
                UPDATE customers 
                SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
                """,
                (sector_id, customer_id),
            )

            rows_affected = cursor.rowcount
            logger.info(f"Update affected {rows_affected} rows for customer {customer_id}")

            if rows_affected > 0:
                updated_count += 1
                
                # Verify the update
                cursor.execute("SELECT customer_id, normalized_name, sector_id FROM customers WHERE customer_id = ?", (customer_id,))
                after_update = cursor.fetchone()
                logger.info(f"After update: {after_update}")
                
                # Log audit trail
                try:
                    cursor.execute(
                        """
                        INSERT INTO sector_assignment_audit 
                        (customer_id, new_sector_id, assignment_method, assigned_by)
                        VALUES (?, ?, 'bulk_update', 'web_interface')
                        """,
                        (customer_id, sector_id),
                    )
                    logger.info(f"Audit logged for customer {customer_id}")
                except Exception as audit_error:
                    logger.warning(f"Audit logging failed for customer {customer_id}: {audit_error}")
            else:
                logger.warning(f"No rows updated for customer {customer_id}")

        # COMMIT ALL CHANGES
        logger.info(f"=== COMMITTING BULK TRANSACTION ===")
        logger.info(f"About to commit: {updated_count} updates, {created_count} creates")
        
        try:
            conn.commit()
            logger.info("Bulk transaction committed successfully")
        except Exception as commit_error:
            logger.error(f"BULK COMMIT FAILED: {commit_error}")
            conn.rollback()
            logger.info("Bulk transaction rolled back")
            return jsonify({
                "success": False, 
                "error": f"Failed to commit bulk changes: {commit_error}"
            }), 500

        # FINAL VERIFICATION - Check a few updated customers
        try:
            if updated_count > 0:
                sample_ids = [cu.get("id") for cu in customer_updates[:3]]  # Check first 3
                sample_ids = [cid for cid in sample_ids if cid != 0]  # Filter out unresolved
                
                if sample_ids:
                    placeholders = ','.join(['?'] * len(sample_ids))
                    cursor.execute(
                        f"""
                        SELECT c.customer_id, c.normalized_name, c.sector_id, s.sector_name
                        FROM customers c
                        LEFT JOIN sectors s ON c.sector_id = s.sector_id
                        WHERE c.customer_id IN ({placeholders})
                        """,
                        sample_ids
                    )
                    sample_verification = cursor.fetchall()
                    logger.info(f"Sample verification after commit: {sample_verification}")
                    
        except Exception as verify_error:
            logger.error(f"Final verification failed: {verify_error}")

        conn.close()
        
        logger.info(f"=== BULK UPDATE DEBUG END ===")

        return jsonify({
            "success": True,
            "message": f"{updated_count} customers updated, {created_count} customers created",
            "updated_count": updated_count,
            "created_count": created_count,
            "debug_info": {
                "sector_id": sector_id,
                "total_processed": len(customer_updates)
            }
        })

    except Exception as e:
        logger.error(f"ERROR in bulk_update_sectors: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "error": str(e)}), 500


@customer_sector_bp.route("/sectors", methods=["POST"])
def add_sector():
    """Add a new sector"""
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


@customer_sector_bp.route("/debug/test-db-write", methods=["GET"])
def test_database_write():
    """Test endpoint to verify database write capability - FIXED JSON SERIALIZATION"""
    try:
        logger.info("=== DATABASE WRITE TEST START ===")
        
        container = get_container()
        db = container.get("database_connection")
        
        # Debug connection details
        logger.info(f"Database connection type: {type(db)}")
        if hasattr(db, 'database_path'):
            logger.info(f"Database path: {db.database_path}")
        if hasattr(db, 'db_path'):
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
            if db_entry[1] == 'main':
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
                        "exists": True
                    }
                    logger.info(f"File size: {file_info['size']} bytes")
                    logger.info(f"File permissions: {file_info['permissions']}")
                    logger.info(f"File writable: {file_info['writable']}")
                else:
                    file_info = {
                        "path": db_file_path,
                        "exists": False,
                        "error": "File does not exist"
                    }
                    logger.error(f"Database file does not exist!")
        
        # Get current timestamp for testing
        import time
        test_time = f"test-{int(time.time())}"
        
        # Find a customer to test with - FIXED: Convert row to serializable
        cursor.execute("SELECT customer_id, normalized_name, sector_id FROM customers LIMIT 1")
        test_customer_raw = cursor.fetchone()
        
        if not test_customer_raw:
            return jsonify({"error": "No customers found for testing"})
            
        test_customer = list(test_customer_raw)  # Convert to list
        customer_id = test_customer[0]
        customer_name = test_customer[1]
        original_sector = test_customer[2]
        
        logger.info(f"Testing with customer {customer_id} ('{customer_name}'), original sector: {original_sector}")
        
        # Test 1: Simple update
        logger.info("=== TEST 1: Simple update ===")
        cursor.execute(
            "UPDATE customers SET updated_date = ? WHERE customer_id = ?",
            (test_time, customer_id)
        )
        rows_affected_1 = cursor.rowcount
        logger.info(f"Update affected {rows_affected_1} rows")
        
        # Verify before commit
        cursor.execute("SELECT updated_date FROM customers WHERE customer_id = ?", (customer_id,))
        date_before_commit_raw = cursor.fetchone()
        date_before_commit = date_before_commit_raw[0] if date_before_commit_raw else None
        logger.info(f"Date before commit: {date_before_commit}")
        
        # Commit
        conn.commit()
        logger.info("Committed test update")
        
        # Verify after commit
        cursor.execute("SELECT updated_date FROM customers WHERE customer_id = ?", (customer_id,))
        date_after_commit_raw = cursor.fetchone()
        date_after_commit = date_after_commit_raw[0] if date_after_commit_raw else None
        logger.info(f"Date after commit: {date_after_commit}")
        
        test1_passed = date_after_commit == test_time
        logger.info(f"Test 1 passed: {test1_passed}")
        
        # Test 2: Sector assignment
        logger.info("=== TEST 2: Sector assignment ===")
        
        # Get a sector to test with - FIXED: Convert row to serializable
        cursor.execute("SELECT sector_id, sector_name FROM sectors WHERE is_active = 1 LIMIT 1")
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
                (test_sector_id, customer_id)
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
                (customer_id,)
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
                "final_sector": final_sector
            }
            
            # Restore original sector
            cursor.execute(
                "UPDATE customers SET sector_id = ? WHERE customer_id = ?",
                (original_sector, customer_id)
            )
            conn.commit()
            logger.info("Restored original sector")
        else:
            logger.warning("No sectors found for testing")
            sector_test_info = {"error": "No sectors available for testing"}
        
        # Restore original timestamp
        cursor.execute(
            "UPDATE customers SET updated_date = CURRENT_TIMESTAMP WHERE customer_id = ?",
            (customer_id,)
        )
        conn.commit()
        
        conn.close()
        
        logger.info("=== DATABASE WRITE TEST END ===")
        
        return jsonify({
            "success": True,
            "message": "Database write test completed",
            "test_results": {
                "test_customer": {
                    "id": customer_id,
                    "name": customer_name,
                    "original_sector": original_sector
                },
                "test_timestamp": test_time,
                "test1_simple_update": {
                    "passed": test1_passed,
                    "rows_affected": rows_affected_1,
                    "date_before_commit": date_before_commit,
                    "date_after_commit": date_after_commit
                },
                "test2_sector_assignment": {
                    "passed": test2_passed,
                    **sector_test_info
                },
                "overall_passed": test1_passed and test2_passed
            },
            "database_info": {
                "connection_type": str(type(db)),
                "database_list": db_info,
                "file_info": file_info
            }
        })
        
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
            db_info.append({
                "seq": row[0],
                "name": row[1], 
                "file": row[2]
            })
        
        # Get basic stats
        cursor.execute("SELECT COUNT(*) FROM customers")
        customer_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM sectors") 
        sector_count = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            "success": True,
            "database_info": db_info,
            "stats": {
                "customers": customer_count,
                "sectors": sector_count
            }
        })
        
    except Exception as e:
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