from flask import Blueprint, request, jsonify, current_app
from src.services.container import get_container
import logging

customer_sector_bp = Blueprint('customer_sector_api', __name__, url_prefix='/api/customer-sector')
logger = logging.getLogger(__name__)

@customer_sector_bp.route('/customers', methods=['GET'])
def get_customers():
    """Get customers with Internal Ad Sales revenue, excluding WorldLink agency"""
    try:
        container = get_container()
        db = container.get('database_connection')
        
        query = """
        SELECT DISTINCT
            c.customer_id,
            c.normalized_name as name,
            s.sector_name as sector,
            c.updated_date as lastUpdated,
            SUM(sp.gross_rate) as total_revenue,
            COUNT(sp.spot_id) as spot_count
        FROM customers c
        LEFT JOIN sectors s ON c.sector_id = s.sector_id
        LEFT JOIN agencies a ON c.agency_id = a.agency_id
        INNER JOIN spots sp ON c.customer_id = sp.customer_id
        WHERE c.is_active = 1
          AND sp.revenue_type = 'Internal Ad Sales'
          AND (a.agency_name != 'WorldLink' OR a.agency_name IS NULL)
          AND sp.gross_rate > 0
        GROUP BY c.customer_id, c.normalized_name, s.sector_name, c.updated_date
        HAVING COUNT(sp.spot_id) > 0
        ORDER BY c.normalized_name
        """
        
        conn = db.connect()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        customers = []
        for row in rows:
            customers.append({
                'id': row[0],
                'name': row[1],
                'sector': row[2],
                'lastUpdated': str(row[3])[:10] if row[3] else '2025-01-01',
                'totalRevenue': float(row[4]) if row[4] else 0.0,
                'spotCount': row[5] if row[5] else 0
            })
        
        conn.close()
        logger.info(f"Fetched {len(customers)} Internal Ad Sales customers (excluding WorldLink)")
        return jsonify({'success': True, 'data': customers})
        
    except Exception as e:
        logger.error(f"Error fetching customers: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/sectors', methods=['GET'])
def get_sectors():
    """Get all sectors"""
    try:
        container = get_container()
        db = container.get('database_connection')
        
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
        colors = ['#88c0d0', '#81a1c1', '#5e81ac', '#bf616a', '#d08770', '#ebcb8b', '#a3be8c', '#b48ead']
        
        for i, row in enumerate(rows):
            sectors.append({
                'id': row[0],
                'name': row[1],
                'description': row[2],
                'color': colors[i % len(colors)]
            })
        
        conn.close()
        return jsonify({'success': True, 'data': sectors})
        
    except Exception as e:
        logger.error(f"Error fetching sectors: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/customers/<int:customer_id>/sector', methods=['PUT'])
def update_customer_sector(customer_id):
    """Update customer sector assignment"""
    try:
        data = request.get_json()
        sector_name = data.get('sector')
        
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # Get sector_id from sector_name
        sector_id = None
        if sector_name:
            cursor.execute("SELECT sector_id FROM sectors WHERE sector_name = ? AND is_active = 1", (sector_name,))
            result = cursor.fetchone()
            if result:
                sector_id = result[0]
        
        # Update customer
        cursor.execute("""
            UPDATE customers 
            SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
            WHERE customer_id = ?
        """, (sector_id, customer_id))
        
        # Log the sector assignment change for audit trail
        if cursor.rowcount > 0:
            try:
                cursor.execute("""
                    INSERT INTO sector_assignment_audit 
                    (customer_id, new_sector_id, assignment_method, assigned_by)
                    VALUES (?, ?, 'manual_direct', 'web_interface')
                """, (customer_id, sector_id))
            except Exception as audit_error:
                logger.warning(f"Audit logging failed: {audit_error}")
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Customer sector updated successfully'})
        
    except Exception as e:
        logger.error(f"Error updating customer sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/customers/bulk-sector', methods=['PUT'])
def bulk_update_sectors():
    """Bulk update customer sectors"""
    try:
        data = request.get_json()
        customer_ids = data.get('customer_ids', [])
        sector_name = data.get('sector')
        
        if not customer_ids:
            return jsonify({'success': False, 'error': 'No customers selected'}), 400
        
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # Get sector_id from sector_name
        sector_id = None
        if sector_name:
            cursor.execute("SELECT sector_id FROM sectors WHERE sector_name = ? AND is_active = 1", (sector_name,))
            result = cursor.fetchone()
            if result:
                sector_id = result[0]
        
        # Update customers
        updated_count = 0
        for customer_id in customer_ids:
            cursor.execute("""
                UPDATE customers 
                SET sector_id = ?, updated_date = CURRENT_TIMESTAMP
                WHERE customer_id = ?
            """, (sector_id, customer_id))
            
            if cursor.rowcount > 0:
                updated_count += 1
                # Log the sector assignment change for audit trail
                try:
                    cursor.execute("""
                        INSERT INTO sector_assignment_audit 
                        (customer_id, new_sector_id, assignment_method, assigned_by)
                        VALUES (?, ?, 'bulk_update', 'web_interface')
                    """, (customer_id, sector_id))
                except Exception as audit_error:
                    logger.warning(f"Audit logging failed for customer {customer_id}: {audit_error}")
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'{updated_count} customers updated successfully'
        })
        
    except Exception as e:
        logger.error(f"Error bulk updating sectors: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/sectors', methods=['POST'])
def add_sector():
    """Add a new sector"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        if not name:
            return jsonify({'success': False, 'error': 'Sector name is required'}), 400
        
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # Check if sector already exists
        cursor.execute("SELECT sector_id FROM sectors WHERE sector_name = ?", (name,))
        if cursor.fetchone():
            return jsonify({'success': False, 'error': 'Sector already exists'}), 400
        
        # Insert new sector
        cursor.execute("""
            INSERT INTO sectors (sector_code, sector_name, sector_group, is_active)
            VALUES (?, ?, ?, 1)
        """, (name.upper().replace(' ', '_'), name, description))
        
        conn.commit()
        sector_id = cursor.lastrowid
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': 'Sector added successfully',
            'data': {
                'id': sector_id,
                'name': name,
                'description': description
            }
        })
        
    except Exception as e:
        logger.error(f"Error adding sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/sectors/<int:sector_id>', methods=['PUT'])
def update_sector(sector_id):
    """Update an existing sector"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        description = data.get('description', '').strip()
        
        if not name:
            return jsonify({'success': False, 'error': 'Sector name is required'}), 400
        
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # Update sector
        cursor.execute("""
            UPDATE sectors 
            SET sector_name = ?, sector_group = ?, updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ?
        """, (name, description, sector_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Sector updated successfully'})
        
    except Exception as e:
        logger.error(f"Error updating sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/sectors/<int:sector_id>', methods=['PUT'])
def update_sector_name(sector_id):
    """Update sector name and group"""
    try:
        data = request.get_json()
        name = data.get('name', '').strip()
        group = data.get('group', '').strip()
        
        if not name:
            return jsonify({'success': False, 'error': 'Sector name is required'}), 400
        
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # Check if another sector already has this name
        cursor.execute(
            "SELECT sector_id FROM sectors WHERE sector_name = ? AND sector_id != ? AND is_active = 1", 
            (name, sector_id)
        )
        if cursor.fetchone():
            return jsonify({'success': False, 'error': 'A sector with this name already exists'}), 400
        
        # Update sector
        cursor.execute("""
            UPDATE sectors 
            SET sector_name = ?, 
                sector_group = ?, 
                sector_code = UPPER(REPLACE(?, ' ', '_')),
                updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ? AND is_active = 1
        """, (name, group, name, sector_id))
        
        if cursor.rowcount == 0:
            return jsonify({'success': False, 'error': 'Sector not found or inactive'}), 404
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'Sector updated to "{name}"',
            'data': {
                'id': sector_id,
                'name': name,
                'group': group
            }
        })
        
    except Exception as e:
        logger.error(f"Error updating sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@customer_sector_bp.route('/sectors/<int:sector_id>', methods=['DELETE'])
def delete_sector(sector_id):
    """Delete a sector (unassigns customers first)"""
    try:
        container = get_container()
        db = container.get('database_connection')
        conn = db.connect()
        cursor = conn.cursor()
        
        # First, unassign all customers from this sector
        cursor.execute("""
            UPDATE customers 
            SET sector_id = NULL, updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ?
        """, (sector_id,))
        
        # Then deactivate the sector (don't hard delete)
        cursor.execute("""
            UPDATE sectors 
            SET is_active = 0, updated_date = CURRENT_TIMESTAMP
            WHERE sector_id = ?
        """, (sector_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Sector deleted successfully'})
        
    except Exception as e:
        logger.error(f"Error deleting sector: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

