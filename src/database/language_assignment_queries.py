from typing import Optional, List
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models.language_assignment import SpotLanguageData, LanguageAssignment

class LanguageAssignmentQueries:
    """Database operations with undetermined language support"""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    def get_spot_language_data(self, spot_id: int) -> Optional[SpotLanguageData]:
        """Get spot data including language code"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT spot_id, language_code, revenue_type, market_id, gross_rate, bill_code
            FROM spots 
            WHERE spot_id = ?
            AND (revenue_type != 'Trade' OR revenue_type IS NULL)
        """, (spot_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        return SpotLanguageData(
            spot_id=row[0],
            language_code=row[1],
            revenue_type=row[2], 
            market_id=row[3],
            gross_rate=row[4],
            bill_code=row[5]
        )
    
    def get_undetermined_language_spots(self, limit: Optional[int] = None) -> List[int]:
        """Get spots with language_code = 'L' (undetermined)"""
        cursor = self.db.cursor()
        query = """
            SELECT spot_id FROM spots s
            WHERE s.language_code = 'L'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.market_id IS NOT NULL
            ORDER BY s.gross_rate DESC, s.spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    
    def get_invalid_language_spots(self, limit: Optional[int] = None) -> List[int]:
        """Get spots with invalid language codes"""
        cursor = self.db.cursor()
        query = """
            SELECT s.spot_id FROM spots s
            LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)
            WHERE s.language_code IS NOT NULL 
            AND s.language_code != 'L'
            AND l.language_id IS NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            ORDER BY s.gross_rate DESC
        """
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    
    def get_high_value_undetermined_spots(self, min_value: float = 1000.0) -> List[int]:
        """Get high-value spots with undetermined language"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT spot_id FROM spots s
            WHERE s.language_code = 'L'
            AND s.gross_rate >= ?
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            ORDER BY s.gross_rate DESC
        """, (min_value,))
        return [row[0] for row in cursor.fetchall()]
    
    def save_language_assignment(self, assignment: LanguageAssignment):
        """Save language assignment with review flags"""
        cursor = self.db.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO spot_language_assignments 
            (spot_id, language_code, language_status, confidence, assignment_method, 
             assigned_date, requires_review, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            assignment.spot_id,
            assignment.language_code,
            assignment.language_status.value,
            assignment.confidence,
            assignment.assignment_method,
            assignment.assigned_date.isoformat(),
            assignment.requires_review,
            assignment.notes
        ))
        self.db.commit()
    
    def get_unassigned_spots(self, limit: Optional[int] = None) -> List[int]:
        """Get spots without language assignments"""
        cursor = self.db.cursor()
        query = """
            SELECT s.spot_id FROM spots s
            LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
            WHERE sla.spot_id IS NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND s.market_id IS NOT NULL
            ORDER BY s.spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]

    def get_all_review_required_spots(self, limit: Optional[int] = None) -> List[int]:
        """Get all spots requiring review (L codes + invalid codes)"""
        cursor = self.db.cursor()
        query = """
            SELECT s.spot_id FROM spots s
            LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)
            WHERE (
                s.language_code = 'L' OR 
                (s.language_code IS NOT NULL AND l.language_id IS NULL)
            )
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            ORDER BY s.gross_rate DESC
        """
        if limit:
            query += f" LIMIT {limit}"
            
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]