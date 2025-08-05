from typing import Dict, List, Optional
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models.language_assignment import LanguageAssignment, LanguageStatus
from database.language_assignment_queries import LanguageAssignmentQueries
from models.spot_category import SpotCategory

class LanguageAssignmentService:
    """Language assignment with undetermined language detection"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.queries = LanguageAssignmentQueries(db_connection)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Valid language codes (from your languages table)
        self.valid_language_codes = self._load_valid_language_codes()
    
    def assign_spot_language(self, spot_id: int) -> LanguageAssignment:
        """Assign language with undetermined language detection"""
        spot_data = self.queries.get_spot_language_data(spot_id)
        
        if not spot_data:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code='english',
                language_status=LanguageStatus.INVALID,
                confidence=0.0,
                assignment_method='error_fallback',
                requires_review=True,
                notes='Spot data not found'
            )
        
        # Handle missing language code
        if not spot_data.language_code:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code='english',
                language_status=LanguageStatus.DEFAULT,
                confidence=0.5,
                assignment_method='default_english',
                requires_review=False,
                notes='No language code provided, defaulted to English'
            )
        
        # Special case: 'L' is always undetermined
        if spot_data.language_code == 'L':
            return LanguageAssignment(
                spot_id=spot_id,
                language_code='L',
                language_status=LanguageStatus.UNDETERMINED,
                confidence=0.0,
                assignment_method='undetermined_flagged',
                requires_review=True,
                notes='Language not determined - requires manual review'
            )
        
        # Check if language code exists in languages table
        if spot_data.language_code.upper() in self.valid_language_codes:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code=spot_data.language_code.lower(),
                language_status=LanguageStatus.DETERMINED,
                confidence=1.0,
                assignment_method='direct_mapping',
                requires_review=False,
                notes=None
            )
        
        # Any language code NOT in the languages table requires review
        return LanguageAssignment(
            spot_id=spot_id,
            language_code=spot_data.language_code,
            language_status=LanguageStatus.INVALID,
            confidence=0.0,
            assignment_method='invalid_code_flagged',
            requires_review=True,
            notes=f'Language code "{spot_data.language_code}" not found in languages table - requires manual review'
        )
    
    def batch_assign_languages(self, spot_ids: List[int]) -> Dict[int, LanguageAssignment]:
        """Batch assign languages to multiple spots"""
        results = {}
        for i, spot_id in enumerate(spot_ids):
            try:
                results[spot_id] = self.assign_spot_language(spot_id)
                if (i + 1) % 100 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(spot_ids)} spots...")
            except Exception as e:
                self.logger.error(f"Error processing spot {spot_id}: {e}")
                results[spot_id] = LanguageAssignment(
                    spot_id=spot_id,
                    language_code='error',
                    language_status=LanguageStatus.INVALID,
                    confidence=0.0,
                    assignment_method='error',
                    requires_review=True,
                    notes=f'Processing error: {str(e)}'
                )
        return results
    
    def get_review_required_spots(self, limit: Optional[int] = None) -> List[LanguageAssignment]:
        """Get all spots requiring review (L codes + invalid codes)"""
        review_spot_ids = self.queries.get_all_review_required_spots(limit)
        return [self.assign_spot_language(spot_id) for spot_id in review_spot_ids]

    def get_review_summary(self) -> Dict[str, int]:
        """Get summary of all spots requiring manual review"""
        cursor = self.db.cursor()
        
        # Count L codes (undetermined)
        cursor.execute("""
            SELECT COUNT(*) FROM spots s
            WHERE s.language_code = 'L'
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """)
        undetermined_count = cursor.fetchone()[0]
        
        # Count invalid codes (not in languages table, excluding L)
        cursor.execute("""
            SELECT COUNT(*) FROM spots s
            LEFT JOIN languages l ON UPPER(s.language_code) = UPPER(l.language_code)
            WHERE s.language_code IS NOT NULL 
            AND s.language_code != 'L'
            AND l.language_id IS NULL
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """)
        invalid_count = cursor.fetchone()[0]
        
        # Count high-value undetermined (L codes with high revenue)
        cursor.execute("""
            SELECT COUNT(*) FROM spots s
            WHERE s.language_code = 'L'
            AND s.gross_rate >= 1000
            AND (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
        """)
        high_value_undetermined = cursor.fetchone()[0]
        
        return {
            'undetermined_language': undetermined_count,
            'invalid_codes': invalid_count,
            'total_review_required': undetermined_count + invalid_count,
            'high_value_undetermined': high_value_undetermined
        }
    
    def _load_valid_language_codes(self) -> set:
        """Load valid language codes from database"""
        cursor = self.db.cursor()
        cursor.execute("SELECT UPPER(language_code) FROM languages WHERE language_code IS NOT NULL")
        return {row[0] for row in cursor.fetchall()}

    def get_spots_by_category(self, category: SpotCategory, limit: Optional[int] = None) -> List[int]:
        """Get spot IDs by category"""
        cursor = self.db.cursor()
        query = """
            SELECT spot_id FROM spots 
            WHERE spot_category = ?
            ORDER BY spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, (category.value,))
        return [row[0] for row in cursor.fetchall()]

    def process_language_required_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots requiring language assignment"""
        self.logger.info(f"Processing {len(spot_ids):,} language assignment required spots...")
        
        results = {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        
        for i, spot_id in enumerate(spot_ids):
            try:
                # Use existing assign_spot_language logic
                assignment = self.assign_spot_language(spot_id)
                
                # Save assignment
                self.queries.save_language_assignment(assignment)
                
                if assignment.requires_review:
                    results["review_flagged"] += 1
                else:
                    results["assigned"] += 1
                    
                results["processed"] += 1
                
                # Progress logging
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1:,}/{len(spot_ids):,} language required spots...")
                    
            except Exception as e:
                self.logger.error(f"Error processing language required spot {spot_id}: {e}")
                results["errors"] += 1
        
        return results

    def process_review_category_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots in review category - flag for business review"""
        self.logger.info(f"Processing {len(spot_ids):,} review category spots...")
        
        assignments = []
        for spot_id in spot_ids:
            assignment = LanguageAssignment(
                spot_id=spot_id,
                language_code='english',  # Default
                language_status=LanguageStatus.DEFAULT,
                confidence=0.5,
                assignment_method='business_review_required',
                requires_review=True,
                notes='Spot requires business review - revenue type/spot type combination needs manual evaluation'
            )
            assignments.append(assignment)
        
        # Save all assignments
        saved_count = 0
        for assignment in assignments:
            try:
                self.queries.save_language_assignment(assignment)
                saved_count += 1
            except Exception as e:
                self.logger.error(f"Error saving review category assignment for spot {assignment.spot_id}: {e}")
        
        return {"processed": len(spot_ids), "flagged_for_review": saved_count, "errors": len(spot_ids) - saved_count}

    def process_default_english_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots that default to English - no language assignment needed"""
        self.logger.info(f"Processing {len(spot_ids):,} default English spots...")
        
        batch_size = 3000
        total_saved = 0
        total_errors = 0
        
        for i in range(0, len(spot_ids), batch_size):
            batch = spot_ids[i:i + batch_size]
            
            # Create assignments for batch
            assignments = []
            for spot_id in batch:
                assignment = LanguageAssignment(
                    spot_id=spot_id,
                    language_code='english',
                    language_status=LanguageStatus.DETERMINED,
                    confidence=1.0,
                    assignment_method='business_rule_default_english',
                    requires_review=False,
                    notes='Default English by business rule - no language assignment required'
                )
                assignments.append(assignment)
            
            # Save batch
            for assignment in assignments:
                try:
                    self.queries.save_language_assignment(assignment)
                    total_saved += 1
                except Exception as e:
                    self.logger.error(f"Error saving default English assignment for spot {assignment.spot_id}: {e}")
                    total_errors += 1
            
            # Progress logging
            if (i + batch_size) % (batch_size * 5) == 0:
                self.logger.info(f"Processed {total_saved:,}/{len(spot_ids):,} default English spots...")
        
        return {"processed": len(spot_ids), "assigned": total_saved, "errors": total_errors}
    def get_spots_by_category(self, category, limit: Optional[int] = None) -> List[int]:
        """Get spot IDs by category"""
        cursor = self.db.cursor()
        query = """
            SELECT spot_id FROM spots 
            WHERE spot_category = ?
            ORDER BY spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, (category.value,))
        return [row[0] for row in cursor.fetchall()]

    def process_language_required_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots requiring language assignment"""
        self.logger.info(f"Processing {len(spot_ids):,} language assignment required spots...")
        
        results = {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        
        for i, spot_id in enumerate(spot_ids):
            try:
                # Use existing assign_spot_language logic
                assignment = self.assign_spot_language(spot_id)
                
                # Save assignment
                self.queries.save_language_assignment(assignment)
                
                if assignment.requires_review:
                    results["review_flagged"] += 1
                else:
                    results["assigned"] += 1
                    
                results["processed"] += 1
                
                # Progress logging
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1:,}/{len(spot_ids):,} language required spots...")
                    
            except Exception as e:
                self.logger.error(f"Error processing language required spot {spot_id}: {e}")
                results["errors"] += 1
        
        return results

    def process_review_category_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots in review category - flag for business review"""
        self.logger.info(f"Processing {len(spot_ids):,} review category spots...")
        
        from models.language_assignment import LanguageAssignment, LanguageStatus
        
        assignments = []
        for spot_id in spot_ids:
            assignment = LanguageAssignment(
                spot_id=spot_id,
                language_code='english',  # Default
                language_status=LanguageStatus.DEFAULT,
                confidence=0.5,
                assignment_method='business_review_required',
                requires_review=True,
                notes='Spot requires business review - revenue type/spot type combination needs manual evaluation'
            )
            assignments.append(assignment)
        
        # Save all assignments
        saved_count = 0
        for assignment in assignments:
            try:
                self.queries.save_language_assignment(assignment)
                saved_count += 1
            except Exception as e:
                self.logger.error(f"Error saving review category assignment for spot {assignment.spot_id}: {e}")
        
        return {"processed": len(spot_ids), "flagged_for_review": saved_count, "errors": len(spot_ids) - saved_count}

    def process_default_english_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots that default to English - no language assignment needed"""
        self.logger.info(f"Processing {len(spot_ids):,} default English spots...")
        
        from models.language_assignment import LanguageAssignment, LanguageStatus
        
        batch_size = 3000
        total_saved = 0
        total_errors = 0
        
        for i in range(0, len(spot_ids), batch_size):
            batch = spot_ids[i:i + batch_size]
            
            # Create assignments for batch
            assignments = []
            for spot_id in batch:
                assignment = LanguageAssignment(
                    spot_id=spot_id,
                    language_code='english',
                    language_status=LanguageStatus.DETERMINED,
                    confidence=1.0,
                    assignment_method='business_rule_default_english',
                    requires_review=False,
                    notes='Default English by business rule - no language assignment required'
                )
                assignments.append(assignment)
            
            # Save batch
            for assignment in assignments:
                try:
                    self.queries.save_language_assignment(assignment)
                    total_saved += 1
                except Exception as e:
                    self.logger.error(f"Error saving default English assignment for spot {assignment.spot_id}: {e}")
                    total_errors += 1
            
            # Progress logging
            if (i + batch_size) % (batch_size * 5) == 0:
                self.logger.info(f"Processed {total_saved:,}/{len(spot_ids):,} default English spots...")
        
        return {"processed": len(spot_ids), "assigned": total_saved, "errors": total_errors}

    def get_spots_by_category(self, category, limit: Optional[int] = None) -> List[int]:
        """Get spot IDs by category"""
        cursor = self.db.cursor()
        query = """
            SELECT spot_id FROM spots 
            WHERE spot_category = ?
            ORDER BY spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, (category.value,))
        return [row[0] for row in cursor.fetchall()]

    def process_language_required_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots requiring language assignment"""
        self.logger.info(f"Processing {len(spot_ids):,} language assignment required spots...")
        
        results = {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        
        for i, spot_id in enumerate(spot_ids):
            try:
                assignment = self.assign_spot_language(spot_id)
                self.queries.save_language_assignment(assignment)
                
                if assignment.requires_review:
                    results["review_flagged"] += 1
                else:
                    results["assigned"] += 1
                    
                results["processed"] += 1
                
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1:,}/{len(spot_ids):,} language required spots...")
                    
            except Exception as e:
                self.logger.error(f"Error processing language required spot {spot_id}: {e}")
                results["errors"] += 1
        
        return results

    def process_review_category_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots in review category - flag for business review"""
        self.logger.info(f"Processing {len(spot_ids):,} review category spots...")
        
        from models.language_assignment import LanguageAssignment, LanguageStatus
        
        saved_count = 0
        errors = 0
        
        for i, spot_id in enumerate(spot_ids):
            try:
                assignment = LanguageAssignment(
                    spot_id=spot_id,
                    language_code='english',
                    language_status=LanguageStatus.DEFAULT,
                    confidence=0.5,
                    assignment_method='business_review_required',
                    requires_review=True,
                    notes='Spot requires business review - revenue type/spot type combination needs manual evaluation'
                )
                
                self.queries.save_language_assignment(assignment)
                saved_count += 1
                
                # Progress logging
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1:,}/{len(spot_ids):,} review category spots...")
                
            except Exception as e:
                self.logger.error(f"Error saving review category assignment for spot {spot_id}: {e}")
                errors += 1
        
        return {"processed": len(spot_ids), "flagged_for_review": saved_count, "errors": errors}

    def process_default_english_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        """Process spots that default to English - no language assignment needed"""
        self.logger.info(f"Processing {len(spot_ids):,} default English spots...")
        
        from models.language_assignment import LanguageAssignment, LanguageStatus
        
        batch_size = 3000
        total_saved = 0
        total_errors = 0
        
        for i in range(0, len(spot_ids), batch_size):
            batch = spot_ids[i:i + batch_size]
            
            for spot_id in batch:
                try:
                    assignment = LanguageAssignment(
                        spot_id=spot_id,
                        language_code='english',
                        language_status=LanguageStatus.DETERMINED,
                        confidence=1.0,
                        assignment_method='business_rule_default_english',
                        requires_review=False,
                        notes='Default English by business rule - no language assignment required'
                    )
                    
                    self.queries.save_language_assignment(assignment)
                    total_saved += 1
                    
                except Exception as e:
                    self.logger.error(f"Error saving default English assignment for spot {spot_id}: {e}")
                    total_errors += 1
            
            # Progress logging every 15,000 spots (5 batches)
            if (i + batch_size) % (batch_size * 5) == 0:
                self.logger.info(f"Processed {total_saved:,}/{len(spot_ids):,} default English spots...")
        
        return {"processed": len(spot_ids), "assigned": total_saved, "errors": total_errors}
