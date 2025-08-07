from typing import Dict, Optional
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models.spot_category import SpotCategory
from models.language_assignment import LanguageAssignment, LanguageStatus
from services.language_assignment_service import LanguageAssignmentService
from services.spot_categorization_service import SpotCategorizationService

class LanguageProcessingOrchestrator:
    """Orchestrates language processing for different spot categories"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.language_service = LanguageAssignmentService(db_connection)
        self.categorization_service = SpotCategorizationService(db_connection)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def process_language_required_category(self) -> Dict[str, int]:
        """Process all LANGUAGE_ASSIGNMENT_REQUIRED spots"""
        spot_ids = self.language_service.get_spots_by_category(SpotCategory.LANGUAGE_ASSIGNMENT_REQUIRED)
        
        if not spot_ids:
            self.logger.info("No language assignment required spots found")
            return {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        
        return self.language_service.process_language_required_spots(spot_ids)
    
    def process_review_category(self) -> Dict[str, int]:
        """Process all REVIEW_CATEGORY spots"""  
        spot_ids = self.language_service.get_spots_by_category(SpotCategory.REVIEW_CATEGORY)
        
        if not spot_ids:
            self.logger.info("No review category spots found")
            return {"processed": 0, "flagged_for_review": 0, "errors": 0}
        
        return self.language_service.process_review_category_spots(spot_ids)
    
    def process_default_english_category(self) -> Dict[str, int]:
        """Process all DEFAULT_ENGLISH spots"""
        spot_ids = self.language_service.get_spots_by_category(SpotCategory.DEFAULT_ENGLISH)
        
        if not spot_ids:
            self.logger.info("No default English spots found")
            return {"processed": 0, "assigned": 0, "errors": 0}
        
        return self.language_service.process_default_english_spots(spot_ids)
    
    def process_all_categories(self) -> Dict[str, Dict[str, int]]:
        """Process all categories"""
        self.logger.info("Starting processing of all categories...")
        
        results = {}
        
        # Process Language Required (most complex)
        self.logger.info("Processing Language Assignment Required category...")
        results['language_required'] = self.process_language_required_category()
        
        # Process Review Category
        self.logger.info("Processing Review Category...")
        results['review_category'] = self.process_review_category()
        
        # Process Default English (largest volume)
        self.logger.info("Processing Default English category...")
        results['default_english'] = self.process_default_english_category()
        
        # Summary
        total_processed = (
            results['language_required']['processed'] +
            results['review_category']['processed'] + 
            results['default_english']['processed']
        )
        
        results['summary'] = {
            'total_processed': total_processed,
            'language_assigned': results['language_required']['assigned'],
            'flagged_for_review': (
                results['language_required']['review_flagged'] + 
                results['review_category']['flagged_for_review']
            ),
            'default_english_assigned': results['default_english']['assigned'],
            'total_errors': (
                results['language_required']['errors'] +
                results['review_category']['errors'] +
                results['default_english']['errors']
            )
        }
        
        self.logger.info(f"All categories processing complete: {results['summary']}")
        return results
    
    def get_processing_status(self) -> Dict[str, int]:
        """Get status of language processing by category"""
        cursor = self.db.cursor()
        
        # Count processed spots by category
        cursor.execute("""
            SELECT s.spot_category, COUNT(sla.spot_id) as processed_count
            FROM spots s
            LEFT JOIN spot_language_assignments sla ON s.spot_id = sla.spot_id
            WHERE s.spot_category IS NOT NULL
            GROUP BY s.spot_category
        """)
        
        status = {}
        for row in cursor.fetchall():
            category, processed_count = row
            status[f"{category}_processed"] = processed_count
        
        # Get total counts by category
        summary = self.categorization_service.get_category_summary()
        for category_key, total_count in summary.items():
            if category_key != 'uncategorized':
                status[f"{category_key}_total"] = total_count
        
        return status
    def process_batch_categories(self, batch_id: str) -> Dict[str, Dict[str, int]]:
        """Process all categories for a specific import batch"""
        self.logger.info(f"Starting processing of batch {batch_id} categories...")
        results = {}
        
        # Process Language Required (batch-specific)
        self.logger.info(f"Processing Language Assignment Required category for batch {batch_id}...")
        results['language_required'] = self.process_language_required_category_batch(batch_id)
        
        # Process Review Category (batch-specific)  
        self.logger.info(f"Processing Review Category for batch {batch_id}...")
        results['review_category'] = self.process_review_category_batch(batch_id)
        
        # Process Default English (batch-specific)
        self.logger.info(f"Processing Default English category for batch {batch_id}...")
        results['default_english'] = self.process_default_english_category_batch(batch_id)
        
        # Calculate summary
        total_processed = (
            results['language_required']['processed'] +
            results['review_category']['processed'] + 
            results['default_english']['processed']
        )
        
        results['summary'] = {
            'total_processed': total_processed,
            'language_assigned': results['language_required']['assigned'],
            'default_english_assigned': results['default_english']['assigned'],
            'flagged_for_review': results['review_category']['flagged_for_review']
        }
        
        self.logger.info(f"Batch {batch_id} processing complete: {total_processed} spots processed")
        return results

    def process_language_required_category_batch(self, batch_id: str) -> Dict[str, int]:
        """Process LANGUAGE_ASSIGNMENT_REQUIRED spots for specific batch"""
        spot_ids = self.language_service.get_spots_by_category_and_batch(SpotCategory.LANGUAGE_ASSIGNMENT_REQUIRED, batch_id)
        if not spot_ids:
            self.logger.info(f"No language assignment required spots found for batch {batch_id}")
            return {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        return self.language_service.process_language_required_spots(spot_ids)

    def process_review_category_batch(self, batch_id: str) -> Dict[str, int]:
        """Process REVIEW_CATEGORY spots for specific batch"""
        spot_ids = self.language_service.get_spots_by_category_and_batch(SpotCategory.REVIEW_CATEGORY, batch_id)
        if not spot_ids:
            self.logger.info(f"No review category spots found for batch {batch_id}")
            return {"processed": 0, "flagged_for_review": 0, "errors": 0}
        return self.language_service.process_review_spots(spot_ids)

    def process_default_english_category_batch(self, batch_id: str) -> Dict[str, int]:
        """Process DEFAULT_ENGLISH spots for specific batch"""
        spot_ids = self.language_service.get_spots_by_category_and_batch(SpotCategory.DEFAULT_ENGLISH, batch_id)
        if not spot_ids:
            self.logger.info(f"No default English spots found for batch {batch_id}")
            return {"processed": 0, "assigned": 0, "errors": 0}
        return self.language_service.process_default_english_spots(spot_ids)
