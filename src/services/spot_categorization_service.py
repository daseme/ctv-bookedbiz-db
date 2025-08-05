from typing import List, Dict, Optional
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from models.spot_category import SpotCategory, categorize_spot

class SpotCategorizationService:
    """Service for categorizing spots based on business rules"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def categorize_spot_by_id(self, spot_id: int) -> Optional[SpotCategory]:
        """Categorize a single spot by ID"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT revenue_type, spot_type 
            FROM spots 
            WHERE spot_id = ?
        """, (spot_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        revenue_type, spot_type = row
        return categorize_spot(revenue_type, spot_type)
    
    def categorize_spots_batch(self, spot_ids: List[int]) -> Dict[int, SpotCategory]:
        """Categorize multiple spots by ID"""
        if not spot_ids:
            return {}
        
        # Build query with placeholders
        placeholders = ",".join(["?"] * len(spot_ids))
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT spot_id, revenue_type, spot_type 
            FROM spots 
            WHERE spot_id IN ({placeholders})
        """, spot_ids)
        
        results = {}
        for row in cursor.fetchall():
            spot_id, revenue_type, spot_type = row
            results[spot_id] = categorize_spot(revenue_type, spot_type)
        
        return results
    
    def get_uncategorized_spots(self, limit: Optional[int] = None) -> List[int]:
        """Get spots that haven't been categorized yet"""
        cursor = self.db.cursor()
        query = """
            SELECT spot_id FROM spots 
            WHERE spot_category IS NULL
            ORDER BY spot_id
        """
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        return [row[0] for row in cursor.fetchall()]
    
    def save_spot_categories(self, categorizations: Dict[int, SpotCategory]) -> int:
        """Save categorizations to database"""
        if not categorizations:
            return 0
        
        cursor = self.db.cursor()
        updates = [(category.value, spot_id) for spot_id, category in categorizations.items()]
        
        cursor.executemany("""
            UPDATE spots 
            SET spot_category = ? 
            WHERE spot_id = ?
        """, updates)
        
        self.db.commit()
        return cursor.rowcount
    
    def categorize_all_uncategorized(self, batch_size: int = 5000) -> Dict[str, int]:
        """Categorize all uncategorized spots in batches"""
        uncategorized_spots = self.get_uncategorized_spots()
        
        if not uncategorized_spots:
            return {"processed": 0, "categorized": 0}
        
        self.logger.info(f"Categorizing {len(uncategorized_spots):,} uncategorized spots...")
        
        total_processed = 0
        total_categorized = 0
        
        # Process in batches
        for i in range(0, len(uncategorized_spots), batch_size):
            batch = uncategorized_spots[i:i + batch_size]
            
            # Categorize batch
            categorizations = self.categorize_spots_batch(batch)
            
            # Save to database
            saved_count = self.save_spot_categories(categorizations)
            
            total_processed += len(batch)
            total_categorized += saved_count
            
            if (i + batch_size) % (batch_size * 5) == 0:  # Log every 5 batches
                self.logger.info(f"Processed {total_processed:,}/{len(uncategorized_spots):,} spots...")
        
        self.logger.info(f"Categorization complete: {total_categorized:,} spots categorized")
        
        return {
            "processed": total_processed,
            "categorized": total_categorized
        }
    
    def get_category_summary(self) -> Dict[str, int]:
        """Get summary of spots by category"""
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT 
                spot_category,
                COUNT(*) as count
            FROM spots 
            WHERE spot_category IS NOT NULL
            GROUP BY spot_category
        """)
        
        summary = {}
        for row in cursor.fetchall():
            category, count = row
            summary[category] = count
        
        # Add uncategorized count
        cursor.execute("SELECT COUNT(*) FROM spots WHERE spot_category IS NULL")
        summary['uncategorized'] = cursor.fetchone()[0]
        
        return summary