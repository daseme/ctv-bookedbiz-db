from typing import List, Dict, Optional
import logging
import sys
import os
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.models.spot_category import SpotCategory, categorize_spot

class SpotCategorizationService:
    """Service for categorizing spots based on business rules"""
    
    def __init__(self, db_connection):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def _decide_category(self, revenue_type: Optional[str], spot_type: Optional[str], language_code: Optional[str]) -> SpotCategory:
        t = (spot_type or "").strip().upper()
        code = (language_code or "").strip().upper()

        # Rule A: explicit undetermined -> REVIEW
        if code == "L":
            return SpotCategory.REVIEW_CATEGORY

        # Rule B: COM/BB with an actual language present -> avoid review bin
        # (send through normal assignment path; DO NOT default to English here)
        if t in {"COM", "BB"} and code != "":
            return SpotCategory.LANGUAGE_ASSIGNMENT_REQUIRED

        # Rule C: COM/BB with missing language -> default English path (no review)
        if t in {"COM", "BB"} and code == "":
            return SpotCategory.DEFAULT_ENGLISH

        # Fallback to your legacy business rules
        return categorize_spot(revenue_type, spot_type)

    
    def categorize_spot_by_id(self, spot_id: int) -> Optional[SpotCategory]:
        cursor = self.db.cursor()
        cursor.execute("""
            SELECT revenue_type, spot_type, language_code
            FROM spots
            WHERE spot_id = ?
        """, (spot_id,))
        row = cursor.fetchone()
        if not row:
            return None
        revenue_type, spot_type, language_code = row
        return self._decide_category(revenue_type, spot_type, language_code)

    
    def categorize_spots_batch(self, spot_ids: List[int]) -> Dict[int, SpotCategory]:
        if not spot_ids:
            return {}
        placeholders = ",".join(["?"] * len(spot_ids))
        cursor = self.db.cursor()
        cursor.execute(f"""
            SELECT spot_id, revenue_type, spot_type, language_code
            FROM spots
            WHERE spot_id IN ({placeholders})
        """, spot_ids)

        results = {}
        for spot_id, revenue_type, spot_type, language_code in cursor.fetchall():
            results[spot_id] = self._decide_category(revenue_type, spot_type, language_code)
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
        """Categorize all uncategorized spots in batches (with progress bar)."""
        uncategorized_spots = self.get_uncategorized_spots()
        if not uncategorized_spots:
            return {"processed": 0, "categorized": 0}

        self.logger.info(f"Categorizing {len(uncategorized_spots):,} uncategorized spots...")

        total_processed = 0
        total_categorized = 0
        disable_bar = not sys.stderr.isatty()  # auto-disable in non-TTY (cron/log file)

        with tqdm(
            total=len(uncategorized_spots),
            desc="Categorizing",
            unit="spot",
            dynamic_ncols=True,
            mininterval=0.3,
            disable=disable_bar,
        ) as pbar:
            for i in range(0, len(uncategorized_spots), batch_size):
                batch = uncategorized_spots[i : i + batch_size]
                try:
                    categorizations = self.categorize_spots_batch(batch)
                    saved_count = self.save_spot_categories(categorizations)
                except Exception as e:
                    # keep the bar smooth; log and continue
                    self.logger.warning(f"Batch {i//batch_size+1} error: {e}")
                    saved_count = 0

                total_processed += len(batch)
                total_categorized += saved_count

                pbar.update(len(batch))
                pbar.set_postfix(categorized=total_categorized)

        self.logger.info(f"Categorization complete: {total_categorized:,} spots categorized")
        return {"processed": total_processed, "categorized": total_categorized}

    
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