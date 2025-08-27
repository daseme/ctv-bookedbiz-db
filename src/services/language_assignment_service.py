from typing import Dict, List, Optional
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.models.language_assignment import LanguageAssignment, LanguageStatus
from src.database.language_assignment_queries import LanguageAssignmentQueries
from src.models.spot_category import SpotCategory

class LanguageAssignmentService:
    """Language assignment with undetermined language detection"""

    def __init__(self, db_connection):
        self.db = db_connection
        self.queries = LanguageAssignmentQueries(db_connection)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.valid_language_codes = self._load_valid_language_codes()
        self._english_code_cache: Optional[str] = None

    # ---------- helpers ----------

    @staticmethod
    def _get(row, key, default=None):
        if row is None:
            return default
        if hasattr(row, "get"):  # dict-like
            return row.get(key, default)
        try:                     # sqlite3.Row
            return row[key]
        except Exception:
            return getattr(row, key, default)

    def _english_code(self) -> str:
        """Prefer code from table; fallback to EN."""
        if self._english_code_cache:
            return self._english_code_cache
        cur = self.db.cursor()
        cur.execute("""
            SELECT language_code
            FROM languages
            WHERE UPPER(language_name) = 'ENGLISH'
               OR UPPER(language_code) IN ('EN','ENG')
            LIMIT 1
        """)
        row = cur.fetchone()
        code = (row[0] if row else "EN").upper()
        self._english_code_cache = code
        return code

    # ---------- core ----------

    def assign_spot_language(self, spot_id: int) -> LanguageAssignment:
        """Assign language with undetermined language detection (COM/BB override)."""
        sd = self.queries.get_spot_language_data(spot_id)
        if not sd:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code=self._english_code(),
                language_status=LanguageStatus.INVALID,
                confidence=0.0,
                assignment_method='error_fallback',
                requires_review=True,
                notes='Spot data not found',
            )

        code = self._get(sd, "language_code")
        code_u = str(code).strip().upper() if code is not None else None
        effective_type = (self._get(sd, "spot_type", "") or "").upper()

        # COM/BB: if missing or 'L', auto-default to English (no review)
        if effective_type in {"COM", "BB"} and (not code_u or code_u == "L"):
            return LanguageAssignment(
                spot_id=spot_id,
                language_code=self._english_code(),
                language_status=LanguageStatus.DETERMINED,
                confidence=1.0,
                assignment_method="auto_default_com_bb",
                requires_review=False,
                notes="COM/BB auto-default to English",
            )

        # Missing language code → default English (general rule)
        if not code_u:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code=self._english_code(),
                language_status=LanguageStatus.DEFAULT,
                confidence=0.5,
                assignment_method='default_english',
                requires_review=False,
                notes='No language code provided, defaulted to English',
            )

        # 'L' is undetermined (unless handled by COM/BB above)
        if code_u == 'L':
            return LanguageAssignment(
                spot_id=spot_id,
                language_code='L',
                language_status=LanguageStatus.UNDETERMINED,
                confidence=0.0,
                assignment_method='undetermined_flagged',
                requires_review=True,
                notes='Language not determined - requires manual review',
            )

        # Valid code present?
        if code_u in self.valid_language_codes:
            return LanguageAssignment(
                spot_id=spot_id,
                language_code=code_u,  # store canonical uppercase
                language_status=LanguageStatus.DETERMINED,
                confidence=1.0,
                assignment_method='direct_mapping',
                requires_review=False,
                notes=None,
            )

        # Anything else is invalid → review
        return LanguageAssignment(
            spot_id=spot_id,
            language_code=str(code) if code is not None else "",
            language_status=LanguageStatus.INVALID,
            confidence=0.0,
            assignment_method='invalid_code_flagged',
            requires_review=True,
            notes=f'Language code "{code}" not found in languages table - requires manual review',
        )

    def batch_assign_languages(self, spot_ids: List[int]) -> Dict[int, LanguageAssignment]:
        results: Dict[int, LanguageAssignment] = {}
        for i, sid in enumerate(spot_ids):
            try:
                results[sid] = self.assign_spot_language(sid)
                if (i + 1) % 100 == 0:
                    self.logger.info(f"Processed {i + 1}/{len(spot_ids)} spots...")
            except Exception as e:
                self.logger.error(f"Error processing spot {sid}: {e}")
                results[sid] = LanguageAssignment(
                    spot_id=sid,
                    language_code='ERROR',
                    language_status=LanguageStatus.INVALID,
                    confidence=0.0,
                    assignment_method='error',
                    requires_review=True,
                    notes=f'Processing error: {e}',
                )
        return results

    def get_review_required_spots(self, limit: Optional[int] = None) -> List[LanguageAssignment]:
        """Return only spots that *still* require review after applying rules."""
        ids = self.queries.get_all_review_required_spots(limit)
        out = []
        for sid in ids:
            a = self.assign_spot_language(sid)
            if a.requires_review:
                out.append(a)
        return out

    def get_review_summary(self) -> Dict[str, int]:
        """Summary of spots requiring manual review, excluding COM/BB spot types."""
        cur = self.db.cursor()
        cur.execute("""
            SELECT
                SUM(CASE WHEN s.language_code = 'L' THEN 1 ELSE 0 END),
                SUM(CASE WHEN s.language_code = 'L' AND s.gross_rate >= 1000 THEN 1 ELSE 0 END),
                SUM(CASE WHEN s.language_code IS NOT NULL
                            AND s.language_code != 'L'
                            AND l.language_id IS NULL
                        THEN 1 ELSE 0 END)
            FROM spots s
            LEFT JOIN languages l ON s.language_code = l.language_code
            WHERE (s.revenue_type != 'Trade' OR s.revenue_type IS NULL)
            AND UPPER(COALESCE(s.spot_type, '')) NOT IN ('COM','BB')
        """)
        row = cur.fetchone() or (0, 0, 0)
        undetermined_count = int(row[0] or 0)
        high_value_undetermined = int(row[1] or 0)
        invalid_count = int(row[2] or 0)
        return {
            "undetermined_language": undetermined_count,
            "invalid_codes": invalid_count,
            "total_review_required": undetermined_count + invalid_count,
            "high_value_undetermined": high_value_undetermined,
        }

    def _load_valid_language_codes(self) -> set:
        cur = self.db.cursor()
        cur.execute("SELECT UPPER(language_code) FROM languages WHERE language_code IS NOT NULL")
        return {r[0] for r in cur.fetchall()}

    # ---- category APIs (single, de-duped versions) ----

    def get_spots_by_category(self, category: SpotCategory, limit: Optional[int] = None) -> List[int]:
        cur = self.db.cursor()
        q = "SELECT spot_id FROM spots WHERE spot_category = ? ORDER BY spot_id"
        if limit:
            q += " LIMIT ?"
            cur.execute(q, (category.value, limit))
        else:
            cur.execute(q, (category.value,))
        return [r[0] for r in cur.fetchall()]

    def process_language_required_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        self.logger.info(f"Processing {len(spot_ids):,} language assignment required spots...")
        res = {"processed": 0, "assigned": 0, "errors": 0, "review_flagged": 0}
        for i, sid in enumerate(spot_ids):
            try:
                a = self.assign_spot_language(sid)
                self.queries.save_language_assignment(a)
                res["processed"] += 1
                if a.requires_review:
                    res["review_flagged"] += 1
                else:
                    res["assigned"] += 1
                if (i + 1) % 1000 == 0:
                    self.logger.info(f"Processed {i + 1:,}/{len(spot_ids):,} language required spots...")
            except Exception as e:
                self.logger.error(f"Error processing language required spot {sid}: {e}")
                res["errors"] += 1
        return res

    def process_review_category_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        self.logger.info(f"Processing {len(spot_ids):,} review category spots...")
        saved = errors = flagged = 0
        for sid in spot_ids:
            try:
                a = self.assign_spot_language(sid)  # applies COM/BB + 'L' + validation

                if a.requires_review:
                    # Keep specific reasons (L or invalid) as-is; only generalize to business_review_required otherwise
                    if a.language_status not in (LanguageStatus.UNDETERMINED, LanguageStatus.INVALID):
                        a = LanguageAssignment(
                            spot_id=sid,
                            language_code=self._english_code(),           # or keep the spot code if you prefer
                            language_status=LanguageStatus.DEFAULT,
                            confidence=0.5,
                            assignment_method="business_review_required",
                            requires_review=True,
                            notes="Spot requires business review - revenue type/spot type combination needs manual evaluation",
                        )
                    flagged += 1

                self.queries.save_language_assignment(a)
                saved += 1

            except Exception as e:
                self.logger.debug(f"Error saving review category assignment for spot {sid}: {e}")
                errors += 1

        return {"processed": len(spot_ids), "flagged_for_review": flagged, "errors": errors}



    def process_default_english_spots(self, spot_ids: List[int]) -> Dict[str, int]:
        self.logger.info(f"Processing {len(spot_ids):,} default English spots...")
        batch_size = 3000
        saved = errors = 0
        code = self._english_code()
        for i in range(0, len(spot_ids), batch_size):
            for sid in spot_ids[i:i + batch_size]:
                try:
                    a = LanguageAssignment(
                        spot_id=sid,
                        language_code=code,
                        language_status=LanguageStatus.DETERMINED,
                        confidence=1.0,
                        assignment_method='business_rule_default_english',
                        requires_review=False,
                        notes='Default English by business rule - no language assignment required',
                    )
                    self.queries.save_language_assignment(a)
                    saved += 1
                except Exception as e:
                    self.logger.error(f"Error saving default English assignment for spot {sid}: {e}")
                    errors += 1
            if (i + batch_size) % (batch_size * 5) == 0:
                self.logger.info(f"Processed {saved:,}/{len(spot_ids):,} default English spots...")
        return {"processed": len(spot_ids), "assigned": saved, "errors": errors}

    def get_spots_by_category_and_batch(self, category, batch_id: str, limit: Optional[int] = None) -> List[int]:
        cur = self.db.cursor()
        q = """
            SELECT spot_id FROM spots
            WHERE spot_category = ? AND import_batch_id = ?
            ORDER BY spot_id
        """
        if limit:
            q += " LIMIT ?"
            cur.execute(q, (category.value if hasattr(category, 'value') else category, batch_id, limit))
        else:
            cur.execute(q, (category.value if hasattr(category, 'value') else category, batch_id))
        return [r[0] for r in cur.fetchall()]
