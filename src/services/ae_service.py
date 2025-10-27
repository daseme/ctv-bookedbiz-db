"""AE Service - Handles Account Executive operations."""

import json
import os
import sqlite3
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class AEService:
    """Service for Account Executive operations."""

    def __init__(self, db_path: str, config_path: str):
        """Initialize with database and config paths."""
        self.db_path = db_path
        self.config_file = os.path.join(config_path, "ae_config.json")
        self._config_cache = None

    def _get_db_connection(self):
        """Get database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_ae_config(self) -> Dict[str, Any]:
        """Load AE configuration with caching."""
        if self._config_cache is not None:
            return self._config_cache

        try:
            with open(self.config_file, "r") as f:
                self._config_cache = json.load(f)
            return self._config_cache
        except FileNotFoundError:
            logger.error(f"AE config file not found: {self.config_file}")
            # Return default config
            return {
                "ae_settings": {
                    "Charmaine Lane": {
                        "active": True,
                        "include_in_review": True,
                        "territory": "North",
                    },
                    "House": {
                        "active": True,
                        "include_in_review": True,
                        "territory": "Central",
                    },
                    "WorldLink": {
                        "active": True,
                        "include_in_review": True,
                        "territory": "General",
                    },
                }
            }
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in AE config: {e}")
            return {"ae_settings": {}}

    def get_filtered_ae_list(self) -> List[Dict[str, Any]]:
        """Get list of AEs filtered by configuration."""
        config = self._load_ae_config()
        ae_settings = config.get("ae_settings", {})

        # Filter to only active AEs that should be in review
        filtered_aes = []
        ae_id_counter = 1

        for ae_name, settings in ae_settings.items():
            if settings.get("active", True) and settings.get("include_in_review", True):
                # Get real revenue data from database
                revenue_data = self._get_ae_revenue_data(ae_name)

                filtered_aes.append(
                    {
                        "ae_id": f"AE{ae_id_counter:03d}",
                        "name": ae_name,
                        "territory": settings.get("territory", "General"),
                        "ytd_actual": revenue_data["total_revenue"],
                        "avg_deal_size": revenue_data["avg_rate"],
                        "active": True,
                    }
                )
                ae_id_counter += 1

        # Sort by total revenue descending
        filtered_aes.sort(key=lambda x: x["ytd_actual"], reverse=True)
        return filtered_aes

    def _get_ae_revenue_data(self, ae_name: str) -> Dict[str, float]:
        """Get revenue data for specific AE for current year only."""
        from datetime import datetime

        current_year = str(datetime.now().year)

        conn = self._get_db_connection()

        try:
            if ae_name == "WorldLink":
                # WorldLink is an agency - current year only
                query = """
                SELECT COUNT(*) as spot_count,
                       ROUND(SUM(gross_rate), 2) as total_revenue,
                       ROUND(AVG(gross_rate), 2) as avg_rate
                FROM spots 
                WHERE (agency_id = 'WorldLink' OR bill_code LIKE 'WorldLink:%')
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND strftime('%Y', broadcast_month) = ?
                """
                cursor = conn.execute(query, (current_year,))
            else:
                # Regular AE - current year only
                query = """
                SELECT COUNT(*) as spot_count,
                       ROUND(SUM(gross_rate), 2) as total_revenue,
                       ROUND(AVG(gross_rate), 2) as avg_rate
                FROM spots 
                WHERE sales_person = ?
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                AND strftime('%Y', broadcast_month) = ?
                """
                cursor = conn.execute(query, (ae_name, current_year))

            result = cursor.fetchone()
            return {
                "spot_count": result[0] if result else 0,
                "total_revenue": int(result[1] if result and result[1] else 0),
                "avg_rate": int(result[2] if result and result[2] else 0),
            }
        finally:
            conn.close()

    def get_ae_by_id(self, ae_id: str) -> Optional[Dict[str, Any]]:
        """Get AE by ID."""
        ae_list = self.get_filtered_ae_list()
        return next((ae for ae in ae_list if ae["ae_id"] == ae_id), None)

    def get_monthly_revenue(self, ae_name: str, month: str) -> float:
        """Get monthly revenue for AE."""
        conn = self._get_db_connection()

        try:
            if ae_name == "WorldLink":
                query = """
                SELECT ROUND(SUM(gross_rate), 2) as revenue
                FROM spots
                WHERE (agency_id = 'WorldLink' OR bill_code LIKE 'WorldLink:%')
                AND strftime('%Y-%m', broadcast_month) = ?
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """
                cursor = conn.execute(query, (month,))
            else:
                query = """
                SELECT ROUND(SUM(gross_rate), 2) as revenue
                FROM spots
                WHERE sales_person = ?
                AND strftime('%Y-%m', broadcast_month) = ?
                AND gross_rate IS NOT NULL
                AND (revenue_type != 'Trade' OR revenue_type IS NULL)
                """
                cursor = conn.execute(query, (ae_name, month))

            result = cursor.fetchone()
            return result[0] if result and result[0] else 0
        finally:
            conn.close()
