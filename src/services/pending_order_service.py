# src/services/pending_order_service.py
"""Service for reading pending insertion order data from JSON."""

import json
import logging
import os

logger = logging.getLogger(__name__)


class PendingOrderService:
    """Reads pending insertion orders from the scanner-generated JSON file.

    No database access needed — operates purely on the JSON file
    produced by scripts/scan_insertion_orders.py.
    """

    def __init__(self, json_path="data/pending_orders.json"):
        self.json_path = json_path

    def get_pending_orders(self, ae_name=None):
        """Return pending orders, optionally filtered by AE name.

        Args:
            ae_name: If provided, filter to orders where sales_person
                     matches (case-insensitive). Pass None for all orders.

        Returns:
            Dict with scanned_at, orders list, and order_count.
            Returns empty result if JSON file is missing or malformed.
        """
        empty = {
            "scanned_at": None,
            "orders": [],
            "order_count": 0,
        }

        if not os.path.exists(self.json_path):
            logger.debug(
                "Pending orders JSON not found: %s", self.json_path
            )
            return empty

        try:
            with open(self.json_path, "r") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "Could not read pending orders JSON: %s", exc
            )
            return empty

        orders = data.get("orders", [])

        if ae_name:
            ae_lower = ae_name.lower()
            orders = [
                o for o in orders
                if not o.get("sales_person")
                or o["sales_person"].lower() == ae_lower
            ]

        return {
            "scanned_at": data.get("scanned_at"),
            "orders": orders,
            "order_count": len(orders),
        }
