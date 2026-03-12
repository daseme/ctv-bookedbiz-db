# src/services/pending_order_service.py
"""Service for scanning pending insertion orders from the K drive."""

import logging
import os
from datetime import datetime

from scripts.scan_insertion_orders import scan_all

logger = logging.getLogger(__name__)

DEFAULT_IO_PATH = "/mnt/k-drive/Insertion Orders"


class PendingOrderService:
    """Scans pending insertion orders live from the K drive directory."""

    def __init__(self, io_path=DEFAULT_IO_PATH):
        self.io_path = io_path

    def get_pending_orders(self, ae_name=None):
        """Scan the IO directory and return pending orders.

        Args:
            ae_name: If provided, filter to orders where sales_person
                     matches (case-insensitive). Pass None for all orders.

        Returns:
            Dict with scanned_at, orders list, and order_count.
            Returns empty result if directory is missing or scan fails.
        """
        empty = {
            "scanned_at": None,
            "orders": [],
            "order_count": 0,
        }

        if not os.path.isdir(self.io_path):
            logger.debug(
                "Insertion orders path not found: %s", self.io_path
            )
            return empty

        try:
            orders, errors = scan_all(self.io_path)
        except Exception as exc:
            logger.warning("Could not scan insertion orders: %s", exc)
            return empty

        if errors:
            for err in errors:
                logger.warning("IO scan error: %s", err)

        if ae_name:
            ae_lower = ae_name.lower()
            orders = [
                o for o in orders
                if not o.get("sales_person")
                or o["sales_person"].lower() == ae_lower
            ]

        return {
            "scanned_at": datetime.now().strftime(
                "%Y-%m-%dT%H:%M:%S"
            ),
            "orders": orders,
            "order_count": len(orders),
        }
