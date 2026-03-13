"""Service for scanning pending insertion orders from the K drive."""

import logging
import os
from datetime import datetime

from scripts.scan_insertion_orders import parse_folder_name

logger = logging.getLogger(__name__)
DEFAULT_IO_PATH = "/mnt/k-drive/Insertion Orders"


class PendingOrderService:
    """Scans pending insertion orders live from the K drive directory."""

    def __init__(self, io_path=DEFAULT_IO_PATH):
        self.io_path = io_path

    def _scan_with_ae_folders(self):
        """Walk a two-level hierarchy: AE folders then customer folders.

        Structure:
            <io_path>/
                <AE Name>/
                    <NNNN Customer>/

        Each customer subfolder becomes one order row.
        """
        orders = []

        try:
            ae_names = sorted(os.listdir(self.io_path))
        except OSError as exc:
            logger.warning("Could not list IO path %s: %s", self.io_path, exc)
            return []

        skip_folders = {"traffic only"}

        for ae_name in ae_names:
            if ae_name.lower() in skip_folders:
                continue
            ae_path = os.path.join(self.io_path, ae_name)
            if not os.path.isdir(ae_path):
                continue

            try:
                customer_folders = sorted(os.listdir(ae_path))
            except OSError as exc:
                logger.warning(
                    "Could not list AE folder %s: %s", ae_name, exc
                )
                continue

            for customer_folder in customer_folders:
                customer_path = os.path.join(ae_path, customer_folder)
                if not os.path.isdir(customer_path):
                    continue

                contract, customer = parse_folder_name(customer_folder)

                orders.append({
                    "sales_person": ae_name,
                    "customer": customer,
                    "contract": contract,
                    "market": "",
                    "spot_count": 0,
                    "total_gross": 0.0,
                    "total_net": 0.0,
                    "date_range_start": None,
                    "date_range_end": None,
                })

        return orders

    def get_pending_orders(self, ae_name=None):
        """Scan the IO directory and return pending orders.

        Args:
            ae_name: If provided, filter to orders where sales_person
                     matches (case-insensitive). Pass None for all orders.

        Returns:
            Dict with scanned_at, orders list, and order_count.
            Returns empty result if directory is missing.
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

        orders = self._scan_with_ae_folders()

        if ae_name:
            ae_lower = ae_name.lower()
            orders = [
                o for o in orders
                if o["sales_person"].lower() == ae_lower
            ]

        return {
            "scanned_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "orders": orders,
            "order_count": len(orders),
        }
