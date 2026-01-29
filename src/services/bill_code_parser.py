"""
Bill code parsing service for extracting agency and customer information.
Handles the "Agency:Customer" format from Excel bill codes with robust error handling.
"""

import logging
from typing import Optional, Tuple, List

logger = logging.getLogger(__name__)


class BillCodeParseError(Exception):
    """Raised when there's an error parsing a bill code."""

    pass


class BillCodeParser:
    """Parses bill codes to extract agency and customer information."""

    # Suffixes to remove from customer names for normalization
    CUSTOMER_SUFFIXES_TO_REMOVE = [
        " PRODUCTION",
        " Production",
        " production",
        " PROD",
        " Prod",
        " prod",
    ]

    def __init__(self, normalize_customer_names: bool = True):
        """
        Initialize the parser.

        Args:
            normalize_customer_names: If True, removes suffixes like "PRODUCTION" from customer names
        """
        self.normalize_customer_names = normalize_customer_names
        self.parse_stats = {
            "total_parsed": 0,
            "agency_customer_format": 0,
            "customer_only_format": 0,
            "errors": 0,
            "empty_codes": 0,
            "customers_normalized": 0,
        }

    def parse(self, bill_code: str) -> Tuple[Optional[str], str]:
        """
        Parse bill_code into (agency_name, customer_name).

        Examples:
        - "Acento:City Colleges of Chicago PRODUCTION" -> ("Acento", "City Colleges of Chicago PRODUCTION")
        - "Hoffman Lewis:Toyota PRODUCTION" -> ("Hoffman Lewis", "Toyota PRODUCTION")
        - "Direct Client Name" -> (None, "Direct Client Name")

        Args:
            bill_code: The bill code string from Excel

        Returns:
            Tuple of (agency_name, customer_name) where agency_name is None if no agency

        Raises:
            BillCodeParseError: If bill code is empty or malformed
        """
        self.parse_stats["total_parsed"] += 1

        # Handle None or empty bill codes
        if not bill_code:
            self.parse_stats["empty_codes"] += 1
            raise BillCodeParseError("Bill code cannot be empty or None")

        # Clean up the bill code
        bill_code = str(bill_code).strip()

        if not bill_code:
            self.parse_stats["empty_codes"] += 1
            raise BillCodeParseError("Bill code cannot be empty after trimming")

        try:
            if ":" in bill_code:
                # Agency:Customer format
                return self._parse_agency_customer_format(bill_code)
            else:
                # Customer only format (direct client)
                return self._parse_customer_only_format(bill_code)

        except Exception as e:
            self.parse_stats["errors"] += 1
            logger.warning(f"Failed to parse bill code '{bill_code}': {str(e)}")
            raise BillCodeParseError(
                f"Failed to parse bill code '{bill_code}': {str(e)}"
            )

    def _parse_agency_customer_format(self, bill_code: str) -> Tuple[str, str]:
        """Parse 'Agency:Customer' format."""
        # Split on first colon only (in case customer name has colons)
        parts = bill_code.split(":", 1)

        if len(parts) != 2:
            raise BillCodeParseError(f"Expected exactly one colon in '{bill_code}'")

        agency_name = parts[0].strip()
        customer_name = parts[1].strip()

        # Validate both parts exist
        if not agency_name:
            raise BillCodeParseError(f"Empty agency name in '{bill_code}'")

        if not customer_name:
            raise BillCodeParseError(f"Empty customer name in '{bill_code}'")

        # Normalize customer name if enabled
        if self.normalize_customer_names:
            original_customer = customer_name
            customer_name = self._normalize_customer_name(customer_name)
            if customer_name != original_customer:
                self.parse_stats["customers_normalized"] += 1
                logger.debug(
                    f"Normalized customer: '{original_customer}' → '{customer_name}'"
                )

        self.parse_stats["agency_customer_format"] += 1
        logger.debug(f"Parsed agency:customer - '{agency_name}' : '{customer_name}'")

        return (agency_name, customer_name)

    def _parse_customer_only_format(self, bill_code: str) -> Tuple[None, str]:
        """Parse customer-only format (direct client)."""
        customer_name = bill_code.strip()

        if not customer_name:
            raise BillCodeParseError("Customer name cannot be empty")

        # Normalize customer name if enabled
        if self.normalize_customer_names:
            original_customer = customer_name
            customer_name = self._normalize_customer_name(customer_name)
            if customer_name != original_customer:
                self.parse_stats["customers_normalized"] += 1
                logger.debug(
                    f"Normalized customer: '{original_customer}' → '{customer_name}'"
                )

        self.parse_stats["customer_only_format"] += 1
        logger.debug(f"Parsed direct customer - '{customer_name}'")

        return (None, customer_name)

    def _normalize_customer_name(self, customer_name: str) -> str:
        """
        Normalize customer name by removing known suffixes.

        Args:
            customer_name: Original customer name

        Returns:
            Normalized customer name with suffixes removed
        """
        if not customer_name:
            return customer_name

        normalized = customer_name

        # Remove suffixes (case-sensitive check for each suffix)
        for suffix in self.CUSTOMER_SUFFIXES_TO_REMOVE:
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)].strip()
                break  # Only remove one suffix

        return normalized

    def parse_batch(self, bill_codes: List[str]) -> List[Tuple[Optional[str], str]]:
        """
        Parse a batch of bill codes efficiently.

        Args:
            bill_codes: List of bill code strings

        Returns:
            List of (agency_name, customer_name) tuples

        Note:
            Errors are logged but don't stop processing. Invalid codes return (None, original_code)
        """
        results = []

        for bill_code in bill_codes:
            try:
                agency, customer = self.parse(bill_code)
                results.append((agency, customer))
            except BillCodeParseError:
                # Log error but continue processing
                logger.warning(
                    f"Failed to parse bill code, using as direct customer: '{bill_code}'"
                )
                results.append((None, bill_code if bill_code else "UNKNOWN"))

        return results

    def get_statistics(self) -> dict:
        """Get parsing statistics."""
        total = self.parse_stats["total_parsed"]
        if total == 0:
            return self.parse_stats.copy()

        stats = self.parse_stats.copy()
        stats["success_rate"] = (total - stats["errors"]) / total * 100
        stats["agency_percentage"] = stats["agency_customer_format"] / total * 100
        stats["direct_percentage"] = stats["customer_only_format"] / total * 100

        return stats

    def reset_statistics(self):
        """Reset parsing statistics."""
        self.parse_stats = {
            "total_parsed": 0,
            "agency_customer_format": 0,
            "customer_only_format": 0,
            "errors": 0,
            "empty_codes": 0,
            "customers_normalized": 0,
        }

    def extract_unique_agencies(self, bill_codes: List[str]) -> List[str]:
        """
        Extract all unique agency names from a list of bill codes.

        Args:
            bill_codes: List of bill code strings

        Returns:
            List of unique agency names (excluding None for direct clients)
        """
        agencies = set()

        for bill_code in bill_codes:
            try:
                agency, _ = self.parse(bill_code)
                if agency:
                    agencies.add(agency)
            except BillCodeParseError:
                continue  # Skip invalid codes

        return sorted(list(agencies))

    def extract_unique_customers(self, bill_codes: List[str]) -> List[str]:
        """
        Extract all unique customer names from a list of bill codes.

        Args:
            bill_codes: List of bill code strings

        Returns:
            List of unique customer names
        """
        customers = set()

        for bill_code in bill_codes:
            try:
                _, customer = self.parse(bill_code)
                if customer:
                    customers.add(customer)
            except BillCodeParseError:
                continue  # Skip invalid codes

        return sorted(list(customers))

    def validate_bill_code_format(self, bill_code: str) -> bool:
        """
        Validate if a bill code has a valid format without parsing.

        Args:
            bill_code: The bill code to validate

        Returns:
            True if valid format, False otherwise
        """
        if not bill_code or not str(bill_code).strip():
            return False

        bill_code = str(bill_code).strip()

        # Check for basic validity
        if ":" in bill_code:
            # Should have exactly one colon with non-empty parts
            parts = bill_code.split(":")
            return len(parts) == 2 and all(part.strip() for part in parts)
        else:
            # Direct customer - just needs to be non-empty
            return bool(bill_code.strip())


# Convenience functions for simple usage
def parse_bill_code(bill_code: str) -> Tuple[Optional[str], str]:
    """Simple function to parse a single bill code."""
    parser = BillCodeParser()
    return parser.parse(bill_code)


def extract_agencies_and_customers(
    bill_codes: List[str],
) -> Tuple[List[str], List[str]]:
    """Extract unique agencies and customers from bill codes."""
    parser = BillCodeParser()
    agencies = parser.extract_unique_agencies(bill_codes)
    customers = parser.extract_unique_customers(bill_codes)
    return agencies, customers


# Example usage and testing
if __name__ == "__main__":
    # Test with your real data examples
    parser = BillCodeParser(normalize_customer_names=True)

    test_codes = [
        "Acento:City Colleges of Chicago PRODUCTION",
        "Hoffman Lewis:Toyota PRODUCTION",
        "Direct Client Name",
        "Agency:Customer",
        "",  # This should fail
        "Invalid::",  # This should fail
        "IW Group:CMS",
        "CMS",
        "Some Company PRODUCTION",  # Direct client with PRODUCTION
        "Another Agency:Client Production",  # Different case
        "iGRAPHIX:Pechanga Resort Casino PROD",  # New: PROD suffix
        "Test Company PROD",  # New: Direct client with PROD
        "Agency:Client Prod",  # New: Title case PROD
    ]

    print("Testing Bill Code Parser with Customer Normalization:")
    print("=" * 60)

    for code in test_codes:
        try:
            agency, customer = parser.parse(code)
            print(f"✓ '{code}'")
            print(f"  Agency: {agency}")
            print(f"  Customer: {customer}")
        except BillCodeParseError as e:
            print(f"✗ '{code}' - Error: {e}")
        print()

    print("Statistics:")
    stats = parser.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print(
        f"\nCustomer Normalization: {stats.get('customers_normalized', 0)} customers had PRODUCTION removed"
    )
