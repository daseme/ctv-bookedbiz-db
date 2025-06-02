"""
Bill code parsing service for extracting agency and customer information.
Handles the "Agency:Customer" format from Excel bill codes.
"""

from typing import Optional, Tuple


class BillCodeParser:
    """Parses bill codes to extract agency and customer information."""
    
    def parse(self, bill_code: str) -> Tuple[Optional[str], str]:
        """
        Parse bill_code into (agency_name, customer_name).
        
        Examples:
        - "IW Group:CMS" -> ("IW Group", "CMS")
        - "CMS" -> (None, "CMS")
        
        Args:
            bill_code: The bill code string from Excel
            
        Returns:
            Tuple of (agency_name, customer_name) where agency_name is None if no agency
            
        Raises:
            ValueError: If bill code is empty or malformed
        """
        if not bill_code or not bill_code.strip():
            raise ValueError("Bill code cannot be empty")
        
        bill_code = bill_code.strip()
        
        if ':' in bill_code:
            parts = bill_code.split(':', 1)  # Split on first colon only
            agency_name = parts[0].strip()
            customer_name = parts[1].strip()
            
            if not agency_name or not customer_name:
                raise ValueError(f"Invalid bill code format: {bill_code}")
            
            return (agency_name, customer_name)
        else:
            return (None, bill_code)