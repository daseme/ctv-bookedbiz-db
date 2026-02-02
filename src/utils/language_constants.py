#!/usr/bin/env python3
"""
Language Constants for CTV Application

Consolidated language group mappings used across multiple analysis modules.
Extracted from market_analysis.py and services/market_analysis_service.py to eliminate duplication.

Uses the canonical mapping from services/market_analysis_service.py which is actively used
in production routes and factory configurations.
"""

from typing import Dict, List


class LanguageConstants:
    """Constants and utilities for language code mapping in the CTV application."""
    
    # Canonical language group mappings
    # Based on services/market_analysis_service.py (actively used in production)
    LANGUAGE_GROUPS: Dict[str, str] = {
        "M": "Chinese",        # Mandarin
        "C": "Chinese",        # Cantonese  
        "M/C": "Chinese",      # Mandarin/Cantonese
        "V": "Vietnamese",
        "T": "Filipino",       # Tagalog
        "K": "Korean", 
        "J": "Japanese",
        "SA": "South Asian",
        "HM": "Hmong",
        "E": "English",
        "EN": "English",
        "ENG": "English",
        "P": "South Asian",    # Punjabi - using canonical production mapping
    }
    
    @classmethod
    def get_language_group(cls, language_code: str) -> str:
        """
        Get the language group for a given language code.
        
        Args:
            language_code: The language code to look up (e.g., "M", "V", "T")
            
        Returns:
            The language group name (e.g., "Chinese", "Vietnamese", "Filipino")
            Falls back to the original code if not found in mapping.
            
        Examples:
            >>> LanguageConstants.get_language_group("M")
            "Chinese"
            >>> LanguageConstants.get_language_group("V") 
            "Vietnamese"
            >>> LanguageConstants.get_language_group("UNKNOWN")
            "UNKNOWN"
        """
        return cls.LANGUAGE_GROUPS.get(language_code, language_code)
    
    @classmethod
    def get_all_groups(cls) -> set:
        """
        Get all unique language groups.
        
        Returns:
            Set of all unique language group names
            
        Examples:
            >>> sorted(LanguageConstants.get_all_groups())
            ['Chinese', 'English', 'Filipino', 'Hmong', 'Japanese', 'Korean', 'South Asian', 'Vietnamese']
        """
        return set(cls.LANGUAGE_GROUPS.values())
    
    @classmethod 
    def get_codes_for_group(cls, group_name: str) -> List[str]:
        """
        Get all language codes that map to a specific group.
        
        Args:
            group_name: The language group name to find codes for
            
        Returns:
            List of language codes that map to the given group
            
        Examples:
            >>> sorted(LanguageConstants.get_codes_for_group("Chinese"))
            ['C', 'M', 'M/C']
            >>> sorted(LanguageConstants.get_codes_for_group("English"))
            ['E', 'EN', 'ENG']
        """
        return [code for code, group in cls.LANGUAGE_GROUPS.items() if group == group_name]