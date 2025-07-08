#!/usr/bin/env python3
"""
Revenue Analysis CLI
===================

Main entry point for revenue analysis system.

Usage:
    python revenue_analysis.py --year 2024 --format summary
    python revenue_analysis.py --year 2024 --format markdown --output reports/revenue_2024.md
    python revenue_analysis.py --year 2024 --format json --output reports/revenue_2024.json
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from revenue_analysis import main

if __name__ == "__main__":
    main()
