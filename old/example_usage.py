#!/usr/bin/env python3
"""
Example Usage of Revenue Analysis System
=======================================

This shows how to use the clean revenue analysis system.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from revenue_analysis import RevenueAnalysisEngine

def main():
    """Example usage of the revenue analysis system"""
    
    print("🚀 Revenue Analysis System Example")
    print("=" * 50)
    
    # Use the clean system
    with RevenueAnalysisEngine() as engine:
        result = engine.analyze_complete_revenue("2024")
    
    # Print summary
    print(f"Total Revenue: ${result.total_revenue:,.2f}")
    print(f"Total Spots: {result.total_spots:,}")
    print(f"Reconciliation: {'✅ Perfect' if result.reconciliation_perfect else '❌ Issues'}")
    
    print(f"\nTop 3 Categories:")
    for i, cat in enumerate(result.categories[:3], 1):
        print(f"  {i}. {cat.name}: ${cat.revenue:,.2f} ({cat.percentage:.1f}%)")
    
    print(f"\nStrategic Insights:")
    insights = result.strategic_insights
    print(f"  • Chinese Strategy Total: ${insights['chinese_strategy_total']:,.2f}")
    print(f"  • Cross-Audience Revenue: ${insights['cross_audience_revenue']:,.2f}")
    
    print(f"\nTop Languages:")
    for i, lang in enumerate(insights['top_languages'][:3], 1):
        print(f"  {i}. {lang['language']}: ${lang['revenue']:,.2f}")

if __name__ == "__main__":
    main()
