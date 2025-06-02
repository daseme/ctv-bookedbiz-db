#!/usr/bin/env python3
"""
Test script for the Excel reader.
Run from project root: python test_excel_reader.py data/raw/test.xlsx
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Check if openpyxl is available
try:
    import openpyxl
    print("✓ openpyxl is available")
except ImportError:
    print("❌ openpyxl is not installed. Please run: uv add openpyxl")
    sys.exit(1)

from importers.excel_reader import ExcelReader, ExcelReadError

def test_excel_reader(file_path: str):
    """Test the Excel reader with a file."""
    print(f"Testing Excel reader with: {file_path}")
    
    try:
        with ExcelReader(file_path) as reader:
            # Show file info
            info = reader.get_file_info()
            print(f"\nFile Information:")
            print(f"  File: {info['file_name']}")
            print(f"  Total rows: {info['total_rows']}")
            print(f"  Columns mapped: {info['columns_mapped']}")
            print(f"  Worksheet: {info['worksheet_name']}")
            
            # Read spots
            print(f"\nReading spots...")
            spots = list(reader.read_spots())
            print(f"Successfully read {len(spots)} spots")
            
            if spots:
                print(f"\nFirst 3 spots:")
                for i, spot in enumerate(spots[:3]):
                    print(f"\n  Spot {i+1}:")
                    print(f"    Bill Code: {spot.bill_code}")
                    print(f"    Air Date: {spot.air_date}")
                    print(f"    Gross Rate: ${spot.gross_rate}")
                    print(f"    Sales Person: {spot.sales_person}")
                    print(f"    Market: {spot.market_name}")
                    print(f"    Revenue Type: {spot.revenue_type}")
                
                print(f"\nData quality check:")
                print(f"  Spots with bill_code: {sum(1 for s in spots if s.bill_code)}")
                print(f"  Spots with air_date: {sum(1 for s in spots if s.air_date)}")
                print(f"  Spots with gross_rate: {sum(1 for s in spots if s.gross_rate)}")
                print(f"  Spots with non-zero gross_rate: {sum(1 for s in spots if s.gross_rate and s.gross_rate != 0)}")
                print(f"  Trade revenue spots: {sum(1 for s in spots if s.revenue_type and s.revenue_type.upper() == 'TRADE')}")
                
                # Debug: Show some actual gross_rate values
                print(f"\nDebugging gross_rate values:")
                for i, spot in enumerate(spots[:10]):
                    if spot.gross_rate is not None:
                        print(f"  Spot {i+1}: {spot.gross_rate}")
                        break
                else:
                    print("  No non-null gross_rate values found in first 10 spots")
            
    except ExcelReadError as e:
        print(f"Excel reading error: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_excel_reader.py <excel_file_path>")
        print("Example: python test_excel_reader.py data/raw/test.xlsx")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    success = test_excel_reader(file_path)
    
    if success:
        print(f"\n✅ Excel reader test completed successfully!")
    else:
        print(f"\n❌ Excel reader test failed!")
        sys.exit(1)