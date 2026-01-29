#!/usr/bin/env python3
"""
Test script for language assignment functionality
"""

import sqlite3
import sys
import os

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src"))

from services.language_assignment_service import LanguageAssignmentService
from models.language_assignment import LanguageStatus


def create_test_database():
    """Create a temporary test database with sample data"""
    db = sqlite3.connect(":memory:")
    cursor = db.cursor()

    # Create basic tables
    cursor.execute("""
        CREATE TABLE languages (
            language_id INTEGER PRIMARY KEY,
            language_code TEXT UNIQUE,
            language_name TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE spots (
            spot_id INTEGER PRIMARY KEY,
            language_code TEXT,
            revenue_type TEXT,
            market_id INTEGER,
            gross_rate REAL,
            bill_code TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE spot_language_assignments (
            assignment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            spot_id INTEGER NOT NULL UNIQUE,
            language_code TEXT NOT NULL,
            language_status TEXT NOT NULL,
            confidence REAL DEFAULT 1.0,
            assignment_method TEXT DEFAULT 'direct_mapping',
            assigned_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            requires_review BOOLEAN DEFAULT 0,
            notes TEXT
        )
    """)

    # Insert test data
    cursor.execute("INSERT INTO languages VALUES (1, 'E', 'English')")
    cursor.execute("INSERT INTO languages VALUES (2, 'M', 'Mandarin')")
    cursor.execute("INSERT INTO languages VALUES (3, 'T', 'Tagalog')")

    # Test spots
    cursor.execute(
        "INSERT INTO spots VALUES (1, 'E', 'Standard', 1, 100.0, 'TEST001')"
    )  # English
    cursor.execute(
        "INSERT INTO spots VALUES (2, 'L', 'Standard', 1, 500.0, 'TEST002')"
    )  # Undetermined
    cursor.execute(
        "INSERT INTO spots VALUES (3, NULL, 'Standard', 1, 50.0, 'TEST003')"
    )  # No language
    cursor.execute(
        "INSERT INTO spots VALUES (4, 'X', 'Standard', 1, 200.0, 'TEST004')"
    )  # Invalid
    cursor.execute(
        "INSERT INTO spots VALUES (5, 'M', 'Standard', 1, 1000.0, 'TEST005')"
    )  # Mandarin

    db.commit()
    return db


def test_language_assignment():
    """Test the language assignment functionality"""
    print("🧪 Testing Language Assignment Functionality...")

    # Create test database
    db = create_test_database()
    service = LanguageAssignmentService(db)

    # Test cases
    test_cases = [
        (1, LanguageStatus.DETERMINED, False, "English spot"),
        (2, LanguageStatus.UNDETERMINED, True, "Undetermined (L) spot"),
        (3, LanguageStatus.DEFAULT, False, "No language code spot"),
        (4, LanguageStatus.INVALID, True, "Invalid language code spot"),
        (5, LanguageStatus.DETERMINED, False, "Mandarin spot"),
    ]

    print(f"\n📋 Running {len(test_cases)} test cases...")

    passed = 0
    failed = 0

    for spot_id, expected_status, expected_review, description in test_cases:
        try:
            assignment = service.assign_spot_language(spot_id)

            # Validate results
            status_match = assignment.language_status == expected_status
            review_match = assignment.requires_review == expected_review

            if status_match and review_match:
                print(
                    f"✅ PASS: {description} - Status: {assignment.language_status.value}, Review: {assignment.requires_review}"
                )
                passed += 1
            else:
                print(
                    f"❌ FAIL: {description} - Expected: {expected_status.value}/{expected_review}, Got: {assignment.language_status.value}/{assignment.requires_review}"
                )
                failed += 1

        except Exception as e:
            print(f"❌ ERROR: {description} - {e}")
            failed += 1

    print(f"\n📊 Test Results: {passed} passed, {failed} failed")

    # Test batch processing
    print("\n🔄 Testing batch processing...")
    batch_results = service.batch_assign_languages([1, 2, 3, 4, 5])

    if len(batch_results) == 5:
        print(f"✅ PASS: Batch processing returned {len(batch_results)} results")
    else:
        print(
            f"❌ FAIL: Batch processing returned {len(batch_results)} results, expected 5"
        )

    # Test summary functions
    print("\n📈 Testing summary functions...")
    summary = service.get_review_required_summary()
    print(f"   • Undetermined spots: {summary['undetermined_language']}")
    print(f"   • Invalid codes: {summary['invalid_codes']}")
    print(f"   • High value undetermined: {summary['high_value_undetermined']}")

    db.close()
    print("\n✅ Testing complete!")


if __name__ == "__main__":
    test_language_assignment()
