#!/usr/bin/env python3
"""
Daily Production Database Validation Script

Validates critical data integrity checks:
- Spot counts for closed years (2021-2024)
- Database integrity (PRAGMA integrity_check)

Sends email notifications if validation fails.
"""

import os
import sys
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Configure logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / f"db_validation_{datetime.now().strftime('%Y%m%d')}.log"

# Set up handlers - try file logging, fallback to console only
handlers = [logging.StreamHandler(sys.stdout)]
try:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    handlers.append(logging.FileHandler(LOG_FILE))
except (PermissionError, OSError):
    # If we can't write to log file, just use console
    pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=handlers
)
logger = logging.getLogger(__name__)

# Expected spot counts for closed years
EXPECTED_SPOT_COUNTS = {
    "2021": 108066,
    "2022": 131668,
    "2023": 221245,
    "2024": 403203,
}

# Database path
DB_PATH = os.getenv(
    "DB_PATH",
    str(PROJECT_ROOT / "data" / "database" / "production.db")
)


class DatabaseValidator:
    """Validates production database integrity and data accuracy."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.errors: List[str] = []
        self.warnings: List[str] = []
        
    def validate_database_integrity(self) -> bool:
        """Check SQLite database integrity using PRAGMA integrity_check."""
        logger.info("Checking database integrity...")
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("PRAGMA integrity_check")
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] == "ok":
                logger.info("✅ Database integrity check passed")
                return True
            else:
                error_msg = f"❌ Database integrity check failed: {result[0] if result else 'Unknown error'}"
                logger.error(error_msg)
                self.errors.append(error_msg)
                return False
        except Exception as e:
            error_msg = f"❌ Database integrity check error: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def validate_spot_counts(self) -> bool:
        """Validate spot counts for closed years (2021-2024)."""
        logger.info("Validating spot counts for closed years...")
        all_valid = True
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for year, expected_count in EXPECTED_SPOT_COUNTS.items():
                # Query spots by broadcast_month format (MMM-YY)
                year_suffix = year[-2:]  # Get last 2 digits (21, 22, 23, 24)
                query = """
                    SELECT COUNT(*) 
                    FROM spots 
                    WHERE broadcast_month IS NOT NULL 
                      AND broadcast_month LIKE ?
                """
                cursor.execute(query, (f"%-{year_suffix}",))
                actual_count = cursor.fetchone()[0]
                
                if actual_count == expected_count:
                    logger.info(f"✅ {year}: {actual_count:,} spots (expected: {expected_count:,})")
                else:
                    error_msg = (
                        f"❌ {year}: Expected {expected_count:,} spots, "
                        f"but found {actual_count:,} spots "
                        f"(difference: {actual_count - expected_count:+,})"
                    )
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    all_valid = False
            
            conn.close()
            return all_valid
            
        except Exception as e:
            error_msg = f"❌ Spot count validation error: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def validate_closed_months(self) -> bool:
        """Validate that all 12 months are closed for each year."""
        logger.info("Validating closed months for 2021-2024...")
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all closed months
            cursor.execute("""
                SELECT broadcast_month 
                FROM month_closures 
                WHERE broadcast_month LIKE '%-21' 
                   OR broadcast_month LIKE '%-22' 
                   OR broadcast_month LIKE '%-23' 
                   OR broadcast_month LIKE '%-24'
                ORDER BY broadcast_month
            """)
            closed_months = {row[0] for row in cursor.fetchall()}
            
            # Expected months for each year
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                     'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            years = ['21', '22', '23', '24']
            
            all_valid = True
            for year_suffix in years:
                expected_months = {f"{month}-{year_suffix}" for month in months}
                missing_months = expected_months - closed_months
                
                if missing_months:
                    error_msg = (
                        f"❌ Year 20{year_suffix}: Missing closed months: "
                        f"{', '.join(sorted(missing_months))}"
                    )
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    all_valid = False
                else:
                    logger.info(f"✅ Year 20{year_suffix}: All 12 months are closed")
            
            conn.close()
            return all_valid
            
        except Exception as e:
            error_msg = f"❌ Closed months validation error: {str(e)}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def run_all_validations(self) -> bool:
        """Run all validation checks."""
        logger.info("=" * 80)
        logger.info(f"Starting database validation: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Database: {self.db_path}")
        logger.info("=" * 80)
        
        results = []
        results.append(("Database Integrity", self.validate_database_integrity()))
        results.append(("Spot Counts (2021-2024)", self.validate_spot_counts()))
        results.append(("Closed Months", self.validate_closed_months()))
        
        logger.info("=" * 80)
        logger.info("Validation Summary:")
        for check_name, passed in results:
            status = "✅ PASSED" if passed else "❌ FAILED"
            logger.info(f"  {check_name}: {status}")
        logger.info("=" * 80)
        
        all_passed = all(result[1] for result in results)
        
        if all_passed:
            logger.info("✅ All validations passed!")
        else:
            logger.error(f"❌ Validation failed with {len(self.errors)} error(s)")
            for error in self.errors:
                logger.error(f"  - {error}")
        
        return all_passed


class NotificationSender:
    """Sends email notifications on validation failure."""
    
    @staticmethod
    def send_email(
        smtp_server: str,
        smtp_port: int,
        sender_email: str,
        recipient_emails: List[str],
        subject: str,
        message: str,
        use_tls: bool = True,
        username: Optional[str] = None,
        password: Optional[str] = None
    ) -> bool:
        """Send email notification via SMTP."""
        if not recipient_emails:
            logger.warning("No recipient emails configured")
            return False
        
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = sender_email
            msg['To'] = ', '.join(recipient_emails)
            msg['Subject'] = subject
            
            # Add body
            msg.attach(MIMEText(message, 'plain'))
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                if use_tls:
                    server.starttls()
                
                if username and password:
                    server.login(username, password)
                
                server.send_message(msg)
            
            logger.info(f"Email notification sent to {', '.join(recipient_emails)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    @classmethod
    def send_notifications(cls, errors: List[str], all_passed: bool) -> None:
        """Send email notification if validation failed."""
        if all_passed:
            logger.info("All validations passed - no notifications sent")
            return
        
        # Build email message
        subject = "🚨 CTV Database Validation Failed"
        message = "Database validation failed!\n\n"
        message += "Errors:\n"
        for i, error in enumerate(errors, 1):
            message += f"{i}. {error}\n"
        message += f"\nTimestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        message += f"\nDatabase: {DB_PATH}"
        
        # Get email settings from environment
        smtp_server = os.getenv("SMTP_SERVER", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        sender_email = os.getenv("SMTP_SENDER_EMAIL", "noreply@ctv.local")
        recipient_emails = [
            email.strip() 
            for email in os.getenv("SMTP_RECIPIENT_EMAILS", "").split(",")
            if email.strip()
        ]
        use_tls = os.getenv("SMTP_USE_TLS", "false").lower() == "true"
        smtp_username = os.getenv("SMTP_USERNAME")
        smtp_password = os.getenv("SMTP_PASSWORD")
        
        if not recipient_emails:
            logger.warning("No recipient emails configured - skipping email notification")
            logger.warning("Set SMTP_RECIPIENT_EMAILS environment variable to enable email notifications")
            return
        
        # Send email
        logger.info(f"Sending email notification to {', '.join(recipient_emails)}...")
        cls.send_email(
            smtp_server=smtp_server,
            smtp_port=smtp_port,
            sender_email=sender_email,
            recipient_emails=recipient_emails,
            subject=subject,
            message=message,
            use_tls=use_tls,
            username=smtp_username,
            password=smtp_password
        )


def main():
    """Main execution function."""
    # Check if database exists
    if not os.path.exists(DB_PATH):
        logger.error(f"Database not found: {DB_PATH}")
        sys.exit(1)
    
    # Run validations
    validator = DatabaseValidator(DB_PATH)
    all_passed = validator.run_all_validations()
    
    # Send notifications if validation failed
    if not all_passed:
        NotificationSender.send_notifications(validator.errors, all_passed)
        sys.exit(1)
    else:
        logger.info("✅ All validations passed - no action needed")
        sys.exit(0)


if __name__ == "__main__":
    main()

