"""
Business rule validators for the sales database tool.
Separated from data models for clean architecture.
"""

from datetime import date, timedelta
import re

from .entities import ValidationResult, Spot, Customer, Budget, Pipeline


class SpotValidator:
    """Validates spot data according to business rules."""
    
    def validate(self, spot: Spot) -> ValidationResult:
        """Validate a spot and return validation result."""
        result = ValidationResult()
        
        # Required field validation
        self._validate_required_fields(spot, result)
        
        # Business rule validation
        self._validate_business_rules(spot, result)
        
        # Data format validation
        self._validate_data_formats(spot, result)
        
        # Financial validation
        self._validate_financial_fields(spot, result)
        
        return result
    
    def _validate_required_fields(self, spot: Spot, result: ValidationResult):
        """Validate required fields."""
        if not spot.bill_code or not spot.bill_code.strip():
            result.add_error("bill_code", "Bill code is required", "REQUIRED_FIELD")
            
        if not spot.air_date:
            result.add_error("air_date", "Air date is required", "REQUIRED_FIELD")
    
    def _validate_business_rules(self, spot: Spot, result: ValidationResult):
        """Validate business-specific rules."""
        # Critical business rule: exclude Trade revenue type
        if spot.revenue_type and spot.revenue_type.upper() == "TRADE":
            result.add_error("revenue_type", 
                           "Trade revenue type must be excluded per business rules", 
                           "BUSINESS_RULE_VIOLATION")
        
        # Date range validation
        if spot.air_date:
            # Historical data shouldn't be too old (data quality check)
            if spot.air_date < date(2020, 1, 1):
                result.add_warning("air_date", 
                                 "Air date is very old - verify data quality", 
                                 "DATA_QUALITY")
            
            # Future dates shouldn't be too far out
            if spot.air_date > date.today() + timedelta(days=730):
                result.add_error("air_date", 
                               "Air date is too far in the future", 
                               "INVALID_DATE_RANGE")
        
        # End date validation
        if spot.end_date and spot.air_date and spot.end_date < spot.air_date:
            result.add_error("end_date", 
                           "End date cannot be before air date", 
                           "INVALID_DATE_RANGE")
    
    def _validate_data_formats(self, spot: Spot, result: ValidationResult):
        """Validate data formats."""
        # Time format validation
        if spot.time_in and not self._is_valid_time_format(spot.time_in):
            result.add_error("time_in", 
                           f"Invalid time format: {spot.time_in} (expected HH:MM:SS)", 
                           "INVALID_FORMAT")
        
        if spot.time_out and not self._is_valid_time_format(spot.time_out):
            result.add_error("time_out", 
                           f"Invalid time format: {spot.time_out} (expected HH:MM:SS)", 
                           "INVALID_FORMAT")
        
        # Spot type validation - updated for all possible values
        valid_spot_types = ["AV", "BB", "BNS", "COM", "CRD", "PKG", "PRD", "PRG", "SVC", ""]
        if spot.spot_type and spot.spot_type not in valid_spot_types:
            result.add_error("spot_type", 
                           f"Invalid spot type: {spot.spot_type} (must be one of: {', '.join(valid_spot_types[:-1])})", 
                           "INVALID_ENUM_VALUE")
        
        # Billing type validation
        if spot.billing_type and spot.billing_type not in ["Calendar", "Broadcast", ""]:
            result.add_error("billing_type", 
                           f"Invalid billing type: {spot.billing_type}", 
                           "INVALID_ENUM_VALUE")
        
        # Affidavit flag validation
        if spot.affidavit_flag and spot.affidavit_flag not in ["Y", "N", ""]:
            result.add_error("affidavit_flag", 
                           f"Invalid affidavit flag: {spot.affidavit_flag} (must be Y or N)", 
                           "INVALID_ENUM_VALUE")
    
    def _validate_financial_fields(self, spot: Spot, result: ValidationResult):
        """Validate financial fields."""
        financial_fields = [
            ("gross_rate", spot.gross_rate),
            ("station_net", spot.station_net),
            ("spot_value", spot.spot_value),
            ("broker_fees", spot.broker_fees)
        ]
        
        for field_name, value in financial_fields:
            if value is not None and value < 0:
                result.add_error(field_name, 
                               f"{field_name.replace('_', ' ').title()} cannot be negative", 
                               "INVALID_FINANCIAL_VALUE")
    
    def _is_valid_time_format(self, time_str: str) -> bool:
        """Validate HH:MM:SS time format."""
        pattern = r'^\d{1,2}:\d{2}:\d{2}$'
        if not re.match(pattern, time_str):
            return False
        
        # Additional validation - ensure valid time components
        try:
            parts = time_str.split(':')
            hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
            return 0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds <= 59
        except (ValueError, IndexError):
            return False


class CustomerValidator:
    """Validates customer data."""
    
    def validate(self, customer: Customer) -> ValidationResult:
        """Validate a customer and return validation result."""
        result = ValidationResult()
        
        if not customer.normalized_name or not customer.normalized_name.strip():
            result.add_error("normalized_name", "Normalized name is required", "REQUIRED_FIELD")
        
        # Check for whitespace issues
        if customer.normalized_name and customer.normalized_name != customer.normalized_name.strip():
            result.add_error("normalized_name", 
                           "Normalized name has leading/trailing whitespace", 
                           "DATA_QUALITY")
        
        return result


class BudgetValidator:
    """Validates budget data."""
    
    def validate(self, budget: Budget) -> ValidationResult:
        """Validate a budget and return validation result."""
        result = ValidationResult()
        
        if not budget.ae_name or not budget.ae_name.strip():
            result.add_error("ae_name", "AE name is required", "REQUIRED_FIELD")
        
        if budget.year < 2000 or budget.year > 2100:
            result.add_error("year", f"Year {budget.year} is out of valid range", "INVALID_RANGE")
        
        if budget.month < 1 or budget.month > 12:
            result.add_error("month", f"Month {budget.month} must be between 1 and 12", "INVALID_RANGE")
        
        if budget.budget_amount < 0:
            result.add_error("budget_amount", "Budget amount cannot be negative", "INVALID_FINANCIAL_VALUE")
        
        return result


class PipelineValidator:
    """Validates pipeline data."""
    
    def validate(self, pipeline: Pipeline) -> ValidationResult:
        """Validate a pipeline and return validation result."""
        result = ValidationResult()
        
        if not pipeline.ae_name or not pipeline.ae_name.strip():
            result.add_error("ae_name", "AE name is required", "REQUIRED_FIELD")
        
        if pipeline.year < 2000 or pipeline.year > 2100:
            result.add_error("year", f"Year {pipeline.year} is out of valid range", "INVALID_RANGE")
        
        if pipeline.month < 1 or pipeline.month > 12:
            result.add_error("month", f"Month {pipeline.month} must be between 1 and 12", "INVALID_RANGE")
        
        if pipeline.pipeline_amount < 0:
            result.add_error("pipeline_amount", "Pipeline amount cannot be negative", "INVALID_FINANCIAL_VALUE")
        
        if not pipeline.update_date:
            result.add_error("update_date", "Update date is required", "REQUIRED_FIELD")
        
        return result