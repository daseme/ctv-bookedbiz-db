"""
Business Rules Service (Fixed Imports)
======================================

Service for applying sector-based business rules to language block assignment.
Fixed import paths to work from project root.
"""

import logging
import sys
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.models.business_rules_models import (
    BusinessRule, BusinessRuleType, CustomerIntent, SpotData, 
    BusinessRuleResult, BusinessRuleStats, DEFAULT_BUSINESS_RULES_CONFIG
)


class BusinessRulesService:
    """Service for applying business rules to language block assignment"""
    
    def __init__(self, db_connection=None):
        self.db = db_connection
        self.logger = logging.getLogger(__name__)
        self.rules = self._initialize_rules()
        self.stats = BusinessRuleStats()
        
    def _initialize_rules(self) -> List[BusinessRule]:
        """Initialize business rules with stakeholder-friendly terminology"""
        config = DEFAULT_BUSINESS_RULES_CONFIG
        
        return [
            # Rule 1: Direct Response Sales (295K spots - 40% of database!)
            # Based on MEDIA sector analysis showing Icon Media Direct and similar infomercials
            BusinessRule(
                rule_type=BusinessRuleType.DIRECT_RESPONSE_SALES,
                name="Direct Response Sales Auto-Assignment",
                description="Direct response sales (infomercials) are intentionally broad-reach campaigns that span multiple language blocks by design to maximize audience coverage",
                sector_codes=["MEDIA"],
                # No duration constraint - ALL MEDIA spots are auto-resolved
                customer_intent=CustomerIntent.INDIFFERENT,
                auto_resolve=True,
                priority=1
            ),
            
            # Rule 2: Government Public Service Campaigns (42K spots)
            # Based on GOV sector analysis showing consistent broad-reach patterns
            BusinessRule(
                rule_type=BusinessRuleType.GOVERNMENT_PUBLIC_SERVICE,
                name="Government Public Service Campaigns",
                description="Government public service announcements and campaigns are designed for community-wide reach across all demographics",
                sector_codes=["GOV"],
                customer_intent=CustomerIntent.INDIFFERENT,
                auto_resolve=True,
                priority=2
            ),
            
            # Rule 3: Political Campaigns (3K spots)
            # Based on POLITICAL sector analysis showing high-value broad-reach campaigns
            BusinessRule(
                rule_type=BusinessRuleType.POLITICAL_CAMPAIGNS,
                name="Political Campaign Advertising",
                description="Political campaigns require broad demographic reach to maximize voter engagement across all language communities",
                sector_codes=["POLITICAL"],
                customer_intent=CustomerIntent.INDIFFERENT,
                auto_resolve=True,
                priority=3
            ),
            
            # Rule 4: Nonprofit Awareness Campaigns (93K spots)
            # Based on NPO sector analysis showing long-duration awareness campaigns
            BusinessRule(
                rule_type=BusinessRuleType.NONPROFIT_AWARENESS,
                name="Nonprofit Long-Duration Awareness Campaigns",
                description="Nonprofit organizations with extended campaign durations (5+ hours) are conducting broad awareness campaigns for maximum community impact",
                sector_codes=["NPO"],
                min_duration_minutes=config['nonprofit_campaign_min_duration'],  # 5 hours
                customer_intent=CustomerIntent.INDIFFERENT,
                auto_resolve=True,
                priority=4
            ),
            
            # Rule 5: Extended Content Blocks (91K spots)
            # Based on duration analysis showing 12+ hour content across sectors
            BusinessRule(
                rule_type=BusinessRuleType.EXTENDED_CONTENT_BLOCKS,
                name="Extended Content Blocks",
                description="Content running 12+ hours across any sector represents extended programming that inherently spans multiple language blocks",
                sector_codes=[],  # Any sector
                min_duration_minutes=config['long_duration_content_min_duration'],  # 12 hours
                customer_intent=CustomerIntent.INDIFFERENT,
                auto_resolve=True,
                priority=5
            )
        ]
    
    def evaluate_spot(self, spot_data: SpotData) -> BusinessRuleResult:
        """
        Evaluate a single spot against business rules with enhanced exclusion logic
        
        Args:
            spot_data: SpotData object containing spot information
            
        Returns:
            BusinessRuleResult indicating if and how the spot should be handled
        """
        # STEP 1: Check if spot should be excluded from assignment
        should_exclude, exclusion_reason = self._should_exclude_from_assignment(spot_data)
        if should_exclude:
            self.logger.debug(f"Spot {spot_data.spot_id} excluded: {exclusion_reason}")
            return BusinessRuleResult(
                spot_id=spot_data.spot_id,
                rule_applied=None,
                auto_resolved=False,
                requires_attention=False,
                notes=f"Excluded: {exclusion_reason}"
            )
        
        # STEP 2: Check rules in priority order (existing logic)
        for rule in sorted(self.rules, key=lambda r: r.priority):
            if self._spot_matches_rule(spot_data, rule):
                self.logger.debug(f"Spot {spot_data.spot_id} matches rule: {rule.name}")
                
                # Update stats
                self.stats.add_rule_application(rule.rule_type.value, rule.auto_resolve)
                
                if rule.auto_resolve:
                    return BusinessRuleResult(
                        spot_id=spot_data.spot_id,
                        rule_applied=rule,
                        customer_intent=rule.customer_intent,
                        auto_resolved=True,
                        requires_attention=False,
                        alert_reason=f"AUTO_RESOLVED_{rule.rule_type.value.upper()}",
                        confidence=1.0,
                        notes=f"Auto-resolved: {rule.description}"
                    )
                else:
                    return BusinessRuleResult(
                        spot_id=spot_data.spot_id,
                        rule_applied=rule,
                        customer_intent=rule.customer_intent,
                        auto_resolved=False,
                        requires_attention=True,
                        alert_reason=f"REQUIRES_REVIEW_{rule.rule_type.value.upper()}",
                        confidence=0.8,
                        notes=f"Flagged for review: {rule.description}"
                    )
        
        # STEP 3: No rule matched - return for standard assignment (existing logic)
        self.stats.total_evaluated += 1
        return BusinessRuleResult(
            spot_id=spot_data.spot_id,
            rule_applied=None,
            auto_resolved=False,
            requires_attention=False,
            notes="No business rule matched - use standard assignment"
        )
    
    def _spot_matches_rule(self, spot_data: SpotData, rule: BusinessRule) -> bool:
        """
        Check if a spot matches a business rule
        
        Args:
            spot_data: SpotData object
            rule: BusinessRule to check against
            
        Returns:
            True if the spot matches the rule criteria
        """
        # Check sector codes (empty list means any sector)
        if rule.sector_codes and spot_data.sector_code not in rule.sector_codes:
            return False
        
        # Check duration constraints
        if rule.min_duration_minutes and spot_data.duration_minutes < rule.min_duration_minutes:
            return False
        
        if rule.max_duration_minutes and spot_data.duration_minutes > rule.max_duration_minutes:
            return False
        
        return True

    def _should_exclude_from_assignment(self, spot_data: SpotData) -> tuple[bool, str]:
        """Check if spot should be excluded from assignment processing"""
        
        # Exclude production/editing jobs (by keyword only)
        production_keywords = ['PRODUCTION', 'PROD', 'EDIT', 'DEMO', 'TEST', 'PROOF', 'DRAFT', 'SAMPLE']
        if any(keyword in spot_data.bill_code.upper() for keyword in production_keywords):
            return True, "Production/editing job - not airtime"
        
        # Exclude zero-revenue spots (likely billing/data issues)
        if spot_data.gross_rate == 0 or spot_data.gross_rate is None:
            return True, "Zero revenue - likely billing or data issue"
        
        # Exclude future spots without market assignment (scheduled but not ready)
        if (hasattr(spot_data, 'air_date') and spot_data.air_date and
            spot_data.air_date > '2025-01-01' and 
            spot_data.market_id is None):
            return True, "Future spot - market not assigned yet"
        
        # Exclude spots with critical missing data
        if (spot_data.market_id is None or 
            spot_data.time_in is None or 
            spot_data.time_out is None):
            return True, "Missing critical assignment data"
        
        return False, None

    def get_spot_data_from_db(self, spot_id: int) -> Optional[SpotData]:
        """
        Get spot data from database for business rule evaluation
        
        Args:
            spot_id: ID of the spot to retrieve
            
        Returns:
            SpotData object or None if not found
        """
        if not self.db:
            self.logger.error("Database connection not available")
            return None
        
        cursor = self.db_connection.cursor()
        
        query = """
        SELECT 
            s.spot_id,
            s.customer_id,
            s.bill_code,
            s.time_in,
            s.time_out,
            s.air_date,
            s.market_id,
            s.gross_rate,
            c.normalized_name as customer_name,
            sec.sector_code,
            sec.sector_name,
            CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) as duration_minutes
        FROM spots s
        LEFT JOIN customers c ON s.customer_id = c.customer_id
        LEFT JOIN sectors sec ON c.sector_id = sec.sector_id
        WHERE s.spot_id = ?
        """
        
        try:
            cursor.execute(query, (spot_id,))
            row = cursor.fetchone()
            
            if not row:
                self.logger.warning(f"Spot {spot_id} not found in database")
                return None
            
            return SpotData(
                spot_id=row[0],
                customer_id=row[1],
                bill_code=row[2],
                time_in=row[3],
                time_out=row[4],
                air_date=row[5],
                market_id=row[6],
                gross_rate=row[7] or 0.0,
                customer_name=row[8] or "Unknown",
                sector_code=row[9],
                sector_name=row[10],
                duration_minutes=row[11] or 0
            )
        except Exception as e:
            self.logger.error(f"Error retrieving spot data for {spot_id}: {e}")
            return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get business rules statistics"""
        return self.stats.get_summary()
    
    def reset_stats(self):
        """Reset statistics tracking"""
        self.stats = BusinessRuleStats()
    
    def estimate_total_impact(self) -> Dict[str, int]:
        """
        Estimate total impact of business rules on all spots in database
        
        Returns:
            Dictionary with estimated spot counts for each rule type
        """
        if not self.db:
            self.logger.error("Database connection not available")
            return {}
        
        cursor = self.db_connection.cursor()
        estimates = {}
        
        try:
            # Direct Response Sales (ALL MEDIA spots)
            cursor.execute("""
                SELECT COUNT(*) FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id
                JOIN sectors sec ON c.sector_id = sec.sector_id
                WHERE sec.sector_code = 'MEDIA'
            """)
            estimates['direct_response_sales'] = cursor.fetchone()[0]
            
            # Government Public Service (all GOV spots)
            cursor.execute("""
                SELECT COUNT(*) FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id
                JOIN sectors sec ON c.sector_id = sec.sector_id
                WHERE sec.sector_code = 'GOV'
            """)
            estimates['government_public_service'] = cursor.fetchone()[0]
            
            # Political Campaigns (all POLITICAL spots)
            cursor.execute("""
                SELECT COUNT(*) FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id
                JOIN sectors sec ON c.sector_id = sec.sector_id
                WHERE sec.sector_code = 'POLITICAL'
            """)
            estimates['political_campaigns'] = cursor.fetchone()[0]
            
            # Nonprofit Awareness (5+ hours)
            cursor.execute("""
                SELECT COUNT(*) FROM spots s
                JOIN customers c ON s.customer_id = c.customer_id
                JOIN sectors sec ON c.sector_id = sec.sector_id
                WHERE sec.sector_code = 'NPO'
                AND CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 300
            """)
            estimates['nonprofit_awareness'] = cursor.fetchone()[0]
            
            # Extended Content Blocks (12+ hours)
            cursor.execute("""
                SELECT COUNT(*) FROM spots s
                WHERE CAST((strftime('%s', s.time_out) - strftime('%s', s.time_in)) / 60 AS INTEGER) >= 720
            """)
            estimates['extended_content_blocks'] = cursor.fetchone()[0]
            
            self.logger.info(f"Impact estimates: {estimates}")
            
        except Exception as e:
            self.logger.error(f"Error estimating impact: {e}")
            
        return estimates
    
    def get_rules_summary(self) -> List[Dict[str, Any]]:
        """Get a summary of all configured rules"""
        return [
            {
                'rule_type': rule.rule_type.value,
                'name': rule.name,
                'description': rule.description,
                'sector_codes': rule.sector_codes,
                'min_duration_minutes': rule.min_duration_minutes,
                'max_duration_minutes': rule.max_duration_minutes,
                'customer_intent': rule.customer_intent.value,
                'auto_resolve': rule.auto_resolve,
                'priority': rule.priority
            }
            for rule in self.rules
        ]
    
    def add_custom_rule(self, rule: BusinessRule):
        """Add a custom business rule"""
        self.rules.append(rule)
        self.logger.info(f"Added custom rule: {rule.name}")
    
    def remove_rule(self, rule_type: BusinessRuleType):
        """Remove a business rule by type"""
        original_count = len(self.rules)
        self.rules = [rule for rule in self.rules if rule.rule_type != rule_type]
        removed_count = original_count - len(self.rules)
        
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} rule(s) of type {rule_type.value}")
        else:
            self.logger.warning(f"No rules found of type {rule_type.value}")
    
    def disable_rule(self, rule_type: BusinessRuleType):
        """Disable a business rule by setting auto_resolve to False"""
        for rule in self.rules:
            if rule.rule_type == rule_type:
                rule.auto_resolve = False
                self.logger.info(f"Disabled auto-resolve for rule: {rule.name}")
                break
        else:
            self.logger.warning(f"Rule type {rule_type.value} not found")
    
    def enable_rule(self, rule_type: BusinessRuleType):
        """Enable a business rule by setting auto_resolve to True"""
        for rule in self.rules:
            if rule.rule_type == rule_type:
                rule.auto_resolve = True
                self.logger.info(f"Enabled auto-resolve for rule: {rule.name}")
                break
        else:
            self.logger.warning(f"Rule type {rule_type.value} not found")