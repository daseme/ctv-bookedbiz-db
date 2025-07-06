#!/usr/bin/env python3
"""
Assignment Pipeline Orchestrator
===============================

Coordinates language block and business rules assignment in the proper sequence.
This script orchestrates the existing CLI scripts while maintaining separation of concerns.

Usage:
    python cli_00_assignment_pipeline.py --year 2025           # Assign all spots for year
    python cli_00_assignment_pipeline.py --test 100            # Test with 100 spots
    python cli_00_assignment_pipeline.py --batch 5000          # Process 5000 spots
    python cli_00_assignment_pipeline.py --status              # Show current status
    python cli_00_assignment_pipeline.py --dry-run --year 2025 # Preview what would happen
"""

import argparse
import subprocess
import sys
import sqlite3
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of pipeline execution"""
    success: bool
    stage1_stats: Optional[Dict[str, Any]] = None
    stage2_stats: Optional[Dict[str, Any]] = None
    total_processed: int = 0
    total_assigned: int = 0
    total_errors: int = 0
    execution_time: float = 0.0
    dry_run: bool = False
    error_message: Optional[str] = None


class AssignmentPipeline:
    """Orchestrates the assignment pipeline execution"""
    
    def __init__(self, db_path: str = "data/database/production.db", dry_run: bool = False):
        self.db_path = db_path
        self.dry_run = dry_run
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Validate database connection
        if not self._validate_database():
            raise ValueError(f"Cannot connect to database: {db_path}")
    
    def _validate_database(self) -> bool:
        """Validate database connection"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("SELECT 1")
            conn.close()
            return True
        except Exception as e:
            self.logger.error(f"Database validation failed: {e}")
            return False
    
    def execute_pipeline(self, year: Optional[int] = None, batch: Optional[int] = None, 
                        test: Optional[int] = None) -> PipelineResult:
        """Execute the complete assignment pipeline"""
        start_time = datetime.now()
        
        try:
            self.logger.info(f"🚀 Starting assignment pipeline (dry_run={self.dry_run})")
            
            # Get initial stats
            initial_stats = self._get_assignment_stats()
            self.logger.info(f"📊 Initial state: {initial_stats['unassigned_spots']:,} unassigned spots")
            
            # Stage 1: Language Block Assignment
            self.logger.info("🎯 STAGE 1: Language Block Assignment")
            stage1_result = self._run_stage1(year, batch, test)
            
            if not stage1_result['success']:
                return PipelineResult(
                    success=False,
                    error_message=f"Stage 1 failed: {stage1_result.get('error', 'Unknown error')}",
                    dry_run=self.dry_run
                )
            
            # Stage 2: Business Rules Assignment
            self.logger.info("🎯 STAGE 2: Business Rules Assignment")
            stage2_result = self._run_stage2(year, batch, test)
            
            if not stage2_result['success']:
                return PipelineResult(
                    success=False,
                    stage1_stats=stage1_result.get('stats'),
                    error_message=f"Stage 2 failed: {stage2_result.get('error', 'Unknown error')}",
                    dry_run=self.dry_run
                )
            
            # Get final stats
            final_stats = self._get_assignment_stats()
            
            # Calculate totals
            total_processed = (stage1_result.get('stats', {}).get('processed', 0) + 
                             stage2_result.get('stats', {}).get('processed', 0))
            total_assigned = (initial_stats['assigned_spots'] - final_stats['assigned_spots'])
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            result = PipelineResult(
                success=True,
                stage1_stats=stage1_result.get('stats'),
                stage2_stats=stage2_result.get('stats'),
                total_processed=total_processed,
                total_assigned=total_assigned,
                execution_time=execution_time,
                dry_run=self.dry_run
            )
            
            self._log_final_results(result, initial_stats, final_stats)
            return result
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            return PipelineResult(
                success=False,
                error_message=str(e),
                execution_time=(datetime.now() - start_time).total_seconds(),
                dry_run=self.dry_run
            )
    
    def _run_stage1(self, year: Optional[int], batch: Optional[int], 
                   test: Optional[int]) -> Dict[str, Any]:
        """Run language block assignment (Stage 1)"""
        cmd = ["python", "cli_01_assign_language_blocks.py", "--database", self.db_path]
        
        # Add appropriate arguments
        if test:
            cmd.extend(["--test", str(test)])
        elif batch:
            cmd.extend(["--batch", str(batch)])
        elif year:
            cmd.extend([f"--all-{year}"])
        else:
            cmd.append("--status")
        
        if self.dry_run:
            self.logger.info(f"DRY RUN: Would execute: {' '.join(cmd)}")
            return {
                'success': True,
                'stats': {'processed': 0, 'assigned': 0, 'errors': 0},
                'dry_run': True
            }
        
        return self._execute_cli_script(cmd, "Stage 1")
    
    def _run_stage2(self, year: Optional[int], batch: Optional[int], 
                   test: Optional[int]) -> Dict[str, Any]:
        """Run business rules assignment (Stage 2)"""
        cmd = ["python", "cli_02_assign_business_rules.py"]
        
        # Add appropriate arguments
        if test:
            cmd.extend(["--limit", str(test)])
        elif batch:
            cmd.extend(["--limit", str(batch)])
        elif year:
            # Business rules script processes remaining unassigned spots
            pass
        else:
            cmd.append("--stats")
        
        if self.dry_run:
            self.logger.info(f"DRY RUN: Would execute: {' '.join(cmd)}")
            return {
                'success': True,
                'stats': {'processed': 0, 'assigned': 0, 'errors': 0},
                'dry_run': True
            }
        
        return self._execute_cli_script(cmd, "Stage 2")
    
    def _execute_cli_script(self, cmd: List[str], stage_name: str) -> Dict[str, Any]:
        """Execute a CLI script and parse results"""
        try:
            self.logger.info(f"Executing: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode != 0:
                self.logger.error(f"{stage_name} failed with exit code {result.returncode}")
                self.logger.error(f"STDERR: {result.stderr}")
                return {
                    'success': False,
                    'error': result.stderr or "Unknown error",
                    'exit_code': result.returncode
                }
            
            # Parse output for statistics (basic implementation)
            stats = self._parse_cli_output(result.stdout)
            
            self.logger.info(f"{stage_name} completed successfully")
            return {
                'success': True,
                'stats': stats,
                'output': result.stdout
            }
            
        except subprocess.TimeoutExpired:
            self.logger.error(f"{stage_name} timed out")
            return {
                'success': False,
                'error': "Script execution timed out"
            }
        except Exception as e:
            self.logger.error(f"{stage_name} execution failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _parse_cli_output(self, output: str) -> Dict[str, Any]:
        """Parse CLI script output for statistics"""
        stats = {'processed': 0, 'assigned': 0, 'errors': 0}
        
        # Basic parsing - can be enhanced based on actual output formats
        lines = output.split('\n')
        for line in lines:
            if 'Processed:' in line:
                try:
                    stats['processed'] = int(line.split(':')[1].strip().replace(',', ''))
                except ValueError:
                    pass
            elif 'Assigned:' in line:
                try:
                    stats['assigned'] = int(line.split(':')[1].strip().replace(',', ''))
                except ValueError:
                    pass
            elif 'Errors:' in line:
                try:
                    stats['errors'] = int(line.split(':')[1].strip().replace(',', ''))
                except ValueError:
                    pass
        
        return stats
    
    def _get_assignment_stats(self) -> Dict[str, Any]:
        """Get current assignment statistics from database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Total spots
            cursor.execute("SELECT COUNT(*) FROM spots")
            total_spots = cursor.fetchone()[0]
            
            # Assigned spots
            cursor.execute("SELECT COUNT(*) FROM spot_language_blocks")
            assigned_spots = cursor.fetchone()[0]
            
            # Unassigned spots
            unassigned_spots = total_spots - assigned_spots
            
            conn.close()
            
            return {
                'total_spots': total_spots,
                'assigned_spots': assigned_spots,
                'unassigned_spots': unassigned_spots
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get assignment stats: {e}")
            return {
                'total_spots': 0,
                'assigned_spots': 0,
                'unassigned_spots': 0
            }
    
    def _log_final_results(self, result: PipelineResult, initial_stats: Dict[str, Any], 
                          final_stats: Dict[str, Any]):
        """Log final pipeline results"""
        self.logger.info("🎉 PIPELINE COMPLETED")
        
        if result.dry_run:
            self.logger.info("   • DRY RUN MODE - No changes made")
        
        self.logger.info(f"📊 ASSIGNMENT PIPELINE RESULTS:")
        self.logger.info(f"   • Total spots processed: {result.total_processed:,}")
        self.logger.info(f"   • Previously assigned: {initial_stats['assigned_spots']:,}")
        self.logger.info(f"   • Newly assigned: {final_stats['assigned_spots'] - initial_stats['assigned_spots']:,}")
        self.logger.info(f"   • Still unassigned: {final_stats['unassigned_spots']:,}")
        self.logger.info(f"   • Execution time: {result.execution_time:.1f} seconds")
        
        if result.stage1_stats:
            self.logger.info(f"🎯 STAGE 1 - Language Block Assignment:")
            self.logger.info(f"   • Processed: {result.stage1_stats.get('processed', 0):,}")
            self.logger.info(f"   • Assigned: {result.stage1_stats.get('assigned', 0):,}")
            self.logger.info(f"   • Errors: {result.stage1_stats.get('errors', 0):,}")
        
        if result.stage2_stats:
            self.logger.info(f"🎯 STAGE 2 - Business Rules Assignment:")
            self.logger.info(f"   • Processed: {result.stage2_stats.get('processed', 0):,}")
            self.logger.info(f"   • Assigned: {result.stage2_stats.get('assigned', 0):,}")
            self.logger.info(f"   • Errors: {result.stage2_stats.get('errors', 0):,}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current pipeline status"""
        stats = self._get_assignment_stats()
        
        # Additional status information
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get assignment breakdown by method
        cursor.execute("""
            SELECT assignment_method, COUNT(*) 
            FROM spot_language_blocks 
            GROUP BY assignment_method
        """)
        assignment_methods = dict(cursor.fetchall())
        
        # Get business rule breakdown
        cursor.execute("""
            SELECT business_rule_applied, COUNT(*) 
            FROM spot_language_blocks 
            WHERE business_rule_applied IS NOT NULL
            GROUP BY business_rule_applied
        """)
        business_rules = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_spots': stats['total_spots'],
            'assigned_spots': stats['assigned_spots'],
            'unassigned_spots': stats['unassigned_spots'],
            'assignment_percentage': (stats['assigned_spots'] / stats['total_spots'] * 100) if stats['total_spots'] > 0 else 0,
            'assignment_methods': assignment_methods,
            'business_rules': business_rules
        }


def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(description="Assignment Pipeline Orchestrator")
    parser.add_argument("--database", default="data/database/production.db", help="Database path")
    parser.add_argument("--dry-run", action="store_true", help="Preview actions without making changes")
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--year", type=int, help="Assign all spots for specified year (e.g., 2025)")
    mode_group.add_argument("--test", type=int, metavar="N", help="Test pipeline with N spots")
    mode_group.add_argument("--batch", type=int, metavar="N", help="Process N spots in pipeline")
    mode_group.add_argument("--status", action="store_true", help="Show current pipeline status")
    
    args = parser.parse_args()
    
    try:
        pipeline = AssignmentPipeline(db_path=args.database, dry_run=args.dry_run)
        
        if args.status:
            # Show current status
            status = pipeline.get_status()
            
            print(f"\n📊 ASSIGNMENT PIPELINE STATUS:")
            print(f"   • Total spots: {status['total_spots']:,}")
            print(f"   • Assigned spots: {status['assigned_spots']:,} ({status['assignment_percentage']:.1f}%)")
            print(f"   • Unassigned spots: {status['unassigned_spots']:,}")
            
            if status['assignment_methods']:
                print(f"\n🔧 ASSIGNMENT METHODS:")
                for method, count in status['assignment_methods'].items():
                    print(f"   • {method}: {count:,}")
            
            if status['business_rules']:
                print(f"\n📋 BUSINESS RULES:")
                for rule, count in status['business_rules'].items():
                    print(f"   • {rule}: {count:,}")
            
        else:
            # Execute pipeline
            if args.dry_run:
                print(f"🔍 DRY RUN MODE - No changes will be made")
            
            result = pipeline.execute_pipeline(
                year=args.year,
                batch=args.batch,
                test=args.test
            )
            
            if result.success:
                print(f"\n✅ Pipeline completed successfully!")
                return 0
            else:
                print(f"\n❌ Pipeline failed: {result.error_message}")
                return 1
                
    except Exception as e:
        print(f"❌ Pipeline initialization failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())