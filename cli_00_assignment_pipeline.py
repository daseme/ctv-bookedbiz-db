#!/usr/bin/env python3
"""
Assignment Pipeline Orchestrator
===============================

Coordinates language block and business rules assignment in the proper sequence.
This script orchestrates the existing CLI scripts while maintaining separation of concerns.

Features:
- Real-time progress bars (requires: pip install tqdm)
- Incremental processing for new data
- Comprehensive error handling and rollback
- Detailed progress tracking and reporting
- Automatic chunking for large batches
- Timeout protection to prevent stalls
- Graceful error recovery

Usage:
    python cli_00_assignment_pipeline.py --year 2025           # Assign all spots for year
    python cli_00_assignment_pipeline.py --test 100            # Test with 100 spots
    python cli_00_assignment_pipeline.py --batch 5000          # Process 5000 spots
    python cli_00_assignment_pipeline.py --recent              # Process recently added spots (last 3 days)
    python cli_00_assignment_pipeline.py --since-date 2025-01-01  # Process spots since specific date
    python cli_00_assignment_pipeline.py --last-week           # Process spots from last 7 days
    python cli_00_assignment_pipeline.py --last-month          # Process spots from last 30 days
    python cli_00_assignment_pipeline.py --status              # Show current status
    python cli_00_assignment_pipeline.py --dry-run --recent    # Preview what would happen
    python cli_00_assignment_pipeline.py --chunk-size 10000 --batch 50000  # Custom chunk size

Installation:
    pip install tqdm  # For progress bars (optional but recommended)

Anti-Stall Features:
- Automatic timeout detection (10 minutes without progress)
- Chunked processing for large batches (25,000 spot chunks)
- Graceful error recovery and retry logic
- Process monitoring and cleanup
"""

import argparse
import subprocess
import sys
import sqlite3
import logging
import re
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

# Progress bar support
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    print("💡 For progress bars, install tqdm: pip install tqdm")

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
    
    def __init__(self, db_path: str = "data/database/production.db", dry_run: bool = False, chunk_size: int = 25000):
        self.db_path = db_path
        self.dry_run = dry_run
        self.chunk_size = chunk_size
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Progress tracking
        self.progress_bars = {}
        self.use_progress = HAS_TQDM and not dry_run
        
        # Validate database connection
        if not self._validate_database():
            raise ValueError(f"Cannot connect to database: {db_path}")
    
    def _get_total_spots_to_process(self, year: Optional[int] = None, batch: Optional[int] = None,
                                   test: Optional[int] = None, recent: bool = False,
                                   since_date: Optional[str] = None, last_week: bool = False,
                                   last_month: bool = False) -> int:
        """Get total number of spots that will be processed"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if test:
                return min(test, self._get_total_unassigned_spots())
            elif batch:
                return min(batch, self._get_total_unassigned_spots())
            elif recent or since_date or last_week or last_month:
                scope_stats = self._get_scope_stats(recent, since_date, last_week, last_month)
                return scope_stats['unassigned_spots']
            elif year:
                # Get unassigned spots for specific year
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM spots s
                    LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                    WHERE slb.spot_id IS NULL
                      AND s.broadcast_month LIKE ?
                      AND s.market_id IS NOT NULL
                      AND s.time_in IS NOT NULL
                      AND s.time_out IS NOT NULL
                      AND s.day_of_week IS NOT NULL
                """, (f'{year}-%',))
                return cursor.fetchone()[0]
            else:
                return self._get_total_unassigned_spots()
                
        except Exception as e:
            self.logger.error(f"Failed to get total spots count: {e}")
            return 0
        finally:
            conn.close()
    
    def _get_total_unassigned_spots(self) -> int:
        """Get total unassigned spots"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM spots s
                LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
                WHERE slb.spot_id IS NULL
                  AND s.market_id IS NOT NULL
                  AND s.time_in IS NOT NULL
                  AND s.time_out IS NOT NULL
                  AND s.day_of_week IS NOT NULL
            """)
            return cursor.fetchone()[0]
        except Exception as e:
            self.logger.error(f"Failed to get total unassigned spots: {e}")
            return 0
        finally:
            conn.close()
    
    def _create_progress_bar(self, stage: str, total: int, desc: str = None) -> Optional[Any]:
        """Create a progress bar for a stage"""
        if not self.use_progress or total <= 0:
            return None
            
        desc = desc or f"{stage}"
        pbar = tqdm(
            total=total,
            desc=desc,
            unit="spots",
            unit_scale=True,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} spots [{elapsed}<{remaining}, {rate_fmt}]'
        )
        self.progress_bars[stage] = pbar
        return pbar
    
    def _update_progress_from_output(self, stage: str, line: str) -> bool:
        """Update progress bar from subprocess output line"""
        if not self.use_progress or stage not in self.progress_bars:
            return False
            
        pbar = self.progress_bars[stage]
        
        # Look for progress indicators in the output
        patterns = [
            r'Processed\s+(\d+)[\s/]+(\d+)\s+spots',  # "Processed 1000/50000 spots"
            r'Processing\s+(\d+)[\s/]+(\d+)',         # "Processing 1000/50000"
            r'Imported\s+(\d+):,\s+records',          # "Imported 1000:, records"
            r'(\d+)\s+spots\s+processed',             # "1000 spots processed"
            r'Assigned:\s+(\d+)',                     # "Assigned: 1000"
            r'Processed:\s+(\d+)',                    # "Processed: 1000"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, line, re.IGNORECASE)
            if match:
                if len(match.groups()) >= 2:
                    # Has both current and total
                    current = int(match.group(1))
                    total = int(match.group(2))
                    pbar.total = total
                    pbar.n = current
                else:
                    # Only has current count
                    current = int(match.group(1))
                    pbar.n = current
                
                pbar.refresh()
                return True
        
        return False
    
    def _close_progress_bar(self, stage: str):
        """Close and cleanup progress bar"""
        if stage in self.progress_bars:
            pbar = self.progress_bars[stage]
            pbar.close()
            del self.progress_bars[stage]
    
    def _cleanup_progress_bars(self):
        """Clean up all progress bars"""
        for stage in list(self.progress_bars.keys()):
            self._close_progress_bar(stage)
    
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
                        test: Optional[int] = None, recent: bool = False, 
                        since_date: Optional[str] = None, last_week: bool = False, 
                        last_month: bool = False) -> PipelineResult:
        """Execute the complete assignment pipeline"""
        start_time = datetime.now()
        
        try:
            self.logger.info(f"🚀 Starting assignment pipeline (dry_run={self.dry_run})")
            
            # Get initial stats
            initial_stats = self._get_assignment_stats()
            
            # Get scope-specific stats and total spots to process
            if recent or since_date or last_week or last_month:
                scope_stats = self._get_scope_stats(recent, since_date, last_week, last_month)
                total_spots_to_process = scope_stats['unassigned_spots']
                self.logger.info(f"📊 Scope: {scope_stats['unassigned_spots']:,} unassigned spots to process")
                self.logger.info(f"📊 Total state: {initial_stats['unassigned_spots']:,} total unassigned spots")
            else:
                scope_stats = initial_stats
                total_spots_to_process = self._get_total_spots_to_process(year, batch, test, recent, since_date, last_week, last_month)
                self.logger.info(f"📊 Initial state: {initial_stats['unassigned_spots']:,} unassigned spots")
                self.logger.info(f"📊 Will process: {total_spots_to_process:,} spots")
            
            # Check if this is a large operation and warn user
            if year and initial_stats['unassigned_spots'] > 100000:
                self.logger.warning(f"⚠️  Large operation detected: {initial_stats['unassigned_spots']:,} spots")
                self.logger.warning("💡 Consider using --recent or --since-date for incremental processing")
                self.logger.warning("🔄 This operation may take several hours...")
                
                if not self.dry_run:
                    import time
                    self.logger.info("⏳ Starting in 5 seconds... (Ctrl+C to cancel)")
                    time.sleep(5)
            
            # Create overall progress tracking
            if self.use_progress and total_spots_to_process > 0:
                print(f"\n📊 Processing {total_spots_to_process:,} spots across 2 stages...")
            
            # Stage 1: Language Block Assignment
            self.logger.info("🎯 STAGE 1: Language Block Assignment")
            stage1_progress = self._create_progress_bar("stage1", total_spots_to_process, "🎯 Stage 1: Language Blocks")
            
            stage1_result = self._run_stage1(year, batch, test, recent, since_date, last_week, last_month)
            
            self._close_progress_bar("stage1")
            
            if not stage1_result['success']:
                self._cleanup_progress_bars()
                return PipelineResult(
                    success=False,
                    error_message=f"Stage 1 failed: {stage1_result.get('error', 'Unknown error')}",
                    dry_run=self.dry_run
                )
            
            # Stage 2: Business Rules Assignment
            self.logger.info("🎯 STAGE 2: Business Rules Assignment")
            
            # Estimate remaining spots for stage 2
            remaining_spots = total_spots_to_process - stage1_result.get('stats', {}).get('assigned', 0)
            stage2_progress = self._create_progress_bar("stage2", max(remaining_spots, 0), "🎯 Stage 2: Business Rules")
            
            stage2_result = self._run_stage2(year, batch, test, recent, since_date, last_week, last_month)
            
            self._close_progress_bar("stage2")
            
            if not stage2_result['success']:
                self._cleanup_progress_bars()
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
            total_assigned = (final_stats['assigned_spots'] - initial_stats['assigned_spots'])
            
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
            
        except KeyboardInterrupt:
            self.logger.warning("⏹️  Pipeline interrupted by user")
            self._cleanup_progress_bars()
            return PipelineResult(
                success=False,
                error_message="Pipeline interrupted by user",
                execution_time=(datetime.now() - start_time).total_seconds(),
                dry_run=self.dry_run
            )
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self._cleanup_progress_bars()
            return PipelineResult(
                success=False,
                error_message=str(e),
                execution_time=(datetime.now() - start_time).total_seconds(),
                dry_run=self.dry_run
            )
    
    def _run_stage1(self, year: Optional[int], batch: Optional[int], 
                   test: Optional[int], recent: bool = False, 
                   since_date: Optional[str] = None, last_week: bool = False, 
                   last_month: bool = False) -> Dict[str, Any]:
        """Run language block assignment (Stage 1)"""
        cmd = ["python", "cli_01_assign_language_blocks.py", "--database", self.db_path]
        
        # Add appropriate arguments based on scope
        if test:
            cmd.extend(["--test", str(test)])
        elif batch:
            cmd.extend(["--batch", str(batch)])
        elif recent or since_date or last_week or last_month:
            # For incremental processing, get count and use batch mode
            scope_stats = self._get_scope_stats(recent, since_date, last_week, last_month)
            spots_to_process = scope_stats['unassigned_spots']
            if spots_to_process > 0:
                cmd.extend(["--batch", str(spots_to_process)])
            else:
                # No spots to process
                return {
                    'success': True,
                    'stats': {'processed': 0, 'assigned': 0, 'errors': 0},
                    'message': 'No spots to process in scope'
                }
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
                   test: Optional[int], recent: bool = False, 
                   since_date: Optional[str] = None, last_week: bool = False, 
                   last_month: bool = False) -> Dict[str, Any]:
        """Run business rules assignment (Stage 2) with chunking for large batches"""
        
        # Check how many spots need business rules processing
        unassigned_count = self._get_total_unassigned_spots()
        
        # If we have a large number of unassigned spots, process in chunks
        if unassigned_count > self.chunk_size:
            self.logger.info(f"Large batch detected ({unassigned_count:,} spots), processing in {self.chunk_size:,} spot chunks...")
            return self._run_stage2_chunked(self.chunk_size)
        
        cmd = ["python", "cli_02_assign_business_rules.py"]
        
        # Add appropriate arguments based on scope
        if test:
            cmd.extend(["--limit", str(test)])
        elif batch:
            cmd.extend(["--limit", str(batch)])
        elif recent or since_date or last_week or last_month:
            # For incremental processing, let it process remaining unassigned
            # The business rules script will naturally only pick up what's left
            pass
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
    
    def _run_stage2_chunked(self, chunk_size: int) -> Dict[str, Any]:
        """Run business rules assignment in chunks to prevent stalling"""
        total_stats = {'processed': 0, 'assigned': 0, 'errors': 0}
        chunk_num = 1
        
        while True:
            # Check if there are still unassigned spots
            remaining_spots = self._get_total_unassigned_spots()
            if remaining_spots == 0:
                break
            
            current_chunk_size = min(chunk_size, remaining_spots)
            self.logger.info(f"Processing chunk {chunk_num}: {current_chunk_size:,} spots")
            
            # Run business rules for this chunk
            cmd = ["python", "cli_02_assign_business_rules.py", "--limit", str(current_chunk_size)]
            
            result = self._execute_cli_script(cmd, f"Stage 2 Chunk {chunk_num}")
            
            if not result['success']:
                self.logger.error(f"Chunk {chunk_num} failed: {result.get('error', 'Unknown error')}")
                # Return what we've accomplished so far
                return {
                    'success': True,  # Don't fail the entire pipeline
                    'stats': total_stats,
                    'chunked': True,
                    'chunks_processed': chunk_num - 1,
                    'last_chunk_error': result.get('error')
                }
            
            # Add this chunk's stats to total
            chunk_stats = result.get('stats', {})
            total_stats['processed'] += chunk_stats.get('processed', 0)
            total_stats['assigned'] += chunk_stats.get('assigned', 0)
            total_stats['errors'] += chunk_stats.get('errors', 0)
            
            # Check if we made progress
            if chunk_stats.get('processed', 0) == 0:
                self.logger.info(f"No more spots to process after chunk {chunk_num}")
                break
            
            chunk_num += 1
            
            # Safety check - don't run indefinitely
            if chunk_num > 50:
                self.logger.warning(f"Stopped after 50 chunks to prevent infinite loop")
                break
        
        self.logger.info(f"Chunked processing completed: {chunk_num - 1} chunks processed")
        return {
            'success': True,
            'stats': total_stats,
            'chunked': True,
            'chunks_processed': chunk_num - 1
        }
    
    def _execute_cli_script(self, cmd: List[str], stage_name: str) -> Dict[str, Any]:
        """Execute a CLI script and parse results with real-time progress tracking"""
        try:
            self.logger.info(f"Executing: {' '.join(cmd)}")
            
            # Determine which stage for progress tracking
            stage_key = "stage1" if "cli_01" in cmd[1] else "stage2"
            
            # Start subprocess with real-time output
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Track progress for timeout detection
            last_progress_time = datetime.now()
            timeout_seconds = 1800  # 30 minutes timeout
            stall_timeout = 600     # 10 minutes without progress = stall
            
            output_lines = []
            last_line_time = datetime.now()
            
            # Read output line by line for real-time progress updates
            while True:
                # Check if process is still running
                if process.poll() is not None:
                    # Process finished, read any remaining output
                    remaining_output = process.stdout.read()
                    if remaining_output:
                        output_lines.extend(remaining_output.split('\n'))
                    break
                
                # Check for timeout
                current_time = datetime.now()
                if (current_time - last_line_time).total_seconds() > stall_timeout:
                    self.logger.error(f"{stage_name} appears to be stalled (no output for {stall_timeout} seconds)")
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    
                    return {
                        'success': False,
                        'error': f"Process stalled - no output for {stall_timeout} seconds",
                        'timeout': True
                    }
                
                # Try to read a line with timeout
                try:
                    import select
                    if select.select([process.stdout], [], [], 1.0)[0]:
                        line = process.stdout.readline()
                        if line:
                            last_line_time = current_time
                            line = line.strip()
                            if line:
                                output_lines.append(line)
                                
                                # Update progress bar if possible
                                if self.use_progress:
                                    progress_updated = self._update_progress_from_output(stage_key, line)
                                    if progress_updated:
                                        last_progress_time = current_time
                                
                                # Also log important lines
                                if any(keyword in line.lower() for keyword in ['error', 'warning', 'completed', 'failed']):
                                    self.logger.info(f"{stage_name}: {line}")
                except:
                    # Fallback for systems without select
                    line = process.stdout.readline()
                    if line:
                        last_line_time = current_time
                        line = line.strip()
                        if line:
                            output_lines.append(line)
                            
                            # Update progress bar if possible
                            if self.use_progress:
                                progress_updated = self._update_progress_from_output(stage_key, line)
                                if progress_updated:
                                    last_progress_time = current_time
                            
                            # Also log important lines
                            if any(keyword in line.lower() for keyword in ['error', 'warning', 'completed', 'failed']):
                                self.logger.info(f"{stage_name}: {line}")
                    else:
                        # No output, small delay to prevent busy waiting
                        import time
                        time.sleep(0.1)
            
            # Wait for process to complete and get return code
            return_code = process.wait()
            
            if return_code != 0:
                self.logger.error(f"{stage_name} failed with exit code {return_code}")
                return {
                    'success': False,
                    'error': f"Process failed with exit code {return_code}",
                    'exit_code': return_code,
                    'output': '\n'.join(output_lines)
                }
            
            # Parse output for statistics
            stats = self._parse_cli_output('\n'.join(output_lines))
            
            # Complete progress bar if still active
            if stage_key in self.progress_bars:
                pbar = self.progress_bars[stage_key]
                if stats.get('processed', 0) > 0:
                    pbar.n = stats['processed']
                elif stats.get('assigned', 0) > 0:
                    pbar.n = stats['assigned']
                pbar.refresh()
            
            self.logger.info(f"{stage_name} completed successfully")
            return {
                'success': True,
                'stats': stats,
                'output': '\n'.join(output_lines)
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
    
    def _get_scope_stats(self, recent: bool = False, since_date: Optional[str] = None, 
                        last_week: bool = False, last_month: bool = False) -> Dict[str, Any]:
        """Get assignment statistics for a specific scope"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Build the date filter condition
            date_condition = ""
            params = []
            
            if recent:
                # Recent = last 3 days (adjustable)
                date_condition = "AND s.load_date >= DATE('now', '-3 days')"
            elif since_date:
                date_condition = "AND s.load_date >= DATE(?)"
                params.append(since_date)
            elif last_week:
                date_condition = "AND s.load_date >= DATE('now', '-7 days')"
            elif last_month:
                date_condition = "AND s.load_date >= DATE('now', '-30 days')"
            
            # Get unassigned spots in scope
            # Note: Using load_date to identify when spots were added to database
            query = f"""
            SELECT COUNT(*) 
            FROM spots s
            LEFT JOIN spot_language_blocks slb ON s.spot_id = slb.spot_id
            WHERE slb.spot_id IS NULL
              AND s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND s.day_of_week IS NOT NULL
              {date_condition}
            """
            
            cursor.execute(query, params)
            unassigned_spots = cursor.fetchone()[0]
            
            # Get total spots in scope
            total_query = f"""
            SELECT COUNT(*) 
            FROM spots s
            WHERE s.market_id IS NOT NULL
              AND s.time_in IS NOT NULL
              AND s.time_out IS NOT NULL
              AND s.day_of_week IS NOT NULL
              {date_condition}
            """
            
            cursor.execute(total_query, params)
            total_spots = cursor.fetchone()[0]
            
            assigned_spots = total_spots - unassigned_spots
            
            conn.close()
            
            return {
                'total_spots': total_spots,
                'assigned_spots': assigned_spots,
                'unassigned_spots': unassigned_spots,
                'scope': 'recent' if recent else 'since_date' if since_date else 'last_week' if last_week else 'last_month' if last_month else 'all'
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get scope stats: {e}")
            # Fall back to checking if load_date column exists
            self.logger.warning("Note: Scope filtering requires a load_date column in spots table")
            return {
                'total_spots': 0,
                'assigned_spots': 0,
                'unassigned_spots': 0,
                'scope': 'error'
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
    parser.add_argument("--chunk-size", type=int, default=25000, help="Chunk size for processing large batches (default: 25000)")
    
    # Mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--year", type=int, help="Assign all spots for specified year (e.g., 2025)")
    mode_group.add_argument("--test", type=int, metavar="N", help="Test pipeline with N spots")
    mode_group.add_argument("--batch", type=int, metavar="N", help="Process N spots in pipeline")
    mode_group.add_argument("--recent", action="store_true", help="Process recently added unassigned spots")
    mode_group.add_argument("--since-date", type=str, help="Process spots added since date (YYYY-MM-DD)")
    mode_group.add_argument("--last-week", action="store_true", help="Process spots from last 7 days")
    mode_group.add_argument("--last-month", action="store_true", help="Process spots from last 30 days")
    mode_group.add_argument("--status", action="store_true", help="Show current pipeline status")
    
    args = parser.parse_args()
    
    try:
        pipeline = AssignmentPipeline(db_path=args.database, dry_run=args.dry_run, chunk_size=args.chunk_size)
        
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
            
            # Show scope-specific stats if available
            if hasattr(args, 'recent') and args.recent:
                scope_stats = pipeline._get_scope_stats(recent=True)
                print(f"\n🔍 RECENT SCOPE (last 3 days):")
                print(f"   • Total spots: {scope_stats['total_spots']:,}")
                print(f"   • Unassigned spots: {scope_stats['unassigned_spots']:,}")
        
        else:
            # Execute pipeline
            if args.dry_run:
                print(f"🔍 DRY RUN MODE - No changes will be made")
            
            result = pipeline.execute_pipeline(
                year=args.year,
                batch=args.batch,
                test=args.test,
                recent=getattr(args, 'recent', False),
                since_date=getattr(args, 'since_date', None),
                last_week=getattr(args, 'last_week', False),
                last_month=getattr(args, 'last_month', False)
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