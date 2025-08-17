# src/services/review_session_management.py
"""
Bi-Weekly Review Session Management System
Handles review sessions, bulk calibration, and progress tracking
"""

import json
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SessionStatus(Enum):
    """Review session status."""
    PLANNED = "planned"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class AEReviewStatus(Enum):
    """AE review status within a session."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    SKIPPED = "skipped"


@dataclass
class CalibrationAction:
    """Represents a calibration action during review."""
    ae_id: str
    ae_name: str
    month: str
    old_pipeline: float
    new_pipeline: float
    calibrated_by: str
    timestamp: str
    notes: str = ""
    
    @property
    def adjustment_amount(self) -> float:
        """Calculate the adjustment amount."""
        return self.new_pipeline - self.old_pipeline


@dataclass
class AEReviewSession:
    """Individual AE review within a session."""
    ae_id: str
    ae_name: str
    status: AEReviewStatus
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    calibrations: List[CalibrationAction] = None
    notes: str = ""
    reviewer: str = ""
    
    def __post_init__(self):
        if self.calibrations is None:
            self.calibrations = []


@dataclass
class ReviewSession:
    """Complete review session."""
    session_id: str
    session_name: str
    status: SessionStatus
    created_by: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    ae_reviews: Dict[str, AEReviewSession] = None
    session_notes: str = ""
    target_completion_date: Optional[str] = None
    
    def __post_init__(self):
        if self.ae_reviews is None:
            self.ae_reviews = {}
    
    @property
    def total_aes(self) -> int:
        """Total number of AEs in this session."""
        return len(self.ae_reviews)
    
    @property
    def completed_aes(self) -> int:
        """Number of completed AE reviews."""
        return sum(1 for review in self.ae_reviews.values() 
                  if review.status == AEReviewStatus.COMPLETED)
    
    @property
    def completion_percentage(self) -> float:
        """Session completion percentage."""
        if self.total_aes == 0:
            return 0.0
        return (self.completed_aes / self.total_aes) * 100
    
    @property
    def total_calibrations(self) -> int:
        """Total number of calibrations performed."""
        return sum(len(review.calibrations) for review in self.ae_reviews.values())


class ReviewSessionManager:
    """
    Review Session Management System
    
    Handles bi-weekly review sessions with:
    - Session creation and management
    - AE-by-AE review workflow
    - Bulk calibration operations
    - Progress tracking and reporting
    """
    
    def __init__(self, data_path: str, ae_service, pipeline_service):
        """
        Initialize the review session manager.
        
        Args:
            data_path: Path to data directory
            ae_service: AE service instance
            pipeline_service: Pipeline service with decay engine
        """
        self.data_path = data_path
        self.sessions_file = os.path.join(data_path, 'review_sessions.json')
        self.ae_service = ae_service
        self.pipeline_service = pipeline_service
        
        # Ensure sessions file exists
        self._ensure_sessions_file()
    
    def _ensure_sessions_file(self):
        """Ensure review sessions file exists."""
        if not os.path.exists(self.sessions_file):
            default_data = {
                "schema_version": "1.0",
                "last_updated": datetime.utcnow().isoformat() + "Z",
                "current_session_id": None,
                "sessions": {},
                "session_history": [],
                "metadata": {
                    "created_by": "review_session_manager",
                    "created_date": datetime.utcnow().isoformat() + "Z"
                }
            }
            self._write_sessions_data(default_data)
    
    def _read_sessions_data(self) -> Dict[str, Any]:
        """Read sessions data safely."""
        try:
            with open(self.sessions_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error reading sessions file: {e}")
            return {"sessions": {}, "session_history": []}
    
    def _write_sessions_data(self, data: Dict[str, Any]):
        """Write sessions data safely."""
        try:
            data['last_updated'] = datetime.utcnow().isoformat() + "Z"
            
            with open(self.sessions_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Error writing sessions file: {e}")
            raise
    
    def create_review_session(self, session_name: str, created_by: str,
                            target_completion_date: str = None) -> str:
        """
        Create a new review session.
        
        Args:
            session_name: Name for the session
            created_by: Who created the session
            target_completion_date: Target completion date (ISO format)
            
        Returns:
            Session ID
        """
        session_id = f"RS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Get list of AEs to include in review
        ae_list = self.ae_service.get_filtered_ae_list()
        
        # Create AE review records
        ae_reviews = {}
        for ae in ae_list:
            ae_reviews[ae['ae_id']] = AEReviewSession(
                ae_id=ae['ae_id'],
                ae_name=ae['name'],
                status=AEReviewStatus.PENDING
            )
        
        # Create session
        session = ReviewSession(
            session_id=session_id,
            session_name=session_name,
            status=SessionStatus.PLANNED,
            created_by=created_by,
            created_at=datetime.utcnow().isoformat() + "Z",
            ae_reviews=ae_reviews,
            target_completion_date=target_completion_date
        )
        
        # Save to file
        sessions_data = self._read_sessions_data()
        sessions_data['sessions'][session_id] = asdict(session)
        sessions_data['current_session_id'] = session_id
        self._write_sessions_data(sessions_data)
        
        logger.info(f"Created review session {session_id} with {len(ae_reviews)} AEs")
        return session_id
    
    def start_review_session(self, session_id: str, started_by: str) -> bool:
        """Start a review session."""
        try:
            sessions_data = self._read_sessions_data()
            
            if session_id not in sessions_data['sessions']:
                logger.error(f"Session {session_id} not found")
                return False
            
            session_data = sessions_data['sessions'][session_id]
            session_data['status'] = SessionStatus.IN_PROGRESS.value
            session_data['started_at'] = datetime.utcnow().isoformat() + "Z"
            
            self._write_sessions_data(sessions_data)
            
            logger.info(f"Started review session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting session {session_id}: {e}")
            return False
    
    def complete_review_session(self, session_id: str, completed_by: str,
                              session_notes: str = "") -> bool:
        """Complete a review session."""
        try:
            sessions_data = self._read_sessions_data()
            
            if session_id not in sessions_data['sessions']:
                logger.error(f"Session {session_id} not found")
                return False
            
            session_data = sessions_data['sessions'][session_id]
            session_data['status'] = SessionStatus.COMPLETED.value
            session_data['completed_at'] = datetime.utcnow().isoformat() + "Z"
            session_data['session_notes'] = session_notes
            
            # Move to history
            sessions_data['session_history'].append({
                'session_id': session_id,
                'session_name': session_data['session_name'],
                'completed_at': session_data['completed_at'],
                'completed_by': completed_by,
                'total_aes': len(session_data['ae_reviews']),
                'completed_aes': sum(1 for ae in session_data['ae_reviews'].values() 
                                   if ae['status'] == AEReviewStatus.COMPLETED.value),
                'total_calibrations': sum(len(ae.get('calibrations', [])) 
                                        for ae in session_data['ae_reviews'].values())
            })
            
            # Clear current session
            sessions_data['current_session_id'] = None
            
            self._write_sessions_data(sessions_data)
            
            logger.info(f"Completed review session {session_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error completing session {session_id}: {e}")
            return False
    
    def start_ae_review(self, session_id: str, ae_id: str, reviewer: str) -> bool:
        """Start reviewing a specific AE."""
        try:
            sessions_data = self._read_sessions_data()
            
            if session_id not in sessions_data['sessions']:
                return False
            
            session_data = sessions_data['sessions'][session_id]
            
            if ae_id not in session_data['ae_reviews']:
                return False
            
            ae_review = session_data['ae_reviews'][ae_id]
            ae_review['status'] = AEReviewStatus.IN_PROGRESS.value
            ae_review['started_at'] = datetime.utcnow().isoformat() + "Z"
            ae_review['reviewer'] = reviewer
            
            self._write_sessions_data(sessions_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Error starting AE review {ae_id}: {e}")
            return False
    
    def complete_ae_review(self, session_id: str, ae_id: str, notes: str = "") -> bool:
        """Complete reviewing a specific AE."""
        try:
            sessions_data = self._read_sessions_data()
            
            if session_id not in sessions_data['sessions']:
                return False
            
            session_data = sessions_data['sessions'][session_id]
            
            if ae_id not in session_data['ae_reviews']:
                return False
            
            ae_review = session_data['ae_reviews'][ae_id]
            ae_review['status'] = AEReviewStatus.COMPLETED.value
            ae_review['completed_at'] = datetime.utcnow().isoformat() + "Z"
            ae_review['notes'] = notes
            
            self._write_sessions_data(sessions_data)
            
            return True
            
        except Exception as e:
            logger.error(f"Error completing AE review {ae_id}: {e}")
            return False
    
    def apply_calibration(self, session_id: str, ae_id: str, month: str,
                         new_pipeline: float, calibrated_by: str, notes: str = "") -> bool:
        """
        Apply a calibration during review session.
        
        Args:
            session_id: Review session ID
            ae_id: AE ID
            month: Month to calibrate
            new_pipeline: New pipeline value
            calibrated_by: Who performed calibration
            notes: Calibration notes
            
        Returns:
            Success status
        """
        try:
            # Get current pipeline value
            current_data = self.pipeline_service.get_pipeline_data(ae_id, month)
            old_pipeline = current_data.get('current_pipeline', 0) if current_data else 0
            
            # Apply calibration through pipeline service
            success = self.pipeline_service.set_pipeline_calibration(
                ae_id=ae_id,
                month=month,
                pipeline_value=new_pipeline,
                calibrated_by=calibrated_by,
                session_id=session_id
            )
            
            if not success:
                logger.error(f"Failed to apply calibration for {ae_id} {month}")
                return False
            
            # Record calibration in session
            sessions_data = self._read_sessions_data()
            
            if session_id in sessions_data['sessions']:
                session_data = sessions_data['sessions'][session_id]
                
                if ae_id in session_data['ae_reviews']:
                    calibration = CalibrationAction(
                        ae_id=ae_id,
                        ae_name=session_data['ae_reviews'][ae_id]['ae_name'],
                        month=month,
                        old_pipeline=old_pipeline,
                        new_pipeline=new_pipeline,
                        calibrated_by=calibrated_by,
                        timestamp=datetime.utcnow().isoformat() + "Z",
                        notes=notes
                    )
                    
                    if 'calibrations' not in session_data['ae_reviews'][ae_id]:
                        session_data['ae_reviews'][ae_id]['calibrations'] = []
                    
                    session_data['ae_reviews'][ae_id]['calibrations'].append(asdict(calibration))
                    
                    self._write_sessions_data(sessions_data)
            
            logger.info(f"Applied calibration: {ae_id} {month} {old_pipeline} -> {new_pipeline}")
            return True
            
        except Exception as e:
            logger.error(f"Error applying calibration: {e}")
            return False
    
    def bulk_calibration(self, session_id: str, calibrations: List[Dict[str, Any]], 
                        calibrated_by: str) -> Dict[str, Any]:
        """
        Apply multiple calibrations in bulk.
        
        Args:
            session_id: Review session ID
            calibrations: List of calibration data
            calibrated_by: Who performed calibrations
            
        Returns:
            Results summary
        """
        results = {
            'successful': 0,
            'failed': 0,
            'errors': [],
            'calibrations_applied': []
        }
        
        for calibration in calibrations:
            try:
                ae_id = calibration['ae_id']
                month = calibration['month']
                pipeline_value = calibration['pipeline_value']
                notes = calibration.get('notes', '')
                
                success = self.apply_calibration(
                    session_id=session_id,
                    ae_id=ae_id,
                    month=month,
                    new_pipeline=pipeline_value,
                    calibrated_by=calibrated_by,
                    notes=notes
                )
                
                if success:
                    results['successful'] += 1
                    results['calibrations_applied'].append({
                        'ae_id': ae_id,
                        'month': month,
                        'pipeline_value': pipeline_value
                    })
                else:
                    results['failed'] += 1
                    results['errors'].append(f"Failed to calibrate {ae_id} {month}")
                    
            except Exception as e:
                results['failed'] += 1
                results['errors'].append(f"Error processing calibration: {str(e)}")
        
        logger.info(f"Bulk calibration: {results['successful']} successful, {results['failed']} failed")
        return results
    
    def get_session_status(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a review session."""
        try:
            sessions_data = self._read_sessions_data()
            
            if session_id not in sessions_data['sessions']:
                return None
            
            session_data = sessions_data['sessions'][session_id]
            
            # Calculate progress metrics
            ae_reviews = session_data.get('ae_reviews', {})
            total_aes = len(ae_reviews)
            completed_aes = sum(1 for ae in ae_reviews.values() 
                              if ae['status'] == AEReviewStatus.COMPLETED.value)
            in_progress_aes = sum(1 for ae in ae_reviews.values() 
                                if ae['status'] == AEReviewStatus.IN_PROGRESS.value)
            total_calibrations = sum(len(ae.get('calibrations', [])) for ae in ae_reviews.values())
            
            # Calculate time metrics
            created_at = datetime.fromisoformat(session_data['created_at'].replace('Z', '+00:00'))
            time_elapsed = datetime.utcnow() - created_at.replace(tzinfo=None)
            
            status = {
                'session_id': session_id,
                'session_name': session_data['session_name'],
                'status': session_data['status'],
                'created_by': session_data['created_by'],
                'created_at': session_data['created_at'],
                'started_at': session_data.get('started_at'),
                'completed_at': session_data.get('completed_at'),
                'progress': {
                    'total_aes': total_aes,
                    'completed_aes': completed_aes,
                    'in_progress_aes': in_progress_aes,
                    'pending_aes': total_aes - completed_aes - in_progress_aes,
                    'completion_percentage': (completed_aes / total_aes * 100) if total_aes > 0 else 0,
                    'total_calibrations': total_calibrations
                },
                'timing': {
                    'elapsed_hours': time_elapsed.total_seconds() / 3600,
                    'target_completion_date': session_data.get('target_completion_date'),
                    'estimated_remaining_hours': 0  # Could calculate based on average review time
                },
                'ae_reviews': ae_reviews
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting session status: {e}")
            return None
    
    def get_current_session(self) -> Optional[Dict[str, Any]]:
        """Get the current active session."""
        try:
            sessions_data = self._read_sessions_data()
            current_session_id = sessions_data.get('current_session_id')
            
            if current_session_id:
                return self.get_session_status(current_session_id)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting current session: {e}")
            return None
    
    def get_session_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get review session history."""
        try:
            sessions_data = self._read_sessions_data()
            history = sessions_data.get('session_history', [])
            
            # Sort by completion date (most recent first)
            history.sort(key=lambda x: x.get('completed_at', ''), reverse=True)
            
            return history[:limit]
            
        except Exception as e:
            logger.error(f"Error getting session history: {e}")
            return []
    
    def get_next_review_recommendation(self) -> Dict[str, Any]:
        """Get recommendation for when the next review should be scheduled."""
        try:
            # Get the last completed session
            history = self.get_session_history(limit=1)
            
            if not history:
                return {
                    'recommended_date': (date.today() + timedelta(days=1)).isoformat(),
                    'reason': 'No previous sessions found',
                    'urgency': 'high'
                }
            
            last_session = history[0]
            last_completed = datetime.fromisoformat(last_session['completed_at'].replace('Z', '+00:00'))
            days_since_last = (datetime.utcnow() - last_completed.replace(tzinfo=None)).days
            
            # Recommend bi-weekly (14 days)
            if days_since_last >= 14:
                urgency = 'high' if days_since_last >= 21 else 'medium'
                return {
                    'recommended_date': date.today().isoformat(),
                    'reason': f'Last review was {days_since_last} days ago',
                    'urgency': urgency,
                    'days_overdue': max(0, days_since_last - 14)
                }
            else:
                next_review_date = last_completed.replace(tzinfo=None) + timedelta(days=14)
                return {
                    'recommended_date': next_review_date.date().isoformat(),
                    'reason': 'Bi-weekly schedule',
                    'urgency': 'low',
                    'days_until_due': (next_review_date.date() - date.today()).days
                }
                
        except Exception as e:
            logger.error(f"Error getting next review recommendation: {e}")
            return {
                'recommended_date': (date.today() + timedelta(days=1)).isoformat(),
                'reason': 'Error calculating recommendation',
                'urgency': 'medium'
            }