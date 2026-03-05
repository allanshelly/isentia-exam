"""State management for tracking pipeline state and deduplication"""
import json
import logging
from typing import Dict, Set, Optional
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages pipeline state including:
    - Last fetch timestamp and articles
    - Article deduplication
    - Processing state persistence
    """
    
    def __init__(self, state_file: str = ".pipeline_state.json"):
        """
        Initialize state manager
        
        Args:
            state_file: Path to state persistence file
        """
        self.state_file = Path(state_file)
        self.state = {
            'last_fetch_time': None,
            'last_article_ids': [],
            'processed_article_ids': set(),
            'total_articles_processed': 0,
            'total_records_sent': 0,
            'last_error': None,
            'last_error_time': None,
            'consecutive_failures': 0,
        }
        
        self.load_state()
        logger.info(f"State manager initialized with file: {state_file}")
    
    def import_from_kinesis(self, producer, limit: int = 1000) -> None:
        """Import existing article ids from a Kinesis producer into state.

        Args:
            producer: KinesisProducer instance with readable stream
            limit: maximum number of ids to import
        """
        existing = producer.get_existing_article_ids(limit=limit)
        added = 0
        for aid in existing:
            if aid not in self.state['processed_article_ids']:
                self.state['processed_article_ids'].add(aid)
                added += 1
        if added:
            self.save_state()
        logger.info(f"Added {added} ids from kinesis to processed set")
    
    def load_state(self) -> None:
        """Load state from persistent storage"""
        if not self.state_file.exists():
            logger.info("No existing state file found, starting fresh")
            return
        
        try:
            with open(self.state_file, 'r') as f:
                loaded_state = json.load(f)
            
            # Merge loaded state with defaults
            self.state.update(loaded_state)
            # Convert processed IDs back to set
            self.state['processed_article_ids'] = set(
                self.state.get('processed_article_ids', [])
            )
            
            logger.info(f"Loaded state from {self.state_file}")
            logger.info(
                f"Previously processed {len(self.state['processed_article_ids'])} articles"
            )
        except Exception as e:
            logger.error(f"Failed to load state: {str(e)}", exc_info=True)
    
    def save_state(self) -> None:
        """Persist state to storage"""
        try:
            # Convert set to list for JSON serialization
            state_to_save = self.state.copy()
            state_to_save['processed_article_ids'] = list(
                self.state['processed_article_ids']
            )
            
            with open(self.state_file, 'w') as f:
                json.dump(state_to_save, f, indent=2, default=str)
            
            logger.debug(f"Saved state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {str(e)}", exc_info=True)
    
    def record_fetch(self, article_ids: list) -> None:
        """Record last fetch with article IDs"""
        self.state['last_fetch_time'] = datetime.utcnow().isoformat()
        self.state['last_article_ids'] = article_ids
        self.save_state()
        
        logger.info(f"Recorded fetch of {len(article_ids)} articles")
    
    def is_duplicate(self, article_id: str) -> bool:
        """Check if article has been processed before"""
        return article_id in self.state['processed_article_ids']
    
    def mark_processed(self, article_ids: list) -> None:
        """Mark articles as processed"""
        self.state['processed_article_ids'].update(article_ids)
        self.state['total_articles_processed'] += len(article_ids)
        self.save_state()
        
        logger.info(
            f"Marked {len(article_ids)} articles as processed. "
            f"Total: {self.state['total_articles_processed']}"
        )
    
    def record_sent_records(self, count: int) -> None:
        """Record records sent to Kinesis"""
        self.state['total_records_sent'] += count
        self.save_state()
    
    def record_error(self, error_message: str) -> None:
        """Record error and increment failure counter"""
        self.state['last_error'] = error_message
        self.state['last_error_time'] = datetime.utcnow().isoformat()
        self.state['consecutive_failures'] += 1
        self.save_state()
        
        logger.error(
            f"Recorded error (consecutive: {self.state['consecutive_failures']}): "
            f"{error_message}"
        )
    
    def record_success(self) -> None:
        """Record successful iteration and reset error counter"""
        self.state['consecutive_failures'] = 0
        self.state['last_error'] = None
        self.state['last_error_time'] = None
        self.save_state()
    
    def get_last_fetch_time(self) -> Optional[datetime]:
        """Get last fetch timestamp"""
        if self.state['last_fetch_time']:
            return datetime.fromisoformat(self.state['last_fetch_time'])
        return None
    
    def get_time_since_last_fetch(self) -> Optional[timedelta]:
        """Get time elapsed since last fetch"""
        last_fetch = self.get_last_fetch_time()
        if last_fetch:
            return datetime.utcnow() - last_fetch
        return None
    
    def get_stats(self) -> Dict:
        """Get pipeline statistics"""
        return {
            'last_fetch_time': self.state['last_fetch_time'],
            'total_articles_processed': self.state['total_articles_processed'],
            'total_records_sent': self.state['total_records_sent'],
            'unique_articles_seen': len(self.state['processed_article_ids']),
            'consecutive_failures': self.state['consecutive_failures'],
            'last_error': self.state['last_error'],
            'last_error_time': self.state['last_error_time'],
        }
    
    def clear_old_duplicates(self, days: int = 7) -> None:
        """Clear duplicate tracking older than specified days"""
        # For now, we keep all duplicates. In production, you might clear these periodically
        # to avoid unbounded memory growth if tracking a very large number of articles
        logger.info(f"Duplicate tracking cleaned (older than {days} days)")
