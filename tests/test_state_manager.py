"""Tests for state management and deduplication"""
import unittest
import json
import tempfile
import os
from datetime import datetime, timedelta
from src.state_manager import StateManager


class TestStateManager(unittest.TestCase):
    """Test cases for StateManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create temporary state file
        self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
        self.state_file = self.temp_file.name
        self.temp_file.close()
        
        self.state_manager = StateManager(state_file=self.state_file)
    
    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
    
    def test_initialize_fresh_state(self):
        """Test initialization with fresh state"""
        self.assertIsNone(self.state_manager.get_last_fetch_time())
        self.assertEqual(len(self.state_manager.state['processed_article_ids']), 0)
    
    def test_record_and_load_state(self):
        """Test recording and loading state"""
        article_ids = ['article1', 'article2', 'article3']
        self.state_manager.mark_processed(article_ids)
        
        # Create new instance to load saved state
        new_manager = StateManager(state_file=self.state_file)
        self.assertEqual(
            len(new_manager.state['processed_article_ids']),
            3
        )
    
    def test_duplicate_detection(self):
        """Test duplicate article detection"""
        article_ids = ['article1', 'article2']
        self.state_manager.mark_processed(article_ids)
        
        # Check duplicates
        self.assertTrue(self.state_manager.is_duplicate('article1'))
        self.assertTrue(self.state_manager.is_duplicate('article2'))
        self.assertFalse(self.state_manager.is_duplicate('article3'))
    
    def test_record_fetch(self):
        """Test recording fetch time"""
        article_ids = ['article1', 'article2']
        self.state_manager.record_fetch(article_ids)
        
        last_fetch = self.state_manager.get_last_fetch_time()
        self.assertIsNotNone(last_fetch)
        self.assertIsInstance(last_fetch, datetime)
    
    def test_error_tracking(self):
        """Test error recording"""
        self.state_manager.record_error("Test error")
        
        stats = self.state_manager.get_stats()
        self.assertEqual(stats['consecutive_failures'], 1)
        self.assertEqual(stats['last_error'], "Test error")
        
        # Record another error
        self.state_manager.record_error("Second error")
        stats = self.state_manager.get_stats()
        self.assertEqual(stats['consecutive_failures'], 2)
    
    def test_success_resets_error_count(self):
        """Test that success resets error counter"""
        self.state_manager.record_error("Error 1")
        self.state_manager.record_error("Error 2")
        
        stats = self.state_manager.get_stats()
        self.assertEqual(stats['consecutive_failures'], 2)
        
        self.state_manager.record_success()
        stats = self.state_manager.get_stats()
        self.assertEqual(stats['consecutive_failures'], 0)
        self.assertIsNone(stats['last_error'])
    
    def test_get_time_since_last_fetch(self):
        """Test calculating time since last fetch"""
        self.state_manager.record_fetch(['article1'])
        
        time_delta = self.state_manager.get_time_since_last_fetch()
        self.assertIsNotNone(time_delta)
        self.assertIsInstance(time_delta, timedelta)
        self.assertLess(time_delta.total_seconds(), 1)
    
    def test_import_from_kinesis(self):
        """Simulate importing IDs from a Kinesis-like producer"""
        class DummyProducer:
            def get_existing_article_ids(self, limit=1000):
                return {'a', 'b', 'c'}
        dummy = DummyProducer()
        self.state_manager.import_from_kinesis(dummy, limit=10)
        self.assertTrue(self.state_manager.is_duplicate('a'))
        self.assertTrue(self.state_manager.is_duplicate('b'))
        self.assertTrue(self.state_manager.is_duplicate('c'))


if __name__ == '__main__':
    unittest.main()
