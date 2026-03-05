"""Tests for the data processor module"""
import unittest
from datetime import datetime
from src.data_processor import DataProcessor, Article


class TestDataProcessor(unittest.TestCase):
    """Test cases for DataProcessor"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.processor = DataProcessor()
        self.raw_article = {
            'source': {'id': 'techcrunch', 'name': 'TechCrunch'},
            'title': 'Breaking: AI Breakthrough',
            'content': 'Scientists announce major AI discovery...',
            'url': 'https://example.com/article1',
            'author': 'John Doe',
            'publishedAt': '2024-01-15T10:00:00Z'
        }
    
    def test_generate_article_id(self):
        """Test article ID generation"""
        article_id = self.processor.generate_article_id(
            'TechCrunch',
            'Breaking: AI Breakthrough',
            '2024-01-15T10:00:00Z'
        )
        
        self.assertIsNotNone(article_id)
        self.assertEqual(len(article_id), 16)
        
        # Test consistency
        article_id2 = self.processor.generate_article_id(
            'TechCrunch',
            'Breaking: AI Breakthrough',
            '2024-01-15T10:00:00Z'
        )
        self.assertEqual(article_id, article_id2)
    
    def test_process_article_success(self):
        """Test successful article processing"""
        article = self.processor.process_article(self.raw_article)
        
        self.assertIsNotNone(article)
        self.assertEqual(article.source_name, 'TechCrunch')
        self.assertEqual(article.title, 'Breaking: AI Breakthrough')
        self.assertEqual(article.author, 'John Doe')
        self.assertIsNotNone(article.article_id)
        self.assertIsNotNone(article.ingested_at)
    
    def test_process_article_missing_author(self):
        """Test processing article without author"""
        raw_article = self.raw_article.copy()
        raw_article['author'] = None
        
        article = self.processor.process_article(raw_article)
        
        self.assertIsNotNone(article)
        self.assertIsNone(article.author)
    
    def test_process_article_missing_required_field(self):
        """Test processing article with missing required field"""
        raw_article = self.raw_article.copy()
        del raw_article['title']
        
        article = self.processor.process_article(raw_article)
        
        self.assertIsNone(article)
    
    def test_process_batch(self):
        """Test batch processing"""
        articles = [self.raw_article, self.raw_article.copy()]
        
        processed = self.processor.process_batch(articles)
        
        self.assertEqual(len(processed), 2)
        self.assertTrue(all(isinstance(a, Article) for a in processed))
    
    def test_article_to_dict(self):
        """Test conversion to dictionary"""
        article = self.processor.process_article(self.raw_article)
        article_dict = self.processor.article_to_dict(article)
        
        self.assertIsInstance(article_dict, dict)
        self.assertIn('article_id', article_dict)
        self.assertIn('source_name', article_dict)
        self.assertIn('title', article_dict)


if __name__ == '__main__':
    unittest.main()
