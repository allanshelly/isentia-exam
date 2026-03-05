"""Data processing and validation for news articles"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import hashlib
from pydantic import BaseModel, Field, validator

logger = logging.getLogger(__name__)


class Article(BaseModel):
    """Pydantic model for validated article data"""
    
    article_id: str = Field(..., description="Unique identifier for the article")
    source_name: str = Field(..., description="Name of the news source")
    title: str = Field(..., description="Article title")
    content: str = Field(..., description="Article content")
    url: str = Field(..., description="URL to the article")
    author: Optional[str] = Field(None, description="Article author")
    published_at: str = Field(..., description="Publication timestamp")
    ingested_at: str = Field(..., description="Ingestion timestamp")
    
    @validator('article_id')
    def validate_article_id(cls, v):
        if not v or len(v) == 0:
            raise ValueError('article_id cannot be empty')
        return v
    
    @validator('title')
    def validate_title(cls, v):
        if not v or len(v) == 0:
            raise ValueError('title cannot be empty')
        return v
    
    @validator('published_at', 'ingested_at')
    def validate_timestamps(cls, v):
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            raise ValueError('Invalid timestamp format')
        return v
    
    class Config:
        extra = 'forbid'


class DataProcessor:
    """Process and validate news article data"""
    
    @staticmethod
    def generate_article_id(source: str, title: str, published_at: str) -> str:
        """Generate a unique article ID using hash"""
        unique_string = f"{source}_{title}_{published_at}"
        return hashlib.sha256(unique_string.encode()).hexdigest()[:16]
    
    @staticmethod
    def process_article(raw_article: Dict) -> Optional[Article]:
        """
        Process a raw article from NewsAPI into a structured format
        
        Args:
            raw_article: Raw article dictionary from NewsAPI
            
        Returns:
            Validated Article object or None if processing fails
        """
        try:
            # Extract and clean data
            source_name = raw_article.get('source', {}).get('name', 'Unknown')
            title = raw_article.get('title', '').strip()
            content = raw_article.get('content', '').strip()
            url = raw_article.get('url', '')
            author = raw_article.get('author', '').strip() if raw_article.get('author') else None
            published_at = raw_article.get('publishedAt', '')
            
            # Generate unique article ID
            article_id = DataProcessor.generate_article_id(source_name, title, published_at)
            
            # Get current timestamp in ISO format
            ingested_at = datetime.utcnow().isoformat() + 'Z'
            
            # Create and validate article
            article = Article(
                article_id=article_id,
                source_name=source_name,
                title=title,
                content=content,
                url=url,
                author=author,
                published_at=published_at,
                ingested_at=ingested_at
            )
            
            return article
            
        except Exception as e:
            logger.error(f"Failed to process article: {str(e)}", exc_info=True)
            return None
    
    @staticmethod
    def process_batch(raw_articles: List[Dict]) -> List[Article]:
        """
        Process multiple articles, filtering out any that fail validation
        
        Args:
            raw_articles: List of raw articles from NewsAPI
            
        Returns:
            List of validated Article objects
        """
        processed_articles = []
        
        for raw_article in raw_articles:
            article = DataProcessor.process_article(raw_article)
            if article:
                processed_articles.append(article)
            else:
                logger.warning(f"Skipped invalid article: {raw_article.get('title', 'Unknown')}")
        
        logger.info(f"Successfully processed {len(processed_articles)} out of {len(raw_articles)} articles")
        return processed_articles
    
    @staticmethod
    def article_to_dict(article: Article) -> Dict:
        """Convert Article object to dictionary for JSON serialization"""
        return article.dict()
