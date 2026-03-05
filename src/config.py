"""Configuration management for News Ingest Pipeline"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Application configuration class"""
    
    # NewsAPI Configuration
    NEWS_API_KEY = os.getenv("NEWS_API_KEY")
    NEWS_API_BASE_URL = os.getenv("NEWS_API_BASE_URL", "https://newsapi.org/v2")
    
    # AWS Configuration
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "news-ingest-stream")
    
    # Application Configuration
    FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))
    QUERY_KEYWORDS = os.getenv("QUERY_KEYWORDS", "technology").split(",")
    ARTICLES_PER_REQUEST = int(os.getenv("ARTICLES_PER_REQUEST", "100"))
    
    @classmethod
    def validate(cls):
        """Validate that all required configuration is set"""
        required_fields = [
            ("NEWS_API_KEY", cls.NEWS_API_KEY),
            ("AWS_ACCESS_KEY_ID", cls.AWS_ACCESS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY", cls.AWS_SECRET_ACCESS_KEY),
            ("KINESIS_STREAM_NAME", cls.KINESIS_STREAM_NAME),
        ]
        
        missing_fields = [name for name, value in required_fields if not value]
        
        if missing_fields:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing_fields)}. "
                f"Please set these in your .env file."
            )
