"""News API fetcher for retrieving articles"""
import logging
from typing import Dict, List, Optional
import requests
from src.config import Config
from src.circuit_breaker import CircuitBreaker
from src.retry_handler import RetryHandler, RetryConfig
from src.exceptions import RateLimitError, AuthenticationError, APIError

logger = logging.getLogger(__name__)


class NewsFetcher:
    """Fetch news articles from NewsAPI.org"""
    
    def __init__(self, api_key: str, base_url: str = "https://newsapi.org/v2"):
        """
        Initialize the news fetcher
        
        Args:
            api_key: NewsAPI API key
            base_url: Base URL for NewsAPI
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            'User-Agent': 'News-Ingest-Pipeline/1.0'
        }
        
        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            name="NewsAPI",
            failure_threshold=5,
            recovery_timeout=60
        )
        
        # Initialize retry handler
        retry_config = RetryConfig(
            max_attempts=3,
            initial_delay=1.0,
            max_delay=10.0
        )
        self.retry_handler = RetryHandler(
            retryable_exceptions=(
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                RateLimitError
            ),
            config=retry_config
        )
    
    def fetch_everything(
        self,
        q: str,
        sort_by: str = "publishedAt",
        language: str = "en",
        page_size: int = 100
    ) -> Optional[Dict]:
        """
        Fetch articles using the Everything endpoint
        
        Args:
            q: Search query keywords
            sort_by: Sort order (publishedAt, relevancy, popularity)
            language: Language code (e.g., 'en')
            page_size: Number of articles to fetch
            
        Returns:
            API response dict or None if request fails
        """
        def _fetch():
            endpoint = f"{self.base_url}/everything"
            
            params = {
                'q': q,
                'sortBy': sort_by,
                'language': language,
                'pageSize': min(page_size, 100),  # NewsAPI max is 100
                'apiKey': self.api_key
            }
            
            logger.info(f"Fetching articles for query: {q}")
            response = requests.get(
                endpoint,
                params=params,
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') != 'ok':
                error_message = data.get('message', 'Unknown error')
                logger.error(f"API returned non-ok status: {data.get('status')}")
                logger.error(f"API message: {error_message}")
                
                if 'rate limit' in error_message.lower():
                    raise RateLimitError(f"Rate limit exceeded: {error_message}")
                elif 'unauthorized' in error_message.lower():
                    raise AuthenticationError(f"Authentication failed: {error_message}")
                else:
                    raise APIError(f"API error: {error_message}")
            
            logger.info(f"Successfully fetched {len(data.get('articles', []))} articles")
            return data
        
        try:
            return self.circuit_breaker.call(
                self.retry_handler.execute,
                _fetch,
                operation_name=f"Fetch articles: {q}"
            )
        except Exception as e:
            logger.error(f"Failed to fetch articles: {str(e)}", exc_info=True)
            return None
    
    def fetch_articles(
        self,
        keywords: List[str],
        page_size: int = 100
    ) -> List[Dict]:
        """
        Fetch articles for multiple keywords
        
        Args:
            keywords: List of search keywords
            page_size: Number of articles per query
            
        Returns:
            List of article dictionaries
        """
        all_articles = []
        
        for keyword in keywords:
            response = self.fetch_everything(
                q=keyword,
                page_size=page_size
            )
            
            if response and 'articles' in response:
                articles = response['articles']
                all_articles.extend(articles)
                logger.info(f"Total articles collected: {len(all_articles)}")
            else:
                logger.warning(f"No articles returned for keyword: {keyword}")
        
        # Remove duplicates based on URL
        unique_articles = {}
        for article in all_articles:
            url = article.get('url')
            if url and url not in unique_articles:
                unique_articles[url] = article
        
        logger.info(f"Removed duplicates. Final count: {len(unique_articles)}")
        return list(unique_articles.values())
