"""Main entry point for the News Ingest Pipeline"""
import logging
import time
import sys
from src.config import Config
from src.news_fetcher import NewsFetcher
from src.data_processor import DataProcessor
from src.kinesis_producer import KinesisProducer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class NewsIngestPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config: Config):
        """
        Initialize the pipeline
        
        Args:
            config: Configuration object
        """
        self.config = config
        
        # Initialize components
        self.fetcher = NewsFetcher(
            api_key=config.NEWS_API_KEY,
            base_url=config.NEWS_API_BASE_URL
        )
        
        self.producer = KinesisProducer(
            stream_name=config.KINESIS_STREAM_NAME,
            region_name=config.AWS_REGION,
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY
        )
        
        self.processor = DataProcessor()
        
        logger.info("Pipeline initialized successfully")
    
    def run_once(self) -> bool:
        """
        Run one iteration of the pipeline
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting pipeline iteration")
            
            # Verify Kinesis stream exists
            if not self.producer.stream_exists():
                logger.error("Cannot proceed without Kinesis stream")
                return False
            
            # Fetch articles
            logger.info(f"Fetching articles for keywords: {self.config.QUERY_KEYWORDS}")
            raw_articles = self.fetcher.fetch_articles(
                keywords=self.config.QUERY_KEYWORDS,
                page_size=self.config.ARTICLES_PER_REQUEST
            )
            
            if not raw_articles:
                logger.warning("No articles fetched")
                return False
            
            logger.info(f"Fetched {len(raw_articles)} articles")
            
            # Process articles
            logger.info("Processing articles")
            processed_articles = self.processor.process_batch(raw_articles)
            
            if not processed_articles:
                logger.warning("No articles were successfully processed")
                return False
            
            # Convert to dictionaries for Kinesis
            article_dicts = [
                self.processor.article_to_dict(article)
                for article in processed_articles
            ]
            
            # Send to Kinesis
            logger.info(f"Sending {len(article_dicts)} articles to Kinesis")
            sent_count = self.producer.put_batch(article_dicts)
            
            logger.info(f"Pipeline iteration completed. Sent {sent_count} records")
            return sent_count > 0
            
        except Exception as e:
            logger.error(f"Pipeline iteration failed: {str(e)}", exc_info=True)
            return False
    
    def run_continuous(self) -> None:
        """Run the pipeline continuously at specified intervals"""
        logger.info(f"Starting continuous pipeline with {self.config.FETCH_INTERVAL_SECONDS}s interval")
        
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"--- Pipeline Iteration {iteration} ---")
            
            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("Pipeline interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous run: {str(e)}", exc_info=True)
            
            # Wait before next iteration
            logger.info(f"Waiting {self.config.FETCH_INTERVAL_SECONDS} seconds before next iteration")
            time.sleep(self.config.FETCH_INTERVAL_SECONDS)


def main():
    """Main entry point"""
    try:
        # Validate configuration
        logger.info("Validating configuration")
        Config.validate()
        
        # Initialize and run pipeline
        pipeline = NewsIngestPipeline(Config)
        pipeline.run_continuous()
        
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
