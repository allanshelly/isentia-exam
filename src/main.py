"""Main entry point for the News Ingest Pipeline"""
import logging
import time
import sys
import signal
from src.config import Config
from src.news_fetcher import NewsFetcher
from src.data_processor import DataProcessor
from src.kinesis_producer import KinesisProducer
from src.state_manager import StateManager
from src.exceptions import (
    ConfigurationError,
    PipelineException,
    CircuitBreakerOpen,
    RetryExhausted,
    KinesisError
)

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
        self.running = True
        
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
        
        # Initialize state manager for deduplication and tracking
        self.state_manager = StateManager()

        # optionally import existing records from Kinesis
        if Config.INITIAL_LOAD_KINESIS:
            logger.info("Importing existing article ids from Kinesis")
            try:
                self.state_manager.import_from_kinesis(
                    self.producer,
                    limit=Config.INITIAL_LOAD_KINESIS_LIMIT
                )
            except Exception as e:
                logger.warning(f"Failed to import from kinesis: {e}")
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("Pipeline initialized successfully")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self.running = False
    
    def run_once(self) -> bool:
        """
        Run one iteration of the pipeline
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("Starting pipeline iteration")
            logger.info(f"Pipeline stats: {self.state_manager.get_stats()}")
            
            # Verify Kinesis stream exists
            if not self.producer.stream_exists():
                error_msg = "Cannot proceed without Kinesis stream"
                logger.error(error_msg)
                self.state_manager.record_error(error_msg)
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
            
            # Filter out duplicates
            article_ids_fetched = []
            unique_articles = []
            
            for article in raw_articles:
                article_id = self.processor.generate_article_id(
                    article.get('source', {}).get('name', 'Unknown'),
                    article.get('title', ''),
                    article.get('publishedAt', '')
                )
                article_ids_fetched.append(article_id)
                
                if not self.state_manager.is_duplicate(article_id):
                    unique_articles.append(article)
                else:
                    logger.debug(f"Skipped duplicate article: {article_id}")
            
            # Record fetch for future deduplication
            self.state_manager.record_fetch(article_ids_fetched)
            
            if not unique_articles:
                logger.info("All fetched articles were duplicates")
                self.state_manager.record_success()
                return True
            
            logger.info(f"Processing {len(unique_articles)} unique articles")
            
            # Process articles
            processed_articles = self.processor.process_batch(unique_articles)
            
            if not processed_articles:
                logger.warning("No articles were successfully processed")
                self.state_manager.record_error("No articles processed successfully")
                return False
            
            # Convert to dictionaries for Kinesis
            article_dicts = [
                self.processor.article_to_dict(article)
                for article in processed_articles
            ]
            
            # Track article IDs as processed
            article_ids_processed = [a['article_id'] for a in article_dicts]
            self.state_manager.mark_processed(article_ids_processed)
            
            # Send to Kinesis
            logger.info(f"Sending {len(article_dicts)} articles to Kinesis")
            sent_count = self.producer.put_batch(article_dicts)
            
            if sent_count > 0:
                self.state_manager.record_sent_records(sent_count)
                self.state_manager.record_success()
                logger.info(f"Pipeline iteration completed successfully. Sent {sent_count} records")
                return True
            else:
                logger.warning("No records were sent to Kinesis")
                self.state_manager.record_error("No records sent to Kinesis")
                return False
            
        except CircuitBreakerOpen as e:
            logger.error(f"Circuit breaker is open: {str(e)}")
            self.state_manager.record_error(f"Circuit breaker open: {str(e)}")
            return False
        except RetryExhausted as e:
            logger.error(f"Retry limit exceeded: {str(e)}")
            self.state_manager.record_error(f"Retry exhausted: {str(e)}")
            return False
        except KinesisError as e:
            logger.error(f"Kinesis error: {str(e)}")
            self.state_manager.record_error(f"Kinesis error: {str(e)}")
            return False
        except PipelineException as e:
            logger.error(f"Pipeline error: {str(e)}", exc_info=True)
            self.state_manager.record_error(f"Pipeline error: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in pipeline iteration: {str(e)}", exc_info=True)
            self.state_manager.record_error(f"Unexpected error: {str(e)}")
            return False
    
    def run_continuous(self) -> None:
        """Run the pipeline continuously at specified intervals"""
        logger.info(f"Starting continuous pipeline with {self.config.FETCH_INTERVAL_SECONDS}s interval")
        
        iteration = 0
        while self.running:
            iteration += 1
            logger.info(f"--- Pipeline Iteration {iteration} ---")
            
            try:
                self.run_once()
            except KeyboardInterrupt:
                logger.info("Pipeline interrupted by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in continuous run: {str(e)}", exc_info=True)
            
            if self.running:
                logger.info(f"Waiting {self.config.FETCH_INTERVAL_SECONDS} seconds before next iteration")
                time.sleep(self.config.FETCH_INTERVAL_SECONDS)
        
        # Graceful shutdown
        logger.info("Pipeline shutting down gracefully")
        logger.info(f"Final stats: {self.state_manager.get_stats()}")
        self.state_manager.save_state()


def main():
    """Main entry point"""
    try:
        # Validate configuration
        logger.info("Validating configuration")
        Config.validate()
        
        # Initialize and run pipeline
        pipeline = NewsIngestPipeline(Config)
        pipeline.run_continuous()
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)
    except PipelineException as e:
        logger.error(f"Pipeline error: {str(e)}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
