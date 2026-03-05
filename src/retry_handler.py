"""Retry logic with exponential backoff"""
import logging
import time
from typing import Callable, Any, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior"""
    
    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        """
        Initialize retry configuration
        
        Args:
            max_attempts: Maximum number of retry attempts
            initial_delay: Initial delay in seconds
            max_delay: Maximum delay in seconds
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add random jitter to delays
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter


class RetryHandler:
    """Handle retries with exponential backoff"""
    
    def __init__(
        self,
        retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
        config: RetryConfig = None
    ):
        """
        Initialize retry handler
        
        Args:
            retryable_exceptions: Tuple of exception types to retry on
            config: RetryConfig instance
        """
        self.retryable_exceptions = retryable_exceptions
        self.config = config or RetryConfig()
    
    def execute(
        self,
        func: Callable,
        *args,
        operation_name: str = "Operation",
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic
        
        Args:
            func: Function to execute
            *args: Positional arguments
            operation_name: Name of operation for logging
            **kwargs: Keyword arguments
            
        Returns:
            Function result
            
        Raises:
            RetryExhausted: If all retries are exhausted
        """
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                logger.debug(f"{operation_name}: Attempt {attempt}/{self.config.max_attempts}")
                result = func(*args, **kwargs)
                
                if attempt > 1:
                    logger.info(f"{operation_name}: Succeeded after {attempt - 1} retries")
                
                return result
            
            except self.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_attempts:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"{operation_name}: Attempt {attempt} failed with {type(e).__name__}: "
                        f"{str(e)}. Retrying in {delay:.2f}s..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        f"{operation_name}: All {self.config.max_attempts} attempts failed. "
                        f"Last error: {str(e)}"
                    )
        
        # All retries exhausted
        from src.exceptions import RetryExhausted
        raise RetryExhausted(
            f"{operation_name}: Exhausted {self.config.max_attempts} attempts. "
            f"Last error: {str(last_exception)}"
        ) from last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number using exponential backoff"""
        delay = self.config.initial_delay * (
            self.config.exponential_base ** (attempt - 1)
        )
        
        # Cap at max delay
        delay = min(delay, self.config.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.config.jitter:
            import random
            delay = delay * (0.5 + random.random())
        
        return delay


def retry(
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
):
    """
    Decorator for retry logic
    
    Args:
        retryable_exceptions: Exception types to retry on
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            config = RetryConfig(
                max_attempts=max_attempts,
                initial_delay=initial_delay,
                max_delay=max_delay
            )
            handler = RetryHandler(
                retryable_exceptions=retryable_exceptions,
                config=config
            )
            return handler.execute(
                func,
                *args,
                operation_name=func.__name__,
                **kwargs
            )
        return wrapper
    
    return decorator
