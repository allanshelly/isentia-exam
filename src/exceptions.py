"""Custom exceptions for the News Ingest Pipeline"""


class PipelineException(Exception):
    """Base exception for all pipeline errors"""
    pass


class ConfigurationError(PipelineException):
    """Raised when configuration is invalid or missing"""
    pass


class APIError(PipelineException):
    """Raised when API request fails"""
    pass


class RateLimitError(APIError):
    """Raised when API rate limit is exceeded"""
    pass


class AuthenticationError(APIError):
    """Raised when API authentication fails"""
    pass


class DataValidationError(PipelineException):
    """Raised when data validation fails"""
    pass


class KinesisError(PipelineException):
    """Raised when Kinesis operation fails"""
    pass


class CircuitBreakerOpen(PipelineException):
    """Raised when circuit breaker is open"""
    pass


class RetryExhausted(PipelineException):
    """Raised when all retry attempts are exhausted"""
    pass
