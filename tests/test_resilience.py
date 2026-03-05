"""Tests for retry and circuit breaker functionality"""
import unittest
import time
from src.retry_handler import RetryHandler, RetryConfig
from src.circuit_breaker import CircuitBreaker, CircuitState
from src.exceptions import CircuitBreakerOpen, RetryExhausted


class TestRetryHandler(unittest.TestCase):
    """Test cases for RetryHandler"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.call_count = 0
        self.config = RetryConfig(
            max_attempts=3,
            initial_delay=0.1,
            max_delay=0.5,
            jitter=False
        )
    
    def failing_function(self, fail_count=2):
        """Function that fails n times then succeeds"""
        self.call_count += 1
        if self.call_count <= fail_count:
            raise ValueError(f"Attempt {self.call_count} failed")
        return f"Success on attempt {self.call_count}"
    
    def test_retry_success_after_failures(self):
        """Test successful retry after initial failures"""
        handler = RetryHandler(
            retryable_exceptions=(ValueError,),
            config=self.config
        )
        
        result = handler.execute(
            self.failing_function,
            fail_count=2,
            operation_name="test_operation"
        )
        
        self.assertEqual(result, "Success on attempt 3")
        self.assertEqual(self.call_count, 3)
    
    def test_retry_exhausted(self):
        """Test retry exhaustion"""
        handler = RetryHandler(
            retryable_exceptions=(ValueError,),
            config=self.config
        )
        
        with self.assertRaises(RetryExhausted):
            handler.execute(
                self.failing_function,
                fail_count=5,
                operation_name="test_operation"
            )
    
    def test_immediate_success(self):
        """Test immediate success without retry"""
        handler = RetryHandler(
            retryable_exceptions=(ValueError,),
            config=self.config
        )
        
        result = handler.execute(
            self.failing_function,
            fail_count=0,
            operation_name="test_operation"
        )
        
        self.assertEqual(result, "Success on attempt 1")
        self.assertEqual(self.call_count, 1)


class TestCircuitBreaker(unittest.TestCase):
    """Test cases for CircuitBreaker"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.call_count = 0
        self.breaker = CircuitBreaker(
            name="test_breaker",
            failure_threshold=3,
            recovery_timeout=1
        )
    
    def failing_function(self):
        """Function that always fails"""
        self.call_count += 1
        raise ValueError("Failed")
    
    def success_function(self):
        """Function that always succeeds"""
        self.call_count += 1
        return "Success"
    
    def test_circuit_opens_after_threshold(self):
        """Test circuit opens after failure threshold"""
        for i in range(3):
            try:
                self.breaker.call(self.failing_function)
            except ValueError:
                pass
        
        self.assertEqual(self.breaker.state, CircuitState.OPEN)
    
    def test_circuit_rejects_when_open(self):
        """Test circuit rejects calls when open"""
        # Open the circuit
        for i in range(3):
            try:
                self.breaker.call(self.failing_function)
            except ValueError:
                pass
        
        # Should raise CircuitBreakerOpen
        with self.assertRaises(CircuitBreakerOpen):
            self.breaker.call(self.success_function)
    
    def test_circuit_recovery(self):
        """Test circuit recovery after timeout"""
        # Open the circuit
        for i in range(3):
            try:
                self.breaker.call(self.failing_function)
            except ValueError:
                pass
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Should enter half-open state and eventually close
        result = self.breaker.call(self.success_function)
        self.assertEqual(result, "Success")
        self.assertEqual(self.breaker.state, CircuitState.CLOSED)


if __name__ == '__main__':
    unittest.main()
