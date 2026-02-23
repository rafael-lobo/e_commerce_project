import time
import functools
import threading
from enum import Enum
import logging

class State(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3, 
        recovery_timeout: int = 10,
        half_open_success_threshold: int = 3,
        expected_exceptions: tuple[type[BaseException], ...] = (Exception,)
    ):
        """
        :param failure_threshold: Number of consecutive failures before opening.
        :param recovery_timeout: Seconds to wait before trying again (half-open).
        :param half_open_success_threshold: Successes in half-open before closing.
        :param expected_exceptions: Exceptions that count as failures.
        """
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('CircuitBreaker')

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_success_threshold = half_open_success_threshold
        self.expected_exceptions = expected_exceptions

        self.state = State.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.opened_since = None
        self._lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        with self._lock:
            if not self._can_pass_through():
                raise RuntimeError("Circuit is open")

        try:
            result = func(*args, **kwargs)
            with self._lock:
                self._handle_success()
            return result
        except self.expected_exceptions as e:
            with self._lock:
                self._handle_failure()
            raise

    def _can_pass_through(self) -> bool:
        if self.state == State.CLOSED:
            return True
        
        if self.state == State.OPEN:
            if (time.time() - self.opened_since) >= self.recovery_timeout:
                self._transition_to_half_open()
                return True
            return False
        
        if self.state == State.HALF_OPEN:
            return True

        return False
    
    def _transition_to_open(self) -> None:
        self.state = State.OPEN
        self.opened_since = time.time()
        self.failure_count = 0
        self.success_count = 0
    
    def _transition_to_half_open(self) -> None:
        self.state = State.HALF_OPEN
        self.failure_count = 0
        self.success_count = 0
    
    def _transition_to_closed(self) -> None:
        self.state = State.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.opened_since = None
    
    def _handle_failure(self) -> None:
        if self.state == State.HALF_OPEN:
            self._transition_to_open()
            self.logger.info("Circuit opened due to failure in half-open state.")
            return

        if self.state == State.CLOSED:
            self.failure_count += 1
            if self.failure_count >= self.failure_threshold:
                self._transition_to_open()
                self.logger.info(f"Circuit opened due to {self.failure_count} consecutive failures.")

    def _handle_success(self) -> None:
        if self.state == State.CLOSED:
            self.failure_count = 0
            return
        
        if self.state == State.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.half_open_success_threshold:
                self._transition_to_closed()
                self.logger.info("Circuit closed due to success in half-open state.")


    def __call__(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
