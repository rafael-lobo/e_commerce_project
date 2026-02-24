import time
import logging
from google.api_core.exceptions import ServiceUnavailable
from tenacity import retry, wait_random_exponential, stop_after_delay, retry_if_exception_type
from google.api_core.exceptions import ResourceExhausted, InternalServerError, Aborted

RETRYABLE_EXCEPTIONS = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted)

class Processor:
    def __init__(self):
        self.logger = logging.getLogger('Processor')

    @retry(
        wait=wait_random_exponential(max=10),
        stop=(stop_after_delay(10)),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True
    )
    def run(self, message: dict) -> None:
        try: 
            time.sleep(2) # simulate processing time
            self.logger.info(f"Processing message: {message}...")
            # raise ServiceUnavailable('SERVICE UNAVAILABLE') # simulate retryable error
            # raise Exception('UNEXPECTED ERROR') # simulate non-retryable error
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.warning(f"Retryable exception: {e}. Retrying...")
            raise
        except Exception:
            self.logger.exception(f"Unexpected error processing message")
            raise
