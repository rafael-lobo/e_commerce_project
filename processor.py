import time
import logging
from google.api_core.exceptions import ServiceUnavailable


class Processor:
    def __init__(self):
        self.logger = logging.getLogger('Processor')

    def run(self, message: dict) -> None:
        time.sleep(2) # simulate processing time
        self.logger.info(f"Processing message: {message}...")
        # raise ServiceUnavailable('SERVICE UNAVAILABLE') # simulate retryable error
        # raise Exception('UNEXPECTED ERROR') # simulate non-retryable error
