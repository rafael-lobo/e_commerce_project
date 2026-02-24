import time
import logging

class Processor:
    def __init__(self):
        self.logger = logging.getLogger('Processor')

    def run(self, message: dict) -> None:
        time.sleep(2) # simulate processing time
        self.logger.info(f"Processing message: {message}...")
