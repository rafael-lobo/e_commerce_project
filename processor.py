import time
import logging
from google.api_core.exceptions import ServiceUnavailable
from tenacity import retry, wait_random_exponential, stop_after_delay, retry_if_exception_type
from google.api_core.exceptions import (
    ServiceUnavailable,       # 503: server temporarily down
    ResourceExhausted,        # 429: rate limit or quota exceeded
    InternalServerError,      # 500: server bug
    Aborted                   # concurrent update conflict
)
from firestore_handler import FirestoreHandler, IdempotencyError, MarkAsProcessedError, UnexistingDocumentError
from google.cloud.pubsub_v1.subscriber import message
from circuit_breaker import CircuitBreaker, OpenCircuitError


RETRYABLE_EXCEPTIONS = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted)


class Processor:
    def __init__(self):
        self.logger = logging.getLogger('Processor')
        self.firestore_handler = FirestoreHandler()

    def run(self, message: message.Message) -> None:
        try:
            order_id = message.attributes.get('order_id')
            self.firestore_handler.idempotency_check(order_id=order_id)
            self._process_message(message)
            self.firestore_handler.mark_as_processed(order_id=order_id)
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.warning(f"Unable to recover from retryable exception: {e}")
            raise
        except IdempotencyError:
            raise
        except MarkAsProcessedError:
            raise
        except UnexistingDocumentError:
            raise
        except OpenCircuitError:
            raise
        except Exception:
            self.logger.exception(f"Unexpected error processing message")
            raise

    @retry(
        wait=wait_random_exponential(min=0.5, max=10),
        stop=(stop_after_delay(10)),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True
    )
    @CircuitBreaker()
    def _process_message(self, message: message.Message) -> None:
        try:
            self.logger.info(f"Processing message: {message.data.decode("utf-8")}...")

            # raise ServiceUnavailable('SERVICE UNAVAILABLE') # simulate retryable error
            # raise Exception('UNEXPECTED ERROR') # simulate non-retryable error

            time.sleep(2) # simulate processing time. For simplicity I'm assuming this non-deterministic external process is has an idempotency key
            self.logger.info(f"Message processed successfully!")
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.warning(f"Retryable exception: {e}. Retrying...")
            raise
        except Exception:
            self.logger.exception(f"Unexpected error processing message")
            raise
