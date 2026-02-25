import time
import logging
from google.api_core.exceptions import ServiceUnavailable
from tenacity import retry, wait_random_exponential, stop_after_delay, retry_if_exception_type
from google.api_core.exceptions import ResourceExhausted, InternalServerError, Aborted
from idempotency_handler import IdempotencyHandler
from google.cloud.pubsub_v1.subscriber import message


RETRYABLE_EXCEPTIONS = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted)


class Processor:
    def __init__(self):
        self.logger = logging.getLogger('Processor')

    def run(self, message: message.Message) -> None:
        try:
            order_id = message.attributes['order_id']

            idempotency_handler = IdempotencyHandler()
            processed_message_exists = idempotency_handler.check_processed_message_exists(order_id=order_id)
            if processed_message_exists:
                self.logger.info(f'Message with order_id={order_id} already processed! Skipping processing...')
                return

            message_exists = idempotency_handler.check_message_exists(order_id=order_id)
            if not message_exists:
                self.logger.info(f'Message with order_id={order_id} does not exist. Storing message...')
                idempotency_handler.store_message(
                    order_id=order_id, 
                    message_id=message.message_id, 
                    message_data=message.data.decode("utf-8")
                )

            self._process_message(message)

            idempotency_handler.update_message_processed_status(order_id=order_id, processed_status=True)
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.warning(f"Unable to recover from retryable exception: {e}")
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
    def _process_message(self, message: message.Message) -> None:
        try:
            self.logger.info(f"Processing message: {message.data.decode("utf-8")}...")
            # raise ServiceUnavailable('SERVICE UNAVAILABLE') # simulate retryable error
            # raise Exception('UNEXPECTED ERROR') # simulate non-retryable error
            time.sleep(2) # simulate processing time
            self.logger.info(f"Message processed successfully!")
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.warning(f"Retryable exception: {e}. Retrying...")
            raise
        except Exception:
            self.logger.exception(f"Unexpected error processing message")
            raise
