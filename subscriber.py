import logging
from google.cloud import pubsub_v1
from google.api_core.exceptions import (
    AlreadyExists,
    ServiceUnavailable,       # 503: server temporarily down
    ResourceExhausted,        # 429: rate limit or quota exceeded
    InternalServerError,      # 500: server bug
    Aborted,                  # concurrent update conflict
    NotFound                  # Sometimes takes a while to process the new subscrition creation
)
from google.api_core.retry import Retry, if_exception_type
from tenacity import retry, stop_after_delay, wait_random_exponential, retry_if_exception_type

RETRYABLE_EXCEPTIONS = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted, NotFound)

class Subscriber:
    """
    Subscriber class to listen to messages from a Pub/Sub topic.
    """
    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('Subscriber')
        self.client = pubsub_v1.SubscriberClient()
    
    @retry(
        wait=wait_random_exponential(max=30),
        stop=(stop_after_delay(90)),
        retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
        reraise=True
    )
    def listen_topic(self, project_id: str, topic_id: str) -> None:
        topic_path = self.client.topic_path(project_id, topic_id)
        try:
            subscription_path = self._get_subscription_path(project_id=project_id, topic_id=topic_id, topic_path=topic_path)
            future_listener = self.client.subscribe(subscription_path, callback=self._callback)
            self.logger.info(f'Listening on `{subscription_path}`...')
            future_listener.result()
        except RETRYABLE_EXCEPTIONS as e:
            self.logger.error(f'Retryable exception: {e}. Trying again...')
        except KeyboardInterrupt:
            self.logger.info(f'Stopping subscriber...')
            future_listener.cancel()
        except Exception:
            self.logger.exception(f'Unexpected Error.')
            raise

    def _get_subscription_path(self, project_id: str, topic_id: str, topic_path: str):
        subscription_id = f'{topic_id}-sub' # Assumes the subscription name will be `<topic_id>-sub`
        subscription_path = self.client.subscription_path(project_id, subscription_id)
        try:
            self.client.create_subscription(name=subscription_path, topic=topic_path, retry=self._retry_strategy())
            self.logger.info(f'Subscription created: {subscription_path}')
            return subscription_path
        except AlreadyExists:
            self.logger.info(f'Subscription already exists: {subscription_path}')
            return subscription_path
    
    def _retry_strategy(self) -> Retry:
        return Retry(
            predicate=if_exception_type(RETRYABLE_EXCEPTIONS),
            initial=0.1,
            maximum=5,
            multiplier=2,
            timeout=10,
            on_error=(lambda e: f"Retryable error {e}, trying again...")
        )

    def _callback(self, message: str | dict) -> None:
        try:
            self.logger.info(f'Message received: {message.data.decode("utf-8")}')
            message.ack()
        except Exception as e:
            message.nack()
            raise e


if __name__ == "__main__":
    import os
    project_id = os.environ.get("PROJECT_ID")
    print(f'Project ID: {project_id}') 
    topic_id = os.environ.get("TOPIC_ID")
    print(f'Topic ID: {topic_id}') 
    subscriber = Subscriber()
    result = subscriber.listen_topic(project_id=project_id, topic_id=topic_id)
