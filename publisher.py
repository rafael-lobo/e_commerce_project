import json
import logging
from google.cloud import pubsub_v1
from google.api_core.exceptions import (
    AlreadyExists,
    ServiceUnavailable,       # 503: server temporarily down
    ResourceExhausted,        # 429: rate limit or quota exceeded
    InternalServerError,      # 500: server bug
    Aborted,                  # concurrent update conflict
)
from google.api_core.retry import Retry, if_exception_type


class Publisher:
    """
    Publisher class to publish messages to a Pub/Sub topic.
    """
    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        self.logger = logging.getLogger('Publisher')
        self.client = pubsub_v1.PublisherClient()

    def publish_message(self, project_id: str, topic_id: str, message: str | dict | list) -> str:
        try:
            topic_path = self.client.topic_path(project_id, topic_id) # assumes topic exists
            future = self.client.publish(
                topic_path,
                self._serialize_message(message),
                content_type="application/json",
                retry=self._retry_strategy()
            )
            published_message_id = future.result(timeout=10) # parametrize timeout
            self.logger.info(f'Message published with ID: {published_message_id}')
            return published_message_id
        except Exception:
            self.logger.exception(f'Unexpected error publishing message') 
            raise

    def _retry_strategy(self) -> Retry:
        retryable_exceptions = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted)
        return Retry(
            predicate=if_exception_type(retryable_exceptions),
            initial=0.1,
            maximum=5,
            multiplier=2,
            timeout=10,
            on_error=(lambda e: f"Retryable error {e}, trying again...")
        )

    def _serialize_message(self, message: str | dict | list) -> bytes:
        if isinstance(message, (dict, list)):
            return json.dumps(message).encode("utf-8")
        if isinstance(message, str):
            return message.encode("utf-8")
        raise TypeError(f'Message must be a string or a JSON, got {type(message)}')


if __name__ == "__main__":
    import os
    import random
    project_id = os.environ.get("PROJECT_ID")
    print(f'Project ID: {project_id}') 
    topic_id =os.environ.get("TOPIC_ID")
    print(f'Topic ID: {topic_id}')
    message = {
        "id": random.randint(1, 10_000),
        "name": f"Product {random.randint(1, 10_000)}",
        "price": random.randint(1, 100),
        "stock": random.randint(1, 1_000)
    }
    print(f'Message: {message}')
    publisher = Publisher()
    result = publisher.publish_message(project_id=project_id, topic_id=topic_id, message=message)

