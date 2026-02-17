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
    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        self.logger = logging.getLogger('Publisher')
        self.client = pubsub_v1.PublisherClient()

    def publish_message(self, project_id: str, topic_id: str, message: str | dict) -> str:
        try:
            topic_path = self._get_topic_path(project_id=project_id, topic_id=topic_id)
            future = self.client.publish(topic_path, message.encode("utf-8"), retry=self._retry_strategy())
            published_message_id = future.result(timeout=10) # parametrize timeout
            self.logger.info(f'Message published with ID: {published_message_id}')
            return published_message_id
        except Exception as e: # improve error handling
            self.logger.error(f'Error publishing message: {e}') 
            raise e

    def _get_topic_path(self, project_id: str, topic_id: str) -> str:
        topic_path = self.client.topic_path(project_id, topic_id)
        try:
            self.client.create_topic(name=topic_path)
            self.logger.info(f'Topic created: {topic_path}')
            return topic_path
        except AlreadyExists:
            self.logger.info(f'Topic already exists: {topic_path}')
            return topic_path

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

if __name__ == "__main__":
    import os
    import random
    project_id = os.environ.get("PROJECT_ID")
    print(f'Project ID: {project_id}') 
    topic_id =os.environ.get("TOPIC_ID")
    print(f'Topic ID: {topic_id}')
    message = f'Random message nº{random.randint(1, 10_000)}'
    print(f'Message: {message}')
    publisher = Publisher()
    result = publisher.publish_message(project_id=project_id, topic_id=topic_id, message=message)

