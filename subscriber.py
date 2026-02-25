import logging
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber import message
from google.api_core.exceptions import (
    AlreadyExists,
    ServiceUnavailable,       # 503: server temporarily down
    ResourceExhausted,        # 429: rate limit or quota exceeded
    InternalServerError,      # 500: server bug
    Aborted,                  # concurrent update conflict
    NotFound                  # Sometimes takes a while to process the new subscrition creation
)
from google.api_core.retry import Retry, if_exception_type
from processor import Processor


RETRYABLE_EXCEPTIONS = (ServiceUnavailable, ResourceExhausted, InternalServerError, Aborted, NotFound)


class Subscriber:
    """
    Subscriber class to listen to messages from a Pub/Sub topic.
    """
    def __init__(self) -> None:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger('Subscriber')
        self.client = pubsub_v1.SubscriberClient()

    def listen_topic(self, project_id: str, topic_id: str) -> None:
        topic_path = self.client.topic_path(project_id, topic_id)
        try:
            flow_control = pubsub_v1.types.FlowControl(max_messages=2) # accepts 2 messages at a time
            subscription_path = self._get_subscription_path(project_id=project_id, topic_id=topic_id, topic_path=topic_path)
            future_listener = self.client.subscribe(subscription_path, callback=self._callback, flow_control=flow_control)
            self.logger.info(f'Listening on `{subscription_path}`...')
            future_listener.result()
        except KeyboardInterrupt:
            self.logger.info(f'Stopping subscriber...')
            future_listener.cancel()
        except Exception:
            self.logger.exception(f'Unexpected error listening to topic {topic_path}.')
            raise

    def _get_subscription_path(self, project_id: str, topic_id: str, topic_path: str):
        subscription_id = f'{topic_id}-sub' # Assumes the subscription name will be `<topic_id>-sub`
        subscription_path = self.client.subscription_path(project_id, subscription_id)
        try:
            request = {'name': subscription_path, 'topic': topic_path, 'ack_deadline_seconds': 60} # To create dead-letter topic, we need to add dead_letter_policy to the subscription (https://docs.cloud.google.com/pubsub/docs/dead-letter-topics#set_a_new_dead_letter_topic)
            self.client.create_subscription(request=request)
            self.logger.info(f'Subscription created: {subscription_path}')
            return subscription_path
        except AlreadyExists:
            self.logger.info(f'Subscription already exists: {subscription_path}')
            return subscription_path

    def _callback(self, message: message.Message) -> None:
        try:
            self.logger.info(f'Message received with:\nMessage ID: {message.message_id}\nMessage Data: {message.data.decode("utf-8")}\nMessage Attributes: {message.attributes}')
            processor = Processor()
            processor.run(message=message)
            message.ack()
            self.logger.info(f'Message acknowledged!')
        except Exception as e:
            self.logger.error(f'Exception: {e}. Nacking message...')
            message.nack()

def _run():
    import os
    project_id = os.environ.get("PROJECT_ID")
    print(f'Project ID: {project_id}') 
    topic_id = os.environ.get("TOPIC_ID")
    # topic_id = f'{os.environ.get("TOPIC_ID")}-dlq' # To listen to dead-letter topic
    print(f'Topic ID: {topic_id}') 
    subscriber = Subscriber()
    result = subscriber.listen_topic(project_id=project_id, topic_id=topic_id)

if __name__ == "__main__":
    _run()

