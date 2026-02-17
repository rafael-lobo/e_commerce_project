import logging
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists

"""
- Infrastructure is provisioned via Terraform / Deployment scripts
- Runtime code should not mutate infrastructure
- Topic creation may require permissions not granted to runtime service account
- Better Pattern -> Move infra creation to:
    - Deployment phase
    - Separate bootstrap script
"""
class TopicHandler:
    def __init__(self, client: pubsub_v1.PublisherClient):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        self.logger = logging.getLogger('TopicHandler')
        self.client = client

    
    def get_topic_path(self, project_id: str, topic_id: str) -> str:
        topic_path = self.client.topic_path(project_id, topic_id)
        try:
            self.client.create_topic(name=topic_path)
            self.logger.info(f'Topic created: {topic_path}')
            return topic_path
        except AlreadyExists:
            self.logger.info(f'Topic already exists: {topic_path}')
            return topic_path
        except Exception:
            self.logger.exception(f'Unexpected error creating topic')
            raise