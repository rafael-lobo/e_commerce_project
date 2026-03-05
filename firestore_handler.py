import logging
from firebase_admin import initialize_app, firestore, get_app, _apps

class IdempotencyError(Exception):
    pass

class UnexistingDocumentError(Exception):
    pass

class MarkAsProcessedError(Exception):
    pass

class FirestoreHandler:
    def __init__(self) -> None:
        self.logger = logging.getLogger('FirestoreHandler')
        if not _apps:
            initialize_app(options={"projectId": "pubsub-486511"}) # TODO: move to config
        self.db = firestore.client(database_id="e-commerce-project") # TODO: move to config
    
    def idempotency_check(self, order_id: str) -> None:
        try:
            collection = 'idempotency'
            doc = self.db.collection(collection).document(order_id).get()
            if not doc.exists: raise UnexistingDocumentError

            doc_dict = doc.to_dict() if doc.exists else {}
            if doc_dict.get("processed", False): raise IdempotencyError
            
            self.logger.info(f"Message with order_id={order_id} exists but is not processed yet.")
            return
        except IdempotencyError:
            raise IdempotencyError
        except UnexistingDocumentError:
            raise UnexistingDocumentError
        except Exception:
            raise

    def mark_as_processed(self, order_id: str) -> None:
        try:
            collection = 'idempotency'
            self.db.collection(collection).document(order_id).update({"processed": True})
            self.logger.info(f"Message with order_id={order_id} marked as processed.")
            return
        except Exception:
            raise MarkAsProcessedError
