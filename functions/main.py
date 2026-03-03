# The Cloud Functions for Firebase SDK to create Cloud Functions and set up triggers.
from firebase_functions import firestore_fn, pubsub_fn, https_fn
# The Firebase Admin SDK to access Cloud Firestore.
from firebase_admin import initialize_app, firestore

app = initialize_app()

@https_fn.on_request()
def hello_world(req: https_fn.Request) -> https_fn.Response:
    return https_fn.Response("Hello from Python!")

@pubsub_fn.on_message_published(topic="payment")
def on_payment_received(event: pubsub_fn.CloudEvent[pubsub_fn.MessagePublishedData]) -> None:
    try:
        message_dict = event.data.message.json
        order_id = message_dict["order_id"]
        
        print(f"Received message for order_id={order_id}")

        collection = "payment" # TODO: change this from hardcoded value
        enhanced_message_dict = message_dict | {"processed": False}

        firestore_client = firestore.client(database_id="e-commerce-project")
        transaction = firestore_client.transaction()
        add_message(transaction, firestore_client, collection, message_dict, enhanced_message_dict)

        print(f"Message with order_id={order_id} added to topic `{collection}` and idempotency collection.")
    except Exception:
        print(f"Error processing message with order_id={order_id}.")
        raise


@firestore.transactional
def add_message(
    transaction: firestore.Transaction,
    firestore_client: firestore.Client,
    collection: str,
    message_dict: dict,
    enhanced_message_dict: dict
) -> None:
    order_id = message_dict["order_id"]
    doc_ref_idempotency = firestore_client.collection("idempotency").document(order_id)

    if doc_ref_idempotency.get(transaction=transaction).exists:
        print(f"Message with order_id={order_id} already exists in idempotency collection. Skipping.")
        return

    doc_ref_topic = firestore_client.collection(collection).document()

    transaction.set(doc_ref_topic, message_dict) # overwrites the document if it already exists
    transaction.set(doc_ref_idempotency, enhanced_message_dict) # overwrites the document if it already exists
