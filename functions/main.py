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
    message_dict = event.data.message.json
    print(f"Received message: {message_dict}")
    print('Saving to Firestore...')
    firestore_client = firestore.client(database_id="e-commerce-project")
    _, doc_ref = firestore_client.collection("payments").add(message_dict)
    print(f"Message with ID {doc_ref.id} added.")
