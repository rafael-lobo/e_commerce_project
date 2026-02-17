## Requirements
### Must Include
- Pub/Sub event-driven pipeline
- Idempotent processing
- Retry system with backoff
- Firestore retry queue
- Dead Letter Queue
- Circuit breaker for payment API
- Structured logging
- BigQuery analytics
- Looker dashboard
- Cancellation alert

### Final Architecture
```
Client
  ↓
Order API (Cloud Function)
  ↓ publish
Pub/Sub (order-created)
  ↓
Payment Processor (with circuit breaker + retry)
  ↓
Inventory Processor
  ↓
BigQuery Logger
  ↓
Analytics Dashboard
```

### Retry Queue:
```
Cloud Scheduler → Retry Processor → Firestore retry_queue
```

### DLQ:
```
Failed > 5 attempts → dead-letter-topic
```