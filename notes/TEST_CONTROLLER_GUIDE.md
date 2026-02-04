# RTTM Test Controller Guide

This guide provides sample data and usage instructions for testing the RTTM Kafka client using REST endpoints.

## Table of Contents
1. [Controller Endpoints](#controller-endpoints)
2. [Sample Data](#sample-data)
3. [cURL Examples](#curl-examples)
4. [Postman Collection](#postman-collection)

---

## Controller Endpoints

The test controller provides the following endpoints:

### Trade Events
- **POST** `/api/test/trade-event` - Send trade event (sync)
- **POST** `/api/test/trade-event-async` - Send trade event (async)
- **POST** `/api/test/batch-trade-events?count=5&serviceName=TEST-SERVICE` - Send batch trade events

### DLQ Events
- **POST** `/api/test/dlq-event` - Send DLQ event (sync)
- **POST** `/api/test/dlq-event-async` - Send DLQ event (async)

### Error Events
- **POST** `/api/test/error-event` - Send error event (sync)
- **POST** `/api/test/error-event-async` - Send error event (async)

### Queue Metrics
- **POST** `/api/test/queue-metric` - Send queue metric (sync)
- **POST** `/api/test/queue-metric-async` - Send queue metric (async)

### Health Check
- **GET** `/api/test/health` - Check controller health

---

## Sample Data

### 1. Trade Event Payload

#### Successful Trade - Received Stage
```json
{
  "tradeId": "TRADE-2026-0001",
  "serviceName": "trade-ingestion-service",
  "eventType": "TRADE_RECEIVED",
  "eventStage": "RECEIVED",
  "eventStatus": "SUCCESS",
  "sourceQueue": "external-feed-queue",
  "targetQueue": "validation-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "trade-processor-group",
  "partitionId": 0,
  "offsetValue": 12345,
  "message": "Trade received from external feed for equity BUY order"
}
```

#### Validated Trade
```json
{
  "tradeId": "TRADE-2026-0002",
  "serviceName": "trade-validation-service",
  "eventType": "TRADE_VALIDATED",
  "eventStage": "VALIDATED",
  "eventStatus": "SUCCESS",
  "sourceQueue": "validation-queue",
  "targetQueue": "enrichment-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "validation-processor-group",
  "partitionId": 1,
  "offsetValue": 23456,
  "message": "Trade validation passed for FX SELL order - compliance checks complete"
}
```

#### Enriched Trade
```json
{
  "tradeId": "TRADE-2026-0003",
  "serviceName": "trade-enrichment-service",
  "eventType": "TRADE_ENRICHED",
  "eventStage": "ENRICHED",
  "eventStatus": "SUCCESS",
  "sourceQueue": "enrichment-queue",
  "targetQueue": "commit-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "enrichment-processor-group",
  "partitionId": 2,
  "offsetValue": 34567,
  "message": "Trade enriched with reference data and pricing information"
}
```

#### Committed Trade
```json
{
  "tradeId": "TRADE-2026-0004",
  "serviceName": "trade-commit-service",
  "eventType": "TRADE_COMMITTED",
  "eventStage": "COMMITTED",
  "eventStatus": "SUCCESS",
  "sourceQueue": "commit-queue",
  "targetQueue": "analytics-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "commit-processor-group",
  "partitionId": 0,
  "offsetValue": 45678,
  "message": "Trade committed to portfolio management system ledger"
}
```

#### Rejected Trade
```json
{
  "tradeId": "TRADE-2026-0006",
  "serviceName": "trade-validation-service",
  "eventType": "TRADE_REJECTED",
  "eventStage": "VALIDATED",
  "eventStatus": "FAILED",
  "sourceQueue": "validation-queue",
  "targetQueue": "rejection-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "validation-processor-group",
  "partitionId": 2,
  "offsetValue": 67890,
  "message": "Trade rejected: Invalid instrument code or missing counterparty details"
}
```

#### Failed Trade
```json
{
  "tradeId": "TRADE-2026-0007",
  "serviceName": "trade-enrichment-service",
  "eventType": "TRADE_FAILED",
  "eventStage": "ENRICHED",
  "eventStatus": "ERROR",
  "sourceQueue": "enrichment-queue",
  "targetQueue": "retry-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "enrichment-processor-group",
  "partitionId": 0,
  "offsetValue": 78901,
  "message": "Trade processing failed: Reference data service temporarily unavailable"
}
```

---

### 2. DLQ Event Payload

#### Basic DLQ Event
```json
{
  "tradeId": "TRADE-2026-DLQ-001",
  "serviceName": "trade-validation-service",
  "topicName": "pms.trade.dlq",
  "originalTopic": "pms.trade.events",
  "reason": "Maximum retry attempts exceeded - unable to parse message format",
  "eventStage": "VALIDATED"
}
```

#### DLQ Event - Deserialization Error
```json
{
  "tradeId": "TRADE-2026-DLQ-002",
  "serviceName": "trade-enrichment-service",
  "topicName": "pms.trade.dlq",
  "originalTopic": "pms.trade.events",
  "reason": "Deserialization failed: Protobuf message corrupted or incompatible schema version",
  "eventStage": "ENRICHED"
}
```

#### DLQ Event - Business Logic Error
```json
{
  "tradeId": "TRADE-2026-DLQ-003",
  "serviceName": "trade-commit-service",
  "topicName": "pms.trade.dlq",
  "originalTopic": "pms.trade.events",
  "reason": "Business rule violation: Trade amount exceeds position limits after 3 retry attempts",
  "eventStage": "COMMITTED"
}
```

---

### 3. Error Event Payload

#### Validation Error
```json
{
  "tradeId": "TRADE-2026-ERR-001",
  "serviceName": "trade-validation-service",
  "errorType": "VALIDATION_ERROR",
  "errorMessage": "Required field missing: counterpartyId is null or empty",
  "eventStage": "VALIDATED"
}
```

#### Database Connection Error
```json
{
  "serviceName": "trade-commit-service",
  "errorType": "DATABASE_CONNECTION_ERROR",
  "errorMessage": "Unable to connect to PostgreSQL database: Connection timeout after 30 seconds",
  "eventStage": "COMMITTED"
}
```

#### External Service Error
```json
{
  "tradeId": "TRADE-2026-ERR-002",
  "serviceName": "trade-enrichment-service",
  "errorType": "EXTERNAL_SERVICE_ERROR",
  "errorMessage": "Reference data API returned HTTP 503: Service temporarily unavailable",
  "eventStage": "ENRICHED"
}
```

#### Kafka Consumer Error
```json
{
  "serviceName": "trade-analytics-service",
  "errorType": "KAFKA_CONSUMER_ERROR",
  "errorMessage": "Offset commit failed: Broker not available - rebalancing consumer group",
  "eventStage": "COMMITTED"
}
```

#### Schema Mismatch Error
```json
{
  "tradeId": "TRADE-2026-ERR-003",
  "serviceName": "trade-ingestion-service",
  "errorType": "SCHEMA_MISMATCH_ERROR",
  "errorMessage": "Protobuf schema version mismatch: Expected v2.1.0, received v1.9.5",
  "eventStage": "RECEIVED"
}
```

---

### 4. Queue Metric Payload

#### Healthy Queue Metrics
```json
{
  "serviceName": "trade-validation-service",
  "topicName": "pms.trade.events",
  "partitionId": 0,
  "producedOffset": 100000,
  "consumedOffset": 99950,
  "consumerGroup": "validation-processor-group"
}
```

#### Queue with Lag
```json
{
  "serviceName": "trade-enrichment-service",
  "topicName": "pms.trade.events",
  "partitionId": 1,
  "producedOffset": 250000,
  "consumedOffset": 235000,
  "consumerGroup": "enrichment-processor-group"
}
```

#### High Throughput Queue
```json
{
  "serviceName": "trade-commit-service",
  "topicName": "pms.trade.events",
  "partitionId": 2,
  "producedOffset": 5000000,
  "consumedOffset": 4999800,
  "consumerGroup": "commit-processor-group"
}
```

#### Multiple Partitions Metrics
```json
{
  "serviceName": "trade-analytics-service",
  "topicName": "pms.trade.events",
  "partitionId": 3,
  "producedOffset": 750000,
  "consumedOffset": 750000,
  "consumerGroup": "analytics-processor-group"
}
```

---

## cURL Examples

### Send Trade Event (Sync)
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-2026-CURL-001",
    "serviceName": "trade-ingestion-service",
    "eventType": "TRADE_RECEIVED",
    "eventStage": "RECEIVED",
    "eventStatus": "SUCCESS",
    "topicName": "pms.trade.events",
    "message": "Testing trade event via cURL"
  }'
```

### Send Trade Event (Async)
```bash
curl -X POST http://localhost:8080/api/test/trade-event-async \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-2026-ASYNC-001",
    "serviceName": "trade-validation-service",
    "eventType": "TRADE_VALIDATED",
    "eventStage": "VALIDATED",
    "eventStatus": "SUCCESS",
    "topicName": "pms.trade.events",
    "message": "Testing async trade event"
  }'
```

### Send DLQ Event
```bash
curl -X POST http://localhost:8080/api/test/dlq-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-2026-DLQ-CURL-001",
    "serviceName": "trade-enrichment-service",
    "topicName": "pms.trade.dlq",
    "originalTopic": "pms.trade.events",
    "reason": "Max retries exceeded - testing DLQ via cURL",
    "eventStage": "ENRICHED"
  }'
```

### Send Error Event
```bash
curl -X POST http://localhost:8080/api/test/error-event \
  -H "Content-Type: application/json" \
  -d '{
    "serviceName": "trade-commit-service",
    "errorType": "DATABASE_ERROR",
    "errorMessage": "Connection pool exhausted - testing error event",
    "eventStage": "COMMITTED"
  }'
```

### Send Queue Metric
```bash
curl -X POST http://localhost:8080/api/test/queue-metric \
  -H "Content-Type: application/json" \
  -d '{
    "serviceName": "trade-analytics-service",
    "topicName": "pms.trade.events",
    "partitionId": 0,
    "producedOffset": 150000,
    "consumedOffset": 149900,
    "consumerGroup": "analytics-processor-group"
  }'
```

### Send Batch Trade Events
```bash
curl -X POST "http://localhost:8080/api/test/batch-trade-events?count=10&serviceName=batch-test-service" \
  -H "Content-Type: application/json"
```

### Health Check
```bash
curl -X GET http://localhost:8080/api/test/health
```

---

## Postman Collection

### Import into Postman

Create a new collection with the following requests:

#### 1. Trade Event - Received
- **Method**: POST
- **URL**: `{{base_url}}/api/test/trade-event`
- **Body** (raw JSON):
```json
{
  "tradeId": "TRADE-{{$timestamp}}",
  "serviceName": "trade-ingestion-service",
  "eventType": "TRADE_RECEIVED",
  "eventStage": "RECEIVED",
  "eventStatus": "SUCCESS",
  "topicName": "pms.trade.events",
  "message": "Postman test - trade received"
}
```

#### 2. Trade Event - Validated
- **Method**: POST
- **URL**: `{{base_url}}/api/test/trade-event`
- **Body** (raw JSON):
```json
{
  "tradeId": "TRADE-{{$timestamp}}",
  "serviceName": "trade-validation-service",
  "eventType": "TRADE_VALIDATED",
  "eventStage": "VALIDATED",
  "eventStatus": "SUCCESS",
  "sourceQueue": "validation-queue",
  "targetQueue": "enrichment-queue",
  "topicName": "pms.trade.events",
  "consumerGroup": "validation-processor-group",
  "partitionId": 0,
  "offsetValue": 12345,
  "message": "Postman test - trade validated"
}
```

#### 3. DLQ Event
- **Method**: POST
- **URL**: `{{base_url}}/api/test/dlq-event`
- **Body** (raw JSON):
```json
{
  "tradeId": "TRADE-DLQ-{{$timestamp}}",
  "serviceName": "trade-enrichment-service",
  "topicName": "pms.trade.dlq",
  "originalTopic": "pms.trade.events",
  "reason": "Postman test - max retries exceeded",
  "eventStage": "ENRICHED"
}
```

#### 4. Error Event
- **Method**: POST
- **URL**: `{{base_url}}/api/test/error-event`
- **Body** (raw JSON):
```json
{
  "serviceName": "trade-commit-service",
  "errorType": "TEST_ERROR",
  "errorMessage": "Postman test - simulated error condition",
  "eventStage": "COMMITTED"
}
```

#### 5. Queue Metric
- **Method**: POST
- **URL**: `{{base_url}}/api/test/queue-metric`
- **Body** (raw JSON):
```json
{
  "serviceName": "trade-analytics-service",
  "topicName": "pms.trade.events",
  "partitionId": 0,
  "producedOffset": 100000,
  "consumedOffset": 99950,
  "consumerGroup": "analytics-processor-group"
}
```

#### Environment Variables
Set up these variables in Postman:
- `base_url`: `http://localhost:8080`

---

## Testing Scenarios

### Scenario 1: Complete Trade Lifecycle
Send events in sequence to simulate a complete trade flow:

1. **Received**
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-LIFECYCLE-001",
    "serviceName": "trade-ingestion-service",
    "eventType": "TRADE_RECEIVED",
    "eventStage": "RECEIVED",
    "eventStatus": "SUCCESS",
    "message": "Trade received from external feed"
  }'
```

2. **Validated**
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-LIFECYCLE-001",
    "serviceName": "trade-validation-service",
    "eventType": "TRADE_VALIDATED",
    "eventStage": "VALIDATED",
    "eventStatus": "SUCCESS",
    "message": "Trade validation passed"
  }'
```

3. **Enriched**
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-LIFECYCLE-001",
    "serviceName": "trade-enrichment-service",
    "eventType": "TRADE_ENRICHED",
    "eventStage": "ENRICHED",
    "eventStatus": "SUCCESS",
    "message": "Trade enriched with reference data"
  }'
```

4. **Committed**
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-LIFECYCLE-001",
    "serviceName": "trade-commit-service",
    "eventType": "TRADE_COMMITTED",
    "eventStage": "COMMITTED",
    "eventStatus": "SUCCESS",
    "message": "Trade committed to ledger"
  }'
```

### Scenario 2: Error Flow with DLQ
Simulate an error that results in DLQ routing:

1. **Failed Trade**
```bash
curl -X POST http://localhost:8080/api/test/trade-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-ERROR-001",
    "serviceName": "trade-enrichment-service",
    "eventType": "TRADE_FAILED",
    "eventStage": "ENRICHED",
    "eventStatus": "ERROR",
    "message": "Reference data service unavailable"
  }'
```

2. **Error Event**
```bash
curl -X POST http://localhost:8080/api/test/error-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-ERROR-001",
    "serviceName": "trade-enrichment-service",
    "errorType": "EXTERNAL_SERVICE_ERROR",
    "errorMessage": "Reference data API timeout after 30 seconds",
    "eventStage": "ENRICHED"
  }'
```

3. **DLQ Event**
```bash
curl -X POST http://localhost:8080/api/test/dlq-event \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-ERROR-001",
    "serviceName": "trade-enrichment-service",
    "topicName": "pms.trade.dlq",
    "originalTopic": "pms.trade.events",
    "reason": "Maximum retry attempts (3) exceeded - external service unavailable",
    "eventStage": "ENRICHED"
  }'
```

### Scenario 3: Load Testing
Use the batch endpoint to generate load:

```bash
# Send 100 trade events
curl -X POST "http://localhost:8080/api/test/batch-trade-events?count=100&serviceName=load-test-service"

# Send 1000 trade events
curl -X POST "http://localhost:8080/api/test/batch-trade-events?count=1000&serviceName=load-test-service"
```

---

## Notes

1. **Event Time**: If not provided, `eventTime` defaults to `System.currentTimeMillis()`
2. **Snapshot Time**: For queue metrics, `snapshotTime` defaults to current timestamp
3. **Message Truncation**: Messages are automatically truncated to 1000 characters
4. **Required Fields**: Ensure required fields (marked with `@NonNull`) are included
5. **Event Types**: Use valid values from `EventType` enum
6. **Event Stages**: Use valid values from `EventStage` enum

## Enum Values Reference

### EventType
- `TRADE_RECEIVED`
- `TRADE_VALIDATED`
- `TRADE_ENRICHED`
- `TRADE_COMMITTED`
- `TRADE_REJECTED`
- `TRADE_FAILED`
- `TRADE_SENT_TO_DLQ`

### EventStage
- `RECEIVED`
- `VALIDATED`
- `ENRICHED`
- `COMMITTED`
