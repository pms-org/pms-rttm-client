# Complete End-to-End Testing Guide

## Architecture Overview

```
pms-validation (App + Containers)
    ↓ (publishes via pms-rttm-client library)
Kafka Topics (rttm.trade.events, rttm.dlq.events, etc.)
    ↓ (consumes)
pms-rttm (App + Containers)
    ↓ (processes/stores)
Output/Monitoring
```

## Setup & Execution Order

### Phase 1: Prepare Both Applications

**In pms-validation directory:**
```bash
# Build the project (this includes pms-rttm-client as dependency)
./mvnw clean install -DskipTests
```

**In pms-rttm directory:**
```bash
# Build the project
./mvnw clean install -DskipTests
```

---

### Phase 2: Start pms-validation Containers

**Terminal 1 - pms-validation Kafka Stack:**
```bash
cd path/to/pms-validation
docker-compose up
```

Wait for these logs to appear:
```
Topics created successfully!
schema-registry | [2026-01-28 ...] INFO Ready to serve requests
control-center | [2026-01-28 ...] Listening on ...
```

**Verify containers are running:**
```bash
docker ps | grep -E "kafka|schema-registry|control-center|redis"
```

Expected output:
- kafka (pms-validation)
- schema-registry
- control-center
- redis

---

### Phase 3: Start pms-rttm Containers

**Terminal 2 - pms-rttm Kafka Stack:**
```bash
cd path/to/pms-rttm
docker-compose up
```

Wait for:
```
Kafka ready.
control-center | [2026-01-28 ...] Listening on ...
```

**Note:** This creates a SEPARATE Kafka cluster from pms-validation. Both clusters will have the same topics.

---

### Phase 4: Start pms-validation Application

**Terminal 3 - pms-validation App:**
```bash
cd path/to/pms-validation

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export RTTM_MODE=kafka
export RTTM_SEND_TIMEOUT_MS=5000

# Run the app
./mvnw spring-boot:run
```

Wait for:
```
2026-01-28 ... INFO Started Main in X.XXX seconds
2026-01-28 ... INFO Tomcat started on port(s): 8080
```

Verify it's accessible:
```bash
curl http://localhost:8080/swagger-ui.html
```

---

### Phase 5: Start pms-rttm Application

**Terminal 4 - pms-rttm App:**
```bash
cd path/to/pms-rttm

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run the app
./mvnw spring-boot:run
```

Wait for:
```
2026-01-28 ... INFO Started Main in X.XXX seconds
```

---

## Testing the Integration

### Test 1: Verify Kafka Topics Exist

**In any terminal:**
```bash
# For pms-validation Kafka
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Should output:
# raw-trades-topic
# valid-trades-topic
# invalid-trades-topic
# rttm.trade.events
# rttm.dlq.events
# rttm.queue.metrics
# rttm.error.events
```

---

### Test 2: Send Trade Data to pms-validation

**Option A: Via REST API (if exposed)**
```bash
curl -X POST http://localhost:8080/api/trades \
  -H "Content-Type: application/json" \
  -d '{
    "tradeId": "TRADE-001",
    "symbol": "AAPL",
    "quantity": 100,
    "price": 150.5,
    "buyerParty": "PARTY-A",
    "sellerParty": "PARTY-B"
  }'
```

**Option B: Send to raw-trades-topic via Kafka**
```bash
# Use this to send raw trade JSON
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic raw-trades-topic

# Then type (and press Enter):
{"tradeId":"TRADE-001","symbol":"AAPL","quantity":100,"price":150.5}

# Press Ctrl+C to exit
```

---

### Test 3: Monitor pms-validation Processing

**Terminal 5 - Watch Validation Output:**
```bash
# Listen to valid trades
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic valid-trades-topic \
  --from-beginning

# In another terminal, listen to invalid trades
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic invalid-trades-topic \
  --from-beginning

# In another terminal, listen to rttm trade events
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic rttm.trade.events \
  --from-beginning
```

**Expected output when trade is valid:**
```
{...trade data...}  # in valid-trades-topic
{...protobuf bytes...}  # in rttm.trade.events (sent by pms-rttm-client)
```

**Expected output when trade is invalid:**
```
{...error details...}  # in invalid-trades-topic
{...error protobuf bytes...}  # in rttm.error.events (sent by pms-rttm-client)
```

---

### Test 4: Monitor pms-rttm Consumption

**Terminal 6 - Watch pms-rttm Logs:**
```bash
# Check pms-rttm app terminal (Terminal 4) for logs showing:
# - Consumer receiving from rttm.trade.events
# - Processing/storing trade data
# - Any error handling

# Look for log lines like:
# 2026-01-28 ... INFO Received trade event: TRADE-001
# 2026-01-28 ... INFO Processed trade successfully
```

---

### Test 5: Monitor via Control Center UIs

**Option A: pms-validation Control Center**
```
http://localhost:9021
```
- View Topics → Select rttm.trade.events
- See messages, partitions, lag
- Monitor producer/consumer activity

**Option B: pms-rttm Control Center**
```
http://localhost:9021  (different instance, may need port adjustment)
```

---

## Complete Test Scenario

### Scenario: Submit 5 Valid Trades and Monitor End-to-End

**Step 1: Send trades to pms-validation**
```bash
for i in {1..5}; do
  curl -X POST http://localhost:8080/api/trades \
    -H "Content-Type: application/json" \
    -d "{
      \"tradeId\": \"TRADE-00$i\",
      \"symbol\": \"AAPL\",
      \"quantity\": $((100 + i * 10)),
      \"price\": 150.5,
      \"buyerParty\": \"PARTY-A\",
      \"sellerParty\": \"PARTY-B\"
    }"
  sleep 1
done
```

**Step 2: Monitor flow in real-time**

Terminal 5a:
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic valid-trades-topic \
  --from-beginning
```

Terminal 5b:
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic rttm.trade.events \
  --from-beginning
```

**Step 3: Check pms-rttm logs (Terminal 4)**
- Should see: "Received trade event: TRADE-001"
- Should see: "Received trade event: TRADE-002", etc.

---

## Verification Checklist

✅ **pms-validation Running:**
```bash
curl http://localhost:8080/health
# Should return: {"status":"UP"}
```

✅ **pms-rttm Running:**
```bash
curl http://localhost:8080/health
# Should return: {"status":"UP"}
# (May be different port if configured)
```

✅ **Kafka Connectivity:**
```bash
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
# Should list broker versions
```

✅ **Messages Published:**
```bash
docker exec -it kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
# Should show consumer groups for both apps
```

✅ **No Errors in Logs:**
- pms-validation (Terminal 3): No ERROR/EXCEPTION logs
- pms-rttm (Terminal 4): No ERROR/EXCEPTION logs

---

## Troubleshooting

### Problem: "Connection refused" to Kafka
**Solution:**
```bash
# Ensure containers are running
docker ps

# Ensure correct host:
# From app: localhost:9092 (PLAINTEXT_HOST listener)
# From within docker: kafka:29092 (PLAINTEXT listener)
```

### Problem: "RttmClient bean not found"
**Solution:**
- Ensure `pms-rttm-client` dependency is in pms-validation `pom.xml`
- Run: `./mvnw clean install` to rebuild with dependency
- Check logs for: `Autowiring by type from bean name 'rttmClient'`

### Problem: Topics don't exist
**Solution:**
```bash
# Recreate topics manually
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic rttm.trade.events --partitions 5 --replication-factor 1
```

### Problem: No messages appearing in topics
**Solution:**
1. Check pms-validation logs for errors (Terminal 3)
2. Verify trades are reaching pms-validation:
   ```bash
   curl http://localhost:8080/api/trades/count
   # Should show > 0
   ```
3. Check if pms-rttm-client is configured correctly:
   ```bash
   # Grep logs for "RttmClient"
   # Should show: "mode=kafka", "bootstrap-servers=localhost:9092"
   ```

### Problem: pms-rttm not consuming
**Solution:**
```bash
# Check consumer group lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group pms-rttm-consumer \
  --describe

# If lag is high, app may not be processing fast enough
# Check pms-rttm logs for slow queries or errors
```

---

## Alternative: Shared Kafka Cluster (Recommended for Simplicity)

If you want both apps to share ONE Kafka cluster to reduce resource usage:

1. Remove docker-compose from pms-rttm
2. Only run pms-validation's Kafka
3. Point both apps to the same Kafka:
   ```bash
   # Both apps:
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

---

## Environment Variables Summary

### pms-validation App
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export RTTM_MODE=kafka
export KAFKA_TOPIC_TRADE_EVENTS=rttm.trade.events
export KAFKA_TOPIC_DLQ_EVENTS=rttm.dlq.events
export KAFKA_TOPIC_ERROR_EVENTS=rttm.error.events
export KAFKA_TOPIC_QUEUE_METRICS=rttm.queue.metrics
export RTTM_SEND_TIMEOUT_MS=5000
export RTTM_RETRY_MAX_ATTEMPTS=3
export RTTM_RETRY_BACKOFF_MS=100
```

### pms-rttm App
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_CONSUMER_GROUP_ID=pms-rttm-consumer
export KAFKA_CONSUMER_AUTO_OFFSET_RESET=earliest
```

---

## Stopping Everything Cleanly

```bash
# Stop all apps (Ctrl+C in each terminal)

# Stop all containers
docker-compose -f path/to/pms-validation/docker-compose.yml down
docker-compose -f path/to/pms-rttm/docker-compose.yml down

# Clean up volumes (optional)
docker volume prune
```

---

## Quick Reference: Terminal Layout

```
Terminal 1: pms-validation docker-compose
Terminal 2: pms-rttm docker-compose
Terminal 3: pms-validation app (./mvnw spring-boot:run)
Terminal 4: pms-rttm app (./mvnw spring-boot:run)
Terminal 5a: kafka-console-consumer (valid-trades-topic)
Terminal 5b: kafka-console-consumer (rttm.trade.events)
Terminal 6: Manual testing (curl commands, kafka-topics, etc.)
```
