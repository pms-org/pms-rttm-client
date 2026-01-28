# Kafka Consumer Commands for Docker Container

## First, Find Your Kafka Container Name
```bash
docker ps | findstr kafka
```

---

## Consume Messages from All Topics

### 1. Trade Events Topic (From Beginning)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.trade.events --from-beginning --property print.key=true --property key.separator=" : "
```

### 2. DLQ Events Topic (From Beginning)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.dlq.events --from-beginning --property print.key=true --property key.separator=" : "
```

### 3. Queue Metrics Topic (From Beginning)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.queue.metrics --from-beginning --property print.key=true --property key.separator=" : "
```

### 4. Error Events Topic (From Beginning)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.error.events --from-beginning --property print.key=true --property key.separator=" : "
```

---

## Latest Messages Only (Last 10)

### Trade Events (Latest 10)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.trade.events --max-messages 10 --property print.key=true
```

### DLQ Events (Latest 10)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.dlq.events --max-messages 10 --property print.key=true
```

### Queue Metrics (Latest 10)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.queue.metrics --max-messages 10 --property print.key=true
```

### Error Events (Latest 10)
```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.error.events --max-messages 10 --property print.key=true
```

---

## Check Topics & Metadata

### List All Topics
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Describe Trade Events Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic rttm.trade.events
```

### Describe DLQ Events Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic rttm.dlq.events
```

### Describe Queue Metrics Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic rttm.queue.metrics
```

### Describe Error Events Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic rttm.error.events
```

---

## Check Consumer Groups

### List All Consumer Groups
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
```

### Check Consumer Group Lag
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group pms-consumer-group --describe
```

---

## Common Container Names

If `kafka` doesn't work, try these common names:
- `kafka-1` or `kafka-broker-1`
- `broker`
- `kafka-broker`
- `confluent-kafka`

Use `docker ps` to find the exact name.

---

## If Using Docker Compose

### Consume Messages
```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic rttm.trade.events --from-beginning
```

### List Topics
```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## Additional Useful Commands

### Count Messages in a Topic
```bash
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic rttm.trade.events --time -1
```

### Delete a Topic (Use with Caution)
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic rttm.trade.events
```

### Create a Topic
```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --create --topic rttm.trade.events --partitions 3 --replication-factor 1
```

### Reset Consumer Group Offset
```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group pms-consumer-group --reset-offsets --to-earliest --topic rttm.trade.events --execute
```

---

## Notes

- **Protobuf Messages**: Since the topics use protobuf serialization, the console consumer will show binary data. For human-readable output, you may need to use a custom deserializer or protobuf tools.
  
- **Bootstrap Server**: If your Kafka is configured with a different bootstrap server (e.g., `kafka:29092` for internal Docker network), update the `--bootstrap-server` parameter accordingly.

- **Schema Registry**: If using Confluent Schema Registry at `http://localhost:8081`, you may need additional configuration for proper deserialization.
