# PMS Kafka Protobuf Library

A shared Maven dependency for standardized Kafka communication using Protobuf across PMS microservices.

## Features

- Pre-configured Kafka producers and consumers for Protobuf messages
- Standardized message schemas for RTTM, lifecycle events, and error handling
- Automatic serialization/deserialization of Protobuf messages
- Spring Boot auto-configuration

## Usage in Microservices

### 1. Add Dependency

```xml
<dependency>
    <groupId>com.pms</groupId>
    <artifactId>pms-kafka-protobuf-lib</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 2. Producer Example

```java
@Service
public class TradeService {
    
    @Autowired
    private PmsProtobufProducer producer;
    
    public void publishTrade(String tradeId) {
        RttmTradeEvent event = RttmTradeEvent.newBuilder()
            .setTradeId(tradeId)
            .setServiceName("trade-capture")
            .setEventStage("RECEIVED")
            .setEventTime(System.currentTimeMillis())
            .build();
            
        producer.sendEvent("trade-events", event);
    }
}
```

### 3. Consumer Example

```java
@Service
public class RttmConsumer extends PmsProtobufConsumer {

    @KafkaListener(topics = "trade-events", groupId = "rttm-group")
    public void consume(byte[] data) {
        try {
            RttmTradeEvent event = parseMessage(data, RttmTradeEvent.parser());
            // Process the trade event
        } catch (Exception e) {
            handleParsingError("trade-events", e);
        }
    }
}
```

### 4. Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.ByteArraySerializer
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
      group-id: your-service-group
```

## Available Message Types

- `RttmTradeEvent` - Trade lifecycle events
- `RttmDlqEvent` - Dead letter queue events
- `RttmErrorEvent` - Error events
- `RttmQueueMetric` - Queue metrics
- `LifecycleEvent` - Portfolio lifecycle events

## Installation

1. Clone and build the library:
```bash
mvn clean install
```

2. The library will be available in your local Maven repository for use in other projects.