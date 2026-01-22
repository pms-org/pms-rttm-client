# PMS RTTM Client

Shared Spring Boot helpers for Protobuf-based Kafka messaging (with HTTP and noop fallbacks) across PMS services.

## Requirements
- JDK 21
- Maven 3.9+
- Kafka cluster for `kafka` mode or reachable HTTP ingestion endpoint for `http` mode
- Confluent protobuf serializer on the classpath (io.confluent:kafka-protobuf-serializer is already bundled)

## Build and Install Locally
```bash
./mvnw clean install
```
This installs version `1.0.0` into your local Maven repository for reuse in other projects.

## Maven Dependency
```xml
<dependency>
    <groupId>com.pms</groupId>
    <artifactId>pms-rttm-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

## What the Library Provides
- Protobuf schemas for trade, DLQ, queue-metric, and error events (generated classes live under `com.pms.rttm.proto`).
- Spring Kafka wiring with a `KafkaTemplate<String, MessageLite>` configured for protobuf values (`PmsKafkaConfig`).
- Helpers for producing and parsing messages: `PmsProtobufProducer` and `PmsProtobufConsumer`.
- DTOs that mirror the protobuf contracts: `TradeEventPayload`, `DlqEventPayload`, `QueueMetricPayload`, and `ErrorEventPayload` (with message truncation safeguards on long text fields).
- `RttmClient` interface with implementations: `KafkaRttmClient` (sync/async sends to Kafka), `HttpRttmClient` (HTTP ingestion), and `NoopRttmClient` (safe for local/dev).

## Configuration Example
```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
      acks: ${KAFKA_PRODUCER_ACKS:all}
      retries: ${KAFKA_PRODUCER_RETRIES:3}
    consumer:
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
      group-id: ${KAFKA_CONSUMER_GROUP_ID:pms-consumer-group}
      auto-offset-reset: ${KAFKA_CONSUMER_AUTO_OFFSET_RESET:earliest}
      enable-auto-commit: ${KAFKA_CONSUMER_ENABLE_AUTO_COMMIT:false}
    listener:
      ack-mode: ${KAFKA_LISTENER_ACK_MODE:manual_immediate}

rttm:
  client:
    mode: kafka # kafka | http | noop
    kafka:
      bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
      topics:
        trade-events: rttm.trade.events
        dlq-events: rttm.dlq.events
        queue-metrics: rttm.queue.metrics
        error-events: rttm.error.events
    http:
      base-url: https://rttm.example.com
      api-key: ${RTTM_API_KEY:}
    send-timeout-ms: 3000
    retry:
      max-attempts: 3
      backoff-ms: 100
```

## Quick Start: Kafka Client
Create an `RttmClient` backed by Kafka and send a trade event.

```java
@Configuration
public class RttmClientConfiguration {

    @Bean
    public RttmClient rttmClient(KafkaTemplate<String, MessageLite> kafkaTemplate) {
        RttmClientConfig config = RttmClientConfig.builder()
                .mode("kafka")
                .kafkaBootstrapServers("localhost:9092")
                .kafkaTopicTradeEvents("rttm.trade.events")
                .kafkaTopicDlqEvents("rttm.dlq.events")
                .kafkaTopicQueueMetrics("rttm.queue.metrics")
                .kafkaTopicErrorEvents("rttm.error.events")
                .sendTimeoutMs(3000)
                .retryMaxAttempts(3)
                .retryBackoffMs(100)
                .build();

        return KafkaRttmClient.builder()
                .kafkaTemplate(kafkaTemplate)
                .config(config)
                .build();
    }
}

// Somewhere in your service layer
rttmClient.sendTradeEvent(TradeEventPayload.builder()
        .tradeId(tradeId)
        .serviceName("trade-capture")
        .eventStage("RECEIVED")
        .topicName("trade-events")
        .message("Received trade")
        .build());
```

## Quick Start: HTTP Ingestion
Use the HTTP client when Kafka access is not available.

```java
RttmClientConfig config = RttmClientConfig.builder()
        .mode("http")
        .httpEndpointBaseUrl("https://rttm.example.com")
        .httpApiKey("my-api-key")
        .build();

RttmClient httpClient = HttpRttmClient.builder()
        .config(config)
        .build();

httpClient.sendErrorEvent(ErrorEventPayload.builder()
        .serviceName("trade-capture")
        .errorType("VALIDATION")
        .errorMessage("Missing field")
        .eventStage("RECEIVED")
        .build());
```

## Consumer Example
Extend `PmsProtobufConsumer` to parse incoming protobuf bytes.

```java
@Service
public class RttmConsumer extends PmsProtobufConsumer {

    @KafkaListener(topics = "rttm.trade.events", groupId = "rttm-group")
    public void consume(byte[] data) {
        try {
            RttmTradeEvent event = parseMessage(data, RttmTradeEvent.parser());
            // Process the trade event
        } catch (Exception e) {
            handleParsingError("rttm.trade.events", e);
        }
    }
}
```

## Payload Reference (required fields)
- Trade events: `tradeId`, `serviceName`; optional stage/status/queue/topic/partition/offset/message (message is truncated to 1000 chars if longer).
- DLQ events: `tradeId`, `serviceName`; optional topic/originalTopic/reason/eventStage (reason is truncated to 1000 chars if longer).
- Queue metrics: `serviceName`, `topicName`; optional partition/producedOffset/consumedOffset/consumerGroup.
- Error events: `serviceName`, `errorType`; optional tradeId/errorMessage/eventStage (errorMessage truncated to 1000 chars if longer).

## Testing
```bash
./mvnw test
```

## Publishing
Use `./mvnw clean install` for local reuse or `./mvnw deploy` when publishing to your configured Maven repository (OSSRH configured in the POM).