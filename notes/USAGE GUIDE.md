# PMS RTTM Client: Usage and Testing Guide

This library sends RTTM protobuf events (trade, DLQ, queue metric, error) over Kafka. Use it from downstream services such as `pms-validation` to publish telemetry to the running `pms-rttm` application.

## Prerequisites
- JDK 21, Maven 3.9+
- Kafka broker accessible at `kafka:29092` (if running in Docker) or `localhost:9092` (if running locally)
- Confluent protobuf serializer on the classpath (already in this project)
- Access to the `pms-rttm` service (for manual end-to-end tests)

## Add the dependency
```xml
<dependency>
    <groupId>com.pms</groupId>
    <artifactId>pms-rttm-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

Maven Central is default for Maven/Gradle. If your organization uses a custom settings.xml or an offline mirror, ensure Maven Central is reachable. If needed, explicitly declare it:

```xml
<repositories>
   <repository>
      <id>central</id>
      <name>Maven Central</name>
      <url>https://repo.maven.apache.org/maven2</url>
      <releases>
         <enabled>true</enabled>
      </releases>
      <snapshots>
         <enabled>false</enabled>
      </snapshots>
   </repository>
</repositories>
```

## Configuration

### Bootstrap Server Selection
- **Docker environment**: Use `kafka:29092` (Docker network hostname)
- **Local development**: Use `localhost:9092` (host machine)

Common environment variables:
- `KAFKA_BOOTSTRAP_SERVERS` (e.g., `kafka:29092` for Docker, `localhost:9092` for local)
- `KAFKA_PRODUCER_ACKS` (default `all`), `KAFKA_PRODUCER_RETRIES` (default `3`)
- `KAFKA_CONSUMER_GROUP_ID` (default `pms-consumer-group`), `KAFKA_CONSUMER_AUTO_OFFSET_RESET` (default `earliest`)
- `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT` (default `false`)
- `KAFKA_LISTENER_ACK_MODE` (default `manual_immediate`)

Sample `application.yml` fragment:
```yaml
rttm:
  client:
    mode: kafka
    kafka:
      bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
      topics:
        trade-events: rttm.trade.events
        dlq-events: rttm.dlq.events
        queue-metrics: rttm.queue.metrics
        error-events: rttm.error.events
    send-timeout-ms: 3000
    retry:
      max-attempts: 3
      backoff-ms: 100
```

## Creating the Kafka Client
Load values from `application.yml` (which itself reads env vars with fallbacks) using `@Value`:

```java
@Configuration
public class RttmClientConfiguration {

      @Value("${rttm.client.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}")
      private String kafkaBootstrap;

      @Value("${rttm.client.kafka.topics.trade-events:${RTTM_TRADE_TOPIC:rttm.trade.events}}")
      private String tradeTopic;

      @Value("${rttm.client.kafka.topics.dlq-events:${RTTM_DLQ_TOPIC:rttm.dlq.events}}")
      private String dlqTopic;

      @Value("${rttm.client.kafka.topics.queue-metrics:${RTTM_QUEUE_METRIC_TOPIC:rttm.queue.metrics}}")
      private String queueMetricTopic;

      @Value("${rttm.client.kafka.topics.error-events:${RTTM_ERROR_TOPIC:rttm.error.events}}")
      private String errorTopic;

      @Value("${rttm.client.send-timeout-ms:${RTTM_SEND_TIMEOUT_MS:3000}}")
      private int sendTimeoutMs;

      @Value("${rttm.client.retry.max-attempts:${RTTM_RETRY_MAX_ATTEMPTS:3}}")
      private int retryMaxAttempts;

      @Value("${rttm.client.retry.backoff-ms:${RTTM_RETRY_BACKOFF_MS:100}}")
      private int retryBackoffMs;

      @Bean
      public RttmClient rttmClient(KafkaTemplate<String, MessageLite> kafkaTemplate) {
            RttmClientConfig config = RttmClientConfig.builder()
                        .mode("kafka")
                        .kafkaBootstrapServers(kafkaBootstrap)
                        .kafkaTopicTradeEvents(tradeTopic)
                        .kafkaTopicDlqEvents(dlqTopic)
                        .kafkaTopicQueueMetrics(queueMetricTopic)
                        .kafkaTopicErrorEvents(errorTopic)
                        .sendTimeoutMs(sendTimeoutMs)
                        .retryMaxAttempts(retryMaxAttempts)
                        .retryBackoffMs(retryBackoffMs)
                        .build();

            return KafkaRttmClient.builder()
                        .kafkaTemplate(kafkaTemplate)
                        .config(config)
                        .build();
      }
}
```

Sample `application.yml` fragment that backs the `@Value` bindings (env vars with sensible defaults):

```yaml
rttm:
   client:
      mode: kafka
      kafka:
         bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
         topics:
            trade-events: ${RTTM_TRADE_TOPIC:rttm.trade.events}
            dlq-events: ${RTTM_DLQ_TOPIC:rttm.dlq.events}
            queue-metrics: ${RTTM_QUEUE_METRIC_TOPIC:rttm.queue.metrics}
            error-events: ${RTTM_ERROR_TOPIC:rttm.error.events}
      send-timeout-ms: ${RTTM_SEND_TIMEOUT_MS:3000}
      retry:
         max-attempts: ${RTTM_RETRY_MAX_ATTEMPTS:3}
         backoff-ms: ${RTTM_RETRY_BACKOFF_MS:100}
```

## Sending Data to Kafka

### Overview
The RTTM client provides four types of events to track different aspects of trade processing:

1. **Trade Events** - Track trade lifecycle from ingestion to completion
2. **Error Events** - Capture validation and processing errors
3. **DLQ Events** - Record messages sent to dead letter queues
4. **Queue Metrics** - Monitor Kafka queue health and lag

### 1. Trade Events

**When to send**: At every stage of trade processing (consumed, validated, enriched, persisted, published)

**What to include**: Trade ID, service name, event type/stage, queue information, offsets

**Example - Trade Consumed**:
```java
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;

@KafkaListener(topics = "pms.validation.in", groupId = "pms-validation-cg")
public void consumeTrade(Trade trade, Acknowledgment ack, 
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {
    // Send trade event - consumed
    rttmClient.sendTradeEvent(TradeEventPayload.builder()
        .tradeId(trade.getTradeId())
        .serviceName("pms-validation")
        .eventType(EventType.TRADE_RECEIVED)
        .eventStage(EventStage.CONSUME)
        .eventStatus("RECEIVED")
        .sourceQueue("pms.ingestion.out")
        .targetQueue("pms.validation.in")
        .topicName("pms.validation.in")
        .consumerGroup("pms-validation-cg")
        .partitionId(partition)
        .offsetValue(offset)
        .message("Trade received for validation")
        .build());
    
    // Process trade...
}
```

**Example - Trade Validated Successfully**:
```java
public void validateTrade(Trade trade) {
    try {
        // Validation logic...
        ValidationResult result = validator.validate(trade);
        
        if (result.isValid()) {
            // Send trade event - validated
            rttmClient.sendTradeEvent(TradeEventPayload.builder()
                .tradeId(trade.getTradeId())
                .serviceName("pms-validation")
                .eventType(EventType.TRADE_VALIDATED)
                .eventStage(EventStage.VALIDATED)
                .eventStatus("OK")
                .sourceQueue("pms.validation.in")
                .targetQueue("pms.enrichment.in")
                .topicName("pms.enrichment.in")
                .message("Trade validation successful")
                .build());
        }
    } catch (Exception e) {
        // Handle error (see Error Events section)
    }
}
```

**Example - Trade Enriched**:
```java
rttmClient.sendTradeEvent(TradeEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-enrichment")
    .eventType(EventType.TRADE_ENRICHED)
    .eventStage(EventStage.ENRICHED)
    .eventStatus("ENRICHED")
    .sourceQueue("pms.enrichment.in")
    .targetQueue("pms.persistence.in")
    .message("Trade enriched with reference data")
    .build());
```

**Example - Trade Persisted**:
```java
rttmClient.sendTradeEvent(TradeEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-persistence")
    .eventType(EventType.TRADE_PERSISTED)
    .eventStage(EventStage.PERSISTED)
    .eventStatus("SAVED")
    .message("Trade saved to database")
    .build());
```

### 2. Error Events

**When to send**: When validation fails, business rule violations occur, or processing errors happen

**What to include**: Trade ID, service name, error type, error message, event stage where error occurred

**Example - Validation Error**:
```java
rttmClient.sendErrorEvent(ErrorEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-validation")
    .errorType("VALIDATION_ERROR")
    .errorMessage("Validation failed: Missing required field 'notional'")
    .eventStage(EventStage.VALIDATED)
    .build());
```

**Example - Business Rule Violation**:
```java
rttmClient.sendErrorEvent(ErrorEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-validation")
    .errorType("BUSINESS_RULE_VIOLATION")
    .errorMessage("Notional amount exceeds maximum limit: 10000000 > 5000000")
    .eventStage(EventStage.VALIDATED)
    .build());
```

**Example - Enrichment Failure**:
```java
rttmClient.sendErrorEvent(ErrorEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-enrichment")
    .errorType("REFERENCE_DATA_NOT_FOUND")
    .errorMessage("Failed to find reference data for counterparty: CP-12345")
    .eventStage(EventStage.ENRICH)
    .build());
```

**Example - Database Error**:
```java
rttmClient.sendErrorEvent(ErrorEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-persistence")
    .errorType("DATABASE_ERROR")
    .errorMessage("Failed to persist trade: Connection timeout")
    .eventStage(EventStage.PERSIST)
    .build());
```

### 3. DLQ Events

**When to send**: When a message cannot be processed and is sent to a dead letter queue

**What to include**: Trade ID, service name, original topic, DLQ topic, reason for DLQ, event stage

**Example - Deserialization Error**:
```java
rttmClient.sendDlqEvent(DlqEventPayload.builder()
    .tradeId("TR-20250101-0001")
    .serviceName("pms-validation")
    .topicName("pms.validation.dlq")
    .originalTopic("pms.validation.in")
    .reason("Deserialization error: Unexpected character at position 15")
    .eventStage(EventStage.CONSUME)
    .build());
```

**Example - Max Retry Exceeded**:
```java
rttmClient.sendDlqEvent(DlqEventPayload.builder()
    .tradeId(trade.getTradeId())
    .serviceName("pms-enrichment")
    .topicName("pms.enrichment.in.dlq")
    .originalTopic("pms.enrichment.in")
    .reason("Max retry attempts exceeded (3 attempts)")
    .eventStage(EventStage.ENRICH)
    .build());
```

**Example - Poison Pill Message**:
```java
rttmClient.sendDlqEvent(DlqEventPayload.builder()
    .tradeId("TR-UNKNOWN")
    .serviceName("pms-validation")
    .topicName("pms.validation.dlq")
    .originalTopic("pms.validation.in")
    .reason("Poison pill detected - invalid message format")
    .eventStage(EventStage.CONSUME)
    .build());
```

### 4. Queue Metrics

**When to send**: Periodically (every 30 seconds recommended) via scheduled task

**What to include**: Service name, topic name, partition ID, produced offset, consumed offset, consumer group

**Example - Scheduled Metrics Publishing**:
```java
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class QueueMetricsService {

    private final RttmClient rttmClient;
    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<String, ?> consumerFactory;

    /**
     * Publishes queue metrics every 30 seconds for all active Kafka listeners.
     */
    @Scheduled(fixedRate = 30000)
    public void publishQueueMetrics() {
        log.debug("Publishing queue metrics snapshot");
        
        registry.getListenerContainers().forEach(container -> {
            try {
                Collection<TopicPartition> assignedPartitions = container.getAssignedPartitions();
                
                if (assignedPartitions != null && !assignedPartitions.isEmpty()) {
                    Map<String, Object> consumerProps = consumerFactory.getConfigurationProperties();
                    String consumerGroup = (String) consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG);
                    
                    // Create a consumer to fetch offsets
                    try (Consumer<?, ?> consumer = consumerFactory.createConsumer(consumerGroup, "", "-metrics")) {
                        assignedPartitions.forEach(tp -> {
                            try {
                                // Get end offsets (produced offset)
                                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(List.of(tp));
                                Long producedOffset = endOffsets.get(tp);
                                
                                // Get committed offset (consumed offset)
                                Long consumedOffset = consumer.committed(Set.of(tp)).get(tp).offset();
                                
                                // Send queue metric
                                rttmClient.sendQueueMetric(QueueMetricPayload.builder()
                                    .serviceName("pms-validation")
                                    .topicName(tp.topic())
                                    .partitionId(tp.partition())
                                    .producedOffset(producedOffset != null ? producedOffset : 0L)
                                    .consumedOffset(consumedOffset != null ? consumedOffset : 0L)
                                    .consumerGroup(consumerGroup)
                                    .build());
                                    
                                log.debug("Published metric for topic={}, partition={}, lag={}", 
                                    tp.topic(), tp.partition(), 
                                    (producedOffset != null ? producedOffset : 0L) - 
                                    (consumedOffset != null ? consumedOffset : 0L));
                            } catch (Exception e) {
                                log.error("Failed to publish metric for {}", tp, e);
                            }
                        });
                    }
                }
            } catch (Exception e) {
                log.error("Error publishing metrics for container {}", 
                    container.getListenerId(), e);
            }
        });
    }
}
```

**Configuration Requirements**:
```java
@Configuration
@EnableScheduling
public class SchedulingConfig {
    // @EnableScheduling enables the @Scheduled annotation
}
```

### Key Points
- **EventType** and **EventStage** enums provide type-safe event classification
- All long text fields (message/reason/errorMessage) are auto-truncated to 1000 chars
- **eventTime** and **snapshotTime** default to `System.currentTimeMillis()` unless explicitly set
- The client uses tradeId or serviceName as Kafka keys for stable partitioning
- Queue metrics should be sent every 30 seconds via scheduler
- Always send error events before DLQ events for complete tracking

## Consuming Data from Kafka

To consume RTTM events (e.g., queue metrics) from Kafka topics, use Spring Kafka's `@KafkaListener`:

### Example: Consuming Queue Metrics

```java
import com.pms.rttm.proto.RttmQueueMetric;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class QueueMetricConsumer {

    @KafkaListener(
        topics = "rttm.queue.metrics",
        groupId = "rttm-consumer-group"
    )
    public void consumeQueueMetric(RttmQueueMetric metric, Acknowledgment ack) {
        try {
            log.info("Received queue metric: service={}, topic={}, partition={}, lag={}",
                metric.getServiceName(),
                metric.getTopicName(),
                metric.getPartitionId(),
                metric.getProducedOffset() - metric.getConsumedOffset());
            
            // Process the metric (store in DB, send alert, etc.)
            processMetric(metric);
            
            // Manually acknowledge after successful processing
            if (ack != null) {
                ack.acknowledge();
            }
        } catch (Exception e) {
            log.error("Error processing queue metric: {}", metric, e);
            // Don't acknowledge on error - message will be reprocessed
        }
    }
    
    private void processMetric(RttmQueueMetric metric) {
        // Your business logic here
        // e.g., persist to database, check lag thresholds, send alerts
    }
}
```

### Consumer Configuration

```java
import com.google.protobuf.MessageLite;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}")
    private String bootstrapServers;
    
    @Value("${KAFKA_CONSUMER_GROUP_ID:rttm-consumer-group}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, MessageLite> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, 
                  "com.pms.rttm.proto.RttmQueueMetric");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageLite> 
            kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MessageLite> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
```

### Consuming Other Event Types

Similarly, create listeners for trade, error, and DLQ events:

```java
// Trade events
@KafkaListener(topics = "rttm.trade.events", groupId = "rttm-consumer-group")
public void consumeTradeEvent(RttmTradeEvent event, Acknowledgment ack) {
    log.info("Received trade event: tradeId={}, eventType={}, eventStage={}", 
        event.getTradeId(), event.getEventType(), event.getEventStage());
    // Process trade event
    ack.acknowledge();
}

// Error events
@KafkaListener(topics = "rttm.error.events", groupId = "rttm-consumer-group")
public void consumeErrorEvent(RttmErrorEvent event, Acknowledgment ack) {
    log.warn("Received error event: tradeId={}, errorType={}, message={}", 
        event.getTradeId(), event.getErrorType(), event.getErrorMessage());
    // Process error event
    ack.acknowledge();
}

// DLQ events
@KafkaListener(topics = "rttm.dlq.events", groupId = "rttm-consumer-group")
public void consumeDlqEvent(RttmDlqEvent event, Acknowledgment ack) {
    log.error("Received DLQ event: tradeId={}, originalTopic={}, reason={}", 
        event.getTradeId(), event.getOriginalTopic(), event.getReason());
    // Process DLQ event
    ack.acknowledge();
}
```

## Unit Tests
- Run all tests: `./mvnw test`
- What is covered: DTO truncation safeguards and proto round-trip conversion in `ProtoConverter`.
- Add more coverage: mock `KafkaTemplate` or use `spring-kafka-test` embedded broker for send/ack paths; verify retry/backoff configuration where applicable.

## Manual End-to-End Test with `pms-validation`
1. **Start dependencies**
   - Kafka broker reachable at `kafka:29092` (Docker) or `localhost:9092` (local).
   - `pms-rttm` service running and pointing to the same Kafka cluster.
2. **Configure `pms-validation`**
   - Include the dependency above.
   - Set `rttm.client.mode=kafka`.
   - Set `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` (Docker) or `localhost:9092` (local).
   - Ensure topics match `pms-rttm`: `rttm.trade.events`, `rttm.dlq.events`, `rttm.queue.metrics`, `rttm.error.events`.
3. **Trigger events from `pms-validation`**
   - Send a trade through the normal flow to emit `TradeEventPayload` (expected on `rttm.trade.events`).
   - Force a validation failure to emit `ErrorEventPayload` and optionally `DlqEventPayload`.
   - Wait for scheduled queue metric emission (every 30s) or manually trigger to publish to `rttm.queue.metrics`.
4. **Observe results**
   - Use `kafka-console-consumer` or Spring `@KafkaListener` to read each topic and confirm payload fields.
   - Verify message truncation (long message/reason/errorMessage capped at 1000 chars) and timestamps present.
   - Check queue metric lag calculation (producedOffset - consumedOffset).
5. **Negative scenarios**
   - Bring Kafka down and confirm the client surfaces `RttmClientException` within the configured timeout.
   - Verify retry logic with `max-attempts` and `backoff-ms` configuration.

## Minimal Manual Test Matrix
- Happy path trade event → message appears on `rttm.trade.events`.
- Error event from validation failure → message appears on `rttm.error.events` with `errorType` and truncated message.
- DLQ event when processing fails → message appears on `rttm.dlq.events` with `originalTopic`.
- Scheduled queue metric (every 30s) → offsets and partition published to `rttm.queue.metrics`.
- Consumer receives and processes queue metrics → verify lag calculation and acknowledgment.

## Troubleshooting
- **Connection issues**: Verify `KAFKA_BOOTSTRAP_SERVERS` is set to `kafka:29092` (Docker) or `localhost:9092` (local).
- **Missing protobuf serializer**: Ensure `io.confluent:kafka-protobuf-serializer` is on the runtime classpath.
- **Serialization errors**: Compare payloads against proto contracts under `src/main/proto` and confirm required fields are set.
- **Timeouts**: Raise `send-timeout-ms` and ensure broker address is reachable; check `acks` configuration for sync sends.
- **Scheduler not running**: Verify `@EnableScheduling` is added to your Spring Boot configuration class.
- **Consumer not receiving messages**: Check consumer group ID, topic names, and deserializer configuration.