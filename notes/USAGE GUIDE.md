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

### HTTP client (when Kafka is unavailable)
`` Sending Data to Kafklues when invoked from a service (e.g., `pms-validation`):

```java
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;

// Trade event (happy path)
rttmClient.sendTradeEvent(TradeEventPayload.builder()
        .tradeId("TR-20250101-0001")
        .serviceName("pms-validation")
        .eventType(EventType.TRADE_VALIDATED)
        .eventStage(EventStage.VALIDATED)
        .eventStatus("OK")
        .sourceQueue("pms.validation.in")
        .targetQueue("pms.validation.out")
        .topicName("rttm.trade.events")
        .consumerGroup("pms-validation-cg")
        .partitionId(0)
        .offsetValue(12345L)
        .message("Trade accepted")
        .build());

// Error event (validation failure)
rttmClient.sendErrorEvent(ErrorEventPayload.builder()
        .tradeId("TR-20250101-0002")
        .serviceName("pms-validation")
        .errorType("VALIDATION_ERROR")
        .errorMessage("Missing notional field")
        .eventStage(EventStage.VALIDATE)
        .build());

// DLQ event (processing failure)
rttmClient.sendDlqEvent(DlqEventPayload.builder()
        .tradeId("TR-20250101-0003")
        .serviceName("pms-validation")
        .topicName("rttm.dlq.events")
        .originalTopic("pms.validation.in")
        .reason("Deserialization error")
        .eventStage(EventStage.CONSUME)
        .build());

// Queue metric (snapshot)
rttmClient.sendQueueMetric(QueueMetricPayload.builder()
        .serviceName("pms-validation")
        .topicName("pms.validation.in")
        .partitionId(0)
        .producedOffset(20000L)
        .consumedOffset(19990L)
        .consumerGroup("pms-validation-cg")
        .build());
```

Notes:
- `EventType` and `EventStage` enums provide type-safe event classification (e.g., `TRADE_VALIDATED`, `ENRICHED`, `VALIDATE`, `CONSUME`).
- All long text fields (message/reason/errorMessage) are auto-truncated to 1000 chars by the DTOs.
- `eventTime`/`snapshotTime` default to `System.currentTimeMillis()` unless explicitly set.
- In Kafka mode, the client uses tradeId or serviceName as keys to keep partitioning stable.

## Consuming Data from Kafka

To consume RTTM events (e.g., queue metrics) from Kafka topics, use Spring Kafka's `@KafkaListener`:

### Example: Consuming Queue Metrics

```java
import com.pms.rttm.proto.RttmQukafka:29092` (Docker) or `localhost:9092` (local).
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
   - Verify retry logic with `max-attempts` and `backoff-ms` configuration
            log.info("Received queue metric: service={}, topic={}, partition={}, " +
                     "producedOffset={}, consumedOffset={}, lag={}",
                metric.getServiceName(),
                metric.getTopicName(),
                metric.getPartitionId(),
                metric.getProducedOffset(),
                metric.getConsumedOffset(),
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
public void consumeTradeE→ message appears on `rttm.trade.events`.
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
- **Consumer not receiving messages**: Check consumer group ID, topic names, and deserializer configuration
public void consumeDlqEvent(RttmDlqEvent event, Acknowledgment ack) {
    // Process DLQ event
}
```

## Unit tests
- Run all tests: `./mvnw test`
- What is covered: DTO truncation safeguards and proto round-trip conversion in `ProtoConverter`.
- Add more coverage: mock `KafkaTemplate` or use `spring-kafka-test` embedded broker for send/ack paths; verify retry/backoff configuration where applicable.

## Manual end-to-end test with `pms-validation`
1. **Start dependencies**
   - Kafka broker reachable at `KAFKA_BOOTSTRAP_SERVERS`.
   - `pms-rttm` service running and pointing to the same Kafka cluster (or HTTP ingestion endpoint if you choose `http` mode).
2. **Configure `pms-validation`**
   - Include the dependency above.
   - Set `rttm.client.mode=kafka` (or `http`).
### Key Points
- `EventType` and `EventStage` enums provide type-safe event classification (e.g., `TRADE_VALIDATED`, `ENRICHED`, `VALIDATE`, `CONSUME`).
- All long text fields (message/reason/errorMessage) are auto-truncated to 1000 chars by the DTOs.
- `eventTime`/`snapshotTime` default to `System.currentTimeMillis()` unless explicitly set.
- The client uses tradeId or serviceName as Kafka keys to keep partitioning stable.

### Scheduled Queue Metrics Publishing

To automatically send queue metrics every 30 seconds, use Spring's `@Scheduled` annotation. Example from `pms-validation`:

```java
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

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
                String listenerId = container.getListenerId();
                Collection<TopicPartition> assignedPartitions = container.getAssignedPartitions();
                
                if (assignedPartitions != null && !assignedPartitions.isEmpty()) {
                    Map<String, Object> consumerProps = consumerFactory.getConfigurationProperties();
                    String consumerGroup = (String) consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG);
                    
                    assignedPartitions.forEach(tp -> {
                        try {
                            // Get end offset (produced offset)
                            Long endOffset = container.getContainerProperties()
                                .getConsumerRebalanceListener() != null 
                                ? getEndOffset(tp) : null;
                            
                            // Get committed offset (consumed offset)
                            Long committedOffset = getCommittedOffset(container, tp);
                            
                            rttmClient.sendQueueMetric(QueueMetricPayload.builder()
                                .serviceName("pms-validation")
                                .topicName(tp.topic())
                                .partitionId(tp.partition())
                                .producedOffset(endOffset != null ? endOffset : 0L)
                                .consumedOffset(committedOffset != null ? committedOffset : 0L)
                                .consumerGroup(consumerGroup)
                                .build());
                                
                            log.debug("Published metric for topic={}, partition={}", 
                                tp.topic(), tp.partition());
                        } catch (Exception e) {
                            log.error("Failed to publish metric for {}", tp, e);
                        }
                    });
                }
            } catch (Exception e) {
                log.error("Error publishing metrics for container {}", 
                    container.getListenerId(), e);
            }
        });
    }
    
    private Long getEndOffset(TopicPartition tp) {
        // Implementation to fetch end offset from Kafka admin/consumer
        return null; // Replace with actual implementation
    }
    
    private Long getCommittedOffset(MessageListenerContainer container, TopicPartition tp) {
        // Implementation to fetch committed offset
        return null; // Replace with actual implementation
    }
}
```

**Configuration Requirements:**
- Enable scheduling in your Spring Boot application with `@EnableScheduling` on a configuration class
- The `fixedRate = 30000` means execution every 30 seconds (30,000 milliseconds)
- Adjust the rate based on your monitoring needs
4. **Observe results**
   - Kafka mode: use `kafka-console-consumer` (or Spring `@KafkaListener` in a test app) to read each topic and confirm payload fields.
   - HTTP mode: check `pms-rttm` ingress logs/metrics or its persistence layer for received events.
   - Verify message truncation (long message/reason/errorMessage capped at 1000 chars) and timestamps present.
5. **Negative scenarios**
   - Bring Kafka down and confirm the client surfaces `RttmClientException` within the configured timeout.
   - Provide invalid API key in HTTP mode and confirm 401/403 surfaced to the caller.

## Minimal manual test matrix
- Happy path trade event in Kafka mode → message appears on `rttm.trade.events`.
- Error event from validation failure → message appears on `rttm.error.events` with `errorType` and truncated message.
- DLQ event when processing fails → message appears on `rttm.dlq.events` with `originalTopic`.
- Queue metric snapshot → offsets and partition published to `rttm.queue.metrics`.
- HTTP mode smoke test → POST succeeds (2xx) and RTTM receives payload.
- No-op mode → methods return without throwing; only debug logs emitted.

## Troubleshooting
- Missing protobuf serializer: ensure `io.confluent:kafka-protobuf-serializer` is on the runtime classpath.
- Serialization errors: compare payloads against proto contracts under `src/main/proto` and confirm required fields are set.
- Timeouts: raise `send-timeout-ms` and ensure broker address is reachable; check `acks` configuration for sync sends.
- HTTP failures: confirm `base-url` and `X-API-Key` headers; inspect server logs for validation errors.
