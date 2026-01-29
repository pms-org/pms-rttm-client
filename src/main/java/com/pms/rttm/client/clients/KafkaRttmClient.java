package com.pms.rttm.client.clients;

import com.google.protobuf.MessageLite;
import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.exception.RttmClientException;
import com.pms.rttm.client.util.ProtoConverter;
import com.pms.rttm.client.config.RttmClientConfig;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Kafka-based implementation of RttmClient.
 * Publishes events to configured Kafka topics using protobuf serialization.
 */
@Service
@Builder
@RequiredArgsConstructor
public class KafkaRttmClient implements RttmClient {

    private static final Logger logger = LoggerFactory.getLogger(KafkaRttmClient.class);

    private final KafkaTemplate<String, MessageLite> kafkaTemplate;
    private final RttmClientConfig config;

    @Override
    public void sendTradeEvent(TradeEventPayload payload) throws RttmClientException {
        try {
            MessageLite proto = ProtoConverter.toProto(payload);
            String key = payload.getTradeId();
            kafkaTemplate.send(config.getKafkaTopicTradeEvents(), key, proto)
                    .get(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.debug("Sent trade event for tradeId: {}", payload.getTradeId());
        } catch (Exception e) {
            logger.error("Failed to send trade event for tradeId: {}", payload.getTradeId(), e);
            throw new RttmClientException("Failed to send trade event", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendTradeEventAsync(TradeEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                MessageLite proto = ProtoConverter.toProto(payload);
                String key = payload.getTradeId();
                CompletableFuture<SendResult<String, MessageLite>> sendFuture = kafkaTemplate
                        .send(config.getKafkaTopicTradeEvents(), key, proto);

                sendFuture.orTimeout(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                logger.error("Failed to send trade event async for tradeId: {}", payload.getTradeId(),
                                        ex);
                            } else {
                                logger.debug("Sent trade event async for tradeId: {}", payload.getTradeId());
                            }
                        });
            } catch (Exception e) {
                logger.error("Error in sendTradeEventAsync for tradeId: {}", payload.getTradeId(), e);
                throw new RuntimeException("Failed to send trade event async", e);
            }
        });
    }

    @Override
    public void sendDlqEvent(DlqEventPayload payload) throws RttmClientException {
        try {
            MessageLite proto = ProtoConverter.toProto(payload);
            String key = payload.getTradeId();
            kafkaTemplate.send(config.getKafkaTopicDlqEvents(), key, proto)
                    .get(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.debug("Sent DLQ event for tradeId: {}", payload.getTradeId());
        } catch (Exception e) {
            logger.error("Failed to send DLQ event for tradeId: {}", payload.getTradeId(), e);
            throw new RttmClientException("Failed to send DLQ event", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendDlqEventAsync(DlqEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                MessageLite proto = ProtoConverter.toProto(payload);
                String key = payload.getTradeId();
                CompletableFuture<SendResult<String, MessageLite>> sendFuture = kafkaTemplate
                        .send(config.getKafkaTopicDlqEvents(), key, proto);

                sendFuture.orTimeout(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                logger.error("Failed to send DLQ event async for tradeId: {}", payload.getTradeId(),
                                        ex);
                            } else {
                                logger.debug("Sent DLQ event async for tradeId: {}", payload.getTradeId());
                            }
                        });
            } catch (Exception e) {
                logger.error("Error in sendDlqEventAsync for tradeId: {}", payload.getTradeId(), e);
                throw new RuntimeException("Failed to send DLQ event async", e);
            }
        });
    }

    @Override
    public void sendQueueMetric(QueueMetricPayload payload) throws RttmClientException {
        try {
            MessageLite proto = ProtoConverter.toProto(payload);
            String key = payload.getServiceName() + "-" + payload.getTopicName();
            kafkaTemplate.send(config.getKafkaTopicQueueMetrics(), key, proto)
                    .get(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.debug("Sent queue metric for service: {}, topic: {}", payload.getServiceName(),
                    payload.getTopicName());
        } catch (Exception e) {
            logger.error("Failed to send queue metric for service: {}", payload.getServiceName(), e);
            throw new RttmClientException("Failed to send queue metric", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendQueueMetricAsync(QueueMetricPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                MessageLite proto = ProtoConverter.toProto(payload);
                String key = payload.getServiceName() + "-" + payload.getTopicName();
                CompletableFuture<SendResult<String, MessageLite>> sendFuture = kafkaTemplate
                        .send(config.getKafkaTopicQueueMetrics(), key, proto);

                sendFuture.orTimeout(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                logger.error("Failed to send queue metric async for service: {}",
                                        payload.getServiceName(), ex);
                            } else {
                                logger.debug("Sent queue metric async for service: {}, topic: {}",
                                        payload.getServiceName(), payload.getTopicName());
                            }
                        });
            } catch (Exception e) {
                logger.error("Error in sendQueueMetricAsync for service: {}", payload.getServiceName(), e);
                throw new RuntimeException("Failed to send queue metric async", e);
            }
        });
    }

    @Override
    public void sendErrorEvent(ErrorEventPayload payload) throws RttmClientException {
        try {
            MessageLite proto = ProtoConverter.toProto(payload);
            String key = payload.getServiceName();
            kafkaTemplate.send(config.getKafkaTopicErrorEvents(), key, proto)
                    .get(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS);
            logger.debug("Sent error event for service: {}, errorType: {}", payload.getServiceName(),
                    payload.getErrorType());
        } catch (Exception e) {
            logger.error("Failed to send error event for service: {}", payload.getServiceName(), e);
            throw new RttmClientException("Failed to send error event", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendErrorEventAsync(ErrorEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                MessageLite proto = ProtoConverter.toProto(payload);
                String key = payload.getServiceName();
                CompletableFuture<SendResult<String, MessageLite>> sendFuture = kafkaTemplate
                        .send(config.getKafkaTopicErrorEvents(), key, proto);

                sendFuture.orTimeout(config.getSendTimeoutMs(), TimeUnit.MILLISECONDS)
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                logger.error("Failed to send error event async for service: {}",
                                        payload.getServiceName(), ex);
                            } else {
                                logger.debug("Sent error event async for service: {}, errorType: {}",
                                        payload.getServiceName(), payload.getErrorType());
                            }
                        });
            } catch (Exception e) {
                logger.error("Error in sendErrorEventAsync for service: {}", payload.getServiceName(), e);
                throw new RuntimeException("Failed to send error event async", e);
            }
        });
    }

    @Override
    public void close() {
        logger.info("Closing KafkaRttmClient");
        // KafkaTemplate doesn't need explicit closing as it's managed by Spring
    }
}
