package com.pms.rttm.client.clients;

import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;

import lombok.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * No-operation implementation of RttmClient for development and testing.
 * Logs all method calls at DEBUG level and returns successful futures.
 */
@Service
@Builder
@ConditionalOnProperty(name = "rttm.client.mode", havingValue = "noop")
public class NoopRttmClient implements RttmClient {

    private static final Logger logger = LoggerFactory.getLogger(NoopRttmClient.class);

    @Override
    public void sendTradeEvent(TradeEventPayload payload) {
        logger.debug("NoopRttmClient.sendTradeEvent called for tradeId: {}", payload.getTradeId());
    }

    @Override
    public CompletableFuture<Void> sendTradeEventAsync(TradeEventPayload payload) {
        logger.debug("NoopRttmClient.sendTradeEventAsync called for tradeId: {}", payload.getTradeId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void sendDlqEvent(DlqEventPayload payload) {
        logger.debug("NoopRttmClient.sendDlqEvent called for tradeId: {}", payload.getTradeId());
    }

    @Override
    public CompletableFuture<Void> sendDlqEventAsync(DlqEventPayload payload) {
        logger.debug("NoopRttmClient.sendDlqEventAsync called for tradeId: {}", payload.getTradeId());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void sendQueueMetric(QueueMetricPayload payload) {
        logger.debug("NoopRttmClient.sendQueueMetric called for service: {}, topic: {}",
                payload.getServiceName(), payload.getTopicName());
    }

    @Override
    public CompletableFuture<Void> sendQueueMetricAsync(QueueMetricPayload payload) {
        logger.debug("NoopRttmClient.sendQueueMetricAsync called for service: {}, topic: {}",
                payload.getServiceName(), payload.getTopicName());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void sendErrorEvent(ErrorEventPayload payload) {
        logger.debug("NoopRttmClient.sendErrorEvent called for service: {}, errorType: {}",
                payload.getServiceName(), payload.getErrorType());
    }

    @Override
    public CompletableFuture<Void> sendErrorEventAsync(ErrorEventPayload payload) {
        logger.debug("NoopRttmClient.sendErrorEventAsync called for service: {}, errorType: {}",
                payload.getServiceName(), payload.getErrorType());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
        logger.debug("NoopRttmClient.close called");
    }
}
