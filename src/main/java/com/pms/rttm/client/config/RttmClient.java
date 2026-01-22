package com.pms.rttm.client.config;

import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.exception.RttmClientException;

import java.util.concurrent.CompletableFuture;

/**
 * Main interface for RTTM (Real-Time Telemetry & Metrics) client.
 * Provides methods to send various types of observability events to the RTTM
 * layer.
 */
public interface RttmClient {

    /**
     * Send a trade event synchronously.
     *
     * @param payload The trade event payload
     * @throws RttmClientException if sending fails
     */
    void sendTradeEvent(TradeEventPayload payload) throws RttmClientException;

    /**
     * Send a trade event asynchronously.
     *
     * @param payload The trade event payload
     * @return CompletableFuture that completes when the event is sent
     */
    CompletableFuture<Void> sendTradeEventAsync(TradeEventPayload payload);

    /**
     * Send a DLQ event synchronously.
     *
     * @param payload The DLQ event payload
     * @throws RttmClientException if sending fails
     */
    void sendDlqEvent(DlqEventPayload payload) throws RttmClientException;

    /**
     * Send a DLQ event asynchronously.
     *
     * @param payload The DLQ event payload
     * @return CompletableFuture that completes when the event is sent
     */
    CompletableFuture<Void> sendDlqEventAsync(DlqEventPayload payload);

    /**
     * Send a queue metric synchronously.
     *
     * @param payload The queue metric payload
     * @throws RttmClientException if sending fails
     */
    void sendQueueMetric(QueueMetricPayload payload) throws RttmClientException;

    /**
     * Send a queue metric asynchronously.
     *
     * @param payload The queue metric payload
     * @return CompletableFuture that completes when the metric is sent
     */
    CompletableFuture<Void> sendQueueMetricAsync(QueueMetricPayload payload);

    /**
     * Send an error event synchronously.
     *
     * @param payload The error event payload
     * @throws RttmClientException if sending fails
     */
    void sendErrorEvent(ErrorEventPayload payload) throws RttmClientException;

    /**
     * Send an error event asynchronously.
     *
     * @param payload The error event payload
     * @return CompletableFuture that completes when the event is sent
     */
    CompletableFuture<Void> sendErrorEventAsync(ErrorEventPayload payload);

    /**
     * Close the client and release resources.
     */
    void close();
}
