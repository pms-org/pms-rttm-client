package com.pms.rttm.client.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration class for RttmClient instances.
 * Supports Kafka, HTTP, and Noop modes.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RttmClientConfig {

    // Mode selection
    @Builder.Default
    private String mode = "kafka"; // kafka | http | noop

    // Kafka configuration
    private String kafkaBootstrapServers;

    @Builder.Default
    private String kafkaTopicTradeEvents = "rttm.trade.events";

    @Builder.Default
    private String kafkaTopicDlqEvents = "rttm.dlq.events";

    @Builder.Default
    private String kafkaTopicQueueMetrics = "rttm.queue.metrics";

    @Builder.Default
    private String kafkaTopicErrorEvents = "rttm.error.events";

    // HTTP configuration
    private String httpEndpointBaseUrl;
    private String httpApiKey;

    // Timeout and retry configuration
    @Builder.Default
    private int sendTimeoutMs = 3000;

    @Builder.Default
    private int retryMaxAttempts = 3;

    @Builder.Default
    private int retryBackoffMs = 100;
}
