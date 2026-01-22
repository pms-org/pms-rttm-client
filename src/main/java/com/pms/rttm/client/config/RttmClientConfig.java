package com.pms.rttm.client.config;

/**
 * Configuration class for RttmClient instances.
 * Supports Kafka, HTTP, and Noop modes.
 */
public class RttmClientConfig {

    // Mode selection
    private String mode = "kafka"; // kafka | http | noop

    // Kafka configuration
    private String kafkaBootstrapServers;
    private String kafkaTopicTradeEvents = "rttm.trade.events";
    private String kafkaTopicDlqEvents = "rttm.dlq.events";
    private String kafkaTopicQueueMetrics = "rttm.queue.metrics";
    private String kafkaTopicErrorEvents = "rttm.error.events";

    // HTTP configuration
    private String httpEndpointBaseUrl;
    private String httpApiKey;

    // Timeout and retry configuration
    private int sendTimeoutMs = 3000;
    private int retryMaxAttempts = 3;
    private int retryBackoffMs = 100;

    // Constructors
    public RttmClientConfig() {
    }

    // Getters and Setters
    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaTopicTradeEvents() {
        return kafkaTopicTradeEvents;
    }

    public void setKafkaTopicTradeEvents(String kafkaTopicTradeEvents) {
        this.kafkaTopicTradeEvents = kafkaTopicTradeEvents;
    }

    public String getKafkaTopicDlqEvents() {
        return kafkaTopicDlqEvents;
    }

    public void setKafkaTopicDlqEvents(String kafkaTopicDlqEvents) {
        this.kafkaTopicDlqEvents = kafkaTopicDlqEvents;
    }

    public String getKafkaTopicQueueMetrics() {
        return kafkaTopicQueueMetrics;
    }

    public void setKafkaTopicQueueMetrics(String kafkaTopicQueueMetrics) {
        this.kafkaTopicQueueMetrics = kafkaTopicQueueMetrics;
    }

    public String getKafkaTopicErrorEvents() {
        return kafkaTopicErrorEvents;
    }

    public void setKafkaTopicErrorEvents(String kafkaTopicErrorEvents) {
        this.kafkaTopicErrorEvents = kafkaTopicErrorEvents;
    }

    public String getHttpEndpointBaseUrl() {
        return httpEndpointBaseUrl;
    }

    public void setHttpEndpointBaseUrl(String httpEndpointBaseUrl) {
        this.httpEndpointBaseUrl = httpEndpointBaseUrl;
    }

    public String getHttpApiKey() {
        return httpApiKey;
    }

    public void setHttpApiKey(String httpApiKey) {
        this.httpApiKey = httpApiKey;
    }

    public int getSendTimeoutMs() {
        return sendTimeoutMs;
    }

    public void setSendTimeoutMs(int sendTimeoutMs) {
        this.sendTimeoutMs = sendTimeoutMs;
    }

    public int getRetryMaxAttempts() {
        return retryMaxAttempts;
    }

    public void setRetryMaxAttempts(int retryMaxAttempts) {
        this.retryMaxAttempts = retryMaxAttempts;
    }

    public int getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(int retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    // Builder pattern
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RttmClientConfig config = new RttmClientConfig();

        public Builder mode(String mode) {
            config.mode = mode;
            return this;
        }

        public Builder kafkaBootstrapServers(String servers) {
            config.kafkaBootstrapServers = servers;
            return this;
        }

        public Builder kafkaTopicTradeEvents(String topic) {
            config.kafkaTopicTradeEvents = topic;
            return this;
        }

        public Builder kafkaTopicDlqEvents(String topic) {
            config.kafkaTopicDlqEvents = topic;
            return this;
        }

        public Builder kafkaTopicQueueMetrics(String topic) {
            config.kafkaTopicQueueMetrics = topic;
            return this;
        }

        public Builder kafkaTopicErrorEvents(String topic) {
            config.kafkaTopicErrorEvents = topic;
            return this;
        }

        public Builder httpEndpointBaseUrl(String url) {
            config.httpEndpointBaseUrl = url;
            return this;
        }

        public Builder httpApiKey(String apiKey) {
            config.httpApiKey = apiKey;
            return this;
        }

        public Builder sendTimeoutMs(int timeout) {
            config.sendTimeoutMs = timeout;
            return this;
        }

        public Builder retryMaxAttempts(int attempts) {
            config.retryMaxAttempts = attempts;
            return this;
        }

        public Builder retryBackoffMs(int backoff) {
            config.retryBackoffMs = backoff;
            return this;
        }

        public RttmClientConfig build() {
            return config;
        }
    }
}
