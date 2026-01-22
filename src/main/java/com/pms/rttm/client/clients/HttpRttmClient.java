package com.pms.rttm.client.clients;

import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.exception.RttmClientException;
import com.pms.rttm.client.util.ProtoConverter;
import com.pms.rttm.client.config.RttmClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.CompletableFuture;

/**
 * HTTP-based implementation of RttmClient for services without direct Kafka
 * access. Posts protobuf-encoded events to RTTM HTTP ingestion endpoints.
 */
public class HttpRttmClient implements RttmClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpRttmClient.class);

    private final RestTemplate restTemplate;
    private final RttmClientConfig config;

    public HttpRttmClient(RestTemplate restTemplate, RttmClientConfig config) {
        this.restTemplate = restTemplate;
        this.config = config;
    }

    private HttpHeaders createHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        if (config.getHttpApiKey() != null) {
            headers.set("X-API-Key", config.getHttpApiKey());
        }
        return headers;
    }

    @Override
    public void sendTradeEvent(TradeEventPayload payload) throws RttmClientException {
        try {
            byte[] protoBytes = ProtoConverter.toProto(payload).toByteArray();
            HttpEntity<byte[]> request = new HttpEntity<>(protoBytes, createHeaders());
            String url = config.getHttpEndpointBaseUrl() + "/api/rttm/trade-events";
            restTemplate.postForEntity(url, request, Void.class);
            logger.debug("Sent trade event via HTTP for tradeId: {}", payload.getTradeId());
        } catch (Exception e) {
            logger.error("Failed to send trade event via HTTP for tradeId: {}", payload.getTradeId(), e);
            throw new RttmClientException("Failed to send trade event via HTTP", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendTradeEventAsync(TradeEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                sendTradeEvent(payload);
            } catch (RttmClientException e) {
                logger.error("Error in sendTradeEventAsync via HTTP", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void sendDlqEvent(DlqEventPayload payload) throws RttmClientException {
        try {
            byte[] protoBytes = ProtoConverter.toProto(payload).toByteArray();
            HttpEntity<byte[]> request = new HttpEntity<>(protoBytes, createHeaders());
            String url = config.getHttpEndpointBaseUrl() + "/api/rttm/dlq-events";
            restTemplate.postForEntity(url, request, Void.class);
            logger.debug("Sent DLQ event via HTTP for tradeId: {}", payload.getTradeId());
        } catch (Exception e) {
            logger.error("Failed to send DLQ event via HTTP for tradeId: {}", payload.getTradeId(), e);
            throw new RttmClientException("Failed to send DLQ event via HTTP", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendDlqEventAsync(DlqEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                sendDlqEvent(payload);
            } catch (RttmClientException e) {
                logger.error("Error in sendDlqEventAsync via HTTP", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void sendQueueMetric(QueueMetricPayload payload) throws RttmClientException {
        try {
            byte[] protoBytes = ProtoConverter.toProto(payload).toByteArray();
            HttpEntity<byte[]> request = new HttpEntity<>(protoBytes, createHeaders());
            String url = config.getHttpEndpointBaseUrl() + "/api/rttm/queue-metrics";
            restTemplate.postForEntity(url, request, Void.class);
            logger.debug("Sent queue metric via HTTP for service: {}", payload.getServiceName());
        } catch (Exception e) {
            logger.error("Failed to send queue metric via HTTP for service: {}", payload.getServiceName(), e);
            throw new RttmClientException("Failed to send queue metric via HTTP", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendQueueMetricAsync(QueueMetricPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                sendQueueMetric(payload);
            } catch (RttmClientException e) {
                logger.error("Error in sendQueueMetricAsync via HTTP", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void sendErrorEvent(ErrorEventPayload payload) throws RttmClientException {
        try {
            byte[] protoBytes = ProtoConverter.toProto(payload).toByteArray();
            HttpEntity<byte[]> request = new HttpEntity<>(protoBytes, createHeaders());
            String url = config.getHttpEndpointBaseUrl() + "/api/rttm/error-events";
            restTemplate.postForEntity(url, request, Void.class);
            logger.debug("Sent error event via HTTP for service: {}", payload.getServiceName());
        } catch (Exception e) {
            logger.error("Failed to send error event via HTTP for service: {}", payload.getServiceName(), e);
            throw new RttmClientException("Failed to send error event via HTTP", e);
        }
    }

    @Override
    public CompletableFuture<Void> sendErrorEventAsync(ErrorEventPayload payload) {
        return CompletableFuture.runAsync(() -> {
            try {
                sendErrorEvent(payload);
            } catch (RttmClientException e) {
                logger.error("Error in sendErrorEventAsync via HTTP", e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() {
        logger.info("Closing HttpRttmClient");
        // RestTemplate doesn't need explicit closing
    }

    // Builder for easy instantiation
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private RestTemplate restTemplate;
        private RttmClientConfig config;

        public Builder restTemplate(RestTemplate restTemplate) {
            this.restTemplate = restTemplate;
            return this;
        }

        public Builder config(RttmClientConfig config) {
            this.config = config;
            return this;
        }

        public HttpRttmClient build() {
            if (restTemplate == null) {
                restTemplate = new RestTemplate();
            }
            if (config == null) {
                throw new IllegalStateException("Config is required");
            }
            return new HttpRttmClient(restTemplate, config);
        }
    }
}
