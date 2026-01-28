package com.pms.rttm.client.controller;

import com.pms.rttm.client.clients.RttmClient;
import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;
import com.pms.rttm.client.exception.RttmClientException;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Test controller for sending events to Kafka topics.
 * Provides endpoints to test all RTTM event types.
 */
@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestController {

    private static final Logger logger = LoggerFactory.getLogger(TestController.class);

    private final RttmClient rttmClient;

    /**
     * Send a test trade event (synchronous)
     */
    @PostMapping("/trade-event")
    public ResponseEntity<Map<String, String>> sendTradeEvent(@RequestBody TradeEventPayload payload) {
        try {
            rttmClient.sendTradeEvent(payload);
            logger.info("Successfully sent trade event: {}", payload.getTradeId());
            return ResponseEntity.ok(createSuccessResponse("Trade event sent successfully", payload.getTradeId()));
        } catch (RttmClientException e) {
            logger.error("Failed to send trade event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to send trade event", e.getMessage()));
        }
    }

    /**
     * Send a test trade event (asynchronous)
     */
    @PostMapping("/trade-event-async")
    public ResponseEntity<Map<String, String>> sendTradeEventAsync(@RequestBody TradeEventPayload payload) {
        try {
            rttmClient.sendTradeEventAsync(payload);
            logger.info("Successfully queued async trade event: {}", payload.getTradeId());
            return ResponseEntity.ok(createSuccessResponse("Trade event queued for async send", payload.getTradeId()));
        } catch (Exception e) {
            logger.error("Failed to queue async trade event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to queue async trade event", e.getMessage()));
        }
    }

    /**
     * Send a test DLQ event (synchronous)
     */
    @PostMapping("/dlq-event")
    public ResponseEntity<Map<String, String>> sendDlqEvent(@RequestBody DlqEventPayload payload) {
        try {
            rttmClient.sendDlqEvent(payload);
            logger.info("Successfully sent DLQ event: {}", payload.getTradeId());
            return ResponseEntity.ok(createSuccessResponse("DLQ event sent successfully", payload.getTradeId()));
        } catch (RttmClientException e) {
            logger.error("Failed to send DLQ event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to send DLQ event", e.getMessage()));
        }
    }

    /**
     * Send a test DLQ event (asynchronous)
     */
    @PostMapping("/dlq-event-async")
    public ResponseEntity<Map<String, String>> sendDlqEventAsync(@RequestBody DlqEventPayload payload) {
        try {
            rttmClient.sendDlqEventAsync(payload);
            logger.info("Successfully queued async DLQ event: {}", payload.getTradeId());
            return ResponseEntity.ok(createSuccessResponse("DLQ event queued for async send", payload.getTradeId()));
        } catch (Exception e) {
            logger.error("Failed to queue async DLQ event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to queue async DLQ event", e.getMessage()));
        }
    }

    /**
     * Send a test error event (synchronous)
     */
    @PostMapping("/error-event")
    public ResponseEntity<Map<String, String>> sendErrorEvent(@RequestBody ErrorEventPayload payload) {
        try {
            rttmClient.sendErrorEvent(payload);
            logger.info("Successfully sent error event for service: {}", payload.getServiceName());
            return ResponseEntity.ok(createSuccessResponse("Error event sent successfully", payload.getServiceName()));
        } catch (RttmClientException e) {
            logger.error("Failed to send error event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to send error event", e.getMessage()));
        }
    }

    /**
     * Send a test error event (asynchronous)
     */
    @PostMapping("/error-event-async")
    public ResponseEntity<Map<String, String>> sendErrorEventAsync(@RequestBody ErrorEventPayload payload) {
        try {
            rttmClient.sendErrorEventAsync(payload);
            logger.info("Successfully queued async error event for service: {}", payload.getServiceName());
            return ResponseEntity
                    .ok(createSuccessResponse("Error event queued for async send", payload.getServiceName()));
        } catch (Exception e) {
            logger.error("Failed to queue async error event", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to queue async error event", e.getMessage()));
        }
    }

    /**
     * Send a test queue metric (synchronous)
     */
    @PostMapping("/queue-metric")
    public ResponseEntity<Map<String, String>> sendQueueMetric(@RequestBody QueueMetricPayload payload) {
        try {
            rttmClient.sendQueueMetric(payload);
            logger.info("Successfully sent queue metric for service: {}, topic: {}",
                    payload.getServiceName(), payload.getTopicName());
            return ResponseEntity.ok(createSuccessResponse("Queue metric sent successfully",
                    payload.getServiceName() + "-" + payload.getTopicName()));
        } catch (RttmClientException e) {
            logger.error("Failed to send queue metric", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to send queue metric", e.getMessage()));
        }
    }

    /**
     * Send a test queue metric (asynchronous)
     */
    @PostMapping("/queue-metric-async")
    public ResponseEntity<Map<String, String>> sendQueueMetricAsync(@RequestBody QueueMetricPayload payload) {
        try {
            rttmClient.sendQueueMetricAsync(payload);
            logger.info("Successfully queued async queue metric for service: {}, topic: {}",
                    payload.getServiceName(), payload.getTopicName());
            return ResponseEntity.ok(createSuccessResponse("Queue metric queued for async send",
                    payload.getServiceName() + "-" + payload.getTopicName()));
        } catch (Exception e) {
            logger.error("Failed to queue async queue metric", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(createErrorResponse("Failed to queue async queue metric", e.getMessage()));
        }
    }

    /**
     * Send multiple test events (batch testing)
     */
    @PostMapping("/batch-trade-events")
    public ResponseEntity<Map<String, Object>> sendBatchTradeEvents(
            @RequestParam(defaultValue = "5") int count,
            @RequestParam(defaultValue = "TEST-SERVICE") String serviceName) {

        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < count; i++) {
            try {
                TradeEventPayload payload = TradeEventPayload.builder()
                        .tradeId("BATCH-TRADE-" + System.currentTimeMillis() + "-" + i)
                        .serviceName(serviceName)
                        .eventType(EventType.TRADE_RECEIVED)
                        .eventStage(EventStage.RECEIVED)
                        .eventStatus("SUCCESS")
                        .topicName("pms.trade.events")
                        .message("Batch test trade event " + i)
                        .build();

                rttmClient.sendTradeEvent(payload);
                successCount++;
            } catch (Exception e) {
                logger.error("Failed to send batch trade event {}", i, e);
                failureCount++;
            }
        }

        Map<String, Object> response = new HashMap<>();
        response.put("total", count);
        response.put("success", successCount);
        response.put("failure", failureCount);
        response.put("message", "Batch operation completed");

        return ResponseEntity.ok(response);
    }

    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        Map<String, String> response = new HashMap<>();
        response.put("status", "UP");
        response.put("service", "RTTM Test Controller");
        return ResponseEntity.ok(response);
    }

    private Map<String, String> createSuccessResponse(String message, String identifier) {
        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("message", message);
        response.put("identifier", identifier);
        response.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return response;
    }

    private Map<String, String> createErrorResponse(String message, String error) {
        Map<String, String> response = new HashMap<>();
        response.put("status", "error");
        response.put("message", message);
        response.put("error", error);
        response.put("timestamp", String.valueOf(System.currentTimeMillis()));
        return response;
    }
}
