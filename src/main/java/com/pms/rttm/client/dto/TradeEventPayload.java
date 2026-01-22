package com.pms.rttm.client.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Data;

/**
 * DTO for Trade Event payload matching RttmTradeEvent protobuf message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeEventPayload {

    @NonNull
    private String tradeId;

    @NonNull
    private String serviceName;

    private String eventType;
    private String eventStage;
    private String eventStatus;
    private String sourceQueue;
    private String targetQueue;
    private String topicName;
    private String consumerGroup;
    private Integer partitionId;
    private Long offsetValue;

    @Builder.Default
    private long eventTime = System.currentTimeMillis();

    private String message;

    // Keep message truncation behavior similar to previous implementation.
    public void setMessage(String message) {
        if (message != null && message.length() > 1000) {
            this.message = message.substring(0, 1000);
        } else {
            this.message = message;
        }
    }
}
