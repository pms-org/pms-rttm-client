package com.pms.rttm.client.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * DTO for DLQ Event payload matching RttmDlqEvent protobuf message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqEventPayload {

    @NonNull
    private String tradeId;

    @NonNull
    private String serviceName;

    private String topicName;
    private String originalTopic;

    @Builder.Default
    private long eventTime = System.currentTimeMillis();

    private String reason;
    private String eventStage;

    public void setReason(String reason) {
        if (reason != null && reason.length() > 1000) {
            this.reason = reason.substring(0, 1000);
        } else {
            this.reason = reason;
        }
    }
}
