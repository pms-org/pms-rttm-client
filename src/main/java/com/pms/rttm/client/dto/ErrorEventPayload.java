package com.pms.rttm.client.dto;

import com.pms.rttm.client.enums.EventStage;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * DTO for Error Event payload matching RttmErrorEvent protobuf message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorEventPayload {

    private String tradeId;

    @NonNull
    private String serviceName;

    @NonNull
    private String errorType;

    private String errorMessage;

    private EventStage eventStage;

    @Builder.Default
    private Long eventTime = System.currentTimeMillis();

    public void setErrorMessage(String errorMessage) {
        if (errorMessage != null && errorMessage.length() > 1000) {
            this.errorMessage = errorMessage.substring(0, 1000);
        } else {
            this.errorMessage = errorMessage;
        }
    }
}
