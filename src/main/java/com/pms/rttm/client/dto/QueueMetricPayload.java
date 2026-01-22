package com.pms.rttm.client.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

/**
 * DTO for Queue Metric payload matching RttmQueueMetric protobuf message.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueueMetricPayload {

    @NonNull
    private String serviceName;

    @NonNull
    private String topicName;

    private Integer partitionId;
    private Long producedOffset;
    private Long consumedOffset;
    private String consumerGroup;

    @Builder.Default
    private long snapshotTime = System.currentTimeMillis();
}
