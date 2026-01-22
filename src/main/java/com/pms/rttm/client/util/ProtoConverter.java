package com.pms.rttm.client.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.QueueMetricPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.proto.RttmDlqEvent;
import com.pms.rttm.proto.RttmErrorEvent;
import com.pms.rttm.proto.RttmQueueMetric;
import com.pms.rttm.proto.RttmTradeEvent;
import lombok.experimental.UtilityClass;

/**
 * Utility class for converting between DTOs and Protobuf messages.
 */
@UtilityClass
public class ProtoConverter {

    // TradeEvent conversions
    public static RttmTradeEvent toProto(TradeEventPayload payload) {
        RttmTradeEvent.Builder builder = RttmTradeEvent.newBuilder();

        if (payload.getTradeId() != null) {
            builder.setTradeId(payload.getTradeId());
        }
        if (payload.getServiceName() != null) {
            builder.setServiceName(payload.getServiceName());
        }
        if (payload.getEventType() != null) {
            builder.setEventType(payload.getEventType());
        }
        if (payload.getEventStage() != null) {
            builder.setEventStage(payload.getEventStage());
        }
        if (payload.getEventStatus() != null) {
            builder.setEventStatus(payload.getEventStatus());
        }
        if (payload.getSourceQueue() != null) {
            builder.setSourceQueue(payload.getSourceQueue());
        }
        if (payload.getTargetQueue() != null) {
            builder.setTargetQueue(payload.getTargetQueue());
        }
        if (payload.getTopicName() != null) {
            builder.setTopicName(payload.getTopicName());
        }
        if (payload.getConsumerGroup() != null) {
            builder.setConsumerGroup(payload.getConsumerGroup());
        }
        if (payload.getPartitionId() != null) {
            builder.setPartitionId(payload.getPartitionId());
        }
        if (payload.getOffsetValue() != null) {
            builder.setOffsetValue(payload.getOffsetValue());
        }
        builder.setEventTime(payload.getEventTime());
        if (payload.getMessage() != null) {
            builder.setMessage(payload.getMessage());
        }

        return builder.build();
    }

    public static TradeEventPayload fromProto(RttmTradeEvent proto) {
        return TradeEventPayload.builder()
                .tradeId(proto.getTradeId())
                .serviceName(proto.getServiceName())
                .eventType(proto.getEventType())
                .eventStage(proto.getEventStage())
                .eventStatus(proto.getEventStatus())
                .sourceQueue(proto.getSourceQueue())
                .targetQueue(proto.getTargetQueue())
                .topicName(proto.getTopicName())
                .consumerGroup(proto.getConsumerGroup())
                .partitionId(proto.getPartitionId())
                .offsetValue(proto.getOffsetValue())
                .eventTime(proto.getEventTime())
                .message(proto.getMessage())
                .build();
    }

    public static TradeEventPayload fromProtoBytes(byte[] bytes) throws InvalidProtocolBufferException {
        RttmTradeEvent proto = RttmTradeEvent.parseFrom(bytes);
        return fromProto(proto);
    }

    // DlqEvent conversions
    public static RttmDlqEvent toProto(DlqEventPayload payload) {
        RttmDlqEvent.Builder builder = RttmDlqEvent.newBuilder();

        if (payload.getTradeId() != null) {
            builder.setTradeId(payload.getTradeId());
        }
        if (payload.getServiceName() != null) {
            builder.setServiceName(payload.getServiceName());
        }
        if (payload.getTopicName() != null) {
            builder.setTopicName(payload.getTopicName());
        }
        if (payload.getOriginalTopic() != null) {
            builder.setOriginalTopic(payload.getOriginalTopic());
        }
        if (payload.getReason() != null) {
            builder.setReason(payload.getReason());
        }
        if (payload.getEventStage() != null) {
            builder.setEventStage(payload.getEventStage());
        }
        builder.setEventTime(payload.getEventTime());

        return builder.build();
    }

    public static DlqEventPayload fromProto(RttmDlqEvent proto) {
        return DlqEventPayload.builder()
                .tradeId(proto.getTradeId())
                .serviceName(proto.getServiceName())
                .topicName(proto.getTopicName())
                .originalTopic(proto.getOriginalTopic())
                .reason(proto.getReason())
                .eventStage(proto.getEventStage())
                .eventTime(proto.getEventTime())
                .build();
    }

    public static DlqEventPayload fromProtoBytesForDlqEvent(byte[] bytes)
            throws InvalidProtocolBufferException {
        RttmDlqEvent proto = RttmDlqEvent.parseFrom(bytes);
        return fromProto(proto);
    }

    // QueueMetric conversions
    public static RttmQueueMetric toProto(QueueMetricPayload payload) {
        RttmQueueMetric.Builder builder = RttmQueueMetric.newBuilder();

        if (payload.getServiceName() != null) {
            builder.setServiceName(payload.getServiceName());
        }
        if (payload.getTopicName() != null) {
            builder.setTopicName(payload.getTopicName());
        }
        if (payload.getPartitionId() != null) {
            builder.setPartitionId(payload.getPartitionId());
        }
        if (payload.getProducedOffset() != null) {
            builder.setProducedOffset(payload.getProducedOffset());
        }
        if (payload.getConsumedOffset() != null) {
            builder.setConsumedOffset(payload.getConsumedOffset());
        }
        if (payload.getConsumerGroup() != null) {
            builder.setConsumerGroup(payload.getConsumerGroup());
        }
        builder.setSnapshotTime(payload.getSnapshotTime());

        return builder.build();
    }

    public static QueueMetricPayload fromProto(RttmQueueMetric proto) {
        return QueueMetricPayload.builder()
                .serviceName(proto.getServiceName())
                .topicName(proto.getTopicName())
                .partitionId(proto.getPartitionId())
                .producedOffset(proto.getProducedOffset())
                .consumedOffset(proto.getConsumedOffset())
                .consumerGroup(proto.getConsumerGroup())
                .snapshotTime(proto.getSnapshotTime())
                .build();
    }

    public static QueueMetricPayload fromProtoBytesForQueueMetric(byte[] bytes)
            throws InvalidProtocolBufferException {
        RttmQueueMetric proto = RttmQueueMetric.parseFrom(bytes);
        return fromProto(proto);
    }

    // ErrorEvent conversions
    public static RttmErrorEvent toProto(ErrorEventPayload payload) {
        RttmErrorEvent.Builder builder = RttmErrorEvent.newBuilder();

        if (payload.getTradeId() != null) {
            builder.setTradeId(payload.getTradeId());
        }
        if (payload.getServiceName() != null) {
            builder.setServiceName(payload.getServiceName());
        }
        if (payload.getErrorType() != null) {
            builder.setErrorType(payload.getErrorType());
        }
        if (payload.getErrorMessage() != null) {
            builder.setErrorMessage(payload.getErrorMessage());
        }
        if (payload.getEventStage() != null) {
            builder.setEventStage(payload.getEventStage());
        }
        builder.setEventTime(payload.getEventTime());

        return builder.build();
    }

    public static ErrorEventPayload fromProto(RttmErrorEvent proto) {
        return ErrorEventPayload.builder()
                .tradeId(proto.getTradeId())
                .serviceName(proto.getServiceName())
                .errorType(proto.getErrorType())
                .errorMessage(proto.getErrorMessage())
                .eventStage(proto.getEventStage())
                .eventTime(proto.getEventTime())
                .build();
    }

    public static ErrorEventPayload fromProtoBytesForErrorEvent(byte[] bytes)
            throws InvalidProtocolBufferException {
        RttmErrorEvent proto = RttmErrorEvent.parseFrom(bytes);
        return fromProto(proto);
    }
}
