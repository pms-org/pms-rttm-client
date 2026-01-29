package com.pms.rttm.client.util;

import com.pms.rttm.client.dto.DlqEventPayload;
import com.pms.rttm.client.dto.ErrorEventPayload;
import com.pms.rttm.client.dto.TradeEventPayload;
import com.pms.rttm.client.enums.EventStage;
import com.pms.rttm.client.enums.EventType;
import com.pms.rttm.proto.RttmTradeEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProtoConverterTest {

    @Test
    void tradeEventRoundTripRetainsFields() {
        TradeEventPayload payload = TradeEventPayload.builder()
                .tradeId("T-123")
                .serviceName("validation-service")
                .eventType(EventType.TRADE_VALIDATED)
                .eventStage(EventStage.VALIDATED)
                .eventStatus("OK")
                .sourceQueue("inbound")
                .targetQueue("processed")
                .topicName("rttm.trade.events")
                .consumerGroup("validation-consumer")
                .partitionId(2)
                .offsetValue(42L)
                .eventTime(1720000000L)
                .message("payload accepted")
                .build();

        RttmTradeEvent proto = ProtoConverter.toProto(payload);
        TradeEventPayload converted = ProtoConverter.fromProto(proto);

        assertEquals(payload.getTradeId(), converted.getTradeId());
        assertEquals(payload.getServiceName(), converted.getServiceName());
        assertEquals(payload.getEventType(), converted.getEventType());
        assertEquals(payload.getEventStage(), converted.getEventStage());
        assertEquals(payload.getEventStatus(), converted.getEventStatus());
        assertEquals(payload.getSourceQueue(), converted.getSourceQueue());
        assertEquals(payload.getTargetQueue(), converted.getTargetQueue());
        assertEquals(payload.getTopicName(), converted.getTopicName());
        assertEquals(payload.getConsumerGroup(), converted.getConsumerGroup());
        assertEquals(payload.getPartitionId(), converted.getPartitionId());
        assertEquals(payload.getOffsetValue(), converted.getOffsetValue());
        assertEquals(payload.getEventTime(), converted.getEventTime());
        assertEquals(payload.getMessage(), converted.getMessage());
    }

    @Test
    void tradeEventMessageIsTruncatedAt1000Chars() {
        String longMessage = "x".repeat(1500);
        TradeEventPayload payload = new TradeEventPayload();
        payload.setTradeId("T-456");
        payload.setServiceName("validation-service");
        payload.setMessage(longMessage);

        assertEquals(1000, payload.getMessage().length());
    }

    @Test
    void dlqReasonIsTruncatedAt1000Chars() {
        String longReason = "y".repeat(1200);
        DlqEventPayload payload = new DlqEventPayload();
        payload.setTradeId("T-789");
        payload.setServiceName("validation-service");
        payload.setReason(longReason);

        assertEquals(1000, payload.getReason().length());
    }

    @Test
    void errorMessageIsTruncatedAt1000Chars() {
        String longMessage = "z".repeat(1300);
        ErrorEventPayload payload = new ErrorEventPayload();
        payload.setServiceName("validation-service");
        payload.setErrorType("VALIDATION");
        payload.setErrorMessage(longMessage);

        assertEquals(1000, payload.getErrorMessage().length());
    }

    @Test
    void toProtoAcceptsNonNullFieldsOnly() {
        TradeEventPayload payload = new TradeEventPayload();
        payload.setTradeId("T-001");
        payload.setServiceName("validation-service");
        payload.setEventTime(1700000000L);

        RttmTradeEvent proto = ProtoConverter.toProto(payload);

        assertNotNull(proto.getTradeId());
        assertNotNull(proto.getServiceName());
        assertEquals(1700000000L, proto.getEventTime());
    }

    @Test
    void enumConversionHandlesEventTypeCorrectly() {
        TradeEventPayload payload = TradeEventPayload.builder()
                .tradeId("T-enum-test")
                .serviceName("test-service")
                .eventType(EventType.TRADE_ENRICHED)
                .eventStage(EventStage.ENRICHED)
                .build();

        RttmTradeEvent proto = ProtoConverter.toProto(payload);

        assertEquals("TRADE_ENRICHED", proto.getEventType());
        assertEquals("ENRICHED", proto.getEventStage());
    }

    @Test
    void enumConversionHandlesEventStageInDlqEvent() {
        DlqEventPayload payload = DlqEventPayload.builder()
                .tradeId("T-dlq-enum")
                .serviceName("test-service")
                .eventStage(EventStage.RECEIVED)
                .build();

        var proto = ProtoConverter.toProto(payload);

        assertEquals("RECEIVED", proto.getEventStage());
    }

    @Test
    void enumConversionHandlesEventStageInErrorEvent() {
        ErrorEventPayload payload = ErrorEventPayload.builder()
                .serviceName("test-service")
                .errorType("VALIDATION")
                .eventStage(EventStage.VALIDATED)
                .build();

        var proto = ProtoConverter.toProto(payload);

        assertEquals("VALIDATED", proto.getEventStage());
    }
}
