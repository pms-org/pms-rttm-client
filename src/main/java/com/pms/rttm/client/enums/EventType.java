package com.pms.rttm.client.enums;

/**
 * Enum representing the type of event that occurred.
 */
public enum EventType {
    TRADE_RECEIVED, // initial trade message
    TRADE_VALIDATED, // validation successful
    TRADE_ENRICHED, // enrichment completed
    TRADE_COMMITTED, // committed to system of record
    TRADE_ANALYZED, // analytics completed
    TRADE_REJECTED, // validation failed
    TRADE_FAILED, // processing failure (non-DLQ)
    TRADE_SENT_TO_DLQ // explicitly routed to DLQ
}
