package com.pms.rttm.client.enums;

/**
 * Enum representing the lifecycle stage of an event.
 */
public enum EventStage {
    RECEIVED, // trade accepted by ingress / API
    VALIDATED, // schema + business validation passed
    ENRICHED, // reference data / pricing enrichment done
    COMMITTED, // persisted to core system / ledger
    ANALYZED, // downstream analytics / risk / reporting
    CONSUME, // message consumed from queue
    VALIDATE // validation in progress
}
