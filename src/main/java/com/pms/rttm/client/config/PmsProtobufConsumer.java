package com.pms.rttm.client.config;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PmsProtobufConsumer {

    private static final Logger log = LoggerFactory.getLogger(PmsProtobufConsumer.class);

    protected <T> T parseMessage(byte[] data, Parser<T> parser) throws InvalidProtocolBufferException {
        return parser.parseFrom(data);
    }

    protected void handleParsingError(String topic, Exception e) {
        log.error("Failed to parse Protobuf message from topic: {}", topic, e);
    }
}
