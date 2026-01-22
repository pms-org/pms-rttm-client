package com.pms.rttm.client.config;

import com.google.protobuf.MessageLite;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PmsProtobufProducer {

    private final KafkaTemplate<String, MessageLite> kafkaTemplate;

    public void sendEvent(String topic, MessageLite event) {
        kafkaTemplate.send(topic, event);
    }

    public void sendEvent(String topic, String key, MessageLite event) {
        kafkaTemplate.send(topic, key, event);
    }
}