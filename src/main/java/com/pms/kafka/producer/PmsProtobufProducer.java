package com.pms.kafka.producer;

import com.google.protobuf.MessageLite;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PmsProtobufProducer {
    
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public PmsProtobufProducer(KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendEvent(String topic, MessageLite event) {
        kafkaTemplate.send(topic, event.toByteArray());
    }

    public void sendEvent(String topic, String key, MessageLite event) {
        kafkaTemplate.send(topic, key, event.toByteArray());
    }
}