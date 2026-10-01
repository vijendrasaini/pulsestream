package com.vijendra.pulsestream.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {
    private final ObjectMapper objectMapper;
    Logger log = LoggerFactory.getLogger(PaymentService.class);
    public PaymentService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order-events", groupId = "payment-service-group")
    public void processOrderCreated(EventEnvelope<?> envelope) {
        OrderCreatedEvent payload = objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class);
        log.info("Envelope : eventId:{}, timestamp:{}, eventType:{}, payload:{}", envelope.eventId(), envelope.timestamp(), envelope.eventType(), payload);
    }
}
