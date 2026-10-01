package com.vijendra.pulsestream.payment.service;

import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PaymentService {
    Logger log = LoggerFactory.getLogger(PaymentService.class);
    @KafkaListener(topics = "order-events", groupId = "payment-service-group")
    public void processOrderCreated(EventEnvelope<OrderCreatedEvent> envelope) {
        log.info("Envelope : eventId:{}, timestamp:{}, eventType:{}, payload:{}", envelope.eventId(), envelope.timestamp(), envelope.eventType(), envelope.payload());
        OrderCreatedEvent payload = envelope.payload();
    }
}
