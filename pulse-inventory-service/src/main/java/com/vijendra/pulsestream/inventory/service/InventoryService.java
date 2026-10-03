package com.vijendra.pulsestream.inventory.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.InventoryFailedEvent;
import com.vijendra.pulsestream.common.event.InventoryReservedEvent;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.common.event.PaymentCompletedEvent;
import com.vijendra.pulsestream.inventory.config.KafkaTopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class InventoryService {
    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);
    private final ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order-events")
    void processOrderCreated(EventEnvelope<?> eventEnvelope) {
        log.info("Event Object: {}", eventEnvelope);

        OrderCreatedEvent payload = objectMapper.convertValue(eventEnvelope.payload(), OrderCreatedEvent.class);
        log.info("Event Payload: {}", payload);

        if(payload.quantity() > 5) {
            log.info("Insufficient stock.");

            InventoryFailedEvent failedEvent = new InventoryFailedEvent(
                    payload.orderId(),
                    payload.itemSku(),
                    payload.quantity(),
                    "OUT_OF_STOCK",
                    Instant.now()
            );

            EventEnvelope<InventoryFailedEvent> failedEnvelope = EventEnvelope.of(
                    "INVENTORY_FAILED",
                    payload.orderId(),
                    failedEvent
            );

            log.info("published event : {}", failedEnvelope);
            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload.orderId(), failedEnvelope);
            return;
        }

        String reservationId = ("RES-" + UUID.randomUUID()).substring(0, 12);
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                reservationId,
                payload.orderId(),
                payload.itemSku(),
                payload.quantity(),
                Instant.now()
        );

        EventEnvelope<InventoryReservedEvent> reservedEnvelope = EventEnvelope.of(
                "INVENTORY_RESERVED ",
                payload.orderId(),
                reservedEvent
        );

        log.info("published event : {}", reservedEnvelope);
        kafkaTemplate.send(KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload.orderId(), reservedEnvelope);
    }
}
