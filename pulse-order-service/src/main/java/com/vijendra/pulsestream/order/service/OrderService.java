package com.vijendra.pulsestream.order.service;

import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.order.config.KafkaTopicConfig;
import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderService {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    public OrderService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public OrderResponse createOrder(CreateOrderRequest request) {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                request.customerId(),
                request.totalAmount(),
                request.itemSku(),
                request.requestQuantity(),
                Instant.now()
        );

        EventEnvelope<OrderCreatedEvent> envelope = EventEnvelope.of(
                "ORDER_CREATED",
                orderId,
                event
        );

        kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_TOPIC, orderId, envelope);

        return new OrderResponse(orderId, "PENDING", "Order created and published to Kafka");
    }
}
