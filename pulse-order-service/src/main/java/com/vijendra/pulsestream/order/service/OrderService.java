package com.vijendra.pulsestream.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.order.config.KafkaTopicConfig;
import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import com.vijendra.pulsestream.order.entity.OrderEntity;
import com.vijendra.pulsestream.order.entity.OutboxEventEntity;
import com.vijendra.pulsestream.order.entity.enums.OrderStatus;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import com.vijendra.pulsestream.order.repository.OrderRepository;
import com.vijendra.pulsestream.order.repository.OutboxEventRepository;
import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Service
@AllArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        OrderEntity orderEntity = new OrderEntity();
        orderEntity.setCustomerId(request.customerId());
        orderEntity.setStatus(OrderStatus.PENDING);
        orderEntity.setTotalAmount(new BigDecimal(request.totalAmount().toString()));
        orderEntity.setItemSku(request.itemSku());
        orderEntity.setQuantity(request.requestQuantity());

        orderEntity = orderRepository.save(orderEntity);
        String orderId = orderEntity.getId();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId,
                request.customerId(),
                request.totalAmount(),
                request.itemSku(),
                request.requestQuantity(),
                Instant.now()
        );

        OutboxEventEntity outboxEventEntity = new OutboxEventEntity();
        outboxEventEntity.setStatus(OutboxStatus.PENDING);
        outboxEventEntity.setAggregateId(orderId);
        outboxEventEntity.setEventType("ORDER_CREATED");
        outboxEventEntity.setAggregateType(OrderEntity.class.getName());

        EventEnvelope<OrderCreatedEvent> envelope = EventEnvelope.of(
                "ORDER_CREATED",
                orderId,
                event
        );

        String payload;
        try {
            payload = objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize outbox event payload", e);
        }
        outboxEventEntity.setPayload(payload);

        outboxEventRepository.save(outboxEventEntity);

        return new OrderResponse(orderId, "PENDING", "Order created and published to Kafka");
    }
}
