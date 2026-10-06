package com.vijendra.pulsestream.order.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.InventoryFailedEvent;
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
@AllArgsConstructor
@Slf4j
public class OrderService {
    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    private static final String INVENTORY_FAILED = "INVENTORY_FAILED";

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

    @Transactional
    @KafkaListener(topics = "inventory-events")
    public void inventoryEventsListner(EventEnvelope<?> envelope) {
        if(!envelope.eventType().equals(INVENTORY_FAILED)) {
            return;
        }

        InventoryFailedEvent failedEvent = objectMapper.convertValue(envelope.payload(), InventoryFailedEvent.class);
        String orderId = failedEvent.orderId();

        log.warn("INVENTORY_FAILED received for order: {}. Initiating compensating cancellation action...", orderId);

        Optional<OrderEntity> orderEntity = orderRepository.findById(orderId);
        if(orderEntity.isEmpty()) {
            log.error("Order not found!");
            throw new RuntimeException("Order does not exist!");
        }

        OrderEntity order = orderEntity.get();
        if(order.getStatus().equals(OrderStatus.CANCELLED)) {
            log.warn("Order is already cancelled");
            return;
        }

        order.setStatus(OrderStatus.CANCELLED);
        orderRepository.save(order);
    }
}
