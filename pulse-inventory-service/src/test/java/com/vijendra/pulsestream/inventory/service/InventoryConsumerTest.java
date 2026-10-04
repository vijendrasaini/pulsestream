package com.vijendra.pulsestream.inventory.service;

import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.inventory.config.KafkaTopicConfig;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@Testcontainers
public class InventoryConsumerTest {
    @Container
    public static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @DynamicPropertySource
    public static void overrideBootstrapServer(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
    }

    static BlockingQueue<EventEnvelope<?>> eventEnvelopeQueue = new LinkedBlockingQueue<>();
    static BlockingQueue<EventEnvelope<?>> dltQueue = new LinkedBlockingQueue<>();

    @BeforeEach
    void setUp() {
        eventEnvelopeQueue.clear();
    }

    @KafkaListener(topics = KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, groupId = "inventory-service-test-group")
    void inventoryTopicListener(EventEnvelope<?> eventEnvelope) {
        eventEnvelopeQueue.add(eventEnvelope);
    }

    @KafkaListener(topics = "order-events-dlt", groupId = "inventory-dlt-test-group")
    void inventoryDLTListener(EventEnvelope<?> eventEnvelope) {
        dltQueue.add(eventEnvelope);
    }


    @Test
    public void processOrderCreated_shouldPublishInventoryReserved() throws InterruptedException {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);
        String customerId = ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12);
        String productName = "Hy lux-120";
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                orderId,
                customerId,
                new BigDecimal("2000.00"),
                productName,
                1,
                Instant.now()
        );

        EventEnvelope<OrderCreatedEvent> orderCreated = EventEnvelope.of(
                "ORDER_CREATED",
                orderId,
                orderCreatedEvent
        );

        kafkaTemplate.send("order-events", orderId, orderCreated);

        EventEnvelope<?> envelope = eventEnvelopeQueue.poll(5, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();
        assertThat(envelope.aggregateId()).contains(orderId);

        assertThat(envelope.eventType()).isEqualTo("INVENTORY_RESERVED");
    }


    @Test
    public void processOrderCreated_shouldPublishInventoryFailed() throws InterruptedException {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);
        String customerId = ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12);
        String productName = "Hy lux-120";
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                orderId,
                customerId,
                new BigDecimal("2000.00"),
                productName,
                1000,
                Instant.now()
        );

        EventEnvelope<OrderCreatedEvent> orderCreated = EventEnvelope.of(
                "ORDER_CREATED",
                orderId,
                orderCreatedEvent
        );

        kafkaTemplate.send("order-events", orderId, orderCreated);

        EventEnvelope<?> envelope = eventEnvelopeQueue.poll(5, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();
        assertThat(envelope.payload()).asString().contains(orderId);
        assertThat(envelope.payload()).asString().contains(productName);

        assertThat(envelope.eventType()).isEqualTo("INVENTORY_FAILED");
    }

    @Test
    public void processOrderCreated_shouldGoTODLTHandler_whenEventIsPoisionPill() throws InterruptedException {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);
        String customerId = ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12);
        String itemSku = "poison-pill";
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                orderId,
                customerId,
                new BigDecimal("2000.00"),
                itemSku,
                1000,
                Instant.now()
        );

        EventEnvelope<OrderCreatedEvent> orderCreated = EventEnvelope.of(
                "ORDER_CREATED",
                orderId,
                orderCreatedEvent
        );

        kafkaTemplate.send("order-events", orderId, orderCreated);

//        try {
//            Thread.sleep(5000); // observe default retry logic and listener logs
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        EventEnvelope<?> envelope = dltQueue.poll(7, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();
    }
}