package com.vijendra.pulsestream.payment.service;

import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


import javax.print.Doc;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class PaymentConsumerIntegrationTest {
    @Container
    static KafkaContainer testcontainers = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    @DynamicPropertySource
    static void overrideKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", testcontainers::getBootstrapServers);
    }

    BlockingQueue<EventEnvelope<?>> paymentEventQueue = new LinkedBlockingQueue<>();

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach()
    public void setUp() {
        paymentEventQueue.clear();
    }

    @KafkaListener(topics = "payment-events", groupId = "payment-service-test-group")
    public void spyListner(EventEnvelope<?> envelope) {
        paymentEventQueue.add(envelope);
    }

    @Test
    public void processOrderCreated_shouldListenToOrderCreatedEvent_WhenAmountIsValid() throws InterruptedException {
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

        EventEnvelope<?> envelope = paymentEventQueue.poll(5, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();
        assertThat(envelope.eventType()).isEqualTo("PAYMENT_COMPLETED");
        assertThat(envelope.aggregateId()).isEqualTo(orderId);
    }

    @Test
    public void processOrderCreated_shouldListenToOrderCreatedEvent_WhenAmountIsInSufficient() throws InterruptedException {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);
        String customerId = ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12);
        String productName = "Hy lux-120";
        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent(
                orderId,
                customerId,
                new BigDecimal("10000.00"),
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

        EventEnvelope<?> envelope = paymentEventQueue.poll(5, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();
        assertThat(envelope.eventType()).isEqualTo("PAYMENT_FAILED");
        assertThat(envelope.aggregateId()).isEqualTo(orderId);
    }
}
