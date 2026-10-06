package com.vijendra.pulsestream.order.service;


import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.InventoryFailedEvent;
import com.vijendra.pulsestream.common.event.InventoryReservedEvent;
import com.vijendra.pulsestream.common.event.PaymentCompletedEvent;
import com.vijendra.pulsestream.common.event.PaymentFailedEvent;
import com.vijendra.pulsestream.order.entity.OrderEntity;
import com.vijendra.pulsestream.order.entity.enums.OrderStatus;
import com.vijendra.pulsestream.order.repository.OrderRepository;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.*;

@SpringBootTest
@Testcontainers
public class OrderSagaIntegrationTest{
    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"));

    @Container
    static MySQLContainer<?> mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withTmpFs(java.util.Map.of("/var/lib/mysql", "rw"))
            .withCommand(
                    "--performance_schema=OFF",
                    "--innodb_doublewrite=0",
                    "--innodb_flush_log_at_trx_commit=0"
            );

    @Autowired private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired private OrderRepository orderRepository;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);

        registry.add("spring.datasource.url", mySQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", mySQLContainer::getUsername);
        registry.add("spring.datasource.password", mySQLContainer::getPassword);
    }

    @Test
    void inventoryEventsListener_shouldCancelOrder_whenInventoryFailedIsReceived() {
        // 1. Arrange
        OrderEntity order = new OrderEntity();
        order.setCustomerId("customer-12383isdjf");
        order.setItemSku("test-sku");
        order.setQuantity(2);
        order.setTotalAmount(new BigDecimal("80.00"));
        order.setStatus(OrderStatus.PENDING);
        order = orderRepository.save(order);

        String orderId = order.getId();
        InventoryFailedEvent failedEvent = new InventoryFailedEvent(
                orderId,
                "test-sku",
                2,
                "OUT_OF_STOCK",
                Instant.now()
        );
        EventEnvelope<InventoryFailedEvent> envelope = EventEnvelope.of(
                "INVENTORY_FAILED",
                orderId,
                failedEvent
        );

        // 2. Act
        kafkaTemplate.send("inventory-events", orderId, envelope);

        // 3. Assert
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Optional<OrderEntity> found = orderRepository.findById(orderId);
                    assertThat(found).isPresent();
                    assertThat(found.get().getStatus()).isEqualTo(OrderStatus.CANCELLED);
                });
    }

    @Test
    void paymentEventsListener_shouldCancelOrder_whenPaymentFailedIsReceived() {
        // 1. Arrange
        OrderEntity order = new OrderEntity();
        order.setCustomerId("customer-12383isdjf");
        order.setItemSku("test-sku");
        order.setQuantity(2);
        order.setTotalAmount(new BigDecimal("80.00"));
        order.setStatus(OrderStatus.PENDING);
        order = orderRepository.save(order);

        String orderId = order.getId();
        PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                orderId,
                "INSUFFICIENT_FUNDS",
                Instant.now()
        );
        EventEnvelope<PaymentFailedEvent> envelope = EventEnvelope.of(
                "PAYMENT_FAILED",
                orderId,
                failedEvent
        );

        // 2. Act
        kafkaTemplate.send("payment-events", orderId, envelope);

        // 3. Assert
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Optional<OrderEntity> found = orderRepository.findById(orderId);
                    assertThat(found).isPresent();
                    assertThat(found.get().getStatus()).isEqualTo(OrderStatus.CANCELLED);
                });
    }

    @Test
    void shouldConfirmOrder_whenBothPaymentAndInventorySucceed() {
        // 1. Arrange
        OrderEntity order = new OrderEntity();
        order.setCustomerId("customer-12383isdjf");
        order.setItemSku("test-sku");
        order.setQuantity(2);
        order.setTotalAmount(new BigDecimal("80.00"));
        order.setStatus(OrderStatus.PENDING);
        order = orderRepository.save(order);

        String orderId = order.getId();

        InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent(
                UUID.randomUUID().toString(),
                orderId,
                "test-sku",
                1,
                Instant.now()
        );

        EventEnvelope<InventoryReservedEvent> inventoryEnvelope = EventEnvelope.of(
                "INVENTORY_RESERVED",
                orderId,
                inventoryReservedEvent
        );

        PaymentCompletedEvent paymentCompletedEvent = new PaymentCompletedEvent(
                UUID.randomUUID().toString(),
                orderId,
                BigDecimal.valueOf(100.00),
                Instant.now()
        );

        EventEnvelope<PaymentCompletedEvent> paymentEnvelope = EventEnvelope.of(
                "PAYMENT_COMPLETED",
                orderId,
                paymentCompletedEvent
        );

        //2. Act
        kafkaTemplate.send("inventory-events", orderId, inventoryEnvelope);
        kafkaTemplate.send("payment-events", orderId, paymentEnvelope);

        //3. Assert
        await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    Optional<OrderEntity> found = orderRepository.findById(orderId);
                    assertThat(found).isPresent();
                    assertThat(found.get().getStatus()).isEqualTo(OrderStatus.CONFIRMED);
                });
    }
}