package com.vijendra.pulsestream.payment.service;

import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.InventoryFailedEvent;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.payment.entity.PaymentEntity;
import com.vijendra.pulsestream.payment.entity.ProcessedEventEntity;
import com.vijendra.pulsestream.payment.entity.enums.PaymentStatus;
import com.vijendra.pulsestream.payment.repository.PaymentRepository;
import com.vijendra.pulsestream.payment.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;


import javax.print.Doc;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
public class PaymentConsumerIntegrationTest {
    @Autowired private PaymentRepository paymentRepository;
    @Autowired private ProcessedEventRepository processedEventRepository;

    @Container
    static KafkaContainer testcontainers = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.0"));

    @Container
    static MySQLContainer<?> mySQLContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withTmpFs(java.util.Map.of("/var/lib/mysql", "rw"))
            .withCommand(
                    "--performance_schema=OFF",
                    "--innodb_doublewrite=0",
                    "--innodb_flush_log_at_trx_commit=0"
            );

    @DynamicPropertySource
    static void overrideKafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", testcontainers::getBootstrapServers);

        registry.add("spring.datasource.url", mySQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", mySQLContainer::getUsername);
        registry.add("spring.datasource.password", mySQLContainer::getPassword);
    }

    private static final BlockingQueue<EventEnvelope<?>> paymentEventQueue = new LinkedBlockingQueue<>();
    private static final BlockingQueue<EventEnvelope<?>> inventoryEventQueue = new LinkedBlockingQueue<>();

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @BeforeEach()
    public void setUp() {
        paymentEventQueue.clear();
        inventoryEventQueue.clear();
    }

    @KafkaListener(topics = "payment-events", groupId = "payment-service-test-group")
    public void spyListner(EventEnvelope<?> envelope) {
        paymentEventQueue.add(envelope);
    }

    @KafkaListener(topics = "inventory-events", groupId = "payment-service-test-group")
    public void inventoryListner(EventEnvelope<?> envelope) {
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

    @Test
    public void processOrderCreated_shouldProcessOrderOnce_WhenEventIsPostedTwice() throws InterruptedException {
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

        String eventId = orderCreated.eventId();

        // Duplicate Events
        kafkaTemplate.send("order-events", orderId, orderCreated);
        kafkaTemplate.send("order-events", orderId, orderCreated);

        EventEnvelope<?> envelope = paymentEventQueue.poll(5, TimeUnit.SECONDS);
        assertThat(envelope).isNotNull();

        EventEnvelope<?> envelope2 = paymentEventQueue.poll(2, TimeUnit.SECONDS);
        assertThat(envelope2).isNull();

        List<PaymentEntity> paymentsList = paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.COMPLETED);
        assertThat(paymentsList).hasSize(1);

        Optional<ProcessedEventEntity> processedEvent = processedEventRepository.findById(eventId);
        assertThat(processedEvent).isNotEmpty();
    }

    @Test
    public void processInventoryEvents_shouldRefundPayment_whenInventoryFailedReceived() throws InterruptedException {
        // Arrange:
        String orderId = UUID.randomUUID().toString();
        PaymentEntity payment = new PaymentEntity();
        payment.setOrderId(orderId);
        payment.setAmount(BigDecimal.valueOf(100.50));
        payment.setStatus(PaymentStatus.COMPLETED);
        payment = paymentRepository.save(payment);

        InventoryFailedEvent inventoryFailedEvent = new InventoryFailedEvent(
                orderId,
                "test-item-sku",
                10,
                "OUT_OF_STOCK",
                Instant.now()
        );

        EventEnvelope<InventoryFailedEvent> envelope = EventEnvelope.of(
                "INVENTORY_FAILED",
                orderId,
                inventoryFailedEvent
        );

        // ACT
        kafkaTemplate.send("inventory-events", orderId, envelope);

        // Assert
        EventEnvelope<?> eventEnvelope = inventoryEventQueue.poll(5, TimeUnit.SECONDS);
        Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<PaymentEntity> refundedPayments = paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.REFUNDED);
                    assertThat(refundedPayments).hasSize(1);
                });
    }
}
