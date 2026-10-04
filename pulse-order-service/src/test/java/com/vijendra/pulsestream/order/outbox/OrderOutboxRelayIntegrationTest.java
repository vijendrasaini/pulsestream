package com.vijendra.pulsestream.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import com.vijendra.pulsestream.order.entity.OrderEntity;
import com.vijendra.pulsestream.order.entity.OutboxEventEntity;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import com.vijendra.pulsestream.order.repository.OrderRepository;
import com.vijendra.pulsestream.order.repository.OutboxEventRepository;
import junit.framework.TestCase;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@Testcontainers
@AutoConfigureMockMvc
public class OrderOutboxRelayIntegrationTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

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

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);

        registry.add("spring.datasource.url", mySQLContainer::getJdbcUrl);
        registry.add("spring.datasource.username", mySQLContainer::getUsername);
        registry.add("spring.datasource.password", mySQLContainer::getPassword);
    }

    static BlockingQueue<EventEnvelope<?>> blockingQueue = new LinkedBlockingQueue<>();

    @BeforeEach
    void setUp() {
        blockingQueue.clear();
    }

    @KafkaListener(topics = "order-events", groupId = "order-service-group-test")
    void processCreatedOrderLister(EventEnvelope<?> envelope) {
        blockingQueue.add(envelope);
    }

    @Test
    void shouldPersistOrderAndRelayOutboxEvent_whenValidOrderIsCreated() throws Exception {

        //Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12),
                "zero-to-one",
                1,
                BigDecimal.valueOf(100)
        );

        String response = mockMvc.perform(
                MockMvcRequestBuilders.post("/api/v1/orders")
                        .accept(MediaType.APPLICATION_JSON)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request))
        ).andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();
        ;

        OrderResponse orderResponse = objectMapper.readValue(response, OrderResponse.class);
        String orderId = orderResponse.orderId();

        EventEnvelope<?> eventEnvelope = blockingQueue.poll(5, TimeUnit.SECONDS);
        assertThat(eventEnvelope).isNotNull();
        assertThat(eventEnvelope.aggregateId()).isEqualTo(orderId);

        Optional<OrderEntity> foundOrder = orderRepository.findById(orderId);
        assertThat(foundOrder).isNotEmpty();

        Optional<OutboxEventEntity> outboxEventEntity = outboxEventRepository.findByAggregateIdAndAggregateTypeAndStatus(orderId, OrderEntity.class.getName(), OutboxStatus.PUBLISHED);
        assertThat(outboxEventEntity).isNotEmpty();
    }
}