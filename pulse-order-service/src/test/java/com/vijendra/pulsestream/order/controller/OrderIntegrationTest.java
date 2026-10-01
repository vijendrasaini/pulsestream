package com.vijendra.pulsestream.order.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.order.config.KafkaTopicConfig;
import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka
public class OrderIntegrationTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;
    
    @Autowired EmbeddedKafkaBroker kafkaBroker;

    private Consumer<String, String> testConsumer;

    @BeforeEach
    void setup() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("test-group", "true", kafkaBroker);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);

        testConsumer = consumerFactory.createConsumer();
        testConsumer.subscribe(Collections.singletonList(KafkaTopicConfig.ORDER_EVENTS_TOPIC));
    }

    @AfterEach
    void tearDown() {
        if(testConsumer != null) {
            testConsumer.close();
        }
    }

    @Test
    void createOrder_shouldCreateOrderAndPublishEventToKafka() throws Exception {
        //Arrange
        String customerId = ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12);
        CreateOrderRequest request = new CreateOrderRequest(
                customerId,
                "zero-to-one",
                1,
                BigDecimal.valueOf(100)
        );

        String body = objectMapper.writeValueAsString(request);

        //Act
        String responseJson = mockMvc.perform(post("/api/v1/orders")
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body)
        )
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").exists())
                .andReturn().getResponse().getContentAsString();

        //Assert
        ConsumerRecord<String, String> receivedRecord = KafkaTestUtils.getSingleRecord(testConsumer, KafkaTopicConfig.ORDER_EVENTS_TOPIC, Duration.ofSeconds(5));
        assertThat(receivedRecord).isNotNull();
        assertThat(receivedRecord.key()).isNotNull();
        assertThat(receivedRecord.value()).isNotNull();
        assertThat(receivedRecord.value()).contains("zero-to-one");
        assertThat(receivedRecord.value()).contains(customerId);
    }
}
