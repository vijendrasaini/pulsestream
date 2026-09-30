package com.vijendra.pulsestream.order.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import com.vijendra.pulsestream.order.service.OrderService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderController.class)
class OrderControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private OrderService orderService;

    @Test
    void createOrder_shouldCreateOrderSuccessfully() throws Exception {
        //Arrange
        CreateOrderRequest request = new CreateOrderRequest(
                ("CUSTOMER-" + UUID.randomUUID()).substring(0, 12),
                "zero-to-one",
                1,
                BigDecimal.valueOf(100)
        );

        OrderResponse expectedResponse = new OrderResponse("ORD-12345", "PENDING", "Order created successfully");
        Mockito.when(orderService.createOrder(any()))
                .thenReturn(expectedResponse);


        //Act
        String body = objectMapper.writeValueAsString(request);
        String response = mockMvc.perform(
                post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content(body)
        )
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.orderId").exists())
                .andReturn()
                .getResponse()
                .getContentAsString();

        //Assert
    }
}