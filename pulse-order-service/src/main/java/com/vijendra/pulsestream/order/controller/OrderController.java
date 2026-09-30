package com.vijendra.pulsestream.order.controller;

import com.vijendra.pulsestream.order.dto.CreateOrderRequest;
import com.vijendra.pulsestream.order.dto.OrderResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {
    @PostMapping
    ResponseEntity<OrderResponse> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = ("ORD-" + UUID.randomUUID()).substring(0, 12);
        String status = "PENDING";
        String message = "Order created successfully";
        return ResponseEntity.status(HttpStatus.CREATED).body(new OrderResponse(orderId, status, message));
    }
}
