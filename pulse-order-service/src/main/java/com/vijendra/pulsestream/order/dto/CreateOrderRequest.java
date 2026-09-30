package com.vijendra.pulsestream.order.dto;

public record CreateOrderRequest(
        String customerId,
        String itemSku,
        int requestQuantity
) {
}