package com.vijendra.pulsestream.order.dto;

import java.math.BigDecimal;

public record CreateOrderRequest(
        String customerId,
        String itemSku,
        int requestQuantity,
        BigDecimal totalAmount
) {
}