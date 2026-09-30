package com.vijendra.pulsestream.order.dto;

public record OrderResponse(
        String orderId,
        String status,
        String message
) {
}
