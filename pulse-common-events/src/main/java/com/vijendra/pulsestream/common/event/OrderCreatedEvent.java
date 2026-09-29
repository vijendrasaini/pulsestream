package com.vijendra.pulsestream.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        BigDecimal totalAmount,
        String itemSku,
        int quantity,
        Instant createdAt
) {}
