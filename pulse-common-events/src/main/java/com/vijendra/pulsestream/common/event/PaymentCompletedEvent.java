package com.vijendra.pulsestream.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
        String paymentId,
        String orderId,
        BigDecimal amount,
        Instant completedAt
) {
}