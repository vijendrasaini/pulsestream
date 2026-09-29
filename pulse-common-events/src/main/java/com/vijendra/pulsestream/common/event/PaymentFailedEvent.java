package com.vijendra.pulsestream.common.event;

import java.time.Instant;

public record PaymentFailedEvent(
        String paymentId,
        int orderId,
        String failureReason,
        Instant failedAt
) {
}