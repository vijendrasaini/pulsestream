package com.vijendra.pulsestream.common.event;

import java.time.Instant;

public record PaymentFailedEvent(
        String orderId,
        String failureReason,
        Instant failedAt
) {
}