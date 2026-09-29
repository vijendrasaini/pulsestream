package com.vijendra.pulsestream.common.event;
import java.time.Instant;

public record InventoryFailedEvent(
        String orderId,
        String itemSku,
        int requestedQuantity,
        String failureReason,
        Instant failedAt
) {
}