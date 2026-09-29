package com.vijendra.pulsestream.common.event;
import java.time.Instant;

public record InventoryReservedEvent(
        String reservationId,
        String orderId,
        String itemSku,
        int quantity,
        Instant reservedAt
) {
}