package com.vijendra.pulsestream.common.envelope;

import java.time.Instant;
import java.util.UUID;

public record EventEnvelope<T>(
String eventId,
String eventType,
String aggregateId,
Instant timestamp,
T payload
) {
public static <T> EventEnvelope<T> of(String eventType, String aggregateId, T payload) {
    return new EventEnvelope<>(
            UUID.randomUUID().toString(),
            eventType,
            aggregateId,
            Instant.now(),
            payload
    );
}
}
