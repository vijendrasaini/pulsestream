package com.vijendra.pulsestream.payment.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

@Entity
@Table(name = "processed_events")
@Getter
@Setter
@NoArgsConstructor
public class ProcessedEventEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String eventId;

    private String eventType;

    private String aggregateId;

    @CreationTimestamp
    private Instant processedAt;
}
