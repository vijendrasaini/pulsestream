package com.vijendra.pulsestream.order.entity;

import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
public class OutboxEventEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String aggregateType;

    private String aggregateId;

    private String eventType;

    private String payload;

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    @CreationTimestamp
    private Instant createdAt;

    @UpdateTimestamp
    private Instant publishedAt;
}
