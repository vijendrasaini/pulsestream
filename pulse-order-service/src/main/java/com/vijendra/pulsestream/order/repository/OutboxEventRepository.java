package com.vijendra.pulsestream.order.repository;

import com.vijendra.pulsestream.order.entity.OutboxEventEntity;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {
    List<OutboxEventEntity> findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus status);

    public Optional<OutboxEventEntity> findByAggregateIdAndAggregateTypeAndStatus(String aggregateId, String aggregateType, OutboxStatus status);
}