package com.vijendra.pulsestream.order.repository;

import com.vijendra.pulsestream.order.entity.OutboxEventEntity;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, Long> {
    List<OutboxEventEntity> findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus status);
}
