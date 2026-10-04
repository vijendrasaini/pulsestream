package com.vijendra.pulsestream.payment.repository;

import com.vijendra.pulsestream.payment.entity.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {
    boolean existsByEventId(String eventId);
}
