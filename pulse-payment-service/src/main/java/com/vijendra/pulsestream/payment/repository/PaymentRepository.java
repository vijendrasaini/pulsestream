package com.vijendra.pulsestream.payment.repository;

import com.vijendra.pulsestream.payment.entity.PaymentEntity;
import com.vijendra.pulsestream.payment.entity.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {
    List<PaymentEntity> findByOrderIdAndStatus(String orderId, PaymentStatus status);
}
