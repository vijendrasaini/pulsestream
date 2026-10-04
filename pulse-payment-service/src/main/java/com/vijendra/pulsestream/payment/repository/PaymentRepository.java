package com.vijendra.pulsestream.payment.repository;

import com.vijendra.pulsestream.payment.entity.PaymentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {
}
