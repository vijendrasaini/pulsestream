package com.vijendra.pulsestream.order.repository;

import com.vijendra.pulsestream.order.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, String> {
}
