package com.vijendra.pulsestream.payment.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vijendra.pulsestream.common.envelope.EventEnvelope;
import com.vijendra.pulsestream.common.event.OrderCreatedEvent;
import com.vijendra.pulsestream.common.event.PaymentCompletedEvent;
import com.vijendra.pulsestream.common.event.PaymentFailedEvent;
import com.vijendra.pulsestream.payment.config.KafkaTopicConfig;
import com.vijendra.pulsestream.payment.entity.PaymentEntity;
import com.vijendra.pulsestream.payment.entity.ProcessedEventEntity;
import com.vijendra.pulsestream.payment.entity.enums.PaymentStatus;
import com.vijendra.pulsestream.payment.repository.PaymentRepository;
import com.vijendra.pulsestream.payment.repository.ProcessedEventRepository;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class PaymentService {
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ProcessedEventRepository processedEventRepository;
    private final PaymentRepository paymentRepository;
    private static final String PAYMENT_FAILED = "PAYMENT_FAILED";
    private static final String PAYMENT_COMPLETED = "PAYMENT_COMPLETED";

    private static final int MAX_LIMIT = 5000;
    Logger log = LoggerFactory.getLogger(PaymentService.class);

    public PaymentService(ObjectMapper objectMapper, KafkaTemplate<String, Object> kafkaTemplate, ProcessedEventRepository processedEventRepository, PaymentRepository paymentRepository) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.processedEventRepository = processedEventRepository;
        this.paymentRepository = paymentRepository;
    }

    @Transactional
    @KafkaListener(topics = "order-events", groupId = "payment-service-group")
    public void processOrderCreated(EventEnvelope<?> envelope) {
        OrderCreatedEvent payload = objectMapper.convertValue(envelope.payload(), OrderCreatedEvent.class);
        log.info("Envelope : eventId:{}, timestamp:{}, eventType:{}, payload:{}", envelope.eventId(), envelope.timestamp(), envelope.eventType(), payload);

        String orderId = envelope.aggregateId();
        String eventId = envelope.eventId();
        if(processedEventRepository.existsByEventId(eventId)) {
            log.warn("Duplicate Event received. eventId:{}", envelope.eventId());
            return;
        }

        PaymentEntity paymentEntity = new PaymentEntity();
        paymentEntity.setOrderId(orderId);
        paymentEntity.setStatus(PaymentStatus.PENDING);
        paymentEntity.setAmount(payload.totalAmount());
        paymentEntity = paymentRepository.save(paymentEntity);

        ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
        processedEventEntity.setEventId(eventId);
        processedEventEntity.setEventType(PAYMENT_COMPLETED);
        processedEventEntity.setAggregateId(orderId);

        if(payload.totalAmount().intValue() > MAX_LIMIT) {
            log.warn("[Payment Service] Insufficient credit for Order: {} (Amount: ${} > Limit: ${})",
                    orderId, payload.totalAmount(), MAX_LIMIT);

            // limiting the order till 5000

            paymentEntity.setStatus(PaymentStatus.FAILED);
            paymentEntity = paymentRepository.save(paymentEntity);

            processedEventEntity.setEventType(PAYMENT_COMPLETED);
            processedEventRepository.save(processedEventEntity);

            PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                    orderId,
                    "LIMIT_EXCEEDED",
                    Instant.now()
            );

            EventEnvelope<PaymentFailedEvent> failEnvelope = EventEnvelope.of(
                    PAYMENT_FAILED,
                    envelope.aggregateId(),
                    failedEvent
            );

            kafkaTemplate.send(KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, orderId, failEnvelope);
            return;
        }

        // Proceed for the payment;
        String paymentId = ("PAY-" + UUID.randomUUID()).substring(0, 12);
        log.info("Payment APPROVED for Order: {} | PaymentId: {}", orderId, paymentId);

        paymentEntity.setStatus(PaymentStatus.COMPLETED);
        paymentEntity = paymentRepository.save(paymentEntity);

        processedEventRepository.save(processedEventEntity);

        PaymentCompletedEvent completedEvent = new PaymentCompletedEvent(
                paymentId,
                orderId,
                payload.totalAmount(),
                Instant.now()
        );

        EventEnvelope<PaymentCompletedEvent> paymentCompletedEnvelope = EventEnvelope.of(
                PAYMENT_COMPLETED,
                orderId,
                completedEvent
        );

        kafkaTemplate.send(KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, orderId, paymentCompletedEnvelope);
    }
}
