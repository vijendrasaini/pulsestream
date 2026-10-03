package com.vijendra.pulsestream.order.outbox;

import com.vijendra.pulsestream.order.config.KafkaTopicConfig;
import com.vijendra.pulsestream.order.entity.OutboxEventEntity;
import com.vijendra.pulsestream.order.entity.enums.OutboxStatus;
import com.vijendra.pulsestream.order.repository.OutboxEventRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class OutboxRelay {
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;


    @Scheduled(fixedDelay = 1000)
    public void run() {
        List<OutboxEventEntity> pendingEvents = outboxEventRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if(pendingEvents.isEmpty()) {
            return;
        }

        for(OutboxEventEntity outboxEventEntity: pendingEvents) {
            publishToKafka(outboxEventEntity);
        }
    }

    private void publishToKafka(OutboxEventEntity outboxEventEntity) {
        kafkaTemplate.send(
                KafkaTopicConfig.ORDER_EVENTS_TOPIC,
                outboxEventEntity.getAggregateId(),
                outboxEventEntity.getPayload()
        ).whenComplete((result, ex) -> {
           if(ex == null) {
               outboxEventEntity.setStatus(OutboxStatus.PUBLISHED);
               outboxEventRepository.save(outboxEventEntity);
               log.info("Outbox event {} published successfully for order {}", outboxEventEntity.getId(), outboxEventEntity.getAggregateId());
           } else {
               log.error("Failed to publish outbox event {}: {}", outboxEventEntity.getId(), ex.getMessage());
           }
        });
    }
}
