package com.polyglot.sms.sender.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.data.mongodb.core.MongoTemplate;
import com.polyglot.sms.sender.entity.FailedKafkaEvent;

import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final MongoTemplate mongoTemplate;

    public void sendMessage(String topic, String key, Object event) {
        try {
            // If Metadata fetch fails (Kafka down), this throws Exception immediately.since max blocking time for producer is set to 2s for fetching metadata
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, key, event);

            // Asynchronous failures when kafka crashes after data being sent to broker
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Async Kafka Failure. Saving to Mongo. Key: {}", key, ex);
                    saveToFallback(topic, key, event);
                } else {
                    log.info("Success: Offset {}", result.getRecordMetadata().offset());
                }
            });

        } catch (Exception e) {
            // Synchronous failures (Kafka totally down / Metadata timeout)
            log.error("Sync Kafka Failure (Metadata/Timeout). Saving to Mongo. Key: {}", key, e);
            saveToFallback(topic, key, event);
        }
    }

    private void saveToFallback(String topic, String key, Object event) {
        try {
            FailedKafkaEvent fallback = FailedKafkaEvent.builder()
                    .topic(topic)
                    .key(key)
                    .event(event)
                    .createdAt(System.currentTimeMillis())
                    .build();
            mongoTemplate.save(fallback);
        } catch (Exception ex) {
            log.error("CRITICAL: Both Kafka and Mongo are DOWN. Data lost for user: {}", key);
        }
    }
}