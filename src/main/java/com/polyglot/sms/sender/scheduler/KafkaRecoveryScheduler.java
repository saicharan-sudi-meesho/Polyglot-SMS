package com.polyglot.sms.sender.scheduler;

import com.polyglot.sms.sender.entity.FailedKafkaEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.util.List;
// import java.util.concurrent.TimeUnit;
// import org.apache.kafka.clients.admin.AdminClient;
// import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.data.mongodb.core.query.Query;
// import java.util.Map;
// import java.util.HashMap;
// import org.apache.kafka.clients.admin.AdminClientConfig;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaRecoveryScheduler {

    private final MongoTemplate mongoTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    // private final KafkaAdmin kafkaAdmin;

    @Scheduled(fixedDelay = 60000) // every 1 minute
    public void retryFailedEvents() {

        // 1. Check if Kafka is actually up
        // if (!isKafkaUp()) {
        //     log.warn("Kafka is still unreachable. Skipping recovery run.");
        //     return;
        // }

        // 2. Count check
        long count = mongoTemplate.count(new Query(), FailedKafkaEvent.class);
        if (count == 0) return;

        // 3. Batch processing (only 50 at a time) to prevent memory issues
        Query batchQuery = new Query().limit(50);
        List<FailedKafkaEvent> failures = mongoTemplate.find(batchQuery, FailedKafkaEvent.class);

        log.info("Kafka is UP. Processing {}/{} failed events...", failures.size(), count);

        for (FailedKafkaEvent failure : failures) {
            try {
                kafkaTemplate.send(failure.getTopic(), failure.getKey(), failure.getEvent()).get();
                mongoTemplate.remove(failure);
            } catch (Exception e) {
                log.error("Recovery failed mid-batch. key: {}", failure.getKey());
                break;
            }
        }
    }

    // private boolean isKafkaUp() {
    //     Map<String, Object> configs = new HashMap<>(kafkaAdmin.getConfigurationProperties());
    //     // Tell the client NOT to retry internally if it fails
    //     configs.put(AdminClientConfig.RETRIES_CONFIG, 0);
    //     // Set a very short reconnection backoff
    //     configs.put(AdminClientConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
    //     // 1. Try-with-resources to create a temporary AdminClient
    //     try (AdminClient client = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
    //         // 2. Ask for the Cluster ID
    //         // 3. Set a strict 2-second deadline
    //         client.describeCluster().clusterId().get(2, TimeUnit.SECONDS);
    //         // 4. If we reached here without an error, Kafka is UP
    //         return true;
    //     } catch (Exception e) {
    //         // 5. If ANY error happens (timeout, connection refused), Kafka is DOWN
    //         return false;
    //     }
    // }
}