package com.polyglot.sms.sender.entity;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "failed_kafka_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FailedKafkaEvent {
    @Id
    private String id;
    private String topic;
    private String key;
    private Object event; // Jackson will convert your SmsEvent into a BSON document
    private long createdAt;
}
