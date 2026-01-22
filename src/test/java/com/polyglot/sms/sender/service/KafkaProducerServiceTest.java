package com.polyglot.sms.sender.service;

import com.polyglot.sms.sender.entity.FailedKafkaEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private MongoTemplate mongoTemplate;

    @InjectMocks
    private KafkaProducerService kafkaProducerService;

    private final String topic = "test-topic";
    private final String key = "user-123";
    private final String payload = "{ \"msg\": \"hello\" }";

    @Test
    @DisplayName("Should successfully send message to Kafka")
    void testSendMessage_Success() {
        // Arrange
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        @SuppressWarnings("unchecked")
        SendResult<String, Object> sendResult = (SendResult<String, Object>) mock(SendResult.class);
        RecordMetadata metadata = new RecordMetadata(new TopicPartition(topic, 0), 0L, 0, 0L, 0, 0);
        
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        future.complete(sendResult);
        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        // Act
        kafkaProducerService.sendMessage(topic, key, payload);

        // Assert
        verify(kafkaTemplate).send(topic, key, payload);
        verify(mongoTemplate, never()).save(any(FailedKafkaEvent.class));
    }

    @Test
    @DisplayName("Should save to Mongo when Kafka throws Synchronous Exception")
    void testSendMessage_SyncFailure() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException("Kafka Down"));

        // Act
        kafkaProducerService.sendMessage(topic, key, payload);

        // Assert
        verify(mongoTemplate, times(1)).save(any(FailedKafkaEvent.class));
        
        // Verify correct data saved to fallback
        ArgumentCaptor<FailedKafkaEvent> captor = ArgumentCaptor.forClass(FailedKafkaEvent.class);
        verify(mongoTemplate).save(captor.capture());
        assertThat(captor.getValue().getKey()).isEqualTo(key);
        assertThat(captor.getValue().getTopic()).isEqualTo(topic);
    }

    @Test
    @DisplayName("Should save to Mongo when Kafka fails Asynchronously")
    void testSendMessage_AsyncFailure() {
        // Arrange: send() works, but the future fails later
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Network issue mid-send"));
        
        when(kafkaTemplate.send(topic, key, payload)).thenReturn(future);

        // Act
        kafkaProducerService.sendMessage(topic, key, payload);

        // Assert
        await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(mongoTemplate, times(1)).save(any(FailedKafkaEvent.class));
        });
    }

    @Test
    @DisplayName("Should log error when both Kafka and Mongo fail")
    void testSendMessage_TotalFailure() {
        // Arrange
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenThrow(new RuntimeException("Kafka Dead"));
        when(mongoTemplate.save(any(FailedKafkaEvent.class)))
                .thenThrow(new RuntimeException("Mongo Dead"));

        // Act & Assert
        kafkaProducerService.sendMessage(topic, key, payload);
        
        verify(kafkaTemplate).send(topic, key, payload);
        verify(mongoTemplate).save(any(FailedKafkaEvent.class));
    }
}