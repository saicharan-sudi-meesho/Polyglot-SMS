package com.polyglot.sms.sender.service;

import org.mockito.Mock;
import org.mockito.InjectMocks;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import com.polyglot.sms.sender.dto.SmsRequest;
import com.polyglot.sms.sender.dto.SmsResponse;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.any;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import org.mockito.ArgumentCaptor;
import com.polyglot.sms.sender.dto.SmsEvent;

@ExtendWith(MockitoExtension.class)
public class SmsServiceTest {

    @Mock
    private RedisService redisService;

    @Mock
    private KafkaProducerService kafkaProducerService;

    @InjectMocks
    private SmsService smsService;
    

    @BeforeEach
    void setUp(){
        ReflectionTestUtils.setField(smsService, "topicName", "sms-events-test-topic");
    }

    @Test
    void processSmsRequest_UserBlocked_ReturnsBlockedResponse() {
        // Arrange
        SmsRequest request = new SmsRequest( "1234567890", "Hello");
        when(redisService.isBlocked("1234567890")).thenReturn(true);

        // Act
        SmsResponse response = smsService.sendSms(request);

        // Assert
        assertThat(response.getStatus()).isEqualTo("BLOCKED");
        verify(kafkaProducerService, never()).sendMessage(any(), any(), any());
    }


    @Test
    void processSmsRequest_RedisDown_ReturnsInternalErrorResponse() {
        // Arrange
        SmsRequest request = new SmsRequest("1234567890", "Hello");
        when(redisService.isBlocked("1234567890")).thenThrow(new RuntimeException("Redis is down"));

        // Act
        SmsResponse response = smsService.sendSms(request);

        // Assert
        assertThat(response.getStatus()).isEqualTo("INTERNAL_ERROR");
        verify(kafkaProducerService, never()).sendMessage(any(), any(), any());
    }

    @Test
    void processSmsRequest_KafkaDown_ReturnsInternalErrorResponse() {
        // Arrange
        SmsRequest request = new SmsRequest("1234567890", "Hello");
        when(redisService.isBlocked("1234567890")).thenReturn(false);
        doThrow(new RuntimeException("Kafka is down")).when(kafkaProducerService).sendMessage(any(), any(), any());

        // Act
        SmsResponse response = smsService.sendSms(request);

        // Assert
        assertThat(response.getStatus()).isEqualTo("INTERNAL_ERROR");
    }

    @Test
    void processSmsRequest_Success_ReturnsSuccessResponse() {
        // Arrange
        SmsRequest request = new SmsRequest("1234567890", "Hello");
        when(redisService.isBlocked("1234567890")).thenReturn(false);

        // Act
        SmsResponse response = smsService.sendSms(request);

        // Assert
        assertThat(response.getStatus()).isEqualTo("SUCCESS");

        // Verify
        ArgumentCaptor<SmsEvent> eventCaptor = ArgumentCaptor.forClass(SmsEvent.class);
        verify(kafkaProducerService).sendMessage(eq("sms-events-test-topic"), eq("1234567890"), eventCaptor.capture());

        SmsEvent capturedEvent = eventCaptor.getValue();
        assertThat(capturedEvent.getUserId()).isEqualTo("1234567890");
        assertThat(capturedEvent.getStatus()).isEqualTo("SUCCESS");
    }
}
