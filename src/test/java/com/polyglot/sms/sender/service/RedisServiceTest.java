package com.polyglot.sms.sender.service;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.data.redis.core.RedisTemplate;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RedisServiceTest {
    
    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @InjectMocks
    private RedisService redisService;

    @Test
    void isBlocked_keyExists_shouldReturnTrue() {
        String userId = "1234567890";
        when(redisTemplate.hasKey(userId)).thenReturn(true);
        assertTrue(redisService.isBlocked(userId));
    }

    @Test
    void isBlocked_keyDoesNotExist_shouldReturnFalse() {
        String userId = "1234567890";
        when(redisTemplate.hasKey(userId)).thenReturn(false);
        assertFalse(redisService.isBlocked(userId));
    }

    @Test
    void isBlocked_redisReturnsNull_shouldReturnFalse() {
        String userId = "1234567890";
        when(redisTemplate.hasKey(userId)).thenReturn(null);
        assertFalse(redisService.isBlocked(userId));
    }

    @Test
    void isBlocked_redisThrowsException_shouldThrowRuntimeException() {
        String userId = "1234567890";
        when(redisTemplate.hasKey(userId)).thenThrow(new RuntimeException("Redis is unreachable"));
        assertThrows(RuntimeException.class, () -> redisService.isBlocked(userId));
    }
}
