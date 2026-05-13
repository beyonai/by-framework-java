package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RedisClient singleton pattern and resource management.
 */
class RedisClientTest {

    @Test
    void getInstanceReturnsNonNull() {
        // Given - use reflection to reset singleton for testing
        resetSingleton();

        // When
        RedisClient instance = RedisClient.getInstance();

        // Then
        assertNotNull(instance);
    }

    @Test
    void initCreatesNewInstance() {
        // Given - reset singleton first
        resetSingleton();

        // When
        RedisClient.init("localhost", 6379, 0, null, null);

        // Then
        RedisClient instance = RedisClient.getInstance();
        assertNotNull(instance);
    }

    @Test
    void initWithCredentialsCreatesInstance() {
        // Given
        resetSingleton();

        // When
        RedisClient.init("localhost", 6379, 0, "user", "password");

        // Then
        RedisClient instance = RedisClient.getInstance();
        assertNotNull(instance);
    }

    @Test
    void doubleInitReplacesExistingInstance() {
        // Given
        resetSingleton();
        RedisClient.init("localhost", 6379, 0, null, null);
        RedisClient firstInstance = RedisClient.getInstance();

        // When
        RedisClient.init("remotehost", 6380, 1, "user", "pass");

        // Then
        RedisClient secondInstance = RedisClient.getInstance();
        assertNotNull(secondInstance);
        assertNotSame(firstInstance, secondInstance);
    }

    @Test
    void closeHandlesNullPool() {
        // Given
        resetSingleton();
        RedisClient instance = RedisClient.getInstance();

        // When/Then - close should not throw
        assertDoesNotThrow(() -> instance.close());
    }

    private void resetSingleton() {
        try {
            java.lang.reflect.Field instanceField = RedisClient.class.getDeclaredField("instance");
            instanceField.setAccessible(true);
            instanceField.set(null, null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to reset singleton", e);
        }
    }
}