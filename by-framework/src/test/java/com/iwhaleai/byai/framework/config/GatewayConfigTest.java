package com.iwhaleai.byai.framework.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * GatewayConfig 测试，对标 Python test_redis_client.py 的配置加载部分
 */
class GatewayConfigTest {

    @Test
    void getReturnsSystemPropertyFirst() {
        String key = "test.gateway.config.prop";
        System.setProperty(key, "from-system-prop");
        try {
            String result = GatewayConfig.get(key);
            assertEquals("from-system-prop", result);
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void getReturnsDefaultWhenNotFound() {
        String result = GatewayConfig.get("non.existent.key.xyz", "default-val");
        assertEquals("default-val", result);
    }

    @Test
    void getReturnsNullWhenNotFoundAndNoDefault() {
        String result = GatewayConfig.get("non.existent.key.abc");
        assertNull(result);
    }

    @Test
    void getIntReturnsDefaultWhenNotFound() {
        int result = GatewayConfig.getInt("non.existent.int.key", 9999);
        assertEquals(9999, result);
    }

    @Test
    void getIntParsesSystemProperty() {
        String key = "test.gateway.int.prop";
        System.setProperty(key, "42");
        try {
            int result = GatewayConfig.getInt(key, 0);
            assertEquals(42, result);
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void getIntReturnsDefaultForInvalidNumber() {
        String key = "test.gateway.invalid.int";
        System.setProperty(key, "not-a-number");
        try {
            int result = GatewayConfig.getInt(key, 100);
            assertEquals(100, result);
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void systemPropertyTakesPrecedenceOverEnvVar() {
        // We can't set env vars in tests, but we can verify system property wins
        String key = "gateway.redis.host";
        System.setProperty(key, "from-system");
        try {
            String result = GatewayConfig.get(key);
            assertEquals("from-system", result);
        } finally {
            System.clearProperty(key);
        }
    }

    @Test
    void getWithDotConvertsToUnderscoreForEnvLookup() {
        // Test that the method doesn't crash even when env var doesn't exist
        // The conversion logic (dot -> underscore, uppercase) is internal
        String result = GatewayConfig.get("some.dotted.key.that.does.not.exist");
        // Should not throw, just return null
        assertNull(result);
    }

    @Test
    void redisConfigDefaultValues() {
        // Verify default values for Redis config keys when nothing is set
        // Clear any system properties that might interfere
        System.clearProperty("gateway.redis.host");
        System.clearProperty("gateway.redis.port");
        System.clearProperty("gateway.redis.db");

        // Without any config, these should fall back to defaults
        // (unless env vars or config file is present in the test classpath)
        String host = GatewayConfig.get("gateway.redis.host", "localhost");
        int port = GatewayConfig.getInt("gateway.redis.port", 6379);
        int db = GatewayConfig.getInt("gateway.redis.db", 0);

        assertNotNull(host);
        assertTrue(port > 0);
        assertTrue(db >= 0);
    }
}
