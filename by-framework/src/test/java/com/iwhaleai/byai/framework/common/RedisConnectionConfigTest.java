package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for RedisConnectionConfig.fromEnv(). Uses System properties to
 * simulate env vars, matching GatewayConfig's precedence (system property
 * checked before the real OS environment variable) - the same trick
 * GatewayConfigTest already relies on, since real env vars can't be set at
 * JVM runtime.
 */
class RedisConnectionConfigTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty("REDIS_MODE");
        System.clearProperty("REDIS_HOST");
        System.clearProperty("REDIS_PORT");
        System.clearProperty("REDIS_DB");
        System.clearProperty("REDIS_USERNAME");
        System.clearProperty("REDIS_PASSWORD");
        System.clearProperty("REDIS_CLUSTER_NODES");
    }

    @Test
    void fromEnvDefaultsToStandaloneWithNoEnvVarsSet() {
        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.STANDALONE, config.getMode());
        assertEquals("localhost", config.getHost());
        assertEquals(6379, config.getPort());
        assertEquals(0, config.getDb());
        assertNull(config.getUsername());
        assertNull(config.getPassword());
        assertTrue(config.getClusterNodes().isEmpty());
    }

    @Test
    void fromEnvParsesClusterModeAndNodes() {
        System.setProperty("REDIS_MODE", "cluster");
        System.setProperty("REDIS_CLUSTER_NODES", "h1:6379,h2:6380");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.CLUSTER, config.getMode());
        List<HostAndPort> nodes = config.getClusterNodes();
        assertEquals(2, nodes.size());
        assertEquals(new HostAndPort("h1", 6379), nodes.get(0));
        assertEquals(new HostAndPort("h2", 6380), nodes.get(1));
    }
}
