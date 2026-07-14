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
        System.clearProperty("REDIS_DATABASE");
        System.clearProperty("REDIS_DB");
        System.clearProperty("REDIS_USERNAME");
        System.clearProperty("REDIS_PASSWORD");
        System.clearProperty("REDIS_CLUSTER_NODES");
        System.clearProperty("REDIS_CLUSTER_HOST");
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

    @Test
    void fromEnvClusterHostAloneImpliesClusterModeWithoutExplicitRedisMode() {
        System.setProperty(
                "REDIS_CLUSTER_HOST",
                "10.10.168.203:6371,10.10.168.203:6372,10.10.168.203:6373");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.CLUSTER, config.getMode());
        List<HostAndPort> nodes = config.getClusterNodes();
        assertEquals(3, nodes.size());
        assertEquals(new HostAndPort("10.10.168.203", 6371), nodes.get(0));
        assertEquals(new HostAndPort("10.10.168.203", 6372), nodes.get(1));
        assertEquals(new HostAndPort("10.10.168.203", 6373), nodes.get(2));
    }

    @Test
    void fromEnvEmptyClusterHostIsTreatedAsUnset() {
        System.setProperty("REDIS_CLUSTER_HOST", "");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.STANDALONE, config.getMode());
        assertTrue(config.getClusterNodes().isEmpty());
    }

    @Test
    void fromEnvEmptyClusterHostFallsBackToClusterNodes() {
        System.setProperty("REDIS_MODE", "cluster");
        System.setProperty("REDIS_CLUSTER_HOST", "");
        System.setProperty("REDIS_CLUSTER_NODES", "h1:6379,h2:6380");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.CLUSTER, config.getMode());
        List<HostAndPort> nodes = config.getClusterNodes();
        assertEquals(2, nodes.size());
        assertEquals(new HostAndPort("h1", 6379), nodes.get(0));
        assertEquals(new HostAndPort("h2", 6380), nodes.get(1));
    }

    @Test
    void fromEnvExplicitRedisModeOverridesClusterHost() {
        System.setProperty("REDIS_MODE", "standalone");
        System.setProperty("REDIS_CLUSTER_HOST", "h1:6379");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(RedisConnectionConfig.Mode.STANDALONE, config.getMode());
    }

    @Test
    void fromEnvReadsRedisDatabase() {
        System.setProperty("REDIS_DATABASE", "3");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(3, config.getDb());
    }

    @Test
    void fromEnvFallsBackToDeprecatedRedisDbWhenRedisDatabaseUnset() {
        System.setProperty("REDIS_DB", "5");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(5, config.getDb());
    }

    @Test
    void fromEnvRedisDatabaseTakesPrecedenceOverDeprecatedRedisDb() {
        System.setProperty("REDIS_DATABASE", "2");
        System.setProperty("REDIS_DB", "9");

        RedisConnectionConfig config = RedisConnectionConfig.fromEnv();

        assertEquals(2, config.getDb());
    }
}
