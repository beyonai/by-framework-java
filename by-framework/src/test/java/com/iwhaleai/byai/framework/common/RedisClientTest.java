package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;

/**
 * Tests for RedisClient singleton pattern and resource management.
 */
class RedisClientTest {

    @AfterEach
    void clearKeySchemaVersionProperty() {
        System.clearProperty("REDIS_KEY_SCHEMA_VERSION");
        System.clearProperty("REDIS_MODE");
        System.clearProperty("REDIS_CLUSTER_NODES");
        resetSingleton();
    }

    @Test
    void clusterModeRequiresV2SchemaFailsFastWithoutConnecting() {
        RedisConnectionConfig config = new RedisConnectionConfig();
        config.setMode(RedisConnectionConfig.Mode.CLUSTER);
        config.setClusterNodes(List.of(new HostAndPort("unreachable-host", 6379)));

        assertThrows(IllegalStateException.class, () -> new RedisClient(config));
    }

    @Test
    void clusterModeWithV2SchemaBuildsClusterClientWithoutFailingFast() {
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", "v2");
        RedisConnectionConfig config = new RedisConnectionConfig();
        config.setMode(RedisConnectionConfig.Mode.CLUSTER);
        config.setClusterNodes(List.of(
                new HostAndPort("h1", 6379), new HostAndPort("h2", 6380)));

        List<Object> capturedArgs = new ArrayList<>();
        try (MockedConstruction<JedisCluster> mocked = mockConstruction(JedisCluster.class,
                (mock, context) -> capturedArgs.addAll(context.arguments()))) {
            RedisClient client = new RedisClient(config);

            assertEquals(1, mocked.constructed().size());
            assertNotNull(client.getCommands());
            assertSame(mocked.constructed().get(0), client.getCommands());

            @SuppressWarnings("unchecked")
            Set<HostAndPort> nodesArg = (Set<HostAndPort>) capturedArgs.get(0);
            assertEquals(Set.of(new HostAndPort("h1", 6379), new HostAndPort("h2", 6380)), nodesArg);
        }
    }

    @Test
    void getInstanceInClusterModeWithoutV2SchemaFailsFast() {
        // Given - reset singleton and configure cluster mode via the same
        // env vars RedisConnectionConfig.fromEnv() reads, with no
        // REDIS_KEY_SCHEMA_VERSION set (defaults to v1)
        resetSingleton();
        System.setProperty("REDIS_MODE", "cluster");
        System.setProperty("REDIS_CLUSTER_NODES", "h1:6379");

        // When/Then - the default init path must apply the same v2 guard
        // as the RedisConnectionConfig constructor
        assertThrows(IllegalStateException.class, RedisClient::getInstance);
    }

    @Test
    void getInstanceInClusterModeWithV2SchemaBuildsClusterBackedInstance() {
        // Given - reset singleton, configure cluster mode + the v2 schema
        // it requires, same env vars RedisConnectionConfig.fromEnv() reads
        resetSingleton();
        System.setProperty("REDIS_MODE", "cluster");
        System.setProperty("REDIS_CLUSTER_NODES", "h1:6379,h2:6380");
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", "v2");

        try (MockedConstruction<JedisCluster> mocked = mockConstruction(JedisCluster.class)) {
            // When
            RedisClient instance = RedisClient.getInstance();

            // Then - the default init path handed back a Cluster-backed
            // client instead of silently falling back to standalone
            assertEquals(1, mocked.constructed().size());
            assertNotNull(instance.getJedisCluster());
            assertSame(mocked.constructed().get(0), instance.getJedisCluster());
        }
    }

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
    void initWithClusterConfigReplacesExistingInstanceWithClusterBackedOne() {
        // Given - an existing standalone instance (e.g. from a prior init()
        // call, mirroring the Spring DevTools forced-reinit use case)
        resetSingleton();
        RedisClient.init("localhost", 6379, 0, null, null);
        RedisClient firstInstance = RedisClient.getInstance();

        // When - forced reinit with a Cluster config, same v2 guard as the
        // RedisConnectionConfig constructor
        System.setProperty("REDIS_KEY_SCHEMA_VERSION", "v2");
        RedisConnectionConfig config = new RedisConnectionConfig();
        config.setMode(RedisConnectionConfig.Mode.CLUSTER);
        config.setClusterNodes(List.of(new HostAndPort("h1", 6379)));

        try (MockedConstruction<JedisCluster> mocked = mockConstruction(JedisCluster.class)) {
            RedisClient.init(config);

            RedisClient secondInstance = RedisClient.getInstance();
            assertNotSame(firstInstance, secondInstance);
            assertNotNull(secondInstance.getJedisCluster());
        }
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