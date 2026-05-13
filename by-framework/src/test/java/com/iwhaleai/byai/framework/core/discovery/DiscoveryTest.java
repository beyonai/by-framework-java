package com.iwhaleai.byai.framework.core.discovery;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DiscoveryTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private Pipeline pipeline;

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        lenient().when(redisClient.getHost()).thenReturn("127.0.0.1");
        lenient().when(redisClient.getPort()).thenReturn(6379);
        lenient().when(jedis.pipelined()).thenReturn(pipeline);
    }

    @Test
    void testServiceRegistration() {
        ServiceRegistry registry = new ServiceRegistry(redisClient);
        registry.register("test-service", 8080);

        ServiceInstance instance = registry.getCurrentInstance();
        assertNotNull(instance);
        assertEquals("test-service", registry.getCurrentInstance().getId().split(":")[0]);
        assertEquals(8080, instance.getPort());

        verify(jedis).hset(eq(Constants.RegistryKeys.sdInstanceDetails("test-service")), eq(instance.getId()),
                anyString());
        verify(jedis).sadd(eq(Constants.RegistryKeys.SD_SERVICES), eq("test-service"));
        verify(jedis, atLeastOnce()).zadd(eq(Constants.RegistryKeys.sdActiveInstances("test-service")), anyDouble(),
                eq(instance.getId()));

        registry.unregister();
        verify(jedis).pipelined();
    }

    @Test
    void testServiceRegistrationWithPrefixAndProtocol() {
        ServiceRegistry registry = new ServiceRegistry(redisClient);
        registry.register("prefix-service", "https", "127.0.0.1", 8443, "/v2", 1, null, 30);

        ServiceInstance instance = registry.getCurrentInstance();
        assertNotNull(instance);
        assertEquals("https", instance.getProtocol());
        assertEquals("/v2", instance.getPathPrefix());
        assertEquals(8443, instance.getPort());

        verify(jedis).hset(eq(Constants.RegistryKeys.sdInstanceDetails("prefix-service")), anyString(), contains("\"protocol\":\"https\""));
        verify(jedis).hset(eq(Constants.RegistryKeys.sdInstanceDetails("prefix-service")), anyString(), contains("\"path_prefix\":\"/v2\""));
    }

    @Test
    void testServiceRegistrationCompatibility() {
        ServiceRegistry registry = new ServiceRegistry(redisClient);
        
        // Test old 6-param overload
        registry.register("old-service", "127.0.0.1", 8080, 1, null, 30);
        ServiceInstance instance = registry.getCurrentInstance();
        assertEquals("http", instance.getProtocol());
        assertNull(instance.getPathPrefix());

        // Test 7-param overload (prefix)
        registry.register("prefix-service", "127.0.0.1", 8081, "/api", 1, null, 30);
        instance = registry.getCurrentInstance();
        assertEquals("http", instance.getProtocol());
        assertEquals("/api", instance.getPathPrefix());
    }

    @Test
    void testServiceRegistrationOnly() {
        ServiceRegistry registry = new ServiceRegistry(redisClient);
        registry.registerOnly("only-reg-service", 8080);

        ServiceInstance instance = registry.getCurrentInstance();
        assertNotNull(instance);
        assertEquals(8080, instance.getPort());

        // Verify registration details are written
        verify(jedis).hset(eq(Constants.RegistryKeys.sdInstanceDetails("only-reg-service")), eq(instance.getId()), anyString());
        
        // Verify registration visibility (zadd should be called once during registration)
        verify(jedis, times(1)).zadd(eq(Constants.RegistryKeys.sdActiveInstances("only-reg-service")), anyDouble(), anyString());
    }

    @Test
    void testServiceRegistrationOnlyWithFullParams() {
        ServiceRegistry registry = new ServiceRegistry(redisClient);
        registry.registerOnly("full-only-service", "https", "127.0.0.1", 8443, "/v3");

        ServiceInstance instance = registry.getCurrentInstance();
        assertNotNull(instance);
        assertEquals("https", instance.getProtocol());
        assertEquals("/v3", instance.getPathPrefix());
        assertEquals(8443, instance.getPort());

        verify(jedis).hset(eq(Constants.RegistryKeys.sdInstanceDetails("full-only-service")), anyString(), contains("\"protocol\":\"https\""));
        verify(jedis, times(1)).zadd(eq(Constants.RegistryKeys.sdActiveInstances("full-only-service")), anyDouble(), anyString());
    }

    @Test
    void testDiscoveryClient() {
        DiscoveryClient client = new DiscoveryClient(redisClient, 10);
        String serviceName = "test-service";
        String instanceId = "test-service:123";
        ServiceInstance instance = ServiceInstance.builder()
                .id(instanceId)
                .host("127.0.0.1")
                .port(8080)
                .build();

        // Mock Tuple for newer Jedis
        Tuple tuple = mock(Tuple.class);
        when(tuple.getElement()).thenReturn(instanceId);
        when(tuple.getScore()).thenReturn((double) System.currentTimeMillis());

        when(jedis.zrangeWithScores(eq(Constants.RegistryKeys.sdActiveInstances(serviceName)), eq(0L), eq(-1L)))
                .thenReturn(List.of(tuple));
        when(jedis.hmget(eq(Constants.RegistryKeys.sdInstanceDetails(serviceName)), eq(instanceId)))
                .thenReturn(List.of(instance.toJson()));

        List<ServiceInstance> instances = client.getInstances(serviceName);
        assertEquals(1, instances.size());
        assertEquals(instanceId, instances.get(0).getId());

        // Test caching (should not call jedis again)
        client.getInstances(serviceName);
        verify(jedis, times(1)).zrangeWithScores(anyString(), anyLong(), anyLong());

        // Test load balancing
        Optional<ServiceInstance> discovered = client.discover(serviceName, "round-robin");
        assertTrue(discovered.isPresent());
        assertEquals(instanceId, discovered.get().getId());
    }

    @Test
    void testDiscoveryLivenessCheck() {
        DiscoveryClient client = new DiscoveryClient(redisClient, 10);
        String serviceName = "test-service";

        long now = System.currentTimeMillis();

        // Instance 1: Healthy (10s ago)
        String id1 = "s1";
        Tuple t1 = mock(Tuple.class);
        when(t1.getElement()).thenReturn(id1);
        when(t1.getScore()).thenReturn((double) (now - 10000));

        // Instance 2: Stale (40s ago, default threshold is 30s)
        String id2 = "s2";
        Tuple t2 = mock(Tuple.class);
        when(t2.getElement()).thenReturn(id2);
        when(t2.getScore()).thenReturn((double) (now - 40000));

        when(jedis.zrangeWithScores(anyString(), anyLong(), anyLong()))
                .thenReturn(List.of(t1, t2));
        when(jedis.hmget(anyString(), anyString(), anyString()))
                .thenReturn(List.of(
                        ServiceInstance.builder().id(id1).build().toJson(),
                        ServiceInstance.builder().id(id2).build().toJson()));

        // 1. Default discovery (threshold 30s) -> should only return id1
        List<ServiceInstance> healthy = client.getInstances(serviceName);
        assertEquals(1, healthy.size());
        assertEquals(id1, healthy.get(0).getId());

        // 2. Skip liveness check -> should return both
        List<ServiceInstance> all = client.getInstances(serviceName, false, Constants.SD_NO_HEALTH_CHECK);
        assertEquals(2, all.size());
    }

    @Test
    void testLocalIpDetection() {
        String ip = DiscoveryUtils.getLocalIp();
        assertNotNull(ip);
        assertNotEquals("0.0.0.0", ip);
        // It could be 127.0.0.1 in some environments, but usually a local IP
    }
}
