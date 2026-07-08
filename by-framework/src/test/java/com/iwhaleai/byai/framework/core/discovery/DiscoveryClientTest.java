package com.iwhaleai.byai.framework.core.discovery;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * DiscoveryClient tested against the RedisOps seam (Cluster-compatible by
 * construction) instead of a raw Jedis mock, per the discovery Cluster
 * compatibility work.
 */
@ExtendWith(MockitoExtension.class)
class DiscoveryClientTest {

    @Mock
    private RedisOps redisOps;

    @Test
    void getInstancesReturnsInstancesFromFetchActiveServiceInstances() {
        String serviceName = "test-service";
        String instanceId = "test-service:123";
        ServiceInstance instance = ServiceInstance.builder()
                .id(instanceId)
                .host("127.0.0.1")
                .port(8080)
                .build();

        when(redisOps.fetchActiveServiceInstances(serviceName)).thenReturn(Map.of(
                instanceId, new RedisOps.ActiveInstanceRecord(instance.toJson(), System.currentTimeMillis())));

        DiscoveryClient client = new DiscoveryClient(redisOps, 10);
        List<ServiceInstance> instances = client.getInstances(serviceName);

        assertEquals(1, instances.size());
        assertEquals(instanceId, instances.get(0).getId());

        // Caching: a second call within cacheIntervalSeconds must not hit RedisOps again
        client.getInstances(serviceName);
        verify(redisOps, times(1)).fetchActiveServiceInstances(serviceName);
    }

    @Test
    void getInstancesFiltersOutStaleInstancesByHealthThreshold() {
        String serviceName = "test-service";
        long now = System.currentTimeMillis();

        String healthyId = "s1";
        String staleId = "s2";
        when(redisOps.fetchActiveServiceInstances(serviceName)).thenReturn(Map.of(
                healthyId, new RedisOps.ActiveInstanceRecord(
                        ServiceInstance.builder().id(healthyId).build().toJson(), now - 10_000),
                staleId, new RedisOps.ActiveInstanceRecord(
                        ServiceInstance.builder().id(staleId).build().toJson(), now - 40_000)));

        DiscoveryClient client = new DiscoveryClient(redisOps, 10);

        // Default threshold is 30s -> only the healthy instance survives
        List<ServiceInstance> healthy = client.getInstances(serviceName);
        assertEquals(1, healthy.size());
        assertEquals(healthyId, healthy.get(0).getId());

        // Skipping the liveness check returns both
        List<ServiceInstance> all = client.getInstances(serviceName, false, Constants.SD_NO_HEALTH_CHECK);
        assertEquals(2, all.size());
    }

    @Test
    void discoverRoundRobinPicksFromFetchedInstances() {
        String serviceName = "test-service";
        String instanceId = "test-service:123";
        ServiceInstance instance = ServiceInstance.builder().id(instanceId).host("127.0.0.1").port(8080).build();

        when(redisOps.fetchActiveServiceInstances(serviceName)).thenReturn(Map.of(
                instanceId, new RedisOps.ActiveInstanceRecord(instance.toJson(), System.currentTimeMillis())));

        DiscoveryClient client = new DiscoveryClient(redisOps, 10);
        Optional<ServiceInstance> discovered = client.discover(serviceName, "round-robin");

        assertTrue(discovered.isPresent());
        assertEquals(instanceId, discovered.get().getId());
    }
}
