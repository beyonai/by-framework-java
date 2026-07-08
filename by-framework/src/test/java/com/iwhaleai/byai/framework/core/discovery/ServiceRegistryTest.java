package com.iwhaleai.byai.framework.core.discovery;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * ServiceRegistry tested against the RedisOps seam (Cluster-compatible by
 * construction) instead of a raw Jedis mock, per the discovery Cluster
 * compatibility work.
 */
@ExtendWith(MockitoExtension.class)
class ServiceRegistryTest {

    @Mock
    private RedisOps redisOps;

    @Test
    void registerWritesInstanceDetailsActiveIndexAndServiceIndex() {
        ServiceRegistry registry = new ServiceRegistry(redisOps);

        registry.register("test-service", 8080);

        ServiceInstance instance = registry.getCurrentInstance();
        assertNotNull(instance);
        assertEquals(8080, instance.getPort());

        verify(redisOps).registerServiceInstance(eq("test-service"), eq(instance.getId()), anyString(), anyLong());
        verify(redisOps).sadd(Constants.RegistryKeys.sdServices(), "test-service");
    }
}
