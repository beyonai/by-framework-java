package com.iwhaleai.byai.framework.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.XAddParams;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for WorkerManager: admin API for worker lifecycle and agent-type
 * admission control. Delegates registry state changes to WorkerRegistry and
 * pushes lifecycle commands directly to the worker's own ctrl stream.
 */
@ExtendWith(MockitoExtension.class)
class WorkerManagerTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private WorkerRegistry registry;

    private WorkerManager manager;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        manager = new WorkerManager(redisClient, registry);
    }

    @Test
    void suspendWorkerUpdatesAdminStateRemovesFromMembersAndPushesCommand() throws Exception {
        manager.suspendWorker("worker-1", "maintenance");

        verify(registry).setWorkerAdminState("worker-1", "suspended", "maintenance");
        verify(registry).removeWorkerFromTypeMembers("worker-1");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(Constants.QueueNames.workerCtrlStream("worker-1")),
                any(XAddParams.class), fieldsCaptor.capture());
        Map<String, Object> data = objectMapper.readValue(fieldsCaptor.getValue().get("data"), Map.class);
        assertEquals("maintenance", ((Map<?, ?>) data.get("body")).get("reason"));
    }

    @Test
    void resumeWorkerUpdatesAdminStateRestoresMembersAndPushesCommand() {
        manager.resumeWorker("worker-1");

        verify(registry).setWorkerAdminState("worker-1", "active", "");
        verify(registry).restoreWorkerToTypeMembers("worker-1");
        verify(jedis).xadd(eq(Constants.QueueNames.workerCtrlStream("worker-1")),
                any(XAddParams.class), anyMap());
    }

    @Test
    void evictWorkerUpdatesAdminStateRemovesFromMembersAndPushesCommand() throws Exception {
        manager.evictWorker("worker-1", true, "shutting down");

        verify(registry).setWorkerAdminState("worker-1", "evicted", "shutting down");
        verify(registry).removeWorkerFromTypeMembers("worker-1");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(Constants.QueueNames.workerCtrlStream("worker-1")),
                any(XAddParams.class), fieldsCaptor.capture());
        Map<String, Object> data = objectMapper.readValue(fieldsCaptor.getValue().get("data"), Map.class);
        Map<?, ?> body = (Map<?, ?>) data.get("body");
        assertEquals("shutting down", body.get("reason"));
        assertEquals(true, body.get("force"));
    }

    @Test
    void denyAndAllowWorkerForTypeDelegateToRegistry() {
        manager.denyWorkerForType("agent-x", "worker-1");
        verify(registry).denyWorkerForType("agent-x", "worker-1");

        manager.allowWorkerForType("agent-x", "worker-1");
        verify(registry).allowWorkerForType("agent-x", "worker-1");
    }

    @Test
    void getTypeDenylistDelegatesToRegistry() {
        when(registry.getAgentTypeDenylist("agent-x")).thenReturn(List.of("worker-1", "worker-2"));

        List<String> result = manager.getTypeDenylist("agent-x");

        assertEquals(List.of("worker-1", "worker-2"), result);
    }

    @Test
    void getAndClearWorkerAdminStateDelegateToRegistry() {
        when(registry.getWorkerAdminState("worker-1")).thenReturn(Map.of("lifecycle", "suspended"));

        assertEquals(Map.of("lifecycle", "suspended"), manager.getWorkerAdminState("worker-1"));

        manager.clearWorkerAdminState("worker-1");
        verify(registry).clearWorkerAdminState("worker-1");
    }
}
