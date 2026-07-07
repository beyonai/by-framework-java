package com.iwhaleai.byai.framework.core;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * WorkerRegistry 测试，对标 Python test_registry.py
 */
@ExtendWith(MockitoExtension.class)
class WorkerRegistryTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private WorkerRegistry registry;

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        registry = new WorkerRegistry(redisClient);
    }

    @Test
    void registerWorkerMembershipAddsToAgentTypeMembers() {
        registry.registerWorkerMembership("worker-1", List.of("agent-a", "agent-b"));

        verify(jedis).sadd(eq(Constants.RegistryKeys.knownWorkers()), eq("worker-1"));
        // Implementation calls sadd separately for each agent type
        verify(jedis, times(2)).sadd(eq(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")), anyString());
        verify(jedis).sadd(eq(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")), eq("agent-a"));
        verify(jedis).sadd(eq(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")), eq("agent-b"));
        verify(jedis).sadd(eq(Constants.RegistryKeys.agentTypeMembers("agent-a")), eq("worker-1"));
        verify(jedis).sadd(eq(Constants.RegistryKeys.agentTypeMembers("agent-b")), eq("worker-1"));
    }

    @Test
    void registerWorkerMembershipWithNullAgentTypes() {
        registry.registerWorkerMembership("worker-1", null);

        verify(jedis, never()).sadd(eq(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")), any(String[].class));
    }

    @Test
    void registerWorkerMembershipWithEmptyAgentTypes() {
        registry.registerWorkerMembership("worker-1", List.of());

        verify(jedis, never()).sadd(eq(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")), any(String[].class));
    }

    @Test
    void heartbeatWorkerUpdatesPresenceLease() {
        registry.heartbeatWorker("worker-1", 15);

        verify(jedis, never()).zadd(anyString(), anyDouble(), anyString());
        verify(jedis).setex(eq(Constants.RegistryKeys.workerOnlineLease("worker-1")), eq(15L),
                contains("\"last_seen\""));
        verify(jedis).sadd(eq(Constants.RegistryKeys.knownWorkers()), eq("worker-1"));
    }

    @Test
    void getTargetWorkerDelegatesToSrandmember() {
        // Implementation uses smembers to get all workers, then filters by isWorkerOnline
        when(jedis.smembers(Constants.RegistryKeys.agentTypeMembers("agent-x")))
                .thenReturn(Set.of("worker-42"));
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-42"))).thenReturn("1");

        String result = registry.getTargetWorker("agent-x");

        assertEquals("worker-42", result);
    }

    @Test
    void getTargetWorkerReturnsNullWhenNoWorkerAvailable() {
        when(jedis.smembers(Constants.RegistryKeys.agentTypeMembers("agent-x"))).thenReturn(Collections.emptySet());

        String result = registry.getTargetWorker("agent-x");

        assertNull(result);
    }

    @Test
    void claimWorkerIdSucceeds() {
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");

        String token = registry.claimWorkerId("worker-1");

        assertNotNull(token);
        assertFalse(token.isEmpty());
        verify(jedis).set(eq(Constants.RegistryKeys.workerOnlineLease("worker-1")), contains(token), any(SetParams.class));
        verify(jedis).sadd(eq(Constants.RegistryKeys.knownWorkers()), eq("worker-1"));
    }

    @Test
    void claimWorkerIdDuplicateThrowsException() {
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn(null);

        assertThrows(RuntimeException.class, () -> registry.claimWorkerId("worker-1"));
    }

    @Test
    void refreshWorkerIdLockExtendsExpiry() throws Exception {
        // Directly inject the token into lockTokens map via reflection
        String token = "test-token-123";
        java.lang.reflect.Field lockTokensField = WorkerRegistry.class.getDeclaredField("lockTokens");
        lockTokensField.setAccessible(true);
        @SuppressWarnings("unchecked")
        java.util.Map<String, String> lockTokens = (java.util.Map<String, String>) lockTokensField.get(registry);
        lockTokens.put("worker-1", token);

        // Mock jedis.get to return the token and expire to return success
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":\"" + token + "\",\"last_seen\":0}");
        when(jedis.expire(Constants.RegistryKeys.workerOnlineLease("worker-1"), 60)).thenReturn(1L);

        boolean result = registry.refreshWorkerIdLock("worker-1");

        assertTrue(result);
        verify(jedis).expire(Constants.RegistryKeys.workerOnlineLease("worker-1"), 60);
    }

    @Test
    void refreshWorkerIdLockSkipsWhenRedisTokenDoesNotMatch() {
        // First claim the worker to populate lockTokens
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        registry.claimWorkerId("worker-1");

        // But Redis has a different token stored
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":\"different-token\",\"last_seen\":0}");

        boolean result = registry.refreshWorkerIdLock("worker-1");

        assertFalse(result);
        verify(jedis, never()).expire(anyString(), anyLong());
    }

    @Test
    void releaseWorkerIdDeletesLockWhenTokenMatches() {
        String token = "lock-token";
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":\"" + token + "\",\"last_seen\":0}");

        boolean result = registry.releaseWorkerId("worker-1", token);

        assertTrue(result);
        verify(jedis).del(Constants.RegistryKeys.workerOnlineLease("worker-1"));
    }

    @Test
    void releaseWorkerIdSkipsDeleteWhenTokenMismatch() {
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":\"different-token\",\"last_seen\":0}");

        boolean result = registry.releaseWorkerId("worker-1", "my-token");

        assertFalse(result);
        verify(jedis, never()).del(anyString());
    }

    @Test
    void saveExecutionStoresInRedisHash() {
        Map<String, Object> execution = new HashMap<>();
        execution.put("execution_id", "exec-1");
        execution.put("message_id", "msg-1");
        execution.put("session_id", "sess-1");
        execution.put("worker_id", "worker-1");
        execution.put("status", "RUNNING");

        registry.saveExecution(execution);

        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");
        verify(jedis).hset(eq(regKey), eq("exec:exec-1"), anyString());
        verify(jedis).hset(eq(regKey), eq("msg_map:msg-1"), eq("exec-1"));
        verify(jedis).expire(eq(regKey), eq((long) Constants.DEFAULT_SESSION_TTL));
    }

    @Test
    void getExecutionReturnsDecodedData() {
        String json = "{\"execution_id\":\"exec-1\",\"worker_id\":\"worker-1\",\"status\":\"RUNNING\",\"created_at\":1700000000,\"cancel_requested\":false}";
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");

        when(jedis.hget(regKey, "exec:exec-1")).thenReturn(json);

        Map<String, Object> result = registry.getExecution("exec-1", "sess-1");

        assertNotNull(result);
        assertEquals("exec-1", result.get("execution_id"));
        assertEquals("worker-1", result.get("worker_id"));
        assertEquals("RUNNING", result.get("status"));
        assertEquals(1700000000, result.get("created_at"));
        assertEquals(false, result.get("cancel_requested"));
    }

    @Test
    void getExecutionReturnsNullWhenNotFound() {
        when(jedis.hget(anyString(), anyString())).thenReturn(null);

        Map<String, Object> result = registry.getExecution("nonexistent", "sess-1");

        assertNull(result);
    }

    @Test
    void getExecutionByMessageIdLooksUpExecutionId() {
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");
        when(jedis.hget(regKey, "msg_map:msg-1")).thenReturn("exec-1");
        String json = "{\"execution_id\":\"exec-1\",\"status\":\"RUNNING\"}";
        when(jedis.hget(regKey, "exec:exec-1")).thenReturn(json);

        Map<String, Object> result = registry.getExecutionByMessageId("msg-1", "sess-1");

        assertNotNull(result);
        assertEquals("exec-1", result.get("execution_id"));
    }

    @Test
    void getExecutionByMessageIdReturnsNullWhenNoMapping() {
        when(jedis.hget(anyString(), anyString())).thenReturn(null);

        Map<String, Object> result = registry.getExecutionByMessageId("missing-msg", "sess-1");

        assertNull(result);
    }

    @Test
    void markExecutionCancellingSetsStatus() {
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");
        String existingJson = "{\"execution_id\":\"exec-1\",\"status\":\"RUNNING\"}";
        when(jedis.hget(regKey, "exec:exec-1")).thenReturn(existingJson);

        registry.markExecutionCancelling("exec-1", "sess-1", "user requested");

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(jedis).hset(eq(regKey), eq("exec:exec-1"), captor.capture());

        String updatedJson = captor.getValue();
        assertTrue(updatedJson.contains("\"status\":\"CANCELLING\""));
        assertTrue(updatedJson.contains("\"cancel_requested\":true"));
        assertTrue(updatedJson.contains("\"cancel_reason\":\"user requested\""));
    }

    @Test
    void markExecutionCancellingNoopWhenNotFound() {
        when(jedis.hget(anyString(), anyString())).thenReturn(null);

        registry.markExecutionCancelling("nonexistent", "sess-1", "reason");

        verify(jedis, never()).hset(anyString(), anyString(), anyString());
    }

    @Test
    void markExecutionFinishedSetsStatusAndTimestamps() {
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");
        String existingJson = "{\"execution_id\":\"exec-1\",\"status\":\"RUNNING\"}";
        when(jedis.hget(regKey, "exec:exec-1")).thenReturn(existingJson);

        registry.markExecutionFinished("exec-1", "sess-1", "COMPLETED");

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(jedis).hset(eq(regKey), eq("exec:exec-1"), captor.capture());

        String updatedJson = captor.getValue();
        assertTrue(updatedJson.contains("\"status\":\"COMPLETED\""));
        assertTrue(updatedJson.contains("\"finished_at\""));
        assertTrue(updatedJson.contains("\"updated_at\""));
    }

    @Test
    void markExecutionFinishedNoopWhenNotFound() {
        when(jedis.hget(anyString(), anyString())).thenReturn(null);

        registry.markExecutionFinished("nonexistent", "sess-1", "COMPLETED");

        verify(jedis, never()).hset(anyString(), anyString(), anyString());
    }

    @Test
    void executionLifecycleFullFlow() {
        // Save
        Map<String, Object> execution = new HashMap<>();
        execution.put("execution_id", "exec-lifecycle");
        execution.put("message_id", "msg-lifecycle");
        execution.put("session_id", "sess-lifecycle");
        execution.put("worker_id", "worker-lifecycle");
        execution.put("status", "RUNNING");
        execution.put("cancel_requested", false);

        registry.saveExecution(execution);
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-lifecycle");
        verify(jedis).hset(eq(regKey), eq("exec:exec-lifecycle"), anyString());

        // Get by message_id
        when(jedis.hget(regKey, "msg_map:msg-lifecycle")).thenReturn("exec-lifecycle");
        String json = "{\"execution_id\":\"exec-lifecycle\",\"status\":\"RUNNING\",\"cancel_requested\":false}";
        when(jedis.hget(regKey, "exec:exec-lifecycle")).thenReturn(json);

        Map<String, Object> fetched = registry.getExecutionByMessageId("msg-lifecycle", "sess-lifecycle");
        assertNotNull(fetched);
        assertEquals("RUNNING", fetched.get("status"));
        assertEquals(false, fetched.get("cancel_requested"));

        // Mark cancelling
        registry.markExecutionCancelling("exec-lifecycle", "sess-lifecycle", "timeout");
        verify(jedis, atLeastOnce()).hset(eq(regKey), eq("exec:exec-lifecycle"), anyString());

        // Mark finished
        registry.markExecutionFinished("exec-lifecycle", "sess-lifecycle", "CANCELLED");
    }

    @Test
    void getWorkerReturnsAgentTypesAndLastSeen() {
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":null,\"last_seen\":1700000000}");
        when(jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")))
                .thenReturn(Set.of("agent-a", "agent-b"));

        Map<String, Object> result = registry.getWorker("worker-1");

        assertNotNull(result);
        assertEquals(1700000000L, result.get("last_seen"));
        assertTrue(((Set<String>) result.get("agent_types")).contains("agent-a"));
    }

    @Test
    void getWorkerReturnsNullWhenNotRegistered() {
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("unknown"))).thenReturn(null);

        Map<String, Object> result = registry.getWorker("unknown");

        assertNull(result);
    }

    @Test
    void decodeBooleanFieldsCorrectly() {
        String json = "{\"cancel_requested\":true}";
        String regKey = Constants.RegistryKeys.sessionRegistry("sess-1");
        when(jedis.hget(regKey, "exec:exec-bool")).thenReturn(json);

        Map<String, Object> result = registry.getExecution("exec-bool", "sess-1");
        assertEquals(true, result.get("cancel_requested"));
    }

    @Test
    void getAllWorkersReturnsActiveWorkersOnly() {
        // Given
        when(jedis.smembers(Constants.RegistryKeys.knownWorkers()))
                .thenReturn(Set.of("worker-1", "worker-2"));
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1")))
                .thenReturn("{\"version\":1,\"token\":null,\"last_seen\":1000}");
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-2")))
                .thenReturn("{\"version\":1,\"token\":null,\"last_seen\":0}");
        when(jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes("worker-1")))
                .thenReturn(Set.of("agent-a"));

        // When
        Map<String, Map<String, Object>> result = registry.getAllWorkers();

        // Then
        assertEquals(1, result.size());
        assertTrue(result.containsKey("worker-1"));
        assertFalse(result.containsKey("worker-2"));
        assertEquals(1000L, result.get("worker-1").get("last_seen"));
    }

    @Test
    void getAllWorkersReturnsEmptyMapWhenNoWorkers() {
        // Given
        when(jedis.smembers(Constants.RegistryKeys.knownWorkers())).thenReturn(Set.of());

        // When
        Map<String, Map<String, Object>> result = registry.getAllWorkers();

        // Then
        assertTrue(result.isEmpty());
    }
}
