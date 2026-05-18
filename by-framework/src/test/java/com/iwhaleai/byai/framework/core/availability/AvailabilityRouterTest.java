package com.iwhaleai.byai.framework.core.availability;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentMatchers;

/**
 * Tests for AvailabilityRouter, mirroring Python's test_availability.py.
 */
@ExtendWith(MockitoExtension.class)
class AvailabilityRouterTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private WorkerRegistry workerRegistry;

    private AvailabilityRouter router;

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        // Default: circuit breaker and quota are not exceeded
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:circuit_breaker:"))).thenReturn(null);
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:quota:"))).thenReturn(null);
        router = new AvailabilityRouter(redisClient, workerRegistry, 30000, 5, 1000);
    }

    // ---- helper to create a test delivery intent ----

    private DeliveryIntent makeIntent(String agentType, String policy) {
        return makeIntent(agentType, policy, System.currentTimeMillis(), "exec-123", "msg-123");
    }

    private DeliveryIntent makeIntent(String agentType, String policy, String executionId, String messageId) {
        return makeIntent(agentType, policy, System.currentTimeMillis(), executionId, messageId);
    }

    private DeliveryIntent makeIntent(String agentType, String policy, long timeoutMs, String executionId, String messageId) {
        return DeliveryIntent.builder()
                .executionId(executionId)
                .messageId(messageId)
                .sessionId("sess-1")
                .traceId("trace-1")
                .source("caller-agent")
                .targetAgentType(agentType)
                .userCode("user-1")
                .policy(policy)
                .timeoutMs(timeoutMs > 0 ? timeoutMs : 30000)
                .metadata(new HashMap<>())
                .build();
    }

    // ===== Tests =====

    @Test
    void failFastWithNoWorkerShouldReject() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.REJECT, result.getStatus());
        assertTrue(result.getReason().contains("No online worker for agent_type"));
    }

    @Test
    void failFastWithWorkerShouldDeliverNow() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(true, List.of("worker-1")));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.DELIVER_NOW, result.getStatus());
        assertEquals("agent-x", result.getSelectedAgentType());
        assertEquals(Constants.QueueNames.ctrlStream("agent-x"), result.getStreamName());
    }

    @Test
    void sendAnywayShouldDeliverNowWithoutOnlineCheck() {
        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.SEND_ANYWAY);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.DELIVER_NOW, result.getStatus());
        // SEND_ANYWAY should NOT check online workers
        verify(workerRegistry, never()).hasOnlineAgentType(anyString(), anyBoolean(), anyLong());
    }

    @Test
    void wakeAndWaitHappyPathShouldWaitAndDeliver() {
        // No online workers initially
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        // XADD for wakeup emission
        when(jedis.xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));

        // XREAD returns a READY decision
        Map<String, String> decisionFields = new HashMap<>();
        decisionFields.put("execution_id", "exec-123");
        decisionFields.put("status", WakeupDecisionStatus.READY);
        decisionFields.put("reason", "Worker woke up");
        decisionFields.put("worker_id", "worker-woke");
        decisionFields.put("timestamp", String.valueOf(System.currentTimeMillis()));

        StreamEntry decisionEntry = new StreamEntry(
                new StreamEntryID("1-0"), decisionFields);

        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(
                        Constants.QueueNames.controlPlaneDecisionStream("exec-123"),
                        List.of(decisionEntry)));

        when(jedis.xread(any(XReadParams.class), ArgumentMatchers.<Map<String, StreamEntryID>>any()))
                .thenReturn(xreadResult);
        when(jedis.xdel(anyString(), (StreamEntryID) any())).thenReturn(1L);

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.WAKE_AND_WAIT);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.WAIT_AND_DELIVER, result.getStatus());
        assertEquals("agent-x", result.getSelectedAgentType());
        assertEquals("worker-woke", result.getTargetWorkerId());
        assertEquals(Constants.QueueNames.ctrlStream("agent-x"), result.getStreamName());

        // Verify wakeup was emitted
        verify(jedis).xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap());
    }

    @Test
    void wakeAndWaitTimeoutShouldReject() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        when(jedis.xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));

        // XREAD returns empty (no decision within timeout)
        when(jedis.xread(any(XReadParams.class), ArgumentMatchers.<Map<String, StreamEntryID>>any())).thenReturn(null);

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.WAKE_AND_WAIT, 500, "exec-123", "msg-123");
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.REJECT, result.getStatus());
        assertTrue(result.getReason().contains("TIMEOUT"));
    }

    @Test
    void wakeAndQueueShouldReturnQueuePending() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        when(jedis.xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));
        when(jedis.xadd(eq(Constants.QueueNames.controlPlanePendingQueue("agent-x")),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.WAKE_AND_QUEUE);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.QUEUE_PENDING, result.getStatus());
        assertEquals("agent-x", result.getSelectedAgentType());

        // Verify wakeup was emitted AND pending delivery was queued
        verify(jedis).xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap());
        verify(jedis).xadd(eq(Constants.QueueNames.controlPlanePendingQueue("agent-x")),
                (StreamEntryID) isNull(), anyMap());
    }

    @Test
    void queueOnlyShouldReturnQueuePending() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        when(jedis.xadd(eq(Constants.QueueNames.controlPlanePendingQueue("agent-x")),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.QUEUE_ONLY);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.QUEUE_PENDING, result.getStatus());

        // Verify pending delivery was queued, but NO wakeup emitted
        verify(jedis).xadd(eq(Constants.QueueNames.controlPlanePendingQueue("agent-x")),
                (StreamEntryID) isNull(), anyMap());
        verify(jedis, never()).xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap());
    }

    @Test
    void circuitBreakerOpenShouldReject() {
        // Circuit breaker: key returns threshold value
        String cbKey = Constants.QueueNames.controlPlaneCircuitBreakerKey() + ":agent-x";
        when(jedis.get(cbKey)).thenReturn("5"); // >= threshold

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.REJECT, result.getStatus());
        assertTrue(result.getReason().contains("Circuit breaker open"));
        assertEquals("ERR_AGENT_CIRCUIT_OPEN", result.getErrorCode());
    }

    @Test
    void quotaExceededShouldReject() {
        // Quota: key returns limit value
        String quotaKey = Constants.QueueNames.controlPlaneQuotaKey("user-1");
        when(jedis.get(quotaKey)).thenReturn("1000"); // >= limit

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.REJECT, result.getStatus());
        assertTrue(result.getReason().contains("Quota exceeded"));
        assertEquals("ERR_TENANT_QUOTA_EXCEEDED", result.getErrorCode());
    }

    @Test
    void circuitBreakerUnderThresholdShouldPass() {
        String cbKey = Constants.QueueNames.controlPlaneCircuitBreakerKey() + ":agent-x";
        when(jedis.get(cbKey)).thenReturn("3"); // < threshold

        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(true, List.of("worker-1")));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.DELIVER_NOW, result.getStatus());
    }

    @Test
    void quotaUnderLimitShouldPass() {
        String quotaKey = Constants.QueueNames.controlPlaneQuotaKey("user-1");
        when(jedis.get(quotaKey)).thenReturn("500"); // < limit

        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(true, List.of("worker-1")));

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.FAIL_FAST);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.DELIVER_NOW, result.getStatus());
    }

    @Test
    void executionIdCorrelationInWakeup() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        when(jedis.xadd(eq(Constants.QueueNames.controlPlaneManagementStream()),
                (StreamEntryID) isNull(), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));

        // XREAD returns a READY decision
        Map<String, String> decisionFields = new HashMap<>();
        decisionFields.put("execution_id", "my-exec-id");
        decisionFields.put("status", WakeupDecisionStatus.READY);
        decisionFields.put("reason", "");
        decisionFields.put("worker_id", "w-1");
        decisionFields.put("timestamp", String.valueOf(System.currentTimeMillis()));

        StreamEntry decisionEntry = new StreamEntry(new StreamEntryID("1-0"), decisionFields);
        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(
                        Constants.QueueNames.controlPlaneDecisionStream("my-exec-id"),
                        List.of(decisionEntry)));

        when(jedis.xread(any(XReadParams.class), ArgumentMatchers.<Map<String, StreamEntryID>>any())).thenReturn(xreadResult);
        when(jedis.xdel(anyString(), (StreamEntryID) any())).thenReturn(1L);

        DeliveryIntent intent = makeIntent("agent-x", RoutePolicy.WAKE_AND_WAIT, 30000, "my-exec-id", "msg-xyz");
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.WAIT_AND_DELIVER, result.getStatus());

        // Verify decision stream key uses the correct execution_id
        verify(jedis).xdel(eq(Constants.QueueNames.controlPlaneDecisionStream("my-exec-id")),
                (StreamEntryID) any());
    }

    @Test
    void wakeupDecisionReadyStatus() {
        Map<String, String> fields = new HashMap<>();
        fields.put("execution_id", "exec-1");
        fields.put("status", WakeupDecisionStatus.READY);
        fields.put("reason", "ok");
        fields.put("worker_id", "w-1");
        fields.put("timestamp", "1000");

        WakeupDecision decision = WakeupDecision.fromDict(fields);
        assertEquals("exec-1", decision.getExecutionId());
        assertEquals(WakeupDecisionStatus.READY, decision.getStatus());
        assertEquals("w-1", decision.getWorkerId());
    }

    @Test
    void wakeupDecisionFailedStatus() {
        Map<String, String> fields = new HashMap<>();
        fields.put("execution_id", "exec-2");
        fields.put("status", WakeupDecisionStatus.FAILED);
        fields.put("reason", "Scaling limit reached");
        fields.put("worker_id", "");
        fields.put("timestamp", "2000");

        WakeupDecision decision = WakeupDecision.fromDict(fields);
        assertEquals(WakeupDecisionStatus.FAILED, decision.getStatus());
        assertEquals("Scaling limit reached", decision.getReason());
    }

    @Test
    void wakeupDecisionRejectedStatus() {
        Map<String, String> fields = new HashMap<>();
        fields.put("execution_id", "exec-3");
        fields.put("status", WakeupDecisionStatus.REJECTED);
        fields.put("reason", "Circuit breaker open");
        fields.put("worker_id", "");
        fields.put("timestamp", "3000");

        WakeupDecision decision = WakeupDecision.fromDict(fields);
        assertEquals(WakeupDecisionStatus.REJECTED, decision.getStatus());
    }

    @Test
    void defaultPolicyIsFailFastWhenNull() {
        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        DeliveryIntent intent = makeIntent("agent-x", null);
        AvailabilityResult result = router.prepareDelivery(intent);

        assertEquals(AvailabilityStatus.REJECT, result.getStatus());
    }

    @Test
    void recordFailureIncrementsCircuitBreaker() {
        when(jedis.incr(anyString())).thenReturn(1L);
        when(jedis.expire(anyString(), anyLong())).thenReturn(1L);

        router.recordFailure("agent-x");

        String cbKey = Constants.QueueNames.controlPlaneCircuitBreakerKey() + ":agent-x";
        verify(jedis).incr(cbKey);
        verify(jedis).expire(cbKey, 60L);
    }

    @Test
    void wakeupRequestToRedisPayloadRoundTrip() {
        WakeupRequest request = WakeupRequest.builder()
                .executionId("exec-1")
                .agentType("agent-x")
                .reason("test")
                .priority("high")
                .requestedAt(System.currentTimeMillis())
                .ttlMs(30000)
                .metadata(Map.of("key", "value"))
                .build();

        Map<String, String> payload = request.toRedisPayload();
        assertNotNull(payload);
        assertTrue(payload.containsKey("data"));

        WakeupRequest back = WakeupRequest.fromDict(payload);
        assertEquals("exec-1", back.getExecutionId());
        assertEquals("agent-x", back.getAgentType());
        assertEquals("high", back.getPriority());
    }

    @Test
    void pendingDeliveryToRedisPayloadRoundTrip() {
        PendingDelivery pending = PendingDelivery.builder()
                .executionId("exec-1")
                .messageId("msg-1")
                .sessionId("sess-1")
                .traceId("trace-1")
                .source("caller")
                .targetAgentType("agent-x")
                .userCode("user-1")
                .priority("high")
                .policy(RoutePolicy.WAKE_AND_QUEUE)
                .queuedAt(System.currentTimeMillis())
                .timeoutMs(30000)
                .commandPayload(Map.of("cmd", "data"))
                .metadata(Map.of("m", "v"))
                .build();

        Map<String, String> payload = pending.toRedisPayload();
        assertNotNull(payload);

        PendingDelivery back = PendingDelivery.fromDict(payload);
        assertEquals("exec-1", back.getExecutionId());
        assertEquals("agent-x", back.getTargetAgentType());
        assertEquals(RoutePolicy.WAKE_AND_QUEUE, back.getPolicy());
    }

    @Test
    void deliveryIntentDefaultsApplied() {
        DeliveryIntent intent = DeliveryIntent.builder()
                .executionId("exec-1")
                .messageId("msg-1")
                .sessionId("sess-1")
                .traceId("trace-1")
                .targetAgentType("agent-x")
                .build();

        // Null policy and zero timeout should be defaulted in prepareDelivery
        assertNull(intent.getPolicy());
        assertEquals(0, intent.getTimeoutMs());

        when(workerRegistry.hasOnlineAgentType(eq("agent-x"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(true, List.of("worker-1")));

        AvailabilityResult result = router.prepareDelivery(intent);
        assertEquals(AvailabilityStatus.DELIVER_NOW, result.getStatus());
    }
}
