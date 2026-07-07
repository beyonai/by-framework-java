package com.iwhaleai.byai.framework.core.availability;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for WakeupController: reads wakeup requests from the management
 * stream, dedupes/claims them, delegates to a WakeupProvider, and writes
 * the decision back.
 */
@ExtendWith(MockitoExtension.class)
class WakeupControllerTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private WakeupProvider wakeupProvider;

    private WakeupController controller;

    private WakeupRequest sampleRequest() {
        return WakeupRequest.builder()
                .executionId("exec-1")
                .targetAgentType("agent-x")
                .userCode("user-1")
                .region("us-east")
                .build();
    }

    private void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        controller = new WakeupController(redisClient, wakeupProvider, 3600, 3);
    }

    @Test
    void runOnceReturnsNullWhenNoEntriesArrive() {
        setUp();
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(null);

        StreamEntryID result = controller.runOnce(null, 1000);

        assertNull(result);
        verify(jedis).xread(any(XReadParams.class), eq(Map.of(
                Constants.QueueNames.controlPlaneManagementStream(), new StreamEntryID("0-0"))));
    }

    @Test
    void runOnceProcessesEntryAndClaimsDedupeThenWritesDecision() {
        setUp();
        WakeupRequest request = sampleRequest();
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), request.toRedisPayload());
        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(Constants.QueueNames.controlPlaneManagementStream(), List.of(entry)));
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(xreadResult);

        String dedupeKey = Constants.QueueNames.controlPlaneWakeupDedupe("agent-x", "user-1", "us-east");
        when(jedis.get(dedupeKey)).thenReturn(null);
        when(jedis.set(eq(dedupeKey), eq("exec-1"), any(SetParams.class))).thenReturn("OK");

        WakeupDecision decision = WakeupDecision.builder()
                .executionId("exec-1")
                .targetAgentType("agent-x")
                .status(WakeupDecisionStatus.READY)
                .workerId("worker-1")
                .timestamp(1000L)
                .build();
        when(wakeupProvider.wakeup(any(WakeupRequest.class))).thenReturn(decision);
        when(jedis.xadd(anyString(), any(redis.clients.jedis.params.XAddParams.class), anyMap()))
                .thenReturn(new StreamEntryID("2-0"));

        StreamEntryID result = controller.runOnce(null, 1000);

        assertEquals(new StreamEntryID("1-0"), result);
        verify(jedis).set(eq(dedupeKey), eq("exec-1"), any(SetParams.class));

        String decisionStream = Constants.QueueNames.controlPlaneDecisionStream("exec-1");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(decisionStream), any(redis.clients.jedis.params.XAddParams.class),
                fieldsCaptor.capture());
        WakeupDecision written = WakeupDecision.fromDict(fieldsCaptor.getValue());
        assertEquals(WakeupDecisionStatus.READY, written.getStatus());
        assertEquals("worker-1", written.getWorkerId());
        verify(jedis).expire(decisionStream, 300);
    }

    @Test
    void runOnceSkipsAlreadyClaimedWakeup() {
        setUp();
        WakeupRequest request = sampleRequest();
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), request.toRedisPayload());
        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(Constants.QueueNames.controlPlaneManagementStream(), List.of(entry)));
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(xreadResult);

        String dedupeKey = Constants.QueueNames.controlPlaneWakeupDedupe("agent-x", "user-1", "us-east");
        when(jedis.get(dedupeKey)).thenReturn(null);
        // SET NX fails (returns null) because another process already claimed it
        when(jedis.set(eq(dedupeKey), eq("exec-1"), any(SetParams.class))).thenReturn(null);

        controller.runOnce(null, 1000);

        verify(wakeupProvider, never()).wakeup(any());
        verify(jedis, never()).xadd(anyString(), any(redis.clients.jedis.params.XAddParams.class), anyMap());
    }

    @Test
    void runOnceDropsWakeupExceedingMaxAttempts() {
        setUp();
        WakeupRequest request = sampleRequest();
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), request.toRedisPayload());
        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(Constants.QueueNames.controlPlaneManagementStream(), List.of(entry)));
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(xreadResult);

        String dedupeKey = Constants.QueueNames.controlPlaneWakeupDedupe("agent-x", "user-1", "us-east");
        when(jedis.get(dedupeKey)).thenReturn("3");

        controller.runOnce(null, 1000);

        verify(wakeupProvider, never()).wakeup(any());
        verify(jedis, never()).set(eq(dedupeKey), anyString(), any(SetParams.class));
    }

    @Test
    void runOnceWritesFailedDecisionWhenProviderThrows() {
        setUp();
        WakeupRequest request = sampleRequest();
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), request.toRedisPayload());
        List<Map.Entry<String, List<StreamEntry>>> xreadResult = List.of(
                new AbstractMap.SimpleEntry<>(Constants.QueueNames.controlPlaneManagementStream(), List.of(entry)));
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(xreadResult);

        String dedupeKey = Constants.QueueNames.controlPlaneWakeupDedupe("agent-x", "user-1", "us-east");
        when(jedis.get(dedupeKey)).thenReturn(null);
        when(jedis.set(eq(dedupeKey), eq("exec-1"), any(SetParams.class))).thenReturn("OK");
        when(wakeupProvider.wakeup(any(WakeupRequest.class))).thenThrow(new RuntimeException("provider boom"));
        when(jedis.xadd(anyString(), any(redis.clients.jedis.params.XAddParams.class), anyMap()))
                .thenReturn(new StreamEntryID("2-0"));

        controller.runOnce(null, 1000);

        String decisionStream = Constants.QueueNames.controlPlaneDecisionStream("exec-1");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(decisionStream), any(redis.clients.jedis.params.XAddParams.class),
                fieldsCaptor.capture());
        WakeupDecision written = WakeupDecision.fromDict(fieldsCaptor.getValue());
        assertEquals(WakeupDecisionStatus.FAILED, written.getStatus());
        assertTrue(written.getReason().contains("provider boom"));
    }
}
