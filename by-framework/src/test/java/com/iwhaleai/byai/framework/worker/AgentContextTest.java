package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AgentContextTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private AgentContext context;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        // AvailabilityRouter checks circuit breaker and quota before online check
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:circuit:"))).thenReturn(null);
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:quota:"))).thenReturn(null);

        context = new AgentContext(
                "sess-1",
                "trace-1",
                redisClient,
                "test-agent",
                "msg-1");
    }

    @Test
    void emitChunkSendsToRedisPipeline() throws Exception {
        context.emitChunk("hello");

        String expectedStream = "byai_gateway:session:sess-1:data_stream";
        verify(jedis, atLeastOnce()).xadd(eq(expectedStream), any(XAddParams.class), anyMap());
        verify(jedis, atLeastOnce()).expire(eq(expectedStream), eq((long) Constants.DEFAULT_SESSION_TTL));
    }

    @Test
    void emitArtifactSetsArtifactUrl() throws Exception {
        context.emitArtifact("artifact-url");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        Map<String, Object> data = objectMapper.readValue(dataJson, Map.class);
        assertEquals("artifact-url", data.get("artifact_url"));
    }

    @Test
    void askUserReturnsWaitingStatus() throws Exception {
        Map<String, String> result = context.askUser("prompt content");

        assertEquals(Map.of("status", "WAITING_USER"), result);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), anyMap());
    }

    @Test
    void askUserEmitsCorrectPromptInStateMsg() throws Exception {
        context.askUser("Please confirm");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        Map<String, Object> data = objectMapper.readValue(dataJson, Map.class);
        assertEquals("Please confirm", data.get("state_msg"));
    }

    @Test
    void callAgentSendsAskAgentCommand() throws Exception {
        // Mock WorkerRegistry.hasOnlineAgentType redis calls
        String capKey = Constants.RegistryKeys.agentTypeMembers("target-agent");
        when(jedis.smembers(capKey)).thenReturn(java.util.Set.of("worker-1"));
        // isWorkerOnline uses jedis.get(workerOnlineLease(workerId))
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1"))).thenReturn("1");

        Map<String, Object> result = context.callAgent("target-agent", "hello target", Map.of("p", (Object) "v"), true, null);

        assertEquals("QUEUED", result.get("status"));
        assertNotNull(result.get("message_id"));

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);

        assertEquals("target-agent", cmd.header().targetAgentType());
        assertEquals("hello target", cmd.content());
        assertEquals("sess-1", cmd.header().sessionId());
        assertEquals("test-agent", cmd.header().sourceAgentType());
    }

    @Test
    void callAgentFailsWhenNoAliveWorker() throws Exception {
        // Mock WorkerRegistry.hasCapability redis calls - no workers
        String capKey = Constants.RegistryKeys.agentTypeMembers("missing-agent");
        when(jedis.smembers(capKey)).thenReturn(java.util.Collections.emptySet());

        Map<String, Object> result = context.callAgent("missing-agent", "hello");

        assertEquals("FAILED", result.get("status"));
        assertNotNull(result.get("error_code"));
        assertTrue(result.get("error_code").toString().contains("ERR_AGENT_TYPE_UNAVAILABLE"));
        verify(jedis, never()).xadd(anyString(), any(XAddParams.class), anyMap());
    }

    @Test
    void callAgentWithTaskGroupId() throws Exception {
        // Mock WorkerRegistry.hasOnlineAgentType
        when(jedis.smembers(anyString())).thenReturn(java.util.Set.of("worker-1"));
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1"))).thenReturn("1");

        context.callAgent("target-agent", "content", null, true, null, "group-123");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("group-123", cmd.header().taskGroupId());
    }

    @Test
    void dispatchGroupCreatesGroupInRedisAndDispatchesSubTasks() throws Exception {
        // Both targets have an online worker, so both are dispatched for real.
        // Without this the availability probe correctly skips them and queues
        // stand-in failures instead — see the test below.
        lenient().when(jedis.smembers(anyString())).thenReturn(java.util.Set.of("worker-x"));
        // isWorkerOnline reads the lease with GET, not EXISTS.
        lenient().when(jedis.get(startsWith("byai_gateway:registry:worker:online:")))
                .thenReturn("1");

        List<Map<String, Object>> requests = List.of(
                Map.of("agent_type", "a1", "content", "c1"),
                Map.of("agent_type", "a2", "content", "c2"));

        context.dispatchGroup(requests);

        // 1. Group info in Redis (hset with Map overload)
        ArgumentCaptor<Map<String, String>> hsetCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).hset(anyString(), hsetCaptor.capture());
        Map<String, String> groupData = hsetCaptor.getValue();
        assertEquals("2", groupData.get("total"));
        assertEquals("0", groupData.get("completed"));
        verify(jedis).expire(anyString(), eq(86400L));

        // 2. Commands sent via jedis.xadd (not pipeline)
        verify(jedis, times(2)).xadd(anyString(), any(XAddParams.class), anyMap());
    }

    @Test
    void markExecutionFinishedDelegatesToJedis() {

        // This is tricky because markExecutionFinished might be using its own jedis
        // resource
        // Depending on current implementation, it might go through
        // AgentContext/Registry
    }

    @Test
    void checkCancelledThrowsWhenInterrupted() {
        Thread.currentThread().interrupt();
        assertThrows(RuntimeException.class, () -> context.checkCancelled());

        // Clear interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void isCancelRequestedReturnsFalseWhenThreadNotInterrupted() {
        // Given
        AgentContext ctx = new AgentContext(
                "session-x", "trace-x", redisClient, "agent-x", "msg-x");

        // When
        boolean result = ctx.isCancelRequested();

        // Then
        assertFalse(result);
    }

    @Test
    void isCancelRequestedReturnsTrueWhenThreadIsInterrupted() {
        // Given
        AgentContext ctx = new AgentContext(
                "session-y", "trace-y", redisClient, "agent-y", "msg-y");

        // Simulate interrupt
        Thread.currentThread().interrupt();

        // When
        boolean result = ctx.isCancelRequested();

        // Then
        assertTrue(result);

        // Clean up interrupt flag for other tests
        Thread.interrupted();
    }

    @Test
    void gettersReturnCorrectValues() {
        // Given
        AgentContext ctx = new AgentContext(
                "my-session", "my-trace", redisClient, "my-agent", "my-msg");

        // Then
        assertEquals("my-session", ctx.getSessionId());
        assertEquals("my-trace", ctx.getTraceId());
        assertEquals("my-agent", ctx.getCurrentAgentType());
        assertEquals("my-msg", ctx.getCurrentMessageId());
    }

    // ---- E5: group dispatch compensation ----------------------------------

    @Test
    void anUnavailableGroupMemberIsNotDispatchedAndIsQueuedInstead() {
        // No online workers for either target. Blind-dispatching would mean those
        // members never reply and the group never completes, hanging the caller
        // until a sweep notices — and the sweep's compensation half is off by
        // default.
        lenient().when(jedis.smembers(anyString())).thenReturn(java.util.Set.of());

        context.dispatchGroup(List.of(
                Map.of("agent_type", "offline-1", "content", "c1"),
                Map.of("agent_type", "offline-2", "content", "c2")));

        // Nothing dispatched...
        verify(jedis, never()).xadd(anyString(), any(XAddParams.class), anyMap());
        // ...but a stand-in queued per member, so the join still sees one result each.
        assertEquals(2, context.pendingGroupReplies().size());
    }

    @Test
    void standInsAreQueuedNotSentInline() {
        // Sending inline would put a reply on the caller's own control stream
        // strictly BEFORE its execution is recorded as suspended, turning the
        // "a very fast sub-agent replies first" race from unlikely into certain.
        // They go out only after the handler returns (see GatewayWorker).
        lenient().when(jedis.smembers(anyString())).thenReturn(java.util.Set.of());

        context.dispatchGroup(List.of(Map.of("agent_type", "offline-1", "content", "c1")));

        verify(jedis, never()).xadd(anyString(), any(XAddParams.class), anyMap());
        assertFalse(context.pendingGroupReplies().isEmpty());
    }

    @Test
    void theStandInIsAddressedToTheCallerAndNamesTheMember() {
        lenient().when(jedis.smembers(anyString())).thenReturn(java.util.Set.of());

        context.dispatchGroup(List.of(Map.of("agent_type", "offline-1", "content", "c1")));

        var reply = context.pendingGroupReplies().get(0);
        // Keyed by the caller's own id so it can reattach its execution...
        assertEquals(context.getCurrentMessageId(), reply.header().messageId());
        // ...while parentMessageId names the member, which is what the join keys by.
        assertFalse(reply.header().parentMessageId().isBlank());
        assertEquals(com.iwhaleai.byai.framework.core.protocol.AgentState.FAILED, reply.status());
        assertFalse(reply.header().taskGroupId().isBlank());
    }

    @Test
    void theDispatcherNeverAccountsForTheGroupItself() {
        // Writing the result or HINCRBYing `completed` here would be a second
        // accounting implementation, and when its increment is the one that fills
        // the group there is no reply left to trigger the join. Python and TS each
        // had to fix exactly that; Java must not introduce it while adding the probe.
        lenient().when(jedis.smembers(anyString())).thenReturn(java.util.Set.of());

        context.dispatchGroup(List.of(Map.of("agent_type", "offline-1", "content", "c1")));

        verify(jedis, never()).hincrBy(anyString(), eq("completed"), anyLong());
    }
}
