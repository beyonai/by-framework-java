package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.availability.RoutePolicy;
import com.iwhaleai.byai.framework.core.protocol.ActionType;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import com.iwhaleai.byai.framework.client.interceptors.GatewayInterceptor;
import com.iwhaleai.byai.framework.client.interceptors.SendMessageParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * GatewayClient sendMessage/sendCommand 测试，对标 Python test_client.py 的发送部分
 */
@ExtendWith(MockitoExtension.class)
class GatewayClientSendTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        // AvailabilityRouter checks circuit breaker and quota before online check
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:circuit:"))).thenReturn(null);
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:quota:"))).thenReturn(null);
    }

    @Test
    void sendMessageCreatesAskAgentCommandByDefault() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage("demo-agent", "sess-1", "hello");

        assertTrue(response.isSuccess());
        assertEquals("QUEUED", response.getStatus());
        assertNotNull(response.getMessageId());
        assertEquals("", response.getTargetWorkerId());

        // Verify command was sent to Redis stream
        String expectedStream = Constants.QueueNames.ctrlStream("demo-agent");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(expectedStream), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals(ActionType.ASK_AGENT, cmd.actionType());
        assertEquals("hello", cmd.content());
        assertEquals("demo-agent", cmd.header().targetAgentType());
        assertEquals("sess-1", cmd.header().sessionId());
    }

    @Test
    void sendMessageInitializesExecutionBeforePublishingToRedis() {
        FakeSendRegistry registry = spy(new FakeSendRegistry(redisClient, "worker-1"));
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "msg-order", "trace-order", null, null);

        assertTrue(response.isSuccess());
        InOrder inOrder = inOrder(registry, jedis);
        inOrder.verify(registry).initializeExecution(
                anyString(),
                eq("msg-order"),
                eq("sess-1"),
                eq("demo-agent"),
                eq(""),
                eq("trace-order"));
        inOrder.verify(jedis).xadd(
                eq(Constants.QueueNames.ctrlStream("demo-agent")),
                (StreamEntryID) isNull(),
                any(Map.class));
    }

    @Test
    void sendMessageWithTargetWorkerIdRoutesToWorkerStream() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, null, null, null, null,
                "worker-1", RoutePolicy.FAIL_FAST, 0, null, null);

        assertTrue(response.isSuccess());
        assertEquals("QUEUED", response.getStatus());
        assertEquals("worker-1", response.getTargetWorkerId());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(Constants.QueueNames.workerCtrlStream("worker-1")),
                (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("demo-agent", cmd.header().targetAgentType());
        assertEquals("sess-1", cmd.header().sessionId());
    }

    @Test
    void sendMessageWithMetadata() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        Map<String, Object> metadata = Map.of("user", "test-user", "priority", "high");
        GatewayClient.SendResponse response = client.sendMessage("demo-agent", "sess-1", "hello", metadata);

        assertTrue(response.isSuccess());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("test-user", cmd.header().metadata().get("user"));
        assertEquals("high", cmd.header().metadata().get("priority"));
    }

    @Test
    void sendMessagePromotesTraceParentFieldsFromMetadataToHeader() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("request_id", "req-1");
        metadata.put("trace_parent_span_id", "0123456789abcdef");
        metadata.put("langfuse_parent_observation_id", "obs-client-dispatch");

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "msg-client", "trace-client", null, metadata);

        assertTrue(response.isSuccess());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("0123456789abcdef", cmd.header().traceParentSpanId());
        assertEquals("obs-client-dispatch", cmd.header().langfuseParentObservationId());
        assertEquals(Map.of("request_id", "req-1"), cmd.header().metadata());
    }

    @Test
    void sendMessageStartsLangfuseClientDispatchForRootCommand() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        FakeClientDispatchTracer tracer = new FakeClientDispatchTracer("obs-client-dispatch");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of(), tracer);

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                "user-1", "User One", null, "", "msg-client", "trace-client", null,
                Map.of("request_id", "req-1"));

        assertTrue(response.isSuccess());
        assertEquals(1, tracer.startCalls.size());
        ClientDispatchTracer.ClientDispatchRequest start = tracer.startCalls.get(0);
        assertEquals("trace-client", start.traceId());
        assertEquals("msg-client", start.messageId());
        assertEquals("demo-agent", start.targetAgentType());
        assertEquals("sess-1", start.sessionId());
        assertEquals("user-1", start.userCode());
        assertEquals("hello", start.content());
        assertEquals(Map.of("request_id", "req-1"), start.metadata());
        assertEquals("060f92f2a4dc5da4", start.observationId());
        assertEquals(1, tracer.observation.endCalls.size());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("obs-client-dispatch", cmd.header().langfuseParentObservationId());
        assertEquals("060f92f2a4dc5da4", cmd.header().traceParentSpanId());
    }

    @Test
    void sendMessageDoesNotStartLangfuseClientDispatchForChildCommand() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        FakeClientDispatchTracer tracer = new FakeClientDispatchTracer("obs-new-root");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of(), tracer);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("langfuse_parent_observation_id", "obs-existing-parent");

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, "msg-parent", "msg-child", "trace-client", null, metadata);

        assertTrue(response.isSuccess());
        assertTrue(tracer.startCalls.isEmpty());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("obs-existing-parent", cmd.header().langfuseParentObservationId());
        assertEquals("msg-parent", cmd.header().parentMessageId());
    }

    @Test
    void sendMessageDefaultsTraceParentSpanIdToClientDispatchSpan() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "msg-client", "trace-client", null, null);

        assertTrue(response.isSuccess());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("060f92f2a4dc5da4", cmd.header().traceParentSpanId());
    }

    @Test
    void sendMessageRecordsRedisClientDispatchTraceSpan() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "msg-client", "trace-client", null, null);

        assertTrue(response.isSuccess());

        @SuppressWarnings("rawtypes")
        ArgumentCaptor<Map> metaCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).hset(eq("by_framework:trace:trace-client"), metaCaptor.capture());
        Map<Object, Object> mergedMeta = new HashMap<>();
        for (Map<?, ?> captured : metaCaptor.getAllValues()) {
            mergedMeta.putAll(captured);
        }
        assertEquals("trace-client", mergedMeta.get("trace_id"));
        assertEquals("sess-1", mergedMeta.get("session_id"));
        assertEquals("COMPLETED", mergedMeta.get("status"));
        assertEquals("client.dispatch:demo-agent", mergedMeta.get("name"));
        assertEquals("demo-agent", mergedMeta.get("root_agent_type"));
        assertEquals("msg-client", mergedMeta.get("root_message_id"));

        ArgumentCaptor<String> spanCaptor = ArgumentCaptor.forClass(String.class);
        verify(jedis).rpush(eq("by_framework:trace:spans:trace-client"), spanCaptor.capture());
        Map<?, ?> span = objectMapper.readValue(spanCaptor.getValue(), Map.class);
        assertEquals("trace-client", span.get("trace_id"));
        assertEquals("msg-client:client.dispatch", span.get("span_id"));
        assertEquals("client.dispatch", span.get("operation"));
        assertEquals("client", span.get("component"));
        assertEquals("COMPLETED", span.get("status"));
        assertEquals("sess-1", span.get("session_id"));
        assertEquals("msg-client", span.get("message_id"));
        assertEquals("client", span.get("source_agent_type"));
        assertEquals("demo-agent", span.get("target_agent_type"));
        assertEquals("", span.get("worker_id"));
        assertEquals("AGENT_TYPE", span.get("route_policy"));
        assertEquals("redis", span.get("source"));

        verify(jedis).zadd(eq("by_framework:trace:idx:session:sess-1"), anyDouble(), eq("trace-client"));
        verify(jedis, never()).zadd(eq("by_framework:trace:idx:worker:"), anyDouble(), eq("trace-client"));
        verify(jedis).zadd(eq("by_framework:trace:idx:agent:demo-agent"), anyDouble(), eq("trace-client"));
    }

    @Test
    void sendMessageIgnoresRedisTraceWriteFailures() {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());
        doThrow(new RuntimeException("redis trace down")).when(jedis).hset(eq("by_framework:trace:trace-client"), any(Map.class));

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "msg-client", "trace-client", null, null);

        assertTrue(response.isSuccess());
    }

    @Test
    void sendMessageWithResumeActionType() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "SUCCESS");
        payload.put("reply_data", Map.of("result", "ok"));

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "resume content",
                null, null, ActionType.RESUME, null, null, null, payload, null
        );

        assertTrue(response.isSuccess());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        ResumeCommand cmd = objectMapper.readValue(dataJson, ResumeCommand.class);
        assertEquals(ActionType.RESUME, cmd.actionType());
        assertEquals("resume content", cmd.content());
        assertEquals("SUCCESS", cmd.status());
    }

    @Test
    void sendMessageReturnsFailureWhenNoWorkerAvailable() {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, null);
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage("no-agent", "sess-1", "hello");

        assertFalse(response.isSuccess());
        assertEquals("FAILED", response.getStatus());
        assertNotNull(response.getError());
        assertTrue(response.getError().contains("No online worker for agent_type"));
    }

    @Test
    void sendMessageRunsInterceptorsBeforeSend() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");

        // Create an interceptor that modifies the content
        GatewayInterceptor interceptor = params -> {
            params.setContent("[intercepted] " + params.getContent());
            return params;
        };

        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of(interceptor));
        client.sendMessage("demo-agent", "sess-1", "original");

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("[intercepted] original", cmd.content());
    }

    @Test
    void sendMessageWithCustomMessageIdAndTraceId() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                null, null, null, null, "custom-msg-id", "custom-trace-id", null, null
        );

        assertTrue(response.isSuccess());
        assertEquals("custom-msg-id", response.getMessageId());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("custom-msg-id", cmd.header().messageId());
        assertEquals("custom-trace-id", cmd.header().traceId());
    }

    @Test
    void sendMessageWithUserCode() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        GatewayClient.SendResponse response = client.sendMessage(
                "demo-agent", "sess-1", "hello",
                "user-abc", "User ABC", null, null, null, null, null, null
        );

        assertTrue(response.isSuccess());

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(anyString(), (StreamEntryID) isNull(), fieldsCaptor.capture());

        String dataJson = fieldsCaptor.getValue().get("data");
        AskAgentCommand cmd = objectMapper.readValue(dataJson, AskAgentCommand.class);
        assertEquals("user-abc", cmd.header().userCode());
        assertEquals("User ABC", cmd.header().userName());
    }

    @Test
    void addInterceptorAppendsToList() {
        GatewayClient<String> client = new GatewayClient<>(redisClient, new ArrayList<>());

        GatewayInterceptor interceptor = params -> params;
        client.addInterceptor(interceptor);

        assertEquals(1, client.getInterceptors().size());
    }

    @Test
    void multipleInterceptorsRunInOrder() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");

        List<String> callOrder = new ArrayList<>();
        GatewayInterceptor first = params -> { callOrder.add("first"); return params; };
        GatewayInterceptor second = params -> { callOrder.add("second"); return params; };

        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of(first, second));
        client.sendMessage("demo-agent", "sess-1", "test");

        assertEquals(List.of("first", "second"), callOrder);
    }

    @Test
    void sendCommandDirectlyToCustomStream() throws Exception {
        FakeSendRegistry registry = new FakeSendRegistry(redisClient, "worker-1");
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, List.of());

        AskAgentCommand command = AskAgentCommand.of(
                com.iwhaleai.byai.framework.core.protocol.MessageHeader.builder()
                        .messageId("msg-direct")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("agent-x")
                        .build(),
                "direct content",
                false,
                null
        );

        GatewayClient.SendResponse response = client.sendCommand(command, "custom:stream:name");

        assertTrue(response.isSuccess());
        verify(jedis).xadd(eq("custom:stream:name"), (StreamEntryID) isNull(), any(Map.class));
    }

    @Test
    void byaiGatewayClientIncludesByaiInterceptor() {
        ByaiGatewayClient client = new ByaiGatewayClient(redisClient);

        assertFalse(client.getInterceptors().isEmpty());
        assertTrue(client.getInterceptors().get(0)
                instanceof com.iwhaleai.byai.framework.client.interceptors.ByaiMessageInterceptor);
    }

    @Test
    void byaiGatewayClientAcceptsAdditionalInterceptors() {
        GatewayInterceptor extra = params -> params;
        ByaiGatewayClient client = new ByaiGatewayClient(redisClient, List.of(extra));

        assertEquals(2, client.getInterceptors().size());
        assertTrue(client.getInterceptors().get(0)
                instanceof com.iwhaleai.byai.framework.client.interceptors.ByaiMessageInterceptor);
    }

    // Fake registry that allows controlling getTargetWorker response
    private static class FakeSendRegistry extends WorkerRegistry {
        private final String targetWorker;

        FakeSendRegistry(RedisClient redisClient, String targetWorker) {
            super(redisClient);
            this.targetWorker = targetWorker;
        }

        @Override
        public synchronized void initializeExecution(String executionId, String messageId, String sessionId,
                String targetAgentType, String parentMessageId) {
            // Tests verify call ordering; no Redis write is needed here.
        }

        @Override
        public synchronized void initializeExecution(String executionId, String messageId, String sessionId,
                String targetAgentType, String parentMessageId, String traceId) {
            // Tests verify call ordering; no Redis write is needed here.
        }

        @Override
        public String getTargetWorker(String agentType) {
            return targetWorker;
        }

        @Override
        public OnlineAgentCheckResult hasOnlineAgentType(String capability, boolean checkActive, long healthThresholdMs) {
            if (targetWorker == null || targetWorker.isEmpty()) {
                return new OnlineAgentCheckResult(false, Collections.emptyList());
            }
            return new OnlineAgentCheckResult(true, List.of(targetWorker));
        }

        @Override
        public boolean isWorkerOnline(String workerId) {
            return targetWorker != null && targetWorker.equals(workerId);
        }
    }

    private static class FakeClientDispatchTracer implements ClientDispatchTracer {
        private final FakeClientDispatchObservation observation;
        private final List<ClientDispatchRequest> startCalls = new ArrayList<>();

        FakeClientDispatchTracer(String observationId) {
            this.observation = new FakeClientDispatchObservation(observationId);
        }

        @Override
        public ClientDispatchObservation start(ClientDispatchRequest request) {
            startCalls.add(request);
            return observation;
        }
    }

    private static class FakeClientDispatchObservation implements ClientDispatchObservation {
        private final String id;
        private final List<EndCall> endCalls = new ArrayList<>();

        FakeClientDispatchObservation(String id) {
            this.id = id;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public void end(Object output, String error) {
            endCalls.add(new EndCall(output, error));
        }
    }

    private record EndCall(Object output, String error) { }
}
