package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
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
                eq(""));
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
                "worker-1", true);

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
        assertTrue(response.getError().contains("No online worker found for agent_type"));
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
}
