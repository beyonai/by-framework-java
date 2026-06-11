package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.availability.RoutePolicy;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GatewayClientSyncTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private WorkerRegistry registry;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        // AvailabilityRouter checks circuit breaker and quota before online check
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:circuit:"))).thenReturn(null);
        lenient().when(jedis.get(startsWith("byai_gateway:control_plane:quota:"))).thenReturn(null);
    }

    @Test
    void sendCommandWithProbeFailure() {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        when(registry.hasOnlineAgentType(anyString(), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null);

        GatewayClient.SendResponse response = client.sendCommand(command, null, null, RoutePolicy.FAIL_FAST);

        assertFalse(response.isSuccess());
        assertEquals("FAILED", response.getStatus());
        assertTrue(response.getError().contains("No online worker for agent_type"));
    }

    @Test
    void sendCommandToTargetWorker() throws Exception {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        // SEND_ANYWAY bypasses online check and resolves to ctrl stream
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null);

        GatewayClient.SendResponse response = client.sendCommand(command, null, "worker-456", RoutePolicy.SEND_ANYWAY);

        assertTrue(response.isSuccess());
        assertEquals("worker-456", response.getTargetWorkerId());

        String expectedStream = Constants.QueueNames.workerCtrlStream("worker-456");
        verify(jedis).xadd(eq(expectedStream), (StreamEntryID) isNull(), any(Map.class));
    }

    @Test
    void sendMessageWithProbeSuccess() throws Exception {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        when(registry.hasOnlineAgentType(eq("test-agent"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(true, List.of("worker-789")));

        GatewayClient.SendResponse response = client.sendMessage("test-agent", "sess-1", "hello");

        assertTrue(response.isSuccess());
        assertEquals("", response.getTargetWorkerId());
        verify(registry).hasOnlineAgentType(eq("test-agent"), eq(true), anyLong());
    }

    @Test
    void sendCommandWithTargetWorkerProbeFailure() {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        // No online workers for the agent type -> AvailabilityRouter rejects
        when(registry.hasOnlineAgentType(eq("test-agent"), eq(true), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null);

        GatewayClient.SendResponse response = client.sendCommand(command, null, "worker-offline", RoutePolicy.FAIL_FAST);

        assertFalse(response.isSuccess());
        assertEquals("FAILED", response.getStatus());
        assertTrue(response.getError().contains("No online worker for agent_type"));
    }
}
