package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
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
    }

    @Test
    void sendCommandWithProbeFailure() {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());
        
        when(registry.hasOnlineAgentType(anyString(), anyBoolean(), anyLong()))
                .thenReturn(new WorkerRegistry.OnlineAgentCheckResult(false, Collections.emptyList()));

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null
        );

        GatewayClient.SendResponse response = client.sendCommand(command, null, null, true);

        assertFalse(response.isSuccess());
        assertEquals("FAILED", response.getStatus());
        assertEquals("", response.getTargetWorkerId());
        assertTrue(response.getError().contains("No online worker found for agent_type"));
    }

    @Test
    void sendCommandToTargetWorker() throws Exception {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null
        );

        GatewayClient.SendResponse response = client.sendCommand(command, null, "worker-456", false);

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

        // sendMessage defaults to probeCapability=true
        GatewayClient.SendResponse response = client.sendMessage("test-agent", "sess-1", "hello");

        assertTrue(response.isSuccess());
        assertEquals("", response.getTargetWorkerId());
        verify(registry).hasOnlineAgentType(eq("test-agent"), eq(true), anyLong());
        verify(registry, never()).getTargetWorker(anyString());
    }

    @Test
    void sendCommandWithTargetWorkerProbeFailure() {
        GatewayClient<String> client = new GatewayClient<>(redisClient, registry, Collections.emptyList());

        when(registry.isWorkerOnline(eq("worker-offline"))).thenReturn(false);

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .targetAgentType("test-agent")
                        .build(),
                "test",
                false,
                null
        );

        GatewayClient.SendResponse response = client.sendCommand(command, null, "worker-offline", true);

        assertFalse(response.isSuccess());
        assertEquals("FAILED", response.getStatus());
        assertEquals("worker-offline", response.getTargetWorkerId());
        assertEquals("Target worker 'worker-offline' is not online or not registered", response.getError());
        verify(registry).isWorkerOnline(eq("worker-offline"));
    }
}
