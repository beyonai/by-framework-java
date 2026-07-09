package com.iwhaleai.byai.framework.client;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class GatewayClientCancelSessionTest {

    @Mock
    private RedisClient redisClient;

    @Test
    void cancelsEveryNonTerminalExecutionInSession() {
        Map<String, Object> execA = new HashMap<>();
        execA.put("execution_id", "exec-a");
        execA.put("message_id", "msg-a");
        execA.put("session_id", "sess-1");
        execA.put("worker_id", "worker-1");
        execA.put("target_agent_type", "agent-a");
        execA.put("status", "RUNNING");

        Map<String, Object> execB = new HashMap<>();
        execB.put("execution_id", "exec-b");
        execB.put("message_id", "msg-b");
        execB.put("session_id", "sess-1");
        execB.put("worker_id", "worker-2");
        execB.put("target_agent_type", "agent-b");
        execB.put("status", "QUEUED");

        Map<String, Map<String, Object>> allExecutions = new HashMap<>();
        allExecutions.put("exec-a", execA);
        allExecutions.put("exec-b", execB);

        FakeRegistry registry = new FakeRegistry(redisClient, allExecutions);
        CapturingGatewayClient client = new CapturingGatewayClient(redisClient, registry);

        GatewayClient.CancelSessionResponse response = client.cancelSession("sess-1", "user abort");

        assertTrue(response.isSuccess());
        assertEquals("CANCEL_REQUESTED", response.getStatus());
        assertEquals(2, response.getCancelledCount());
        assertEquals(0, response.getAlreadyFinishedCount());

        assertEquals(2, registry.markCancellingCalls.size());
        assertTrue(registry.markCancellingCalls.contains("exec-a"));
        assertTrue(registry.markCancellingCalls.contains("exec-b"));

        assertEquals(2, client.sentCommands.size());
        List<String> workerIds = client.sentCommands.stream()
                .map(cmd -> ((CancelTaskCommand) cmd).body().targetWorkerId())
                .toList();
        assertTrue(workerIds.contains("worker-1"));
        assertTrue(workerIds.contains("worker-2"));
    }

    @Test
    void flagsTerminalExecutionsWithoutRecancellingThem() {
        Map<String, Object> execA = new HashMap<>();
        execA.put("execution_id", "exec-a");
        execA.put("message_id", "msg-a");
        execA.put("session_id", "sess-1");
        execA.put("worker_id", "worker-1");
        execA.put("status", "COMPLETED");

        Map<String, Object> execB = new HashMap<>();
        execB.put("execution_id", "exec-b");
        execB.put("message_id", "msg-b");
        execB.put("session_id", "sess-1");
        execB.put("worker_id", "worker-2");
        execB.put("target_agent_type", "agent-b");
        execB.put("status", "RUNNING");

        Map<String, Map<String, Object>> allExecutions = new HashMap<>();
        allExecutions.put("exec-a", execA);
        allExecutions.put("exec-b", execB);

        FakeRegistry registry = new FakeRegistry(redisClient, allExecutions);
        CapturingGatewayClient client = new CapturingGatewayClient(redisClient, registry);

        GatewayClient.CancelSessionResponse response = client.cancelSession("sess-1", "user abort");

        assertTrue(response.isSuccess());
        assertEquals("CANCEL_REQUESTED", response.getStatus());
        assertEquals(1, response.getCancelledCount());
        assertEquals(1, response.getAlreadyFinishedCount());

        assertEquals(List.of("exec-b"), registry.markCancellingCalls);
        assertEquals(List.of("exec-a"), registry.markCancelRequestedCallsView());

        assertEquals(1, client.sentCommands.size());
        assertEquals("worker-2",
                ((CancelTaskCommand) client.sentCommands.get(0)).body().targetWorkerId());
    }

    @Test
    void returnsAlreadyFinishedWhenEveryExecutionIsTerminal() {
        Map<String, Object> execA = new HashMap<>();
        execA.put("execution_id", "exec-a");
        execA.put("session_id", "sess-1");
        execA.put("worker_id", "worker-1");
        execA.put("status", "COMPLETED");

        Map<String, Object> execB = new HashMap<>();
        execB.put("execution_id", "exec-b");
        execB.put("session_id", "sess-1");
        execB.put("worker_id", "worker-2");
        execB.put("status", "FAILED");

        Map<String, Map<String, Object>> allExecutions = new HashMap<>();
        allExecutions.put("exec-a", execA);
        allExecutions.put("exec-b", execB);

        FakeRegistry registry = new FakeRegistry(redisClient, allExecutions);
        CapturingGatewayClient client = new CapturingGatewayClient(redisClient, registry);

        GatewayClient.CancelSessionResponse response = client.cancelSession("sess-1", "user abort");

        assertFalse(response.isSuccess());
        assertEquals("ALREADY_FINISHED", response.getStatus());
        assertEquals(0, response.getCancelledCount());
        assertEquals(2, response.getAlreadyFinishedCount());
        assertTrue(registry.markCancellingCalls.isEmpty());
        assertEquals(2, registry.markCancelRequestedCallsView().size());
        assertTrue(client.sentCommands.isEmpty());
    }

    @Test
    void returnsNotFoundForEmptySession() {
        FakeRegistry registry = new FakeRegistry(redisClient, new HashMap<>());
        CapturingGatewayClient client = new CapturingGatewayClient(redisClient, registry);

        GatewayClient.CancelSessionResponse response = client.cancelSession("sess-unknown", "");

        assertFalse(response.isSuccess());
        assertEquals("NOT_FOUND", response.getStatus());
        assertEquals(0, response.getCancelledCount());
        assertEquals(0, response.getAlreadyFinishedCount());
        assertTrue(registry.markCancellingCalls.isEmpty());
        assertTrue(registry.markCancelRequestedCallsView().isEmpty());
        assertTrue(client.sentCommands.isEmpty());
    }

    @Test
    void handlesUnclaimedQueuedExecutionTheSameWayCancelTaskDoes() {
        Map<String, Object> execQueued = new HashMap<>();
        execQueued.put("execution_id", "exec-queued");
        execQueued.put("message_id", "msg-queued");
        execQueued.put("session_id", "sess-1");
        execQueued.put("worker_id", "");
        execQueued.put("status", "QUEUED");

        Map<String, Map<String, Object>> allExecutions = new HashMap<>();
        allExecutions.put("exec-queued", execQueued);

        FakeRegistry registry = new FakeRegistry(redisClient, allExecutions);
        CapturingGatewayClient client = new CapturingGatewayClient(redisClient, registry);

        GatewayClient.CancelSessionResponse response = client.cancelSession("sess-1", "user abort");

        assertTrue(response.isSuccess());
        assertEquals("CANCEL_REQUESTED", response.getStatus());
        assertEquals(1, response.getCancelledCount());
        assertEquals(List.of("exec-queued"), registry.markCancellingCalls);
        // Matches cancelTask's existing fallback for unclaimed tasks: also flagged via markCancelRequested.
        assertEquals(List.of("exec-queued"), registry.markCancelRequestedCallsView());
        assertTrue(client.sentCommands.isEmpty());
    }

    private static class FakeRegistry extends WorkerRegistry {
        private final Map<String, Map<String, Object>> allExecutions;
        private final List<String> markCancellingCalls = new ArrayList<>();
        private final List<String> markCancelRequestedCalls = new ArrayList<>();

        FakeRegistry(RedisClient redisClient, Map<String, Map<String, Object>> allExecutions) {
            super(redisClient);
            this.allExecutions = allExecutions;
        }

        @Override
        public synchronized Map<String, Map<String, Object>> getAllSessionExecutions(String sessionId) {
            return allExecutions;
        }

        @Override
        public synchronized void markExecutionCancelling(String executionId, String sessionId, String reason) {
            markCancellingCalls.add(executionId);
        }

        @Override
        public synchronized void markCancelRequested(String executionId, String sessionId, String reason) {
            markCancelRequestedCalls.add(executionId);
        }

        List<String> markCancelRequestedCallsView() {
            return markCancelRequestedCalls;
        }
    }

    private static class CapturingGatewayClient extends GatewayClient<Object> {
        private final List<GatewayCommand> sentCommands = new ArrayList<>();

        CapturingGatewayClient(RedisClient redisClient, WorkerRegistry registry) {
            super(redisClient, registry, List.of());
        }

        @Override
        public synchronized SendResponse sendCommand(GatewayCommand command, String streamName) {
            sentCommands.add(command);
            return SendResponse.builder()
                    .success(true)
                    .messageId(command.header().messageId())
                    .traceId(command.header().traceId())
                    .status("QUEUED")
                    .timestamp(System.currentTimeMillis())
                    .build();
        }
    }
}
