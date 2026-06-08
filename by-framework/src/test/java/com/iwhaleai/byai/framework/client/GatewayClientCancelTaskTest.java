package com.iwhaleai.byai.framework.client;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class GatewayClientCancelTaskTest {

    @Mock
    private RedisClient redisClient;

    @Test
    void returnsNotFoundWhenExecutionDoesNotExist() {
        GatewayClient<Object> client = new GatewayClient<>(
                redisClient,
                new FakeRegistry(redisClient, null, Collections.emptyMap()),
                List.of()
        );

        GatewayClient.CancelTaskResponse response = client.cancelTask("missing-msg", "sess-1");

        assertFalse(response.isSuccess());
        assertEquals("NOT_FOUND", response.getStatus());
        assertEquals("missing-msg", response.getMessageId());
        assertEquals("", response.getExecutionId());
        assertEquals("", response.getWorkerId());
    }

    @Test
    void returnsAlreadyFinishedForTerminalExecution() {
        Map<String, Object> exec = new HashMap<>();
        exec.put("execution_id", "exec-1");
        exec.put("worker_id", "worker-123");
        exec.put("session_id", "sess-1");
        exec.put("status", "CANCELLED");

        GatewayClient<Object> client = new GatewayClient<>(
                redisClient,
                new FakeRegistry(redisClient, exec, Collections.emptyMap()),
                List.of()
        );

        GatewayClient.CancelTaskResponse response = client.cancelTask("msg-1", "sess-1");

        // All tasks are in terminal state, so cascading cancel returns success with 0 cancelled
        assertTrue(response.isSuccess());
        assertEquals("ALREADY_FINISHED", response.getStatus());
        assertEquals("exec-1", response.getExecutionId());
        assertEquals("worker-123", response.getWorkerId());
        assertEquals(0, response.getCancelledCount());
    }

    @Test
    void cancelTaskPropagatesOriginalTraceIdToCancelCommand() {
        Map<String, Object> exec = new HashMap<>();
        exec.put(Constants.ExecutionFields.EXECUTION_ID, "exec-1");
        exec.put(Constants.ExecutionFields.MESSAGE_ID, "msg-1");
        exec.put(Constants.ExecutionFields.SESSION_ID, "sess-1");
        exec.put(Constants.ExecutionFields.TRACE_ID, "trace-original");
        exec.put(Constants.ExecutionFields.WORKER_ID, "worker-123");
        exec.put(Constants.ExecutionFields.TARGET_AGENT_TYPE, "demo-agent");
        exec.put(Constants.ExecutionFields.STATUS, "RUNNING");

        CapturingGatewayClient client = new CapturingGatewayClient(
                redisClient,
                new FakeRegistry(redisClient, exec, Map.of("exec-1", exec))
        );

        GatewayClient.CancelTaskResponse response = client.cancelTask("msg-1", "sess-1");

        assertTrue(response.isSuccess());
        CancelTaskCommand command = (CancelTaskCommand) client.lastCommand;
        assertEquals("trace-original", command.header().traceId());
        assertEquals("msg-1", command.header().parentMessageId());
    }

    private static class FakeRegistry extends WorkerRegistry {
        private final Map<String, Object> execution;
        private final Map<String, Map<String, Object>> allExecutions;

        FakeRegistry(RedisClient redisClient, Map<String, Object> execution,
                     Map<String, Map<String, Object>> allExecutions) {
            super(redisClient);
            this.execution = execution;
            this.allExecutions = allExecutions;
        }

        @Override
        public synchronized Map<String, Object> getExecutionByMessageId(String messageId, String sessionId) {
            return execution;
        }

        @Override
        public synchronized Map<String, Map<String, Object>> getAllSessionExecutions(String sessionId) {
            return allExecutions;
        }

        @Override
        public synchronized void markExecutionCancelling(String executionId, String sessionId, String reason) {
            // no-op
        }

        @Override
        public synchronized void markCancelRequested(String executionId, String sessionId, String reason) {
            // no-op
        }
    }

    private static class CapturingGatewayClient extends GatewayClient<Object> {
        private GatewayCommand lastCommand;

        CapturingGatewayClient(RedisClient redisClient, WorkerRegistry registry) {
            super(redisClient, registry, List.of());
        }

        @Override
        public synchronized SendResponse sendCommand(GatewayCommand command, String streamName) {
            this.lastCommand = command;
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
