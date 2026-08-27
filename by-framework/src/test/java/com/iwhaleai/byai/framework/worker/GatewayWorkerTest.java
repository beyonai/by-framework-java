package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.XAddParams;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * GatewayWorker 测试，对标 Python test_gateway_worker.py
 */
@ExtendWith(MockitoExtension.class)
class GatewayWorkerTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private com.iwhaleai.byai.framework.core.WorkerRegistry registry;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
    }

    private static class EchoWorker extends GatewayWorker {
        public GatewayCommand lastCommand = null;
        public AgentContext lastContext = null;

        public EchoWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("echo-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            lastCommand = command;
            lastContext = context;
            return "echo-result";
        }
    }

    private static class FailingWorker extends GatewayWorker {
        public FailingWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("fail-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            throw new RuntimeException("intentional failure");
        }
    }

    private static class StructuredResultWorker extends GatewayWorker {
        public StructuredResultWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("structured-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return new AgentTaskResult(
                    AgentState.COMPLETED,
                    "structured content",
                    Map.of("answer", 42),
                    Map.of("tokens", 123, "caller", "overridden"),
                    Map.of("debug_id", "dbg-1")
            );
        }
    }

    private static class NoOverrideWorker extends GatewayWorker {
        public NoOverrideWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("no-op-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return null;
        }
    }

    private static class CancelTrackingWorker extends GatewayWorker {
        public CancelTaskCommand lastCancelCommand = null;

        public CancelTrackingWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("cancel-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return "ok";
        }

        @Override
        public void onCancelTask(CancelTaskCommand command) {
            super.onCancelTask(command);
            lastCancelCommand = command;
        }
    }

    private static class CancellableWorker extends GatewayWorker {
        public CancellableWorker(String workerId, RedisClient redisClient, com.iwhaleai.byai.framework.core.WorkerRegistry registry) {
            super(workerId, redisClient, registry);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("cancellable-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            try {
                Thread.sleep(10000);
                return "should-not-reach-here";
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void workerCancellationEmitsCancelledState() throws InterruptedException {
        CancellableWorker worker = new CancellableWorker("worker-1", redisClient, registry);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("cancellable-agent")
                        .build(),
                "test",
                false,
                null
        );

        Thread taskThread = new Thread(() -> worker.handleMessage(command, "exec-1", null, false));
        taskThread.start();

        // Give it a moment to start
        Thread.sleep(100);

        // Simulate interruption as WorkerRunner would do
        taskThread.interrupt();
        taskThread.join(2000);

        // Verify state emission
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasCancelledState = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(AgentState.CANCELLED));
        assertTrue(hasCancelledState, "Should emit CANCELLED state");

        // Verify registry call
        verify(registry).markExecutionFinished(eq("exec-1"), eq("sess-1"), eq("CANCELLED"));
    }

    @Test
    void workerProcessesAskAgentCommandAndEmitsCompleted() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("echo-agent")
                        .build(),
                "hello",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        assertNotNull(worker.lastCommand);
        assertInstanceOf(AskAgentCommand.class, worker.lastCommand);
        assertEquals("hello", ((AskAgentCommand) worker.lastCommand).content());

        // Should emit COMPLETED state (no source_agent_type, so not a callback)
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasCompletedState = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(AgentState.COMPLETED));
        assertTrue(hasCompletedState);
    }

    @Test
    void workerInjectsDecodedCommandIntoContext() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("echo-agent")
                        .build(),
                "test content",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        assertNotNull(worker.lastContext);
        assertEquals("sess-1", worker.lastContext.getSessionId());
        assertEquals("trace-1", worker.lastContext.getTraceId());
        assertEquals("echo-agent", worker.lastContext.getCurrentAgentType());
        assertEquals("msg-1", worker.lastContext.getCurrentMessageId());
    }

    @Test
    void workerGeneratesTraceIdWhenMissing() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("") // empty trace
                        .targetAgentType("echo-agent")
                        .build(),
                "test",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        assertNotNull(worker.lastContext);
        assertNotNull(worker.lastContext.getTraceId());
        assertFalse(worker.lastContext.getTraceId().isEmpty());
    }

    @Test
    void workerResumeCommandPassedToProcessCommand() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        ResumeCommand command = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-2")
                        .sessionId("sess-2")
                        .traceId("trace-2")
                        .targetAgentType("echo-agent")
                        .build(),
                "resume content",
                "SUCCESS",
                Map.of("result", "ok"),
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        assertNotNull(worker.lastCommand);
        assertInstanceOf(ResumeCommand.class, worker.lastCommand);
        assertEquals("SUCCESS", ((ResumeCommand) worker.lastCommand).status());
    }

    @Test
    void workerEmitsResumedStateForResumeCommand() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        ResumeCommand command = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-2")
                        .sessionId("sess-2")
                        .traceId("trace-2")
                        .targetAgentType("echo-agent")
                        .build(),
                "",
                "SUCCESS",
                null,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasResumedState = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(AgentState.RESUMED));
        assertTrue(hasResumedState);
    }

    @Test
    void workerFailureEmitsFailedState() {
        FailingWorker worker = new FailingWorker("worker-fail", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("fail-agent")
                        .build(),
                "trigger failure",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasFailedState = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(AgentState.FAILED));
        assertTrue(hasFailedState);
    }

    @Test
    void workerWithSourceAgentEnqueuesCallback() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .sourceAgentType("caller-agent")
                        .targetAgentType("echo-agent")
                        .build(),
                "delegated task",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        // Should enqueue callback to caller-agent's ctrl stream
        String callerStream = Constants.QueueNames.ctrlStream("caller-agent");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(eq(callerStream), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasResumeCallback = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(ActionType.RESUME));
        assertTrue(hasResumeCallback);
    }

    @Test
    @SuppressWarnings("unchecked")
    void workerAgentTaskResultMapsToCallbackAndMergesMetadata() throws Exception {
        StructuredResultWorker worker = new StructuredResultWorker("worker-structured", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-structured")
                        .sessionId("sess-structured")
                        .traceId("trace-structured")
                        .sourceAgentType("caller-agent")
                        .targetAgentType("structured-agent")
                        .metadata(Map.of("caller", "original", "request_id", "req-1"))
                        .build(),
                "delegated task",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        String callerStream = Constants.QueueNames.ctrlStream("caller-agent");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(eq(callerStream), any(XAddParams.class), fieldsCaptor.capture());

        Map<String, Object> callback = fieldsCaptor.getAllValues().stream()
                .map(fields -> fields.get("data"))
                .filter(Objects::nonNull)
                .map(data -> {
                    try {
                        return objectMapper.readValue(data, Map.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(data -> ActionType.RESUME.equals(data.get("action_type")))
                .findFirst()
                .orElseThrow();

        Map<String, Object> body = (Map<String, Object>) callback.get("body");
        Map<String, Object> header = (Map<String, Object>) callback.get("header");
        assertEquals(AgentState.COMPLETED, body.get("status"));
        assertEquals("structured content", body.get("content"));
        assertEquals(Map.of("answer", 42), body.get("reply_data"));
        assertEquals(Map.of("debug_id", "dbg-1"), body.get("extra_payload"));
        assertEquals(
                Map.of("caller", "overridden", "request_id", "req-1", "tokens", 123),
                header.get("metadata")
        );
    }

    @Test
    void workerFailureWithSourceAgentEnqueuesFailedCallback() {
        FailingWorker worker = new FailingWorker("worker-fail", redisClient);
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .sourceAgentType("caller-agent")
                        .targetAgentType("fail-agent")
                        .build(),
                "will fail",
                false,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        String callerStream = Constants.QueueNames.ctrlStream("caller-agent");
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(eq(callerStream), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasFailedCallback = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains("FAILED"));
        assertTrue(hasFailedCallback);
    }

    @Test
    void workerCancelCommandDelegatesToOnCancelTask() {
        CancelTrackingWorker worker = new CancelTrackingWorker("worker-cancel", redisClient);
        CancelTaskCommand command = CancelTaskCommand.builder()
                .header(MessageHeader.builder()
                        .messageId("cancel-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .build())
                .body(CancelTaskCommand.CancelTaskBody.builder()
                        .targetMessageId("target-msg-1")
                        .reason("user requested")
                        .build())
                .build();

        worker.handleMessage(command, "exec-test-id", null, false);

        assertNotNull(worker.lastCancelCommand);
        assertEquals("target-msg-1", worker.lastCancelCommand.targetMessageId());
    }

    @Test
    void workerCancelCommandDoesNotCallProcessCommand() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        CancelTaskCommand command = CancelTaskCommand.builder()
                .header(MessageHeader.builder()
                        .messageId("cancel-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .build())
                .body(CancelTaskCommand.CancelTaskBody.builder()
                        .targetMessageId("target-msg-1")
                        .build())
                .build();

        worker.handleMessage(command, "exec-test-id", null, false);

        // processCommand should not be called for cancel commands
        assertNull(worker.lastCommand);
    }

    @Test
    void workerResumeWithNoSourceAgentDoesNotEnqueueCallback() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        ResumeCommand command = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-resume")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .sourceAgentType("") // empty - is a resume, not callback
                        .targetAgentType("echo-agent")
                        .build(),
                "resume data",
                "SUCCESS",
                null,
                null
        );

        worker.handleMessage(command, "exec-test-id", null, false);

        // Should emit COMPLETED, not QUEUED
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(anyString(), any(XAddParams.class), fieldsCaptor.capture());

        boolean hasCompletedState = fieldsCaptor.getAllValues().stream()
                .anyMatch(f -> f.get("data") != null && f.get("data").contains(AgentState.COMPLETED));
        assertTrue(hasCompletedState);
    }

    @Test
    void workerGetterMethods() {
        EchoWorker worker = new EchoWorker("worker-abc", redisClient);
        assertEquals("worker-abc", worker.getWorkerId());
        assertEquals(List.of("echo-agent"), worker.getAgentTypes());
        assertNotNull(worker.getPluginRegistry());
    }

    // ---- J2+J3: suspension model and caller restoration -------------------

    /** Suspends the way callAgent(waitForReply=true) does, then unwinds. */
    private static class SuspendingWorker extends GatewayWorker {
        SuspendingWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("echo-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            context.markSuspended(AgentState.WAITING_AGENT);
            return AgentState.QUEUED;
        }
    }

    /** Dispatches, then reaches a terminal state anyway — it owes a reply now. */
    private static class SuspendingThenDoneWorker extends GatewayWorker {
        SuspendingThenDoneWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("echo-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            context.markSuspended(AgentState.WAITING_AGENT);
            return AgentState.COMPLETED;
        }
    }

    private List<String> ctrlStreamWrites() {
        ArgumentCaptor<String> streamCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, String>> fieldsCaptor = ArgumentCaptor.forClass(Map.class);
        // atLeast(0): a suspended execution legitimately writes nothing at all,
        // and that is exactly what one of these tests asserts.
        verify(jedis, atLeast(0)).xadd(streamCaptor.capture(), any(XAddParams.class), fieldsCaptor.capture());
        List<String> out = new java.util.ArrayList<>();
        for (int i = 0; i < streamCaptor.getAllValues().size(); i++) {
            if (streamCaptor.getAllValues().get(i).startsWith(Constants.QueueNames.ctrlStream(""))
                    || streamCaptor.getAllValues().get(i).contains(":ctrl:")) {
                out.add(fieldsCaptor.getAllValues().get(i).get("data"));
            }
        }
        return out;
    }

    private static ResumeCommand resumeFromSubAgent() {
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-b")            // B's own id: what B reattaches by
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .sourceAgentType("agent-c")    // the SUB-agent that just finished
                        .targetAgentType("echo-agent")
                        .parentMessageId("msg-c")      // B's sub-task
                        .build(),
                "sub result", AgentState.COMPLETED, Map.of("from", "c"), Map.of());
    }

    private static Map<String, Object> callerRecord() {
        Map<String, Object> record = new java.util.HashMap<>();
        record.put("execution_id", "exec-b");
        record.put("source_agent_type", "agent-a");   // the REAL caller
        record.put("parent_message_id", "msg-a");     // A's own id
        record.put("task_group_id", "");
        return record;
    }

    @Test
    void aResumedExecutionRepliesToItsOriginalCallerNotToTheSubAgent() {
        EchoWorker worker = new EchoWorker("worker-1", redisClient);

        worker.handleMessage(resumeFromSubAgent(), "exec-b", callerRecord(), true);

        List<String> replies = ctrlStreamWrites();
        assertFalse(replies.isEmpty(), "a resumed execution must reply to its caller");
        String reply = replies.get(replies.size() - 1);
        // Addressed to A, not back down to C.
        assertTrue(reply.contains("agent-a"), "reply must be addressed to the original caller");
        // And keyed by A's own message id, or A cannot reattach its execution.
        assertTrue(reply.contains("msg-a"), "reply must carry the caller's own message id");
    }

    @Test
    void aMissingSourceAgentTypeMeansNoCallerRatherThanAnError() {
        // Java's execution records historically omit the field entirely, so
        // absence is the common case — it must read as "nobody is waiting".
        EchoWorker worker = new EchoWorker("worker-1", redisClient);
        Map<String, Object> record = new java.util.HashMap<>();
        record.put("execution_id", "exec-b");

        assertDoesNotThrow(() -> worker.handleMessage(resumeFromSubAgent(), "exec-b", record, true));
        assertNull(GatewayWorker.resolveReplyCommand(resumeFromSubAgent(), record));
    }

    @Test
    void theClientSentinelIsNotACaller() {
        // Treating it as one posts to a control stream nobody consumes AND
        // suppresses the end-of-stream event the front end waits on.
        Map<String, Object> record = new java.util.HashMap<>();
        record.put("source_agent_type", Constants.CLIENT_SOURCE_AGENT_TYPE);

        assertNull(GatewayWorker.resolveReplyCommand(resumeFromSubAgent(), record));
    }

    @Test
    void aSuspendedExecutionDoesNotReplyYet() {
        // The value a handler returns merely to unwind is not a result. Sending
        // it wakes the caller early and burns the single reply it is parked on.
        SuspendingWorker worker = new SuspendingWorker("worker-1", redisClient);
        AskAgentCommand dispatch = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-b").sessionId("sess-1").traceId("trace-1")
                        .sourceAgentType("agent-a").targetAgentType("echo-agent")
                        .parentMessageId("msg-a").build(),
                "delegate", false, null);

        worker.handleMessage(dispatch, "exec-b", null, false);

        for (String written : ctrlStreamWrites()) {
            assertFalse(written.contains("\"action_type\":\"RESUME\""),
                    "a suspended execution must not reply to its caller yet");
        }
    }

    @Test
    void aTerminalStatusOverridesSuspensionAndRepliesNow() {
        // It dispatched, then finished anyway: no resume is coming, so this is
        // the caller's only chance to hear anything.
        SuspendingThenDoneWorker worker = new SuspendingThenDoneWorker("worker-1", redisClient);
        AskAgentCommand dispatch = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-b").sessionId("sess-1").traceId("trace-1")
                        .sourceAgentType("agent-a").targetAgentType("echo-agent")
                        .parentMessageId("msg-a").build(),
                "delegate", false, null);

        worker.handleMessage(dispatch, "exec-b", null, false);

        boolean replied = ctrlStreamWrites().stream().anyMatch(w -> w.contains("agent-a"));
        assertTrue(replied, "a handler that reached a terminal state owes its caller a reply now");
    }
}
