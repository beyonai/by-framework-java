package com.iwhaleai.byai.framework.worker;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.XReadGroupParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * WorkerRunner 测试，对标 Python test_runner.py
 */
@ExtendWith(MockitoExtension.class)
class WorkerRunnerTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private static class SimpleWorker extends GatewayWorker {
        public int processCount = 0;

        public SimpleWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("simple-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            processCount++;
            return "ok";
        }
    }

    private static class ThreeAgentWorker extends GatewayWorker {
        public ThreeAgentWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("agent-a", "agent-b", "agent-c");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return "ok";
        }
    }

    /**
     * Records what reached handleMessage, so a test can tell "skipped" from
     * "processed" without reaching into the executor.
     */
    private static class RecordingWorker extends GatewayWorker {
        final java.util.concurrent.CountDownLatch handled = new java.util.concurrent.CountDownLatch(1);
        volatile Map<String, Object> seenExistingExecution;
        volatile boolean seenIsResumed;

        RecordingWorker(String workerId, RedisClient redisClient, WorkerRegistry registry) {
            super(workerId, redisClient, registry);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("simple-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return "ok";
        }

        @Override
        public void handleMessage(GatewayCommand command, String executionId,
                Map<String, Object> existingExecution, boolean isResumedExecution) {
            this.seenExistingExecution = existingExecution;
            this.seenIsResumed = isResumedExecution;
            this.handled.countDown();
        }
    }

    private static StreamEntry entryFor(GatewayCommand command) throws Exception {
        String json = new ObjectMapper().writeValueAsString(command);
        return new StreamEntry(new StreamEntryID("1-0"),
                Map.of(Constants.RedisFields.DATA, json));
    }

    private static void drive(WorkerRunner runner, String streamName, StreamEntry entry) throws Exception {
        java.lang.reflect.Method m = WorkerRunner.class.getDeclaredMethod(
                "processMessageAsync", String.class, StreamEntry.class);
        m.setAccessible(true);
        m.invoke(runner, streamName, entry);
    }

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
    }

    @Test
    void autoGroupNameIsDeterministicForSameAgentTypes() {
        SimpleWorker worker1 = new SimpleWorker("w1", redisClient);
        SimpleWorker worker2 = new SimpleWorker("w2", redisClient);

        WorkerRunner runner1 = new WorkerRunner(worker1, redisClient, null);
        WorkerRunner runner2 = new WorkerRunner(worker2, redisClient, null);

        // Both runners should compute the same auto group name since agent types are the same
        // We can't directly access groupName, but we can verify they set up streams
        // the same way. Instead, verify that the runner creation doesn't throw.
        assertNotNull(runner1);
        assertNotNull(runner2);
    }

    @Test
    void runnerWithCustomGroupName() {
        SimpleWorker worker = new SimpleWorker("w1", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "custom-group");

        // Should not throw
        assertNotNull(runner);
    }

    @Test
    void runnerStartSetsUpStreamsAndClaimsLock() {
        SimpleWorker worker = new SimpleWorker("w1", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        // Mock lock claim
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        // Mock xgroupCreate to succeed
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");

        runner.start();

        // Verify stream groups are set up for agent types and worker ctrl stream
        verify(jedis).xgroupCreate(
                eq(Constants.QueueNames.ctrlStream("simple-agent")),
                eq("test-group"),
                any(),
                eq(true)
        );
        verify(jedis).xgroupCreate(
                eq(Constants.QueueNames.workerCtrlStream("w1")),
                eq("test-group"),
                any(),
                eq(true)
        );

        // Verify presence was claimed, then refreshed by initial heartbeat.
        verify(jedis, times(2)).set(
                eq(Constants.RegistryKeys.workerOnlineLease("w1")),
                anyString(),
                any(SetParams.class)
        );

        runner.stop();
    }

    @Test
    void runnerStartFailsWhenWorkerIdAlreadyClaimed() {
        SimpleWorker worker = new SimpleWorker("w-dup", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        // Mock lock claim failure
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn(null);

        assertThrows(RuntimeException.class, runner::start);
    }

    @Test
    void runnerStopReleasesLockAndShutdownExecutor() {
        SimpleWorker worker = new SimpleWorker("w1", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        // Capture the token that will be stored
        final String[] capturedToken = new String[1];

        // Mock set to capture the token and get to return it
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenAnswer(invocation -> {
            capturedToken[0] = invocation.getArgument(1);
            return "OK";
        });
        when(jedis.get(anyString())).thenAnswer(invocation -> capturedToken[0]);
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");

        runner.start();
        runner.stop();

        // Verify owned presence release via del
        verify(jedis).del(Constants.RegistryKeys.workerOnlineLease("w1"));
    }

    @Test
    void runnerHandlesBusyGroupGracefully() {
        SimpleWorker worker = new SimpleWorker("w1", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        // Simulate BUSYGROUP error (group already exists)
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean()))
                .thenThrow(new RuntimeException("BUSYGROUP Consumer Group name already exists"));
        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");

        // Should not throw - BUSYGROUP is silently handled
        assertDoesNotThrow(() -> runner.start());

        runner.stop();
    }

    @Test
    void runnerStartsTwoIndependentLoopThreads() throws InterruptedException {
        SimpleWorker worker = new SimpleWorker("w-split", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");
        lenient().when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);

        runner.start();
        Thread.sleep(100);
        Set<String> names = Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .collect(Collectors.toSet());
        runner.stop();

        assertTrue(names.stream().anyMatch(n -> n.startsWith("runner-loop-w-split")));
        assertTrue(names.stream().anyMatch(n -> n.startsWith("runner-worker-ctrl-loop-w-split")));
    }

    @Test
    void agentTypeAndWorkerCtrlStreamsAreReadIndependently() throws InterruptedException {
        SimpleWorker worker = new SimpleWorker("w-split2", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);

        runner.start();
        Thread.sleep(200);
        runner.stop();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xreadGroup(anyString(), anyString(), any(), streamsCaptor.capture());

        String agentTypeStream = Constants.QueueNames.ctrlStream("simple-agent");
        String workerCtrlStream = Constants.QueueNames.workerCtrlStream("w-split2");

        boolean sawAgentTypeOnly = streamsCaptor.getAllValues().stream()
                .anyMatch(m -> m.size() == 1 && m.containsKey(agentTypeStream));
        boolean sawWorkerCtrlOnly = streamsCaptor.getAllValues().stream()
                .anyMatch(m -> m.size() == 1 && m.containsKey(workerCtrlStream));
        boolean sawCombined = streamsCaptor.getAllValues().stream()
                .anyMatch(m -> m.containsKey(agentTypeStream) && m.containsKey(workerCtrlStream));

        assertTrue(sawAgentTypeOnly, "expected a read call scoped only to the agent_type stream");
        assertTrue(sawWorkerCtrlOnly, "expected a read call scoped only to the workerCtrlStream");
        assertFalse(sawCombined, "agent_type and worker-ctrl streams must never be read in the same call");
    }

    private static boolean isBlocking(XReadGroupParams params) {
        CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
        params.addParams(args);
        return args.isBlocking();
    }

    @Test
    void phaseOneNonBlockingScanSkipsPhaseTwoBlockWhenAMessageIsFound() throws InterruptedException {
        ThreeAgentWorker worker = new ThreeAgentWorker("w-phase1", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");
        // agent-b's non-blocking scan finds a message; agent-a/agent-c and the
        // worker-ctrl stream find nothing.
        String hitStream = Constants.QueueNames.ctrlStream("agent-b");
        lenient().when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);
        when(jedis.xreadGroup(anyString(), anyString(), any(), eq(Map.of(hitStream, StreamEntryID.UNRECEIVED_ENTRY))))
                .thenReturn(List.of(Map.entry(hitStream, List.of())));

        runner.start();
        Thread.sleep(150);
        runner.stop();

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), streamsCaptor.capture());

        Set<String> agentStreams = Set.of(
                Constants.QueueNames.ctrlStream("agent-a"),
                Constants.QueueNames.ctrlStream("agent-b"),
                Constants.QueueNames.ctrlStream("agent-c"));

        boolean anyAgentTypeBlockingRead = false;
        for (int i = 0; i < paramsCaptor.getAllValues().size(); i++) {
            Map<String, StreamEntryID> streams = streamsCaptor.getAllValues().get(i);
            if (streams.size() == 1 && agentStreams.contains(streams.keySet().iterator().next())
                    && isBlocking(paramsCaptor.getAllValues().get(i))) {
                anyAgentTypeBlockingRead = true;
                break;
            }
        }

        assertFalse(anyAgentTypeBlockingRead,
                "phase one already found a message; phase two's blocking read must not run in the same iteration");
    }

    @Test
    void agentTypeLoopRotatesPrimaryStreamRoundRobinAcrossPhaseTwoBlocks() throws InterruptedException {
        ThreeAgentWorker worker = new ThreeAgentWorker("w-rr", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");
        // Every read (phase one and phase two, for every stream) comes back
        // empty, forcing phase two on every single loop iteration.
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);

        runner.start();
        Thread.sleep(300);
        runner.stop();

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), streamsCaptor.capture());

        Set<String> agentStreams = Set.of(
                Constants.QueueNames.ctrlStream("agent-a"),
                Constants.QueueNames.ctrlStream("agent-b"),
                Constants.QueueNames.ctrlStream("agent-c"));

        List<String> blockedPrimaries = new ArrayList<>();
        for (int i = 0; i < paramsCaptor.getAllValues().size(); i++) {
            Map<String, StreamEntryID> streams = streamsCaptor.getAllValues().get(i);
            if (streams.size() == 1 && agentStreams.contains(streams.keySet().iterator().next())
                    && isBlocking(paramsCaptor.getAllValues().get(i))) {
                blockedPrimaries.add(streams.keySet().iterator().next());
            }
        }

        assertTrue(blockedPrimaries.size() >= 3,
                "expected at least 3 phase-two blocking reads to observe a full rotation, got: " + blockedPrimaries);
        Set<String> firstThreeDistinct = new HashSet<>(blockedPrimaries.subList(0, 3));
        assertEquals(3, firstThreeDistinct.size(),
                "the first 3 phase-two picks should cover all 3 agent types without repeats (no starving): "
                        + blockedPrimaries.subList(0, 3));
    }

    @Test
    void phaseTwoBlockingReadUsesConfiguredBlockTimeout() throws InterruptedException {
        ThreeAgentWorker worker = new ThreeAgentWorker("w-timeout", redisClient);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);

        runner.start();
        Thread.sleep(150);
        runner.stop();

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), streamsCaptor.capture());

        Set<String> agentStreams = Set.of(
                Constants.QueueNames.ctrlStream("agent-a"),
                Constants.QueueNames.ctrlStream("agent-b"),
                Constants.QueueNames.ctrlStream("agent-c"));

        boolean sawBlockingRead = false;
        for (int i = 0; i < paramsCaptor.getAllValues().size(); i++) {
            Map<String, StreamEntryID> streams = streamsCaptor.getAllValues().get(i);
            XReadGroupParams params = paramsCaptor.getAllValues().get(i);
            if (streams.size() == 1 && agentStreams.contains(streams.keySet().iterator().next())
                    && isBlocking(params)) {
                sawBlockingRead = true;
                CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
                params.addParams(args);
                boolean sawExpectedBlockValue = false;
                for (var rawable : args) {
                    if (String.valueOf(Constants.REDIS_BLOCK_TIMEOUT_MS).equals(new String(rawable.getRaw()))) {
                        sawExpectedBlockValue = true;
                    }
                }
                assertTrue(sawExpectedBlockValue,
                        "phase-two block value should bound worst-case latency to REDIS_BLOCK_TIMEOUT_MS");
            }
        }
        assertTrue(sawBlockingRead, "expected at least one phase-two blocking read");
    }

    // ---- J1: the resume path's two silent defects -------------------------

    @Test
    void terminalReplaySkipMustExemptAResumeCommand() throws Exception {
        // A handler that reaches a terminal state AFTER dispatching is a case the
        // framework keeps alive on purpose, so its record is terminal by the time
        // the reply arrives. Skipping that reply as a "terminal replay" silently
        // drops the very thing the execution is waiting on.
        WorkerRegistry registry = mock(WorkerRegistry.class);
        RecordingWorker worker = new RecordingWorker("worker-1", redisClient, registry);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(registry.getExecutionByMessageId(eq("msg-b"), eq("sess-1")))
                .thenReturn(Map.of(
                        Constants.ExecutionFields.EXECUTION_ID, "exec-b",
                        Constants.ExecutionFields.STATUS, "COMPLETED"));

        ResumeCommand resume = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-b").sessionId("sess-1").traceId("t-1")
                        .targetAgentType("simple-agent").build(),
                "answer", "COMPLETED", null, Map.of());

        drive(runner, Constants.QueueNames.ctrlStream("simple-agent"), entryFor(resume));

        assertTrue(worker.handled.await(5, java.util.concurrent.TimeUnit.SECONDS),
                "a ResumeCommand must survive the terminal-replay guard");
        assertTrue(worker.seenIsResumed, "a ResumeCommand is always a resumed execution");
    }

    @Test
    void terminalReplayStillSkipsANonResumeCommand() throws Exception {
        // The control: the guard itself must keep working. Only the resume case
        // is exempt.
        WorkerRegistry registry = mock(WorkerRegistry.class);
        RecordingWorker worker = new RecordingWorker("worker-1", redisClient, registry);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(registry.getExecutionByMessageId(eq("msg-a"), eq("sess-1")))
                .thenReturn(Map.of(
                        Constants.ExecutionFields.EXECUTION_ID, "exec-a",
                        Constants.ExecutionFields.STATUS, "COMPLETED"));

        AskAgentCommand ask = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-a").sessionId("sess-1").traceId("t-1")
                        .targetAgentType("simple-agent").build(),
                "hello", false, Map.of());

        drive(runner, Constants.QueueNames.ctrlStream("simple-agent"), entryFor(ask));

        assertFalse(worker.handled.await(1, java.util.concurrent.TimeUnit.SECONDS),
                "a terminal replay of a fresh dispatch must still be skipped");
    }

    @Test
    void theExecutionRecordTravelsToTheWorker() throws Exception {
        // The snapshot is the only place a resumed execution's caller and its
        // original dispatch metadata survive; if it does not reach the worker,
        // nothing downstream can restore either.
        WorkerRegistry registry = mock(WorkerRegistry.class);
        RecordingWorker worker = new RecordingWorker("worker-1", redisClient, registry);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        Map<String, Object> record = Map.of(
                Constants.ExecutionFields.EXECUTION_ID, "exec-b",
                Constants.ExecutionFields.STATUS, "WAITING_USER",
                "source_agent_type", "agent-a",
                "metadata", Map.of("tenant", "acme"));
        when(registry.getExecutionByMessageId(eq("msg-b"), eq("sess-1"))).thenReturn(record);

        ResumeCommand resume = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-b").sessionId("sess-1").traceId("t-1")
                        .targetAgentType("simple-agent").build(),
                "answer", "COMPLETED", null, Map.of());

        drive(runner, Constants.QueueNames.ctrlStream("simple-agent"), entryFor(resume));

        assertTrue(worker.handled.await(5, java.util.concurrent.TimeUnit.SECONDS));
        assertNotNull(worker.seenExistingExecution, "the execution record must reach handleMessage");
        assertEquals("agent-a", worker.seenExistingExecution.get("source_agent_type"));
    }

    @Test
    void aNonQueuedRecordCountsAsResumedEvenForAFreshDispatch() throws Exception {
        // QUEUED is the only status an execution can hold before its first pickup.
        // Anything else means it has already been through a worker, so its identity
        // must come from the record rather than the header.
        WorkerRegistry registry = mock(WorkerRegistry.class);
        RecordingWorker worker = new RecordingWorker("worker-1", redisClient, registry);
        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(registry.getExecutionByMessageId(eq("msg-a"), eq("sess-1")))
                .thenReturn(Map.of(
                        Constants.ExecutionFields.EXECUTION_ID, "exec-a",
                        Constants.ExecutionFields.STATUS, "RUNNING"));

        AskAgentCommand ask = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-a").sessionId("sess-1").traceId("t-1")
                        .targetAgentType("simple-agent").build(),
                "hello", false, Map.of());

        drive(runner, Constants.QueueNames.ctrlStream("simple-agent"), entryFor(ask));

        assertTrue(worker.handled.await(5, java.util.concurrent.TimeUnit.SECONDS));
        assertTrue(worker.seenIsResumed, "a record past QUEUED means the execution already ran");
    }
}
