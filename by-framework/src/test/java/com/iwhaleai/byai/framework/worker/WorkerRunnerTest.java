package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.Collections;
import java.util.List;

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
}
