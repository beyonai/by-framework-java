package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ScatterGatherTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Mock
    private redis.clients.jedis.Pipeline pipeline;

    private TestWorker worker;

    private static class TestWorker extends GatewayWorker {
        public boolean processCommandCalled = false;
        public GatewayCommand lastCommand = null;

        public TestWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return Collections.singletonList("test-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            processCommandCalled = true;
            lastCommand = command;
            return "SUCCESS";
        }
    }

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        lenient().when(jedis.pipelined()).thenReturn(pipeline);
        worker = new TestWorker("worker-1", redisClient);
    }

    @Test
    void testJoinLogic_Waiting() {
        // Arrange
        String groupId = "group-123";
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-1")
                .sessionId("session-1")
                .taskGroupId(groupId)
                .targetAgentType("test-agent")
                .build();
        ResumeCommand command = ResumeCommand.of(header, "", "SUCCESS", "data", null);

        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        when(jedis.hget(groupKey, "total")).thenReturn("3");
        when(jedis.hincrBy(groupKey, "completed", 1)).thenReturn(1L); // 1 out of 3

        // Act
        worker.handleMessage(command, "exec-test-id");

        // Assert
        assert !worker.processCommandCalled;
        verify(jedis).hincrBy(groupKey, "completed", 1);
    }

    @Test
    void testJoinLogic_Completed() {
        // Arrange
        String groupId = "group-123";
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-1")
                .sessionId("session-1")
                .taskGroupId(groupId)
                .targetAgentType("test-agent")
                .build();
        ResumeCommand command = ResumeCommand.of(header, "", "SUCCESS", "data", null);

        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        when(jedis.hget(groupKey, "total")).thenReturn("3");
        when(jedis.hincrBy(groupKey, "completed", 1)).thenReturn(3L); // 3 out of 3

        // Act
        worker.handleMessage(command, "exec-test-id");

        // Assert
        assert worker.processCommandCalled;
        verify(jedis).hincrBy(groupKey, "completed", 1);
    }

    @Test
    void testNormalResume_NoGroup() {
        // Arrange
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-1")
                .sessionId("session-1")
                .targetAgentType("test-agent")
                .build();
        ResumeCommand command = ResumeCommand.of(header, "", "SUCCESS", "data", null);

        // Act
        worker.handleMessage(command, "exec-test-id");

        // Assert
        assert worker.processCommandCalled;
        verify(jedis, never()).hincrBy(anyString(), anyString(), anyLong());
    }

    @Test
    void groupJoinAggregatesAllMemberResultsNotJustTheLastArrival() throws Exception {
        String groupId = "group-agg";
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-last")
                .sessionId("session-1")
                .taskGroupId(groupId)
                .targetAgentType("test-agent")
                .build();
        ResumeCommand command = ResumeCommand.of(header, "", "SUCCESS", "last-member-only-reply", null);

        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        String resultsKey = Constants.RegistryKeys.taskGroupResults(groupId);
        when(jedis.hget(groupKey, "total")).thenReturn("2");
        when(jedis.hget(groupKey, "aborted")).thenReturn(null);
        when(jedis.hincrBy(groupKey, "completed", 1)).thenReturn(2L); // last member to complete
        when(jedis.hgetAll(resultsKey)).thenReturn(Map.of(
                "msg-a", "{\"status\":\"SUCCESS\",\"reply_data\":\"A\",\"content\":\"\",\"metadata\":{},\"extra_payload\":{}}",
                "msg-b", "{\"status\":\"SUCCESS\",\"reply_data\":\"B\",\"content\":\"\",\"metadata\":{},\"extra_payload\":{}}"
        ));

        worker.handleMessage(command, "exec-test-id");

        assertTrue(worker.processCommandCalled);
        assertInstanceOf(ResumeCommand.class, worker.lastCommand);
        Object replyData = ((ResumeCommand) worker.lastCommand).replyData();
        assertInstanceOf(List.class, replyData);
        List<?> aggregate = (List<?>) replyData;
        assertEquals(2, aggregate.size());
        List<?> replies = aggregate.stream()
                .map(entry -> ((Map<?, ?>) entry).get("reply_data"))
                .toList();
        assertTrue(replies.contains("A"));
        assertTrue(replies.contains("B"));
    }

    @Test
    void abortedGroupDiscardsLateReplyInsteadOfResumingTheCaller() {
        String groupId = "group-aborted";
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-late")
                .sessionId("session-1")
                .taskGroupId(groupId)
                .targetAgentType("test-agent")
                .build();
        ResumeCommand command = ResumeCommand.of(header, "", "SUCCESS", "late-reply", null);

        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        when(jedis.hget(groupKey, "total")).thenReturn("2");
        when(jedis.hget(groupKey, "aborted")).thenReturn("1");

        worker.handleMessage(command, "exec-test-id");

        assertFalse(worker.processCommandCalled);
        verify(jedis, never()).hincrBy(anyString(), anyString(), anyLong());
    }

    @Test
    void dispatchGroupIsAnAliasProducingIdenticalRedisWritesToCallAgents() {
        AgentContext callAgentsContext = new AgentContext("sess-1", "trace-1", redisClient, "caller-agent", "parent-msg");
        AgentContext dispatchGroupContext = new AgentContext("sess-2", "trace-2", redisClient, "caller-agent", "parent-msg");
        List<Map<String, Object>> requests = List.of(
                Map.of("agent_type", "sub-agent", "content", "hello")
        );

        callAgentsContext.callAgents(requests, true);
        dispatchGroupContext.dispatchGroup(requests, true);

        // Both entry points must hit the exact same Redis operations (xadd to the same
        // ctrl stream, hsetAll on a task_group hash) — no forked implementation.
        verify(jedis, times(2)).xadd(eq(Constants.QueueNames.ctrlStream("sub-agent")), any(redis.clients.jedis.params.XAddParams.class), anyMap());
    }

    @Test
    void dispatchFailurePartwayThroughFanOutMarksTheGroupAborted() {
        AgentContext context = new AgentContext("sess-abort", "trace-abort", redisClient, "caller-agent", "parent-msg");
        List<Map<String, Object>> requests = List.of(
                Map.of("agent_type", "sub-agent-1", "content", "first"),
                Map.of("agent_type", "sub-agent-2", "content", "second")
        );
        when(jedis.xadd(eq(Constants.QueueNames.ctrlStream("sub-agent-2")), any(redis.clients.jedis.params.XAddParams.class), anyMap()))
                .thenThrow(new RuntimeException("simulated dispatch failure"));

        assertThrows(RuntimeException.class, () -> context.callAgents(requests, true));

        verify(jedis).hset(anyString(), eq(Constants.TASK_GROUP_FIELD_ABORTED), eq("1"));
    }
}
