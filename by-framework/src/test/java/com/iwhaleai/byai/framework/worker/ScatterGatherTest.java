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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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
        public GatewayCommand lastCommand;

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

    /** A reply shaped the way GatewayWorker's agent return shapes it. */
    private static ResumeCommand reply(String groupId, String subTaskMessageId, String sourceAgentType,
            String status, String content, Object replyData) {
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("caller-msg")
                        .sessionId("session-1")
                        .sourceAgentType(sourceAgentType)
                        .targetAgentType("test-agent")
                        .parentMessageId(subTaskMessageId)
                        .taskGroupId(groupId)
                        .build(),
                content, status, replyData, null);
    }

    private void stubV2Group(String groupKey, String total, String taskOrderJson) {
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL)).thenReturn(total);
        lenient().when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_ABORTED)).thenReturn(null);
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_PROTOCOL_VERSION))
                .thenReturn(Constants.TASK_GROUP_PROTOCOL_V2);
        lenient().when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_TASK_ORDER)).thenReturn(taskOrderJson);
    }

    @Test
    void v2GroupKeysResultsBySubTaskIdAndAggregatesInDispatchOrder() {
        String groupId = "group-v2";
        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        String resultsKey = Constants.RegistryKeys.taskGroupResults(groupId);
        stubV2Group(groupKey, "2", "[\"sub-a\",\"sub-b\"]");
        when(jedis.hincrBy(groupKey, Constants.TASK_GROUP_FIELD_COMPLETED, 1)).thenReturn(2L);
        when(jedis.hgetAll(resultsKey)).thenReturn(Map.of(
                "sub-b", "{\"status\":\"COMPLETED\",\"content\":\"B\",\"target_agent_type\":\"agent-b\"}",
                "sub-a", "{\"status\":\"COMPLETED\",\"content\":\"A\",\"target_agent_type\":\"agent-a\"}"));

        worker.handleMessage(reply(groupId, "sub-a", "agent-a", "COMPLETED", "A", null), "exec-1");

        // Stored under the sub-task's own id, not the caller's shared messageId.
        verify(jedis).hset(eq(resultsKey), eq("sub-a"), anyString());
        assert worker.processCommandCalled;
        ResumeCommand resumed = (ResumeCommand) worker.lastCommand;
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> aggregate = (List<Map<String, Object>>) resumed.replyData();
        assert aggregate.size() == 2 : aggregate;
        // task_order wins over the hash's iteration order.
        assert "sub-a".equals(aggregate.get(0).get("message_id")) : aggregate;
        assert "sub-b".equals(aggregate.get(1).get("message_id")) : aggregate;
        assert "agent-a".equals(aggregate.get(0).get("target_agent_type")) : aggregate;
        // replyData is the single aggregation channel.
        assert "".equals(resumed.content()) : resumed.content();
    }

    @Test
    void unstampedGroupKeepsLegacyJoin() {
        String groupId = "group-legacy";
        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        String resultsKey = Constants.RegistryKeys.taskGroupResults(groupId);
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL)).thenReturn("1");
        lenient().when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_ABORTED)).thenReturn(null);
        // No protocol_version: written by a pre-v2 dispatcher, possibly mid-upgrade.
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_PROTOCOL_VERSION)).thenReturn(null);
        when(jedis.hincrBy(groupKey, Constants.TASK_GROUP_FIELD_COMPLETED, 1)).thenReturn(1L);

        worker.handleMessage(reply(groupId, "sub-a", "agent-a", "COMPLETED", "A", "raw"), "exec-1");

        // Legacy key (the caller's messageId) and no aggregation.
        verify(jedis).hset(eq(resultsKey), eq("caller-msg"), anyString());
        verify(jedis, never()).hgetAll(resultsKey);
        assert worker.processCommandCalled;
        ResumeCommand resumed = (ResumeCommand) worker.lastCommand;
        assert "raw".equals(resumed.replyData()) : resumed.replyData();
        assert "A".equals(resumed.content()) : resumed.content();
    }

    @Test
    void abortedGroupDiscardsLateRepliesWithoutCounting() {
        String groupId = "group-aborted";
        String groupKey = Constants.RegistryKeys.taskGroup(groupId);
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL)).thenReturn("2");
        when(jedis.hget(groupKey, Constants.TASK_GROUP_FIELD_ABORTED)).thenReturn("1");

        worker.handleMessage(reply(groupId, "sub-a", "agent-a", "COMPLETED", "A", null), "exec-1");

        assert !worker.processCommandCalled;
        verify(jedis, never()).hincrBy(anyString(), anyString(), anyLong());
    }

    @Test
    void agentReturnCarriesTheCallerMessageIdSoTheSuspendedExecutionResolves() {
        // An inbound sub-task: caller "caller-agent" (message caller-msg) called this
        // worker with dispatch message id sub-a.
        MessageHeader header = MessageHeader.builder()
                .messageId("sub-a")
                .sessionId("session-1")
                .sourceAgentType("caller-agent")
                .targetAgentType("test-agent")
                .parentMessageId("caller-msg")
                .build();

        worker.handleMessage(
                com.iwhaleai.byai.framework.core.protocol.AskAgentCommand.of(header, "work", true, Map.of()),
                "exec-1");

        ArgumentCaptor<Map<String, String>> fields = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xadd(eq(Constants.QueueNames.ctrlStream("caller-agent")),
                any(redis.clients.jedis.params.XAddParams.class), fields.capture());
        String payload = fields.getValue().get(Constants.RedisFields.DATA);
        // WorkerRunner reattaches the suspended caller execution by this field.
        assert payload.contains("\"message_id\":\"caller-msg\"") : payload;
        // Unique per sibling — what Group Join keys the v2 result hash by.
        assert payload.contains("\"parent_message_id\":\"sub-a\"") : payload;
    }
}
