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

import static org.mockito.ArgumentMatchers.anyString;
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
}
