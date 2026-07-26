package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.BaiYingMessage;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class ByaiWorkerTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
    }

    private static class RecordingByaiWorker extends ByaiWorker {
        public GatewayCommand receivedCommand = null;
        public AgentContext receivedContext = null;

        RecordingByaiWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("byai-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            receivedCommand = command;
            receivedContext = context;
            return "done";
        }
    }

    private AskAgentCommand wireMessageCommand() {
        Map<String, Object> wireContent = new HashMap<>();
        wireContent.put("text", "hello from wire");
        Map<String, Object> wireMessage = new HashMap<>();
        wireMessage.put("role", "user");
        wireMessage.put("content", wireContent);

        MessageHeader header = MessageHeader.builder()
                .messageId("msg-byai-1")
                .sessionId("sess-byai-1")
                .traceId("trace-byai-1")
                .targetAgentType("byai-agent")
                .build();
        return AskAgentCommand.of(header, List.of(wireMessage), false, Map.of());
    }

    @Test
    void processCommandReceivesAlreadyDecodedBaiYingMessage() {
        RecordingByaiWorker worker = new RecordingByaiWorker("worker-byai", redisClient);

        worker.handleMessage(wireMessageCommand(), "exec-1");

        assertNotNull(worker.receivedCommand);
        Object content = ((AskAgentCommand) worker.receivedCommand).content();
        assertInstanceOf(BaiYingMessage.class, content);
        BaiYingMessage message = (BaiYingMessage) content;
        assertEquals("user", message.getRole());
        assertInstanceOf(BaiYingMessage.MessageContent.class, message.getContent());
        assertEquals("hello from wire", ((BaiYingMessage.MessageContent) message.getContent()).getText());
    }

    @Test
    void processCommandReceivesAByaiAgentContextInstance() {
        RecordingByaiWorker worker = new RecordingByaiWorker("worker-byai", redisClient);

        worker.handleMessage(wireMessageCommand(), "exec-1");

        assertInstanceOf(ByaiAgentContext.class, worker.receivedContext);
    }

    @Test
    void plainStringWireContentPassesThroughUnchanged() {
        RecordingByaiWorker worker = new RecordingByaiWorker("worker-byai", redisClient);
        MessageHeader header = MessageHeader.builder()
                .messageId("msg-byai-2")
                .sessionId("sess-byai-2")
                .traceId("trace-byai-2")
                .targetAgentType("byai-agent")
                .build();
        AskAgentCommand command = AskAgentCommand.of(header, "plain string content", false, Map.of());

        worker.handleMessage(command, "exec-2");

        assertEquals("plain string content", ((AskAgentCommand) worker.receivedCommand).content());
    }
}
