package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CommandWireTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void serializesAskAgentCommandAsActionTypeHeaderBody() throws Exception {
        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId("msg-1")
                        .sessionId("sess-1")
                        .traceId("trace-1")
                        .targetAgentType("demo-agent-java")
                        .userCode("tenant-1")
                        .userName("user-1")
                        .build(),
                "hello world",
                false,
                Map.of("attachments", java.util.List.of()));

        Map<?, ?> payload = objectMapper.readValue(objectMapper.writeValueAsString(command), Map.class);
        assertEquals(ActionType.ASK_AGENT, payload.get("action_type"));
        assertEquals("demo-agent-java", ((Map<?, ?>) payload.get("header")).get("target_agent_type"));
        assertEquals("hello world", ((Map<?, ?>) payload.get("body")).get("content"));
    }

    @Test
    void decodesResumeCommandFromWirePayload() {
        GatewayCommand command = GatewayCommandFactory.fromMap(Map.of(
                "action_type", ActionType.RESUME,
                "header", Map.of(
                        "message_id", "msg-2",
                        "session_id", "sess-2",
                        "trace_id", "trace-2",
                        "source_agent_type", "agent-a",
                        "target_agent_type", "agent-b",
                        "parent_message_id", "msg-1",
                        "user_code", "",
                        "user_name", "",
                        "metadata", Map.of()),
                "body", Map.of(
                        "content", "",
                        "status", "SUCCESS",
                        "reply_data", Map.of("ok", true))));

        ResumeCommand resumeCommand = assertInstanceOf(ResumeCommand.class, command);
        assertEquals("msg-1", resumeCommand.header().parentMessageId());
        assertEquals("SUCCESS", resumeCommand.status());
    }

    @Test
    void supportsRegisteredCustomCommandDecoding() {
        GatewayCommandFactory.registerCommand("CUSTOM_COMMAND", CustomCommand.class);
        try {
            GatewayCommand command = GatewayCommandFactory.fromMap(Map.of(
                    "action_type", "CUSTOM_COMMAND",
                    "header", Map.of(
                            "message_id", "custom-1",
                            "session_id", "sess-custom",
                            "trace_id", "trace-custom",
                            "source_agent_type", "",
                            "target_agent_type", "custom-agent",
                            "parent_message_id", "",
                            "user_code", "",
                            "user_name", "",
                            "metadata", Map.of()),
                    "body", Map.of(
                            "payload", Map.of("mode", "custom"))));

            CustomCommand customCommand = assertInstanceOf(CustomCommand.class, command);
            assertEquals("custom", customCommand.getBody().getPayload().get("mode"));
        } finally {
            GatewayCommandFactory.unregisterCommand("CUSTOM_COMMAND");
        }
    }

    @lombok.Data
    public static class CustomCommand implements GatewayCommand {
        @com.fasterxml.jackson.annotation.JsonProperty("action_type")
        private String action_type = "CUSTOM_COMMAND";

        @com.fasterxml.jackson.annotation.JsonProperty("header")
        private MessageHeader messageHeader;

        @com.fasterxml.jackson.annotation.JsonProperty("body")
        private CustomBody body;

        @Override
        public String actionType() {
            return action_type;
        }

        @Override
        public MessageHeader header() {
            return messageHeader;
        }
    }

    @lombok.Data
    public static class CustomBody {
        @com.fasterxml.jackson.annotation.JsonProperty("payload")
        private Map<String, String> payload;
    }
}
