package com.iwhaleai.byai.framework.core.protocol;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Protocol 完整测试，对标 Python test_protocol.py
 */
class ProtocolTest {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Test
        void dataMessageCreationAndFieldAccess() {
                DataMessage msg = DataMessage.builder()
                                .messageId("msg-1")
                                .traceId("trace-1")
                                .sessionId("sess-1")
                                .eventType(EventType.ANSWER_DELTA.getValue())
                                .sourceAgentType("agent-a")
                                .data(Map.of("content", "hello"))
                                .stateMsg("")
                                .artifactUrl("")
                                .metadata(Map.of("key", "value"))
                                .build();

                assertEquals("msg-1", msg.getMessageId());
                assertEquals("trace-1", msg.getTraceId());
                assertEquals("sess-1", msg.getSessionId());
                assertEquals("answerDelta", msg.getEventType());
                assertEquals("agent-a", msg.getSourceAgentType());
                assertEquals("hello", msg.getData().get("content"));
                assertEquals("value", msg.getMetadata().get("key"));
        }

        @Test
        void cancelActionAndStatesExist() {
                assertEquals("CANCEL_TASK", ActionType.CANCEL_TASK);
                assertEquals("COMPLETED", AgentState.COMPLETED);
                assertEquals("FAILED", AgentState.FAILED);
                assertEquals("STARTING", AgentState.STARTING);
                assertEquals("RUNNING", AgentState.RUNNING);
                assertEquals("WAITING_USER", AgentState.WAITING_USER);
                assertEquals("CALLING_AGENT", AgentState.CALLING_AGENT);
                assertEquals("RESUMED", AgentState.RESUMED);
                assertEquals("QUEUED", AgentState.QUEUED);
        }

        @Test
        void workerControlStreamName() {
                String streamName = Constants.QueueNames.workerCtrlStream("worker-1");
                assertEquals("byai_gateway:ctrl:worker:worker-1", streamName);
        }

        @Test
        void ctrlStreamName() {
                String streamName = Constants.QueueNames.ctrlStream("demo-agent");
                assertEquals("byai_gateway:ctrl:agent_type:demo-agent", streamName);
        }

        @Test
        void sessionDataStreamName() {
                String streamName = Constants.QueueNames.sessionDataStream("sess-123");
                assertEquals("byai_gateway:session:sess-123:data_stream", streamName);
        }

        @Test
        void askAgentCommandFactoryMethodSetsFields() {
                AskAgentCommand cmd = AskAgentCommand.of(
                                MessageHeader.builder()
                                                .messageId("msg-1")
                                                .sessionId("sess-1")
                                                .traceId("trace-1")
                                                .targetAgentType("agent-x")
                                                .build(),
                                "hello",
                                true,
                                Map.of("key", "val"));

                assertEquals(ActionType.ASK_AGENT, cmd.actionType());
                assertEquals("hello", cmd.content());
                assertTrue(cmd.waitForReply());
                assertEquals("val", cmd.extraPayload().get("key"));
                assertEquals("agent-x", cmd.header().targetAgentType());
        }

        @Test
        void askAgentContentReturnsEmptyWhenBodyNull() {
                AskAgentCommand cmd = AskAgentCommand.builder()
                                .header(MessageHeader.builder().messageId("m").sessionId("s").traceId("t").build())
                                .build();
                assertEquals("", cmd.content());
                assertFalse(cmd.waitForReply());
                assertNotNull(cmd.extraPayload());
        }

        @Test
        void resumeCommandFactoryMethodSetsFields() {
                ResumeCommand cmd = ResumeCommand.of(
                                MessageHeader.builder()
                                                .messageId("msg-2")
                                                .sessionId("sess-2")
                                                .traceId("trace-2")
                                                .parentMessageId("msg-1")
                                                .build(),
                                "resume content",
                                "SUCCESS",
                                Map.of("ok", true),
                                Map.of("extra", "data"));

                assertEquals(ActionType.RESUME, cmd.actionType());
                assertEquals("resume content", cmd.content());
                assertEquals("SUCCESS", cmd.status());
                assertNotNull(cmd.replyData());
                assertEquals("data", cmd.extraPayload().get("extra"));
        }

        @Test
        void resumeCommandDefaultsStatusToEmptyWhenNull() {
                ResumeCommand cmd = ResumeCommand.of(
                                MessageHeader.builder().messageId("m").sessionId("s").traceId("t").build(),
                                "content",
                                null,
                                null,
                                null);
                assertEquals("", cmd.status());
                assertNotNull(cmd.extraPayload());
        }

        @Test
        void resumeContentReturnsEmptyWhenBodyNull() {
                ResumeCommand cmd = ResumeCommand.builder()
                                .header(MessageHeader.builder().messageId("m").sessionId("s").traceId("t").build())
                                .build();
                assertEquals("", cmd.content());
                assertEquals("", cmd.status());
                assertNull(cmd.replyData());
        }

        @Test
        void cancelTaskCommandBuilderSetsFields() {
                CancelTaskCommand cmd = CancelTaskCommand.builder()
                                .header(MessageHeader.builder()
                                                .messageId("cancel-1")
                                                .sessionId("sess-1")
                                                .traceId("trace-1")
                                                .build())
                                .body(CancelTaskCommand.CancelTaskBody.builder()
                                                .targetMessageId("target-msg-1")
                                                .targetExecutionId("exec-1")
                                                .targetWorkerId("worker-1")
                                                .reason("user requested")
                                                .requestedBy("admin")
                                                .cancelMode("force")
                                                .build())
                                .build();

                assertEquals(ActionType.CANCEL_TASK, cmd.actionType());
                assertEquals("target-msg-1", cmd.targetMessageId());
                assertEquals("target-msg-1", cmd.body().targetMessageId());
                assertEquals("exec-1", cmd.body().targetExecutionId());
                assertEquals("worker-1", cmd.body().targetWorkerId());
                assertEquals("user requested", cmd.body().reason());
                assertEquals("admin", cmd.body().requestedBy());
                assertEquals("force", cmd.body().cancelMode());
        }

        @Test
        void cancelTargetMessageIdReturnsEmptyWhenBodyNull() {
                CancelTaskCommand cmd = CancelTaskCommand.builder()
                                .header(MessageHeader.builder().messageId("m").sessionId("s").traceId("t").build())
                                .build();
                assertEquals("", cmd.targetMessageId());
        }

        @Test
        void cancelTaskBodyDefaults() {
                CancelTaskCommand.CancelTaskBody body = CancelTaskCommand.CancelTaskBody.builder()
                                .targetMessageId("msg-1")
                                .build();

                assertEquals("msg-1", body.targetMessageId());
                assertEquals("", body.targetExecutionId());
                assertEquals("", body.targetWorkerId());
                assertEquals("", body.reason());
                assertEquals("", body.requestedBy());
                assertEquals("graceful", body.cancelMode());
        }

        @Test
        void cancelCommandSerializesToWireFormat() throws Exception {
                CancelTaskCommand cmd = CancelTaskCommand.builder()
                                .header(MessageHeader.builder()
                                                .messageId("cancel-1")
                                                .sessionId("sess-1")
                                                .traceId("trace-1")
                                                .targetAgentType("agent-x")
                                                .build())
                                .body(CancelTaskCommand.CancelTaskBody.builder()
                                                .targetMessageId("target-msg-1")
                                                .reason("test reason")
                                                .build())
                                .build();

                String json = objectMapper.writeValueAsString(cmd);
                Map<?, ?> payload = objectMapper.readValue(json, Map.class);

                assertEquals(ActionType.CANCEL_TASK, payload.get("action_type"));
                Map<?, ?> header = (Map<?, ?>) payload.get("header");
                assertEquals("cancel-1", header.get("message_id"));
                assertEquals("sess-1", header.get("session_id"));
                Map<?, ?> body = (Map<?, ?>) payload.get("body");
                assertEquals("target-msg-1", body.get("target_message_id"));
                assertEquals("test reason", body.get("reason"));
        }

        @Test
        void askAgentCommandRoundTripFromWireDict() {
                Map<String, Object> wireDict = Map.of(
                                "action_type", ActionType.ASK_AGENT,
                                "header", Map.of(
                                                "message_id", "msg-round",
                                                "session_id", "sess-round",
                                                "trace_id", "trace-round",
                                                "source_agent_type", "",
                                                "target_agent_type", "round-agent",
                                                "parent_message_id", "",
                                                "user_code", "",
                                                "user_name", "",
                                                "metadata", Map.of()),
                                "body", Map.of(
                                                "content", "round trip content",
                                                "wait_for_reply", true,
                                                "extra_payload", Map.of("key", "val")));

                GatewayCommand decoded = GatewayCommandFactory.fromMap(wireDict);
                AskAgentCommand cmd = assertInstanceOf(AskAgentCommand.class, decoded);
                assertEquals("msg-round", cmd.header().messageId());
                assertEquals("round trip content", cmd.content());
                assertTrue(cmd.waitForReply());
                assertEquals("val", cmd.extraPayload().get("key"));
        }

        @Test
        void resumeCommandRoundTripFromWireDict() {
                Map<String, Object> wireDict = Map.of(
                                "action_type", ActionType.RESUME,
                                "header", Map.of(
                                                "message_id", "msg-resume",
                                                "session_id", "sess-resume",
                                                "trace_id", "trace-resume",
                                                "source_agent_type", "agent-a",
                                                "target_agent_type", "agent-b",
                                                "parent_message_id", "msg-parent",
                                                "user_code", "",
                                                "user_name", "",
                                                "metadata", Map.of()),
                                "body", Map.of(
                                                "content", "resume data",
                                                "status", "SUCCESS",
                                                "reply_data", Map.of("result", "ok")));

                GatewayCommand decoded = GatewayCommandFactory.fromMap(wireDict);
                ResumeCommand cmd = assertInstanceOf(ResumeCommand.class, decoded);
                assertEquals("msg-parent", cmd.header().parentMessageId());
                assertEquals("SUCCESS", cmd.status());
                assertEquals("resume data", cmd.content());
        }

        @Test
        void cancelTaskCommandRoundTripFromWireDict() {
                Map<String, Object> wireDict = Map.of(
                                "action_type", ActionType.CANCEL_TASK,
                                "header", Map.of(
                                                "message_id", "msg-cancel",
                                                "session_id", "sess-cancel",
                                                "trace_id", "trace-cancel",
                                                "source_agent_type", "",
                                                "target_agent_type", "",
                                                "parent_message_id", "",
                                                "user_code", "",
                                                "user_name", "",
                                                "metadata", Map.of()),
                                "body", Map.of(
                                                "target_message_id", "target-1",
                                                "reason", "timeout"));

                GatewayCommand decoded = GatewayCommandFactory.fromMap(wireDict);
                CancelTaskCommand cmd = assertInstanceOf(CancelTaskCommand.class, decoded);
                assertEquals("target-1", cmd.targetMessageId());
                assertEquals("timeout", cmd.body().reason());
        }

        @Test
        void commandToJsonRoundTrip() throws Exception {
                AskAgentCommand original = AskAgentCommand.of(
                                MessageHeader.builder()
                                                .messageId("msg-json")
                                                .sessionId("sess-json")
                                                .traceId("trace-json")
                                                .targetAgentType("agent-json")
                                                .build(),
                                "json content",
                                false,
                                Map.of("attach", "data"));

                String json = objectMapper.writeValueAsString(original);
                GatewayCommand decoded = GatewayCommandFactory.fromJson(json);

                AskAgentCommand cmd = assertInstanceOf(AskAgentCommand.class, decoded);
                assertEquals("msg-json", cmd.header().messageId());
                assertEquals("json content", cmd.content());
        }

        @Test
        void unsupportedActionTypeThrowsException() {
                Map<String, Object> wireDict = Map.of(
                                "action_type", "UNKNOWN_ACTION",
                                "header", Map.of(),
                                "body", Map.of());

                assertThrows(IllegalArgumentException.class, () -> GatewayCommandFactory.fromMap(wireDict));
        }

        @Test
        void registerCommandWithBlankActionTypeThrowsException() {
                assertThrows(IllegalArgumentException.class,
                                () -> GatewayCommandFactory.registerCommand("", AskAgentCommand.class));
                assertThrows(IllegalArgumentException.class,
                                () -> GatewayCommandFactory.registerCommand("   ", AskAgentCommand.class));
        }

        @Test
        void getRegisteredCommandReturnsCorrectClass() {
                assertEquals(AskAgentCommand.class, GatewayCommandFactory.getRegisteredCommand(ActionType.ASK_AGENT));
                assertEquals(ResumeCommand.class, GatewayCommandFactory.getRegisteredCommand(ActionType.RESUME));
                assertEquals(CancelTaskCommand.class,
                                GatewayCommandFactory.getRegisteredCommand(ActionType.CANCEL_TASK));
                assertNull(GatewayCommandFactory.getRegisteredCommand("NON_EXISTENT"));
        }

        @Test
        void eventTypeEnumValues() {
                assertEquals("answerDelta", EventType.ANSWER_DELTA.getValue());
                assertEquals("reasoningLogDelta", EventType.REASONING_LOG_DELTA.getValue());
                assertEquals("reasoningLogStart", EventType.REASONING_LOG_START.getValue());
                assertEquals("reasoningLogEnd", EventType.REASONING_LOG_END.getValue());
                assertEquals("artifact", EventType.ARTIFACT.getValue());
                assertEquals("finalAnswer", EventType.FINAL_ANSWER.getValue());
                assertEquals("appStreamResponse", EventType.APP_STREAM_RESPONSE.getValue());
                assertEquals("error", EventType.ERROR.getValue());
                assertEquals(8, EventType.values().length);
        }


        @Test
        void messageHeaderBuilderDefaults() {
                MessageHeader header = MessageHeader.builder()
                                .messageId("msg-1")
                                .sessionId("sess-1")
                                .traceId("trace-1")
                                .build();

                assertEquals("msg-1", header.messageId());
                assertEquals("sess-1", header.sessionId());
                assertEquals("trace-1", header.traceId());
                assertEquals("", header.sourceAgentType());
                assertEquals("", header.targetAgentType());
                assertEquals("", header.parentMessageId());
                assertEquals("", header.taskGroupId());
                assertEquals("", header.userCode());
                assertEquals("", header.userName());
                assertEquals("", header.traceParentSpanId());
                assertEquals("", header.langfuseParentObservationId());
                assertNotNull(header.metadata());
                assertTrue(header.metadata().isEmpty());
        }

        @Test
        void messageHeaderSerializesTraceParentFields() throws Exception {
                MessageHeader header = MessageHeader.builder()
                                .messageId("msg-1")
                                .sessionId("sess-1")
                                .traceId("trace-1")
                                .traceParentSpanId("0123456789abcdef")
                                .langfuseParentObservationId("obs-parent")
                                .build();

                String json = objectMapper.writeValueAsString(header);
                Map<?, ?> payload = objectMapper.readValue(json, Map.class);

                assertEquals("0123456789abcdef", payload.get("trace_parent_span_id"));
                assertEquals("obs-parent", payload.get("langfuse_parent_observation_id"));

                MessageHeader decoded = objectMapper.readValue(json, MessageHeader.class);
                assertEquals("0123456789abcdef", decoded.traceParentSpanId());
                assertEquals("obs-parent", decoded.langfuseParentObservationId());
        }

        @Test
        void messageHeaderWithMetadata() {
                Map<String, Object> meta = Map.of("user", "test-user", "priority", 1);
                MessageHeader header = MessageHeader.builder()
                                .messageId("msg-1")
                                .sessionId("sess-1")
                                .traceId("trace-1")
                                .metadata(meta)
                                .build();

                assertEquals("test-user", header.metadata().get("user"));
                assertEquals(1, header.metadata().get("priority"));
        }

        @Test
        void registryKeysFormats() {
                assertEquals("byai_gateway:registry:workers", Constants.RegistryKeys.knownWorkers());
                assertEquals("byai_gateway:registry:worker:agent_types:w1",
                                Constants.RegistryKeys.workerDeclaredAgentTypes("w1"));
                assertEquals("byai_gateway:registry:agent_type:workers:cap1",
                                Constants.RegistryKeys.agentTypeMembers("cap1"));
                assertEquals("byai_gateway:registry:worker:online:w1", Constants.RegistryKeys.workerOnlineLease("w1"));
                assertEquals("byai_gateway:task_group:g1", Constants.RegistryKeys.taskGroup("g1"));
        }

        @Test
        void baiYingMessageCreation() {
                BaiYingMessage msg = BaiYingMessage.builder()
                                .role(BaiYingMessage.Role.USER.getValue())
                                .content("Hello")
                                .build();

                assertEquals("user", msg.getRole());
                assertEquals("Hello", msg.getContent());
        }

        @Test
        void baiYingMessageWithStructuredContent() {
                BaiYingMessage.MessageFile file = BaiYingMessage.MessageFile.builder()
                                .fileId("f1")
                                .fileUrl("http://example.com/file.pdf")
                                .fileType("pdf")
                                .fileName("file.pdf")
                                .fileSize(1024L)
                                .build();

                BaiYingMessage.MessageContent content = BaiYingMessage.MessageContent.builder()
                                .text("test message")
                                .files(java.util.List.of(file))
                                .build();

                BaiYingMessage msg = BaiYingMessage.builder()
                                .role(BaiYingMessage.Role.ASSISTANT.getValue())
                                .content(content)
                                .build();

                assertEquals("assistant", msg.getRole());
                BaiYingMessage.MessageContent mc = (BaiYingMessage.MessageContent) msg.getContent();
                assertEquals("test message", mc.getText());
                assertEquals(1, mc.getFiles().size());
                assertEquals("f1", mc.getFiles().get(0).getFileId());
                assertEquals(1024L, mc.getFiles().get(0).getFileSize());
        }

        @Test
        void baiYingRoleEnumValues() {
                assertEquals("user", BaiYingMessage.Role.USER.getValue());
                assertEquals("assistant", BaiYingMessage.Role.ASSISTANT.getValue());
                assertEquals("system", BaiYingMessage.Role.SYSTEM.getValue());
                assertEquals("tool-call", BaiYingMessage.Role.TOOL_CALL.getValue());
                assertEquals("tool-response", BaiYingMessage.Role.TOOL_RESPONSE.getValue());
                assertEquals("response-to-sub-agent", BaiYingMessage.Role.RESPONSE_TO_SUB_AGENT.getValue());
                assertEquals(6, BaiYingMessage.Role.values().length);
        }
}
