package com.iwhaleai.byai.framework.examples;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.iwhaleai.byai.framework.client.GatewayClient;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommandFactory;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.WorkerRunner;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 演示如何定义、注册、发送并处理自定义 Command。
 *
 * 运行方式：
 * 1. 启动 worker: `java ... CustomCommandExample worker`
 * 2. 发送消息: `java ... CustomCommandExample send`
 */
public class CustomCommandExample {
    public static final String ACTION_TYPE = "CUSTOM_AUDIT";
    public static final String CAPABILITY = "custom-audit-agent-java";

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomAuditCommand implements GatewayCommand {
        @Builder.Default
        @JsonProperty("action_type")
        private String actionType = ACTION_TYPE;

        @JsonProperty("header")
        private MessageHeader header;

        @JsonProperty("body")
        private CustomAuditBody body;

        // GatewayCommand interface implementation
        @Override
        public String actionType() {
            return actionType;
        }

        @Override
        public MessageHeader header() {
            return header;
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomAuditBody {
        @JsonProperty("audit_id")
        private String auditId;

        @JsonProperty("payload")
        private Map<String, Object> payload;
    }

    public static class CustomAuditWorker extends GatewayWorker {
        public CustomAuditWorker(String workerId) {
            super(workerId);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of(CAPABILITY);
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            if (command instanceof CustomAuditCommand customCommand) {
                context.emitState("AUDIT:" + customCommand.getBody().getAuditId());
                context.emitChunk("Handled custom audit payload: " + customCommand.getBody().getPayload());
                return Map.of("ok", true, "audit_id", customCommand.getBody().getAuditId());
            }
            throw new IllegalArgumentException("Unsupported command: " + command.actionType());
        }
    }

    private static void sendCustomCommand() {
        RedisClient redisClient = RedisClient.getInstance();
        GatewayClient<Object> client = new GatewayClient<>(redisClient);

        GatewayCommandFactory.registerCommand(ACTION_TYPE, CustomAuditCommand.class);

        CustomAuditCommand command = CustomAuditCommand.builder()
                .header(MessageHeader.builder()
                        .messageId("msg-custom-audit-1")
                        .sessionId("sess-custom-audit")
                        .traceId("trace-custom-audit")
                        .targetAgentType(CAPABILITY)
                        .userCode("demo-user")
                        .userName("Demo User")
                        .metadata(Map.of("source", "java-example"))
                        .build())
                .body(CustomAuditBody.builder()
                        .auditId("audit-001")
                        .payload(Map.of("action", "reindex", "priority", "high"))
                        .build())
                .build();

        GatewayClient.SendResponse response = client.sendCommand(command);
        System.out.println("Custom command queued: " + response);
        redisClient.close();
    }

    private static void startWorker() {
        GatewayCommandFactory.registerCommand(ACTION_TYPE, CustomAuditCommand.class);
        CustomAuditWorker worker = new CustomAuditWorker("custom-audit-worker-java");
        WorkerRunner runner = new WorkerRunner(worker);
        runner.start();
        Runtime.getRuntime().addShutdownHook(new Thread(runner::stop));
    }

    public static void main(String[] args) {
        if (args.length > 0 && "worker".equalsIgnoreCase(args[0])) {
            startWorker();
            return;
        }
        sendCustomCommand();
    }
}
