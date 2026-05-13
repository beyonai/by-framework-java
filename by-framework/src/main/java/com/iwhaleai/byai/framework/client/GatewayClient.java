package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.ActionType;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.ExecutionStatus;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import com.iwhaleai.byai.framework.client.interceptors.GatewayInterceptor;
import com.iwhaleai.byai.framework.client.interceptors.SendMessageParams;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class GatewayClient<T> {
    private final RedisClient redisClient;
    private final WorkerRegistry registry;
    @Getter
    private final List<GatewayInterceptor> interceptors;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public GatewayClient(RedisClient redisClient) {
        this(redisClient, new WorkerRegistry(redisClient), new ArrayList<>());
    }

    public GatewayClient(RedisClient redisClient, List<GatewayInterceptor> interceptors) {
        this(redisClient, new WorkerRegistry(redisClient), interceptors);
    }

    public GatewayClient(RedisClient redisClient, WorkerRegistry registry, List<GatewayInterceptor> interceptors) {
        this.redisClient = redisClient;
        this.registry = registry;
        this.interceptors = interceptors != null ? interceptors : new ArrayList<>();
    }

    public GatewayClient(String host, int port) {
        this(new RedisClient(host, port));
    }

    public void addInterceptor(GatewayInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    @Data
    @Builder
    public static class SendResponse {
        private boolean success;
        private String messageId;
        private String traceId;
        private String targetWorkerId;
        private String status;
        private long timestamp;
        private String error;
        private String errorCode;
    }

    @Data
    @Builder
    public static class CancelTaskResponse {
        private boolean success;
        private String messageId;
        private String executionId;
        private String workerId;
        private String status;
        private long timestamp;
        private String error;
        @Builder.Default
        private int cancelledCount = 0;
    }

    /**
     * 发送异步消息给指定的 Agent 类型。
     */
    public synchronized SendResponse sendCommand(GatewayCommand command) {
        return sendCommand(command, null, null, false);
    }

    public synchronized SendResponse sendCommand(GatewayCommand command, String streamName) {
        return sendCommand(command, streamName, null, false);
    }

    public synchronized SendResponse sendCommand(GatewayCommand command, String streamName, String targetWorkerId,
            boolean requireOnlineWorker) {
        try {
            MessageHeader header = command.header();

            if (requireOnlineWorker) {
                if (targetWorkerId == null || targetWorkerId.isEmpty()) {
                    WorkerRegistry.OnlineAgentCheckResult result = registry.hasOnlineAgentType(
                            header.targetAgentType(),
                            true,
                            Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
                    if (!result.exists) {
                        return SendResponse.builder()
                                .success(false)
                                .status(ExecutionStatus.FAILED)
                                .targetWorkerId("")
                                .error("No online worker found for agent_type '" + header.targetAgentType() + "'")
                                .errorCode(ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE)
                                .timestamp(System.currentTimeMillis())
                                .build();
                    }
                } else {
                    if (!registry.isWorkerOnline(targetWorkerId)) {
                        return SendResponse.builder()
                                .success(false)
                                .status(ExecutionStatus.FAILED)
                                .targetWorkerId(targetWorkerId)
                                .error("Target worker '" + targetWorkerId + "' is not online or not registered")
                                .errorCode(ExecutionStatus.ERR_WORKER_NOT_ONLINE)
                                .timestamp(System.currentTimeMillis())
                                .build();
                    }
                }
            }

            String resolvedStreamName = streamName;
            if (resolvedStreamName == null || resolvedStreamName.isBlank()) {
                if (targetWorkerId != null && !targetWorkerId.isBlank()) {
                    resolvedStreamName = Constants.QueueNames.workerCtrlStream(targetWorkerId);
                } else {
                    resolvedStreamName = Constants.QueueNames.ctrlStream(header.targetAgentType());
                }
            }

            String workerId = targetWorkerId != null ? targetWorkerId : "";

            String jsonPayload = objectMapper.writeValueAsString(command);
            try (Jedis jedis = redisClient.getResource()) {
                Map<String, String> redisData = new HashMap<>();
                redisData.put(Constants.RedisFields.DATA, jsonPayload);
                jedis.xadd(resolvedStreamName, (redis.clients.jedis.StreamEntryID) null, redisData);
            }

            return SendResponse.builder()
                    .success(true)
                    .messageId(header.messageId())
                    .traceId(header.traceId())
                    .targetWorkerId(workerId)
                    .status(ExecutionStatus.QUEUED)
                    .timestamp(System.currentTimeMillis())
                    .build();
        } catch (Exception e) {
            log.error("Failed to send command to gateway", e);
            return SendResponse.builder()
                    .success(false)
                    .error(e.getMessage())
                    .timestamp(System.currentTimeMillis())
                    .build();
        }
    }

    /**
     * Cancel a task with cascading cancellation of all descendant tasks.
     * Uses BFS traversal over the task tree via parent_message_id linkage.
     */
    public synchronized CancelTaskResponse cancelTask(
            String messageId,
            String sessionId,
            String reason,
            String targetAgentType,
            String requestedBy,
            String cancelMode) {
        Map<String, Object> execution = registry.getExecutionByMessageId(messageId, sessionId);
        if (execution == null) {
            return buildCancelNotFoundResponse(messageId, "execution not found for message_id=" + messageId);
        }

        if (!sessionId.equals(execution.get(Constants.ExecutionFields.SESSION_ID))) {
            return buildCancelNotFoundResponse(messageId,
                    "session mismatch for message_id=" + messageId,
                    execution);
        }

        Map<String, Map<String, Object>> allExecutions = registry.getAllSessionExecutions(sessionId);
        Map<String, List<Map<String, Object>>> childrenByParent = buildChildrenByParentMap(allExecutions);

        TaskCancelPlan plan = collectTasksToCancel(execution, messageId, childrenByParent);

        markTerminalAncestors(plan.terminalAncestors, sessionId, reason);

        if (plan.toCancel.isEmpty()) {
            return buildCancelAlreadyFinishedResponse(execution);
        }

        int cancelledCount = cancelTasks(plan.toCancel, sessionId, reason, targetAgentType, requestedBy, cancelMode);

        return buildCancelSuccessResponse(execution, messageId, cancelledCount);
    }

    private CancelTaskResponse buildCancelNotFoundResponse(String messageId, String error) {
        return CancelTaskResponse.builder()
                .success(false)
                .messageId(messageId)
                .executionId("")
                .workerId("")
                .status(AgentState.NOT_FOUND)
                .timestamp(System.currentTimeMillis())
                .error(error)
                .build();
    }

    private CancelTaskResponse buildCancelNotFoundResponse(String messageId, String error, Map<String, Object> execution) {
        return CancelTaskResponse.builder()
                .success(false)
                .messageId(messageId)
                .executionId(String.valueOf(execution.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, "")))
                .workerId(String.valueOf(execution.getOrDefault(Constants.ExecutionFields.WORKER_ID, "")))
                .status(AgentState.NOT_FOUND)
                .timestamp(System.currentTimeMillis())
                .error(error)
                .build();
    }

    private CancelTaskResponse buildCancelAlreadyFinishedResponse(Map<String, Object> execution) {
        return CancelTaskResponse.builder()
                .success(true)
                .messageId(String.valueOf(execution.getOrDefault(Constants.ExecutionFields.MESSAGE_ID, "")))
                .executionId(String.valueOf(execution.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, "")))
                .workerId(String.valueOf(execution.getOrDefault(Constants.ExecutionFields.WORKER_ID, "")))
                .status("ALREADY_FINISHED")
                .timestamp(System.currentTimeMillis())
                .cancelledCount(0)
                .build();
    }

    private CancelTaskResponse buildCancelSuccessResponse(Map<String, Object> execution, String messageId,
            int cancelledCount) {
        String primaryExecId = String.valueOf(execution.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, ""));
        String primaryWorkerId = execution.get(Constants.ExecutionFields.WORKER_ID) != null
                ? String.valueOf(execution.get(Constants.ExecutionFields.WORKER_ID)) : "";
        return CancelTaskResponse.builder()
                .success(true)
                .messageId(messageId)
                .executionId(primaryExecId)
                .workerId(primaryWorkerId)
                .status("CANCEL_REQUESTED")
                .timestamp(System.currentTimeMillis())
                .cancelledCount(cancelledCount)
                .build();
    }

    private Map<String, List<Map<String, Object>>> buildChildrenByParentMap(
            Map<String, Map<String, Object>> allExecutions) {
        Map<String, List<Map<String, Object>>> childrenByParent = new HashMap<>();
        for (Map<String, Object> exec : allExecutions.values()) {
            String parentMsgId = exec.get("parent_message_id") != null
                    ? String.valueOf(exec.get("parent_message_id")) : "";
            if (!parentMsgId.isEmpty()) {
                childrenByParent.computeIfAbsent(parentMsgId, k -> new ArrayList<>()).add(exec);
            }
        }
        return childrenByParent;
    }

    private TaskCancelPlan collectTasksToCancel(
            Map<String, Object> execution,
            String messageId,
            Map<String, List<Map<String, Object>>> childrenByParent) {
        List<Map<String, Object>> toCancel = new ArrayList<>();
        List<Map<String, Object>> terminalAncestors = new ArrayList<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(messageId);

        String rootStatus = String.valueOf(execution.getOrDefault(Constants.ExecutionFields.STATUS, ""));
        if (!Constants.TERMINAL_STATES.contains(rootStatus)) {
            toCancel.add(execution);
        } else {
            terminalAncestors.add(execution);
        }

        while (!queue.isEmpty()) {
            String currentMsgId = queue.poll();
            List<Map<String, Object>> children = childrenByParent.getOrDefault(currentMsgId, List.of());
            for (Map<String, Object> child : children) {
                String childMsgId = String.valueOf(child.getOrDefault(Constants.ExecutionFields.MESSAGE_ID, ""));
                String childStatus = String.valueOf(child.getOrDefault(Constants.ExecutionFields.STATUS, ""));
                if (!Constants.TERMINAL_STATES.contains(childStatus)) {
                    toCancel.add(child);
                } else {
                    terminalAncestors.add(child);
                }
                if (!childMsgId.isEmpty()) {
                    queue.add(childMsgId);
                }
            }
        }
        return new TaskCancelPlan(toCancel, terminalAncestors);
    }

    private void markTerminalAncestors(List<Map<String, Object>> terminalAncestors, String sessionId, String reason) {
        for (Map<String, Object> terminal : terminalAncestors) {
            String execId = String.valueOf(terminal.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, ""));
            if (!execId.isEmpty()) {
                registry.markCancelRequested(execId, sessionId, reason != null ? reason : "");
            }
        }
    }

    private int cancelTasks(List<Map<String, Object>> toCancel, String sessionId, String reason,
            String targetAgentType, String requestedBy, String cancelMode) {
        int cancelledCount = 0;
        for (Map<String, Object> taskExec : toCancel) {
            String execId = String.valueOf(taskExec.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, ""));
            String taskMsgId = String.valueOf(taskExec.getOrDefault(Constants.ExecutionFields.MESSAGE_ID, ""));
            String workerId = taskExec.get(Constants.ExecutionFields.WORKER_ID) != null
                    ? String.valueOf(taskExec.get(Constants.ExecutionFields.WORKER_ID)) : "";

            registry.markExecutionCancelling(execId, sessionId, reason != null ? reason : "");

            if (workerId.isEmpty() || "null".equals(workerId)) {
                registry.markCancelRequested(execId, sessionId, reason != null ? reason : "");
                cancelledCount++;
                continue;
            }

            CancelTaskCommand cancelCommand = buildCancelCommand(taskExec, sessionId, reason,
                    targetAgentType, taskMsgId, execId, workerId, requestedBy, cancelMode);
            SendResponse sendResponse = sendCommand(cancelCommand,
                    Constants.QueueNames.workerCtrlStream(workerId));
            if (sendResponse.isSuccess()) {
                cancelledCount++;
            } else {
                log.warn("Failed to send cancel command for execution {}: {}", execId, sendResponse.getError());
            }
        }
        return cancelledCount;
    }

    private CancelTaskCommand buildCancelCommand(Map<String, Object> taskExec, String sessionId, String reason,
            String targetAgentType, String taskMsgId, String execId, String workerId, String requestedBy,
            String cancelMode) {
        return CancelTaskCommand.builder()
                .header(MessageHeader.builder()
                        .messageId(Constants.CANCEL_MESSAGE_ID_PREFIX
                                + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH))
                        .sessionId(sessionId)
                        .traceId(UUID.randomUUID().toString().replace("-", ""))
                        .targetAgentType(
                                targetAgentType != null && !targetAgentType.isBlank()
                                        ? targetAgentType
                                        : String.valueOf(taskExec
                                                .getOrDefault(Constants.ExecutionFields.TARGET_AGENT_TYPE, "")))
                        .parentMessageId(taskMsgId)
                        .build())
                .body(CancelTaskCommand.CancelTaskBody.builder()
                        .targetMessageId(taskMsgId)
                        .targetExecutionId(execId)
                        .targetWorkerId(workerId)
                        .reason(reason != null ? reason : "")
                        .requestedBy(requestedBy != null ? requestedBy : "client")
                        .cancelMode(cancelMode != null && !cancelMode.isBlank() ? cancelMode : "graceful")
                        .build())
                .build();
    }

    private record TaskCancelPlan(List<Map<String, Object>> toCancel,
            List<Map<String, Object>> terminalAncestors) { }

    public synchronized CancelTaskResponse cancelTask(String messageId, String sessionId) {
        return cancelTask(messageId, sessionId, "", "", "client", "graceful");
    }

    public synchronized SendResponse sendMessage(
            String targetAgentType,
            String sessionId,
            T content,
            String userCode,
            String userName,
            String actionType,
            String parentMessageId,
            String messageId,
            String traceId,
            Map<String, Object> payload,
            Map<String, Object> metadata) {
        return sendMessage(targetAgentType, sessionId, content, userCode, userName, actionType, parentMessageId, messageId,
                traceId, payload, metadata, null, true);
    }

    /**
     * Send a message to the gateway with full control over routing and online worker check.
     *
     * @param targetAgentType Target agent type for routing
     * @param sessionId Session ID
     * @param content Message content
     * @param userCode User Code
     * @param userName User Name
     * @param actionType Action type (ASK_AGENT or RESUME)
     * @param parentMessageId Parent message ID for message chaining
     * @param messageId Optional message ID (generated if null)
     * @param traceId Optional trace ID (generated if null)
     * @param payload Optional payload
     * @param metadata Optional metadata
     * @param targetWorkerId Optional direct worker ID (bypasses agent-type routing)
     * @param requireOnlineWorker If true, check that target route has online worker before sending
     * @return SendResponse with success status and details
     */
    public synchronized SendResponse sendMessage(
            String targetAgentType,
            String sessionId,
            T content,
            String userCode,
            String userName,
            String actionType,
            String parentMessageId,
            String messageId,
            String traceId,
            Map<String, Object> payload,
            Map<String, Object> metadata,
            String targetWorkerId,
            boolean requireOnlineWorker) {
        // 1. Prepare parameters for interceptors
        SendMessageParams params = SendMessageParams.builder()
                .targetAgentType(targetAgentType)
                .sessionId(sessionId)
                .content(content)
                .userCode(userCode != null ? userCode : "")
                .userName(userName != null ? userName : "")
                .actionType(actionType != null ? actionType : ActionType.ASK_AGENT)
                .parentMessageId(parentMessageId != null ? parentMessageId : "")
                .payload(payload != null ? payload : new HashMap<>())
                .metadata(metadata != null ? metadata : new HashMap<>())
                .build();

        // 2. Run interceptors
        for (GatewayInterceptor interceptor : interceptors) {
            params = interceptor.beforeSend(params);
        }

        // 3. Core sending logic
        if (messageId == null || messageId.isEmpty()) {
            messageId = Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
        }
        if (traceId == null || traceId.isEmpty()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }

        try {
            MessageHeader header = MessageHeader.builder()
                    .messageId(messageId)
                    .sessionId(params.getSessionId())
                    .traceId(traceId)
                    .targetAgentType(params.getTargetAgentType())
                    .parentMessageId(params.getParentMessageId())
                    .userCode(params.getUserCode())
                    .userName(params.getUserName())
                    .metadata(params.getMetadata())
                    .build();

            Map<String, Object> payloadMap = new HashMap<>(params.getPayload());
            Object command = ActionType.RESUME.equals(params.getActionType())
                    ? ResumeCommand.of(
                            header,
                            params.getContent(),
                            (String) payloadMap.getOrDefault(Constants.RedisFields.STATUS, ""),
                            payloadMap.get(Constants.RedisFields.REPLY_DATA),
                            payloadMap.entrySet().stream()
                                    .filter(entry -> !Constants.RedisFields.STATUS.equals(entry.getKey())
                                            && !Constants.RedisFields.REPLY_DATA.equals(entry.getKey()))
                                    .collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                                            HashMap::putAll))
                    : AskAgentCommand.of(
                            header,
                            params.getContent(),
                            Boolean.TRUE.equals(payloadMap.get("wait_for_reply")),
                            payloadMap.entrySet().stream()
                                    .filter(entry -> !"wait_for_reply".equals(entry.getKey()))
                                    .collect(HashMap::new, (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                                            HashMap::putAll));

            String executionId = Constants.EXECUTION_ID_PREFIX
                    + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
            try {
                registry.initializeExecution(executionId, messageId, params.getSessionId(),
                        params.getTargetAgentType(), params.getParentMessageId());
            } catch (Exception e) {
                log.warn("Failed to initialize execution tracking: {}", e.getMessage());
            }

            SendResponse response = sendCommand((GatewayCommand) command, null, targetWorkerId, requireOnlineWorker);
            if (response.isSuccess()) {
                log.info("Message sent to gateway: {} (target={})", messageId, params.getTargetAgentType());
            }

            // Ensure traceId is in the response
            return SendResponse.builder()
                    .success(response.isSuccess())
                    .messageId(response.getMessageId())
                    .traceId(traceId)
                    .targetWorkerId(response.getTargetWorkerId())
                    .status(response.getStatus())
                    .timestamp(System.currentTimeMillis())
                    .error(response.getError())
                    .errorCode(response.getErrorCode())
                    .build();

        } catch (Exception e) {
            log.error("Failed to send message to gateway", e);
            return SendResponse.builder()
                    .success(false)
                    .status(ExecutionStatus.FAILED)
                    .error(e.getMessage())
                    .traceId(traceId)
                    .timestamp(System.currentTimeMillis())
                    .build();
        }
    }

    public SendResponse sendMessage(String targetAgentType, String sessionId, T content) {
        return sendMessage(targetAgentType, sessionId, content, null, null, null, null, null, null, null, null);
    }

    public SendResponse sendMessage(String targetAgentType, String sessionId, T content, Map<String, Object> metadata) {
        return sendMessage(targetAgentType, sessionId, content, null, null, null, null, null, null, null, metadata);
    }
}
