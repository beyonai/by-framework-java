package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.ClusterRedisOps;
import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.availability.AvailabilityResult;
import com.iwhaleai.byai.framework.core.availability.AvailabilityRouter;
import com.iwhaleai.byai.framework.core.availability.AvailabilityStatus;
import com.iwhaleai.byai.framework.core.availability.DeliveryIntent;
import com.iwhaleai.byai.framework.core.availability.RoutePolicy;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class GatewayClient<T> {
    private static final String TRACE_PARENT_SPAN_ID = "trace_parent_span_id";
    private static final String LANGFUSE_PARENT_OBSERVATION_ID = "langfuse_parent_observation_id";

    private final RedisClient redisClient;
    private final RedisStreamOps streamOps;
    private final WorkerRegistry registry;
    private final AvailabilityRouter availabilityRouter;
    @Getter
    private final List<GatewayInterceptor> interceptors;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ClientDispatchTracer clientDispatchTracer;
    private final RedisTraceWriter redisTraceWriter;

    public GatewayClient(RedisClient redisClient) {
        this(redisClient, new WorkerRegistry(redisClient), new ArrayList<>());
    }

    public GatewayClient(RedisClient redisClient, List<GatewayInterceptor> interceptors) {
        this(redisClient, new WorkerRegistry(redisClient), interceptors);
    }

    public GatewayClient(RedisClient redisClient, WorkerRegistry registry, List<GatewayInterceptor> interceptors) {
        this(redisClient, registry, interceptors, new LangfuseClientDispatchTracer());
    }

    GatewayClient(
            RedisClient redisClient,
            WorkerRegistry registry,
            List<GatewayInterceptor> interceptors,
            ClientDispatchTracer clientDispatchTracer) {
        this.redisClient = redisClient;
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.registry = registry;
        this.interceptors = interceptors != null ? interceptors : new ArrayList<>();
        this.clientDispatchTracer = clientDispatchTracer;
        this.redisTraceWriter = new RedisTraceWriter(
                redisClient.getJedisCluster() != null
                        ? new ClusterRedisOps(redisClient.getJedisCluster())
                        : new StandaloneRedisOps(redisClient),
                objectMapper);
        this.availabilityRouter = new AvailabilityRouter(redisClient, registry);
    }

    public GatewayClient(String host, int port) {
        this(new RedisClient(host, port));
    }

    public void addInterceptor(GatewayInterceptor interceptor) {
        this.interceptors.add(interceptor);
    }

    /**
     * Convert a stable span key to the same 16-char hex span id used by Python.
     */
    public static String stableTraceSpanIdHex(String value) {
        String normalized = value != null ? value : "";
        if (normalized.length() == 16 && isHex(normalized)) {
            return normalized.toLowerCase();
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(normalized.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(16);
            for (int i = 0; i < 8; i++) {
                builder.append(String.format("%02x", hash[i] & 0xff));
            }
            String spanId = builder.toString();
            return "0000000000000000".equals(spanId) ? "0000000000000001" : spanId;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 digest is unavailable", e);
        }
    }

    /**
     * Convert a framework trace id to the same 32-char hex trace id used by Python.
     */
    public static String stableTraceIdHex(String value) {
        String normalized = value != null ? value : "";
        if (normalized.length() == 32 && isHex(normalized)) {
            return normalized.toLowerCase();
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] hash = digest.digest(normalized.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(32);
            for (byte b : hash) {
                builder.append(String.format("%02x", b & 0xff));
            }
            String traceId = builder.toString();
            return "00000000000000000000000000000000".equals(traceId)
                    ? "00000000000000000000000000000001"
                    : traceId;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 digest is unavailable", e);
        }
    }

    private static boolean isHex(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean digit = c >= '0' && c <= '9';
            boolean lower = c >= 'a' && c <= 'f';
            boolean upper = c >= 'A' && c <= 'F';
            if (!digit && !lower && !upper) {
                return false;
            }
        }
        return true;
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

    @Data
    @Builder
    public static class CancelSessionResponse {
        private boolean success;
        private String sessionId;
        private String status;
        private long timestamp;
        private String error;
        @Builder.Default
        private int cancelledCount = 0;
        @Builder.Default
        private int alreadyFinishedCount = 0;
    }

    /**
     * 发送异步消息给指定的 Agent 类型。
     */
    public synchronized SendResponse sendCommand(GatewayCommand command) {
        return sendCommand(command, null, null, RoutePolicy.FAIL_FAST);
    }

    public synchronized SendResponse sendCommand(GatewayCommand command, String streamName) {
        return sendCommand(command, streamName, null, RoutePolicy.FAIL_FAST);
    }

    /**
     * Send a command with route policy control.
     *
     * @param command     The gateway command
     * @param streamName  Optional explicit stream name (bypasses routing)
     * @param targetWorkerId Optional direct worker ID
     * @param routePolicy Route policy for availability check
     * @return SendResponse
     */
    public synchronized SendResponse sendCommand(GatewayCommand command, String streamName, String targetWorkerId,
            String routePolicy) {
        try {
            MessageHeader header = command.header();

            // Use AvailabilityRouter for the online check
            DeliveryIntent intent = DeliveryIntent.builder()
                    .executionId(null) // sendCommand doesn't have execution ID at this level
                    .messageId(header.messageId())
                    .sessionId(header.sessionId())
                    .traceId(header.traceId())
                    .source(header.sourceAgentType())
                    .targetAgentType(header.targetAgentType())
                    .userCode(header.userCode())
                    .policy(routePolicy != null ? routePolicy : RoutePolicy.FAIL_FAST)
                    .build();

            AvailabilityResult result = availabilityRouter.prepareDelivery(intent);

            if (AvailabilityStatus.REJECT.equals(result.getStatus())) {
                return SendResponse.builder()
                        .success(false)
                        .status(ExecutionStatus.FAILED)
                        .targetWorkerId("")
                        .error(result.getReason())
                        .errorCode(result.getErrorCode() != null ? result.getErrorCode() : ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE)
                        .timestamp(System.currentTimeMillis())
                        .build();
            }

            if (AvailabilityStatus.QUEUE_PENDING.equals(result.getStatus())) {
                return SendResponse.builder()
                        .success(true)
                        .messageId(header.messageId())
                        .traceId(header.traceId())
                        .status(AvailabilityStatus.QUEUE_PENDING)
                        .timestamp(System.currentTimeMillis())
                        .build();
            }

            // DELIVER_NOW, WAIT_AND_DELIVER, FALLBACK_TO_OTHER_AGENT_TYPE
            String resolvedStreamName = streamName;
            if (resolvedStreamName == null || resolvedStreamName.isBlank()) {
                if (targetWorkerId != null && !targetWorkerId.isBlank()) {
                    resolvedStreamName = Constants.QueueNames.workerCtrlStream(targetWorkerId);
                } else if (result.getStreamName() != null && !result.getStreamName().isBlank()) {
                    resolvedStreamName = result.getStreamName();
                } else {
                    String selectedType = result.getSelectedAgentType() != null
                            ? result.getSelectedAgentType() : header.targetAgentType();
                    resolvedStreamName = Constants.QueueNames.ctrlStream(selectedType);
                }
            }

            String workerId = targetWorkerId != null ? targetWorkerId : "";
            if (result.getTargetWorkerId() != null && !result.getTargetWorkerId().isBlank()) {
                workerId = result.getTargetWorkerId();
            }

            String jsonPayload = objectMapper.writeValueAsString(command);
            Map<String, String> redisData = new HashMap<>();
            redisData.put(Constants.RedisFields.DATA, jsonPayload);
            streamOps.xadd(resolvedStreamName, redisData, XAddOptions.noTrim());

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
                        .traceId(resolveExecutionTraceId(taskExec))
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

    /**
     * Cancel every active execution registered under a session in one call,
     * without requiring a starting messageId the way cancelTask does.
     */
    public synchronized CancelSessionResponse cancelSession(
            String sessionId,
            String reason,
            String targetAgentType,
            String requestedBy,
            String cancelMode) {
        Map<String, Map<String, Object>> allExecutions = registry.getAllSessionExecutions(sessionId);

        if (allExecutions.isEmpty()) {
            return CancelSessionResponse.builder()
                    .success(false)
                    .sessionId(sessionId)
                    .status(AgentState.NOT_FOUND)
                    .timestamp(System.currentTimeMillis())
                    .build();
        }

        List<Map<String, Object>> toCancel = new ArrayList<>();
        List<Map<String, Object>> terminalExecutions = new ArrayList<>();
        for (Map<String, Object> exec : allExecutions.values()) {
            String status = String.valueOf(exec.getOrDefault(Constants.ExecutionFields.STATUS, ""));
            if (Constants.TERMINAL_STATES.contains(status)) {
                terminalExecutions.add(exec);
            } else {
                toCancel.add(exec);
            }
        }

        for (Map<String, Object> terminal : terminalExecutions) {
            String execId = String.valueOf(terminal.getOrDefault(Constants.ExecutionFields.EXECUTION_ID, ""));
            if (!execId.isEmpty()) {
                registry.markCancelRequested(execId, sessionId, reason != null ? reason : "");
            }
        }

        if (toCancel.isEmpty()) {
            return CancelSessionResponse.builder()
                    .success(false)
                    .sessionId(sessionId)
                    .status("ALREADY_FINISHED")
                    .timestamp(System.currentTimeMillis())
                    .cancelledCount(0)
                    .alreadyFinishedCount(terminalExecutions.size())
                    .build();
        }

        int cancelledCount = cancelTasks(toCancel, sessionId, reason, targetAgentType, requestedBy, cancelMode);

        return CancelSessionResponse.builder()
                .success(true)
                .sessionId(sessionId)
                .status("CANCEL_REQUESTED")
                .timestamp(System.currentTimeMillis())
                .cancelledCount(cancelledCount)
                .alreadyFinishedCount(terminalExecutions.size())
                .build();
    }

    public synchronized CancelSessionResponse cancelSession(String sessionId, String reason) {
        return cancelSession(sessionId, reason, "", "client", "graceful");
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
                traceId, payload, metadata, null, RoutePolicy.FAIL_FAST, 0, null, null);
    }

    /**
     * Backward-compatible sendMessage method using boolean requireOnlineWorker.
     * Maps requireOnlineWorker to RoutePolicy:
     * - true -> FAIL_FAST
     * - false -> SEND_ANYWAY
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
        String routePolicy = requireOnlineWorker ? RoutePolicy.FAIL_FAST : RoutePolicy.SEND_ANYWAY;
        return sendMessage(targetAgentType, sessionId, content, userCode, userName, actionType, parentMessageId,
                messageId, traceId, payload, metadata, targetWorkerId, routePolicy, 0L, null, null);
    }


    /**
     * Send a message to the gateway with full control over routing and availability.
     *
     * @param targetAgentType      Target agent type for routing
     * @param sessionId            Session ID
     * @param content              Message content
     * @param userCode             User Code
     * @param userName             User Name
     * @param actionType           Action type (ASK_AGENT or RESUME)
     * @param parentMessageId      Parent message ID for message chaining
     * @param messageId            Optional message ID (generated if null)
     * @param traceId              Optional trace ID (generated if null)
     * @param payload              Optional payload
     * @param metadata             Optional metadata
     * @param targetWorkerId       Optional direct worker ID (bypasses agent-type routing)
     * @param routePolicy          Route policy for availability control (FAIL_FAST, SEND_ANYWAY, etc.)
     * @param availabilityTimeoutMs Timeout for WAKE_AND_WAIT policy
     * @param region               Optional region for routing
     * @param priority             Optional priority for routing
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
            String routePolicy,
            long availabilityTimeoutMs,
            String region,
            String priority) {
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

        String executionId = Constants.EXECUTION_ID_PREFIX
                + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
        String resolvedPolicy = routePolicy != null ? routePolicy : RoutePolicy.FAIL_FAST;

        try {
            Map<String, Object> headerMetadata = new HashMap<>(
                    params.getMetadata() != null ? params.getMetadata() : Map.of());
            String traceParentSpanId = stringValue(headerMetadata.remove(TRACE_PARENT_SPAN_ID));
            String langfuseParentObservationId = stringValue(headerMetadata.remove(LANGFUSE_PARENT_OBSERVATION_ID));
            if (traceParentSpanId.isEmpty()) {
                traceParentSpanId = stableTraceSpanIdHex(messageId + ":client.dispatch");
            }
            ClientDispatchObservation clientDispatchObservation = null;
            if (langfuseParentObservationId.isEmpty()) {
                clientDispatchObservation = startClientDispatchObservation(
                        traceId,
                        messageId,
                        params.getTargetAgentType(),
                        params.getSessionId(),
                        params.getUserCode(),
                        params.getUserName(),
                        params.getContent(),
                        headerMetadata,
                        traceParentSpanId);
                if (clientDispatchObservation != null && !clientDispatchObservation.id().isBlank()) {
                    langfuseParentObservationId = clientDispatchObservation.id();
                }
                if (langfuseParentObservationId.isEmpty()) {
                    langfuseParentObservationId = traceParentSpanId;
                }
            }

            DeliveryIntent intent = buildDeliveryIntent(
                    params, executionId, messageId, traceId, resolvedPolicy,
                    availabilityTimeoutMs, region, priority
            );

            AvailabilityResult availResult = availabilityRouter.prepareDelivery(intent);

            // Handle QUEUE_PENDING: command stored by router, skip dispatch
            if (AvailabilityStatus.QUEUE_PENDING.equals(availResult.getStatus())) {
                endClientDispatchObservation(
                        clientDispatchObservation,
                        Map.of(
                                "success", true,
                                "message_id", messageId,
                                "trace_id", traceId,
                                "target_worker_id", "",
                                "status", AvailabilityStatus.QUEUE_PENDING
                        ),
                        "");
                return SendResponse.builder()
                        .success(true)
                        .messageId(messageId)
                        .traceId(traceId)
                        .status(AvailabilityStatus.QUEUE_PENDING)
                        .timestamp(System.currentTimeMillis())
                        .build();
            }

            // Handle REJECT
            if (AvailabilityStatus.REJECT.equals(availResult.getStatus())) {
                String errorReason = availResult.getReason();
                String errorCode = availResult.getErrorCode() != null ? availResult.getErrorCode() : ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE;
                endClientDispatchObservation(
                        clientDispatchObservation,
                        Map.of(
                                "success", false,
                                "message_id", messageId,
                                "trace_id", traceId,
                                "target_worker_id", "",
                                "status", ExecutionStatus.FAILED
                        ),
                        errorReason);
                return SendResponse.builder()
                        .success(false)
                        .messageId(messageId)
                        .traceId(traceId)
                        .status(ExecutionStatus.FAILED)
                        .error(errorReason)
                        .errorCode(errorCode)
                        .timestamp(System.currentTimeMillis())
                        .build();
            }

            // Determine the actual agent type for header (may differ due to fallback)
            String selectedAgentType = availResult.getSelectedAgentType() != null
                    ? availResult.getSelectedAgentType() : params.getTargetAgentType();
            MessageHeader header = MessageHeader.builder()
                    .messageId(messageId)
                    .sessionId(params.getSessionId())
                    .traceId(traceId)
                    .targetAgentType(selectedAgentType)
                    .parentMessageId(params.getParentMessageId())
                    .userCode(params.getUserCode())
                    .userName(params.getUserName())
                    .traceParentSpanId(traceParentSpanId)
                    .langfuseParentObservationId(langfuseParentObservationId)
                    .metadata(headerMetadata)
                    .build();

            Object command = buildGatewayCommand(params, header);

            try {
                registry.initializeExecution(executionId, messageId, params.getSessionId(),
                        selectedAgentType, params.getParentMessageId(), traceId);
            } catch (Exception e) {
                log.warn("Failed to initialize execution tracking: {}", e.getMessage());
            }

            // Dispatch to resolved stream
            String resolvedStream;
            if (targetWorkerId != null && !targetWorkerId.isBlank()) {
                resolvedStream = Constants.QueueNames.workerCtrlStream(targetWorkerId);
            } else if (availResult.getStreamName() != null) {
                resolvedStream = availResult.getStreamName();
            } else {
                resolvedStream = Constants.QueueNames.ctrlStream(selectedAgentType);
            }

            long dispatchStartedAt = System.currentTimeMillis();
            String jsonPayload = objectMapper.writeValueAsString(command);
            Map<String, String> redisData = new HashMap<>();
            redisData.put(Constants.RedisFields.DATA, jsonPayload);
            streamOps.xadd(resolvedStream, redisData, XAddOptions.noTrim());
            long dispatchEndedAt = System.currentTimeMillis();

            log.info("Message sent to gateway: {} (target={}, stream={})", messageId, selectedAgentType, resolvedStream);

            String targetWorker = targetWorkerId != null ? targetWorkerId
                    : (availResult.getTargetWorkerId() != null ? availResult.getTargetWorkerId() : "");
            String status = AvailabilityStatus.WAIT_AND_DELIVER.equals(availResult.getStatus())
                    ? AvailabilityStatus.WAIT_AND_DELIVER : ExecutionStatus.QUEUED;

            SendResponse response = SendResponse.builder()
                    .success(true)
                    .messageId(messageId)
                    .traceId(traceId)
                    .targetWorkerId(targetWorker)
                    .status(status)
                    .timestamp(System.currentTimeMillis())
                    .build();

            endClientDispatchObservation(
                    clientDispatchObservation,
                    Map.of(
                            "success", response.isSuccess(),
                            "message_id", messageId,
                            "trace_id", traceId,
                            "target_worker_id", response.getTargetWorkerId() != null ? response.getTargetWorkerId() : "",
                            "status", response.getStatus() != null ? response.getStatus() : ""
                    ),
                    response.isSuccess() ? "" : stringValue(response.getError()));

            redisTraceWriter.recordClientDispatch(new RedisTraceWriter.ClientDispatchRecord(
                    traceId,
                    messageId,
                    params.getSessionId(),
                    params.getParentMessageId(),
                    selectedAgentType,
                    resolveTraceWorkerId(response),
                    targetWorkerId != null && !targetWorkerId.isBlank() ? "DIRECT_WORKER" : "AGENT_TYPE",
                    response.getStatus() != null ? response.getStatus() : "",
                    dispatchStartedAt,
                    dispatchEndedAt));

            return response;

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

    private DeliveryIntent buildDeliveryIntent(
            SendMessageParams params,
            String executionId,
            String messageId,
            String traceId,
            String resolvedPolicy,
            long availabilityTimeoutMs,
            String region,
            String priority) {
        Map<String, Object> headerMap = new HashMap<>();
        headerMap.put("message_id", messageId);
        headerMap.put("session_id", params.getSessionId());
        headerMap.put("trace_id", traceId);
        headerMap.put("source_agent_type", "");
        headerMap.put("target_agent_type", params.getTargetAgentType());
        headerMap.put("parent_message_id", params.getParentMessageId());
        headerMap.put("task_group_id", "");
        headerMap.put("user_code", params.getUserCode());
        headerMap.put("user_name", params.getUserName());
        headerMap.put("metadata", params.getMetadata());

        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("content", params.getContent());
        bodyMap.put("wait_for_reply", Boolean.TRUE.equals(params.getPayload().get("wait_for_reply")));

        Map<String, Object> commandPayload = new HashMap<>();
        commandPayload.put("action_type", params.getActionType());
        commandPayload.put("header", headerMap);
        commandPayload.put("body", bodyMap);

        return DeliveryIntent.builder()
                .executionId(executionId)
                .messageId(messageId)
                .sessionId(params.getSessionId())
                .traceId(traceId)
                .source("client")
                .targetAgentType(params.getTargetAgentType())
                .userCode(params.getUserCode())
                .region(region)
                .priority(priority)
                .policy(resolvedPolicy)
                .timeoutMs(availabilityTimeoutMs)
                .metadata(params.getMetadata())
                .commandPayload(commandPayload)
                .build();
    }

    private Object buildGatewayCommand(SendMessageParams params, MessageHeader header) {
        Map<String, Object> payloadMap = new HashMap<>(params.getPayload());
        return ActionType.RESUME.equals(params.getActionType())
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
    }

    public SendResponse sendMessage(String targetAgentType, String sessionId, T content) {
        return sendMessage(targetAgentType, sessionId, content, null, null, null, null, null, null, null, null);
    }

    public SendResponse sendMessage(String targetAgentType, String sessionId, T content, Map<String, Object> metadata) {
        return sendMessage(targetAgentType, sessionId, content, null, null, null, null, null, null, null, metadata);
    }

    private static String stringValue(Object value) {
        return value != null ? String.valueOf(value) : "";
    }

    private String resolveTraceWorkerId(SendResponse response) {
        String responseWorkerId = response.getTargetWorkerId() != null ? response.getTargetWorkerId() : "";
        if (!responseWorkerId.isBlank()) {
            return responseWorkerId;
        }
        return "";
    }

    private static String resolveExecutionTraceId(Map<String, Object> execution) {
        String traceId = stringValue(execution.get(Constants.ExecutionFields.TRACE_ID));
        return traceId.isEmpty() ? UUID.randomUUID().toString().replace("-", "") : traceId;
    }

    private ClientDispatchObservation startClientDispatchObservation(
            String traceId,
            String messageId,
            String targetAgentType,
            String sessionId,
            String userCode,
            String userName,
            Object content,
            Map<String, Object> metadata,
            String observationId) {
        try {
            return clientDispatchTracer.start(new ClientDispatchTracer.ClientDispatchRequest(
                    traceId,
                    messageId,
                    targetAgentType,
                    sessionId,
                    userCode,
                    userName,
                    content,
                    metadata,
                    observationId));
        } catch (Exception e) {
            log.warn("Langfuse client.dispatch observation skipped: {}", e.getMessage());
            return null;
        }
    }

    private void endClientDispatchObservation(
            ClientDispatchObservation observation,
            Object output,
            String error) {
        if (observation == null) {
            return;
        }
        try {
            observation.end(output, error);
        } catch (Exception e) {
            log.warn("Langfuse client.dispatch observation end skipped: {}", e.getMessage());
        }
    }
}
