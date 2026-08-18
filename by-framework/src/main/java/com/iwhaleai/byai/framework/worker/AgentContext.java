package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.iwhaleai.byai.framework.common.ClusterRedisOps;
import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import com.iwhaleai.byai.framework.core.availability.AvailabilityResult;
import com.iwhaleai.byai.framework.core.availability.AvailabilityRouter;
import com.iwhaleai.byai.framework.core.availability.AvailabilityStatus;
import com.iwhaleai.byai.framework.core.availability.DeliveryIntent;
import com.iwhaleai.byai.framework.core.availability.RoutePolicy;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.framework.core.protocol.DataMessage;
import com.iwhaleai.byai.framework.core.protocol.EventType;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ExecutionStatus;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Agent 执行上下文，提供事件上报及 Agent 间调用能力。
 */
@Slf4j
public class AgentContext {
    private final String sessionId;
    private final String traceId;
    private final RedisOps redisOps;
    private final RedisStreamOps streamOps;
    private final String currentAgentType;
    private final String currentMessageId;
    private final WorkerRegistry workerRegistry;
    private final AvailabilityRouter availabilityRouter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * Task Group sub-tasks that never reached a worker (their target agent type was
     * unavailable at dispatch time). Each is a fully-formed ResumeCommand addressed back at
     * this caller, so Group Join counts and aggregates it exactly like a real sub-agent's
     * failure reply. GatewayWorker flushes these AFTER processCommand returns — see
     * {@link #drainPendingGroupReplies()}.
     */
    private final List<ResumeCommand> pendingGroupReplies = new java.util.ArrayList<>();

    // SSE Content Types
    private static final String CONTENT_TYPE_TEXT = "1002";
    private static final String CONTENT_TYPE_REASONING_TITLE = "3003";
    private static final String CONTENT_TYPE_ARTIFACT_FILE = "3010";
    private static final String CONTENT_TYPE_USER_INPUT = "3013";
    private boolean streamFinished = false;

    public AgentContext(String sessionId, String traceId, RedisClient redisClient, String currentAgentType,
            String currentMessageId) {
        this.sessionId = sessionId;
        this.traceId = traceId;
        this.redisOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisOps(redisClient.getJedisCluster())
                : new StandaloneRedisOps(redisClient);
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.currentAgentType = currentAgentType;
        this.currentMessageId = currentMessageId;
        this.workerRegistry = new WorkerRegistry(redisClient);
        this.availabilityRouter = new AvailabilityRouter(redisClient, workerRegistry);
    }

    public boolean isStreamFinished() {
        return streamFinished;
    }

    public void setStreamFinished(boolean streamFinished) {
        this.streamFinished = streamFinished;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getCurrentAgentType() {
        return currentAgentType;
    }

    public String getCurrentMessageId() {
        return currentMessageId;
    }

    /**
     * 检查当前任务是否已被请求取消。
     * 同时也检查当前线程的中断状态。
     */
    public boolean isCancelRequested() {
        return Thread.currentThread().isInterrupted();
    }

    /**
     * 如果任务已被取消，抛出运行时中断异常。
     * 建议在长耗时循环中调用。
     */
    public void checkCancelled() {
        if (isCancelRequested()) {
            throw new RuntimeException(new InterruptedException("Task was cancelled"));
        }
    }

    private Map<String, Object> buildSseLayout(Object content, String role, String contentType) {
        Map<String, Object> delta = new HashMap<>();
        delta.put(Constants.RedisFields.CONTENT, content != null ? content : "");
        if (role != null) {
            delta.put("role", role);
        }

        Map<String, Object> choice = new HashMap<>();
        choice.put("index", 0);
        choice.put("delta", delta);

        Map<String, Object> layout = new HashMap<>();
        layout.put("id", "gw-" + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH));
        layout.put("created", System.currentTimeMillis() / 1000);
        layout.put("choices", List.of(choice));
        layout.put("contentType", contentType);
        return layout;
    }

    private void emitEvent(String eventType, Map<String, Object> data, String stateMsg, String artifactUrl,
            Map<String, Object> metadata) {
        DataMessage msg = DataMessage.builder()
                .messageId(currentMessageId)
                .traceId(traceId)
                .sessionId(sessionId)
                .eventType(eventType)
                .sourceAgentType(currentAgentType)
                .data(data != null ? data : new HashMap<>())
                .stateMsg(stateMsg != null ? stateMsg : "")
                .artifactUrl(artifactUrl != null ? artifactUrl : "")
                .metadata(metadata != null ? metadata : new HashMap<>())
                .build();

        try {
            String streamName = Constants.QueueNames.sessionDataStream(sessionId);
            Map<String, String> fields = new HashMap<>();
            fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(msg));
            fields.put(Constants.RedisFields.PAYLOAD, fields.get(Constants.RedisFields.DATA));

            streamOps.xadd(streamName, fields, XAddOptions.noTrim());
            redisOps.expire(streamName, Constants.DEFAULT_SESSION_TTL);
        } catch (Exception e) {
            throw new RuntimeException("Failed to emit event", e);
        }
    }

    public void emitChunk(String content) {
        emitChunk(content, null, null, null);
    }

    public void emitChunk(String content, String eventType) {
        emitChunk(content, eventType, null, null);
    }

    public void emitChunk(String content, String eventType, Map<String, Object> metadata) {
        emitChunk(content, eventType, metadata, null);
    }

    /**
     * Emit a chunk of content with full control over event type, metadata, and content type.
     *
     * @param content The content to emit
     * @param eventType The event type (e.g., "answerDelta")
     * @param metadata Additional metadata
     * @param contentType The content type (e.g., "1002" for text, "3003" for reasoning title)
     */
    public void emitChunk(String content, String eventType, Map<String, Object> metadata, String contentType) {
        Map<String, Object> data = buildSseLayout(content, "assistant", contentType != null ? contentType : CONTENT_TYPE_TEXT);
        emitEvent(eventType != null ? eventType : EventType.ANSWER_DELTA.getValue(), data, "", "", metadata);
        if (EventType.APP_STREAM_RESPONSE.getValue().equals(eventType)) {
            this.streamFinished = true;
        }
    }

    public void emitState(String state) {
        emitState(state, null, null, null);
    }

    public void emitState(String state, String eventType) {
        emitState(state, eventType, null, null);
    }

    public void emitState(String state, String eventType, Map<String, Object> metadata) {
        emitState(state, eventType, metadata, null);
    }

    /**
     * Emit a state change with full control over event type, metadata, and content type.
     *
     * @param state The state message
     * @param eventType The event type
     * @param metadata Additional metadata
     * @param contentType The content type (e.g., "3003" for reasoning title)
     */
    public void emitState(String state, String eventType, Map<String, Object> metadata, String contentType) {
        Map<String, Object> data = buildSseLayout(state, null, contentType != null ? contentType : CONTENT_TYPE_REASONING_TITLE);
        emitEvent(eventType != null ? eventType : EventType.REASONING_LOG_DELTA.getValue(), data, state, "", metadata);
    }

    public void emitArtifact(String url) {
        emitArtifact(url, null, null, null);
    }

    public void emitArtifact(String url, String eventType) {
        emitArtifact(url, eventType, null, null);
    }

    public void emitArtifact(String url, String eventType, Map<String, Object> metadata) {
        emitArtifact(url, eventType, metadata, null);
    }

    /**
     * Emit an artifact with full control over event type, metadata, and content type.
     *
     * @param url The artifact URL
     * @param eventType The event type
     * @param metadata Additional metadata
     * @param contentType The content type (e.g., "3010" for artifact file)
     */
    public void emitArtifact(String url, String eventType, Map<String, Object> metadata, String contentType) {
        // Aligned with Python: artifacts are wrapped as a list in content
        List<Map<String, String>> files = List.of(Map.of("fileUrl", url));
        try {
            String filesJson = objectMapper.writeValueAsString(files);
            Map<String, Object> data = buildSseLayout(filesJson, null, contentType != null ? contentType : CONTENT_TYPE_ARTIFACT_FILE);
            emitEvent(eventType != null ? eventType : EventType.REASONING_LOG_DELTA.getValue(), data, "", url,
                    metadata);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize artifact files", e);
        }
    }

    public Map<String, String> askUser(String prompt) {
        return askUser(prompt, null);
    }

    public Map<String, String> askUser(String prompt, Map<String, Object> metadata) {
        // Aligned with Python: askUser uses special JSON structure in content
        Map<String, Object> inputForm = Map.of(
                "formStatus", 0,
                "pluginMachineFields", List.of(Map.of(
                        "formType", "textarea",
                        "fieldName", "用户输入",
                        "fieldCode", "user_input",
                        "description", prompt,
                        "required", true)));
        try {
            String formJson = objectMapper.writeValueAsString(inputForm);
            Map<String, Object> data = buildSseLayout(formJson, "assistant", CONTENT_TYPE_USER_INPUT);
            emitEvent(EventType.REASONING_LOG_DELTA.getValue(), data, prompt, "", metadata);
            return Map.of(Constants.RedisFields.STATUS, AgentState.WAITING_USER);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize askUser form", e);
        }
    }

    public Map<String, Object> callAgent(String targetAgentType, String content) {
        return callAgent(targetAgentType, content, null, true, null);
    }

    public Map<String, Object> callAgent(String targetAgentType, String content, Map<String, Object> payload, boolean waitForReply,
            Map<String, Object> metadata) {
        return callAgent(targetAgentType, content, payload, waitForReply, metadata, null, RoutePolicy.FAIL_FAST, 0, null, null);
    }

    public Map<String, Object> callAgent(String targetAgentType, String content, Map<String, Object> payload, boolean waitForReply,
            Map<String, Object> metadata, String taskGroupId) {
        return callAgent(targetAgentType, content, payload, waitForReply, metadata, taskGroupId, RoutePolicy.FAIL_FAST, 0, null, null);
    }

    /**
     * Call another agent with full availability control.
     *
     * @param targetAgentType      Target agent type
     * @param content              Message content
     * @param payload              Optional payload
     * @param waitForReply         Whether to wait for reply
     * @param metadata             Optional metadata
     * @param taskGroupId          Optional task group ID
     * @param routePolicy          Route policy for availability control
     * @param availabilityTimeoutMs Timeout for WAKE_AND_WAIT policy
     * @param region               Optional region
     * @param priority             Optional priority
     * @return Response map with status, message_id, etc.
     */
    public Map<String, Object> callAgent(String targetAgentType, String content, Map<String, Object> payload, boolean waitForReply,
            Map<String, Object> metadata, String taskGroupId, String routePolicy, long availabilityTimeoutMs,
            String region, String priority) {
        return dispatchSingleTask(targetAgentType, content, payload, waitForReply, metadata, taskGroupId,
                routePolicy, availabilityTimeoutMs, region, priority, null);
    }

    /**
     * Build, availability-check and dispatch one AskAgentCommand.
     *
     * <p>Shared by {@link #callAgent} (a single task) and {@link #callAgents} (a batch, one
     * call per task), so a task in a group behaves exactly like the equivalent single call.
     * The only extra parameter is {@code messageId}: a batch needs to assign each sub-task a
     * known id, because Task Group results are keyed per sub-task.
     */
    private Map<String, Object> dispatchSingleTask(String targetAgentType, Object content, Map<String, Object> payload,
            boolean waitForReply, Map<String, Object> metadata, String taskGroupId, String routePolicy,
            long availabilityTimeoutMs, String region, String priority, String messageId) {

        String msgId = messageId != null && !messageId.isBlank()
                ? messageId
                : Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8);
        String executionId = Constants.EXECUTION_ID_PREFIX + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
        String resolvedPolicy = routePolicy != null ? routePolicy : RoutePolicy.FAIL_FAST;

        // Build delivery intent and check availability via router
        DeliveryIntent intent = DeliveryIntent.builder()
                .executionId(executionId)
                .messageId(msgId)
                .sessionId(sessionId)
                .traceId(traceId)
                .source(waitForReply ? currentAgentType : "")
                .targetAgentType(targetAgentType)
                .userCode("")
                .region(region)
                .priority(priority)
                .policy(resolvedPolicy)
                .timeoutMs(availabilityTimeoutMs)
                .metadata(metadata != null ? metadata : new HashMap<>())
                .build();

        AvailabilityResult availResult = availabilityRouter.prepareDelivery(intent);

        // Handle REJECT
        if (AvailabilityStatus.REJECT.equals(availResult.getStatus())) {
            log.warn("[{}] Availability rejected for agent type '{}': {}", traceId, targetAgentType, availResult.getReason());
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put(Constants.RedisFields.STATUS, AgentState.FAILED);
            errorResponse.put("message_id", "");
            // The resolved caller message id: callAgents needs it to address the synthetic
            // failure reply back at the caller's own suspended execution.
            errorResponse.put("parent_message_id", currentMessageId);
            errorResponse.put("target_agent_type", targetAgentType);
            errorResponse.put("error", availResult.getReason());
            errorResponse.put("error_code", availResult.getErrorCode() != null ? availResult.getErrorCode() : "ERR_AGENT_TYPE_UNAVAILABLE");
            return errorResponse;
        }

        // Handle QUEUE_PENDING: router has stored the pending delivery, skip dispatch
        if (AvailabilityStatus.QUEUE_PENDING.equals(availResult.getStatus())) {
            Map<String, Object> response = new HashMap<>();
            response.put(Constants.RedisFields.STATUS, AvailabilityStatus.QUEUE_PENDING);
            response.put("message_id", msgId);
            response.put("parent_message_id", currentMessageId);
            response.put("target_agent_type", targetAgentType);
            return response;
        }

        // Determine selected agent type (may differ due to fallback)
        String selectedAgentType = availResult.getSelectedAgentType() != null
                ? availResult.getSelectedAgentType() : targetAgentType;

        AskAgentCommand command = AskAgentCommand.of(
                MessageHeader.builder()
                        .messageId(msgId)
                        .sessionId(sessionId)
                        .traceId(traceId)
                        .sourceAgentType(waitForReply ? currentAgentType : "")
                        .targetAgentType(selectedAgentType)
                        .parentMessageId(currentMessageId)
                        .taskGroupId(taskGroupId != null ? taskGroupId : "")
                        .metadata(metadata != null ? metadata : new HashMap<>())
                        .build(),
                content,
                waitForReply,
                payload != null ? new HashMap<>(payload) : new HashMap<>());

        // Dispatch to the resolved stream
        String resolvedStream = availResult.getStreamName() != null
                ? availResult.getStreamName()
                : Constants.QueueNames.ctrlStream(selectedAgentType);

        try {
            Map<String, String> fields = new HashMap<>();
            fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(command));
            streamOps.xadd(resolvedStream, fields, XAddOptions.noTrim());
        } catch (Exception e) {
            throw new RuntimeException("Failed to enqueue agent call", e);
        }

        // Initialize execution tracking for the dispatched task
        try {
            workerRegistry.initializeExecution(executionId, msgId, sessionId, selectedAgentType, currentMessageId);
        } catch (Exception e) {
            log.warn("Failed to initialize execution tracking for callAgent: {}", e.getMessage());
        }

        Map<String, Object> response = new HashMap<>();
        response.put(Constants.RedisFields.STATUS, AvailabilityStatus.WAIT_AND_DELIVER.equals(availResult.getStatus())
                ? AvailabilityStatus.WAIT_AND_DELIVER : AgentState.QUEUED);
        response.put("message_id", msgId);
        response.put("parent_message_id", currentMessageId);
        response.put("target_agent_type", selectedAgentType);
        return response;
    }

    /**
     * 分派任务组给多个 sub-agents (Scatter-Gather)。
     * 调用者仅当所有任务完成后才会被恢复。
     *
     * @param requests 任务列表，每个包含 agent_type, content, payload, metadata
     * @return 包含 status, task_group_id, dispatched_tasks 的映射
     */
    public Map<String, Object> dispatchGroup(List<Map<String, Object>> requests) {
        return dispatchGroup(requests, true);
    }

    /**
     * 分派任务组给多个 sub-agents (Scatter-Gather)。
     *
     * @param requests 任务列表，每个包含 agent_type, content, payload, metadata
     * @param waitForReply 如果为 true，则设置 Redis 计数器等待所有任务完成
     * @return 包含 status, task_group_id, dispatched_tasks 的映射
     */
    public Map<String, Object> dispatchGroup(List<Map<String, Object>> requests, boolean waitForReply) {
        return callAgents(requests, waitForReply);
    }

    /**
     * Dispatch multiple tasks concurrently as a Task Group — callAgent's plural.
     *
     * <p>Every per-call option {@link #callAgent} takes is accepted per task, with the same
     * defaults, so a task that names no routing options behaves exactly like the equivalent
     * single call. The only increment is that the caller is resumed once, with every task's
     * result aggregated in dispatch order, after all of them complete. On that resume
     * {@code replyData} is the aggregate and {@code content} is {@code ""}.
     *
     * <p>Task map keys: {@code agent_type} (required), {@code content}, {@code payload},
     * {@code metadata}, {@code message_id}, {@code route_policy},
     * {@code availability_timeout_ms}, {@code region}, {@code priority}.
     *
     * <p>Contract: by-framework-python/docs/adr/0001-unify-call-agent-and-call-agents-behavior.md
     */
    public Map<String, Object> callAgents(List<Map<String, Object>> requests) {
        return callAgents(requests, true);
    }

    /** See {@link #callAgents(List)}. */
    @SuppressWarnings("unchecked")
    public Map<String, Object> callAgents(List<Map<String, Object>> requests, boolean waitForReply) {
        if (requests == null || requests.isEmpty()) {
            throw new IllegalArgumentException("callAgents/dispatchGroup requires at least one task");
        }

        String groupId = Constants.TASK_GROUP_ID_PREFIX
                + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
        int total = requests.size();
        List<Map<String, Object>> dispatched = new java.util.ArrayList<>();
        List<ResumeCommand> pendingFailures = new java.util.ArrayList<>();
        String groupKey = Constants.RegistryKeys.taskGroup(groupId);

        if (waitForReply) {
            Map<String, String> groupData = new HashMap<>();
            groupData.put(Constants.TASK_GROUP_FIELD_TOTAL, String.valueOf(total));
            groupData.put(Constants.TASK_GROUP_FIELD_COMPLETED, "0");
            groupData.put(Constants.TASK_GROUP_FIELD_SOURCE_AGENT, currentAgentType);
            groupData.put(Constants.TASK_GROUP_FIELD_PROTOCOL_VERSION, Constants.TASK_GROUP_PROTOCOL_V2);
            redisOps.hsetAll(groupKey, groupData);
            redisOps.expire(groupKey, Constants.TASK_GROUP_TTL_SECONDS);
        }

        for (Map<String, Object> req : requests) {
            String targetAgentType = (String) req.get(Constants.DispatchFields.AGENT_TYPE);
            Object content = req.getOrDefault(Constants.DispatchFields.CONTENT, "");
            Map<String, Object> payload = (Map<String, Object>) req.getOrDefault(
                    Constants.DispatchFields.PAYLOAD, new HashMap<String, Object>());
            Map<String, Object> metadata = (Map<String, Object>) req.getOrDefault(
                    Constants.DispatchFields.METADATA, new HashMap<String, Object>());
            String taskMessageId = (String) req.get("message_id");
            String routePolicy = (String) req.getOrDefault("route_policy", RoutePolicy.FAIL_FAST);
            long availabilityTimeoutMs = req.get("availability_timeout_ms") instanceof Number n
                    ? n.longValue() : 0L;
            String region = (String) req.get("region");
            String priority = (String) req.get("priority");

            Map<String, Object> taskResult;
            try {
                taskResult = dispatchSingleTask(targetAgentType, content, payload, waitForReply, metadata,
                        groupId, routePolicy, availabilityTimeoutMs, region, priority, taskMessageId);
            } catch (RuntimeException e) {
                // A genuine dispatch-time failure (not an availability rejection, which
                // dispatchSingleTask turns into a FAILED result instead of throwing). Stop
                // fanning out and mark the group aborted so already-sent siblings' replies
                // cannot later resume this now-failed caller execution. Synthetic replies
                // queued so far are dropped with the throw: the worker only flushes them
                // once processCommand returns normally.
                if (waitForReply) {
                    redisOps.hset(groupKey, Constants.TASK_GROUP_FIELD_ABORTED, "1");
                }
                throw e;
            }

            String status = String.valueOf(taskResult.get(Constants.RedisFields.STATUS));
            String dispatchedMessageId = String.valueOf(taskResult.getOrDefault("message_id", ""));
            if (dispatchedMessageId.isEmpty()) {
                dispatchedMessageId = taskMessageId != null ? taskMessageId
                        : Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8);
            }
            String resolvedTarget = String.valueOf(taskResult.getOrDefault("target_agent_type", targetAgentType));

            if (AgentState.FAILED.equals(status) && waitForReply) {
                // The target agent type was unavailable, so no worker will ever reply for
                // this sub-task. Rather than book-keeping the group here — a second copy of
                // the accounting GatewayWorker's Group Join owns, and the one that could push
                // `completed` to `total` with nobody left to resume the caller — synthesize
                // the reply a sub-agent WOULD have sent had it started and failed.
                pendingFailures.add(buildGroupFailureReply(groupId, dispatchedMessageId, resolvedTarget,
                        taskResult.get("error"), taskResult.get("error_code"), metadata));
            }

            Map<String, Object> dispatchedEntry = new HashMap<>();
            dispatchedEntry.put("message_id", dispatchedMessageId);
            // taskResult's target_agent_type reflects any fallback reroute the availability
            // router performed, so this stays consistent with what Group Join later reports.
            dispatchedEntry.put("target_agent_type", resolvedTarget);
            dispatchedEntry.put("status", status);
            if (AgentState.FAILED.equals(status)) {
                // Same shape a real sub-agent failure arrives in, so callers read
                // dispatch-time and run-time failures the same way.
                Map<String, Object> failure = new HashMap<>();
                failure.put("error", taskResult.get("error"));
                failure.put("error_code", taskResult.get("error_code"));
                dispatchedEntry.put("reply_data", failure);
            }
            dispatched.add(dispatchedEntry);
        }

        if (waitForReply) {
            // Written after the loop so it records exactly what was dispatched. Group Join
            // aggregates in this order and uses it to name results that never arrived.
            List<String> order = new java.util.ArrayList<>();
            for (Map<String, Object> entry : dispatched) {
                order.add(String.valueOf(entry.get("message_id")));
            }
            try {
                redisOps.hset(groupKey, Constants.TASK_GROUP_FIELD_TASK_ORDER,
                        objectMapper.writeValueAsString(order));
            } catch (JsonProcessingException e) {
                log.error("Failed to record task_order for group {}: {}", groupId, e.getMessage());
            }
            pendingGroupReplies.addAll(pendingFailures);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("status", AgentState.QUEUED);
        result.put("task_group_id", groupId);
        result.put("dispatched_tasks", dispatched);
        return result;
    }

    /**
     * Build the reply a sub-agent WOULD have sent had it started and failed.
     *
     * <p>The header derivation mirrors GatewayWorker's agent return exactly, because that
     * shape is load-bearing in two places: {@code messageId} must be the CALLER's own message
     * id (WorkerRunner reattaches the suspended execution via
     * {@code getExecutionByMessageId(header.messageId())}), and {@code parentMessageId} must
     * be this sub-task's dispatch id (the only per-sibling-unique value, which is what Group
     * Join keys the result hash by).
     *
     * <p>{@code replyData} carries the failure detail because that is how a real failure
     * arrives; putting it anywhere else would make dispatch-time and run-time failures read
     * differently.
     */
    private ResumeCommand buildGroupFailureReply(String taskGroupId, String taskMessageId, String targetAgentType,
            Object error, Object errorCode, Map<String, Object> metadata) {
        Map<String, Object> replyData = new HashMap<>();
        replyData.put("error", error);
        replyData.put("error_code", errorCode != null ? errorCode : ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE);
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId(currentMessageId)
                        .sessionId(sessionId)
                        .traceId(traceId)
                        .sourceAgentType(targetAgentType)
                        .targetAgentType(currentAgentType)
                        .parentMessageId(taskMessageId)
                        .taskGroupId(taskGroupId)
                        .metadata(metadata != null ? new HashMap<>(metadata) : new HashMap<>())
                        .build(),
                "",
                AgentState.FAILED,
                replyData,
                new HashMap<>());
    }

    /**
     * Hand the worker any dispatch-failure replies queued by {@link #callAgents} and clear
     * them. Called after processCommand returns — never inline during dispatch, which would
     * guarantee a reply reaches the caller's control stream before the caller suspends.
     */
    public List<ResumeCommand> drainPendingGroupReplies() {
        List<ResumeCommand> drained = new java.util.ArrayList<>(pendingGroupReplies);
        pendingGroupReplies.clear();
        return drained;
    }

    public List<Map<String, Object>> collectGroupResults(String taskGroupId) {
        return collectGroupResults(taskGroupId, 30.0);
    }

    /**
     * 收集任务组所有子任务的结果。
     * 当最后一个子任务完成后调用，返回所有子任务的结果列表。
     * 如果在超时时间内没有收集到所有结果，返回已收集到的结果。
     *
     * @param taskGroupId dispatchGroup 返回的 task_group_id
     * @param timeoutSeconds 等待所有结果的最大超时时间（秒）
     * @return 包含所有子任务结果的列表，每个元素包含 message_id, status, reply_data, content
     */
    public List<Map<String, Object>> collectGroupResults(String taskGroupId, double timeoutSeconds) {
        if (taskGroupId == null || taskGroupId.isEmpty()) {
            return List.of();
        }

        String resultsKey = Constants.RegistryKeys.taskGroupResults(taskGroupId);
        String groupKey = Constants.RegistryKeys.taskGroup(taskGroupId);
        int total = Integer.MAX_VALUE;

        long startTime = System.currentTimeMillis();
        long timeoutMillis = (long) (timeoutSeconds * 1000);

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            // Get total from group key
            String totalStr = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL);
            if (totalStr != null) {
                total = Integer.parseInt(totalStr);
            }

            // Get all results
            Map<String, String> rawResults = redisOps.hgetAll(resultsKey);
            if (rawResults != null && !rawResults.isEmpty()) {
                List<Map<String, Object>> results = new java.util.ArrayList<>();
                for (Map.Entry<String, String> entry : rawResults.entrySet()) {
                    String msgId = entry.getKey();
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> data = objectMapper.readValue(entry.getValue(), Map.class);
                        Map<String, Object> result = new HashMap<>();
                        result.put("message_id", msgId);
                        result.putAll(data);
                        results.add(result);
                    } catch (Exception e) {
                        log.warn("Failed to parse result for message {}: {}", msgId, e.getMessage());
                    }
                }
                if (results.size() >= total) {
                    return results;
                }
            }

            // Wait before polling again
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Return whatever we have collected
        try {
            Map<String, String> rawResults = redisOps.hgetAll(resultsKey);
            if (rawResults != null && !rawResults.isEmpty()) {
                List<Map<String, Object>> results = new java.util.ArrayList<>();
                for (Map.Entry<String, String> entry : rawResults.entrySet()) {
                    String msgId = entry.getKey();
                    try {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> data = objectMapper.readValue(entry.getValue(), Map.class);
                        Map<String, Object> result = new HashMap<>();
                        result.put("message_id", msgId);
                        result.putAll(data);
                        results.add(result);
                    } catch (Exception e) {
                        log.warn("Failed to parse result for message {}: {}", msgId, e.getMessage());
                    }
                }
                return results;
            }
        } catch (Exception e) {
            log.warn("Failed to collect group results: {}", e.getMessage());
        }

        return List.of();
    }
}
