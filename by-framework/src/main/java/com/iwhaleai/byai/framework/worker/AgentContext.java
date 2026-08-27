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
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.liveness.WaitRegistration;
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

    // SSE Content Types
    private static final String CONTENT_TYPE_TEXT = "1002";
    private static final String CONTENT_TYPE_REASONING_TITLE = "3003";
    private static final String CONTENT_TYPE_ARTIFACT_FILE = "3010";
    private static final String CONTENT_TYPE_USER_INPUT = "3013";
    private boolean streamFinished = false;

    /**
     * Whether this execution suspended — it dispatched to another agent, or asked
     * a person, and its result is not in yet.
     *
     * The framework decides this, not the business handler: AgentContext is what
     * suspended, whereas a handler is free to return anything (the in-tree ones
     * return plain QUEUED, indistinguishable from "still queued behind a worker").
     * GatewayWorker reads it to decide that a suspended execution owes its caller
     * nothing YET — forwarding the value a handler returned merely to unwind
     * would wake the caller early with a placeholder and burn the single reply it
     * is parked on.
     */
    private boolean suspended = false;

    /** Which suspended state this is: "" / WAITING_AGENT / WAITING_USER. */
    private String suspendedState = "";

    public boolean isSuspended() {
        return suspended;
    }

    public String suspendedState() {
        return suspendedState;
    }

    /** Package-private: the framework sets this, and same-package tests drive it. */
    void markSuspended(String state) {
        this.suspended = true;
        this.suspendedState = state;
    }

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
            // childMessageId is "" for an askUser wait: the answer comes from a
            // client whose header.parentMessageId is arbitrary, so the member
            // cannot be derived from it. The gate recovers this entry through its
            // second candidate instead (contract section 4).
            WaitRegistration.register(redisOps, sessionId, currentMessageId, "", "",
                    Constants.DEFAULT_ASK_USER_TIMEOUT_MS);
            markSuspended(AgentState.WAITING_USER);
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
        return callAgent(targetAgentType, content, payload, waitForReply, metadata, taskGroupId,
                routePolicy, availabilityTimeoutMs, region, priority, 0L);
    }

    /**
     * @param replyTimeoutMs bounds how long the caller may stay suspended on this
     *        reply. Only meaningful with {@code waitForReply}; 0 means the default
     *        (1 hour). The bound is enforced from OUTSIDE the caller — it ends its
     *        execution when it dispatches, so nothing inside it can time out.
     */
    public Map<String, Object> callAgent(String targetAgentType, String content, Map<String, Object> payload, boolean waitForReply,
            Map<String, Object> metadata, String taskGroupId, String routePolicy, long availabilityTimeoutMs,
            String region, String priority, long replyTimeoutMs) {

        String msgId = Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8);
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
            errorResponse.put("target_agent_type", targetAgentType);
            errorResponse.put("error", availResult.getReason());
            errorResponse.put("error_code", availResult.getErrorCode() != null ? availResult.getErrorCode() : "ERR_AGENT_TYPE_UNAVAILABLE");
            return errorResponse;
        }

        // Suspend here, deliberately AFTER the REJECT early-return above and
        // BEFORE the QUEUE_PENDING one below.
        //
        // After REJECT: nothing was dispatched, so nothing will ever reply — a
        // context left looking suspended would make its handler owe a reply that
        // never comes. Python needs an explicit rollback for this because it
        // suspends before the availability check; Java's ordering makes the
        // rollback structurally unnecessary, so do NOT port that patch.
        //
        // Before QUEUE_PENDING: the router has stored the delivery and will make
        // it later, so the caller IS waiting on a reply. Same as Python, which
        // keeps _is_suspended set for QUEUE_PENDING and only skips the immediate
        // control-stream write.
        if (waitForReply) {
            markSuspended(AgentState.WAITING_AGENT);
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

        // Initialize execution tracking for the dispatched task.
        //
        // source_agent_type, task_group_id and metadata are recorded here because
        // the callee reads all three back off this record once it suspends and
        // resumes: the ResumeCommand that wakes it describes the hop that just
        // finished, not this dispatch. source_agent_type is written only when
        // waitForReply — a fire-and-forget dispatch has nobody waiting, and
        // recording a caller for it would make the callee reply to an execution
        // that already moved on.
        //
        // The callee's own worker records the same metadata again on first pickup
        // (see WorkerRunner). That is not redundant: this write is the only record
        // that exists before the callee is ever picked up, which is what a wait
        // sweep reads when the callee never starts.
        try {
            Map<String, Object> extraFields = new HashMap<>();
            extraFields.put("source_agent_type", waitForReply ? currentAgentType : "");
            extraFields.put("task_group_id", taskGroupId != null ? taskGroupId : "");
            extraFields.put("metadata", metadata != null ? new HashMap<>(metadata) : new HashMap<>());
            workerRegistry.initializeExecution(executionId, msgId, sessionId, selectedAgentType,
                    currentMessageId, traceId, extraFields);
        } catch (Exception e) {
            log.warn("Failed to initialize execution tracking for callAgent: {}", e.getMessage());
        }

        // Registered next to initializeExecution, i.e. right after the dispatch is
        // committed. The window that must not exist is "dispatched but nobody
        // knows we are waiting".
        if (waitForReply) {
            WaitRegistration.register(redisOps, sessionId, currentMessageId, msgId,
                    taskGroupId != null ? taskGroupId : "",
                    replyTimeoutMs > 0 ? replyTimeoutMs : Constants.DEFAULT_REPLY_TIMEOUT_MS);
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
        if (requests == null || requests.isEmpty()) {
            return Map.of("status", "EMPTY", "task_group_id", "");
        }

        String groupId = Constants.TASK_GROUP_ID_PREFIX + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
        int total = requests.size();
        List<Map<String, String>> dispatched = new java.util.ArrayList<>();

        try {
            String key = Constants.RegistryKeys.taskGroup(groupId);
            Map<String, String> groupData = new HashMap<>();
            groupData.put(Constants.TASK_GROUP_FIELD_TOTAL, String.valueOf(total));
            groupData.put(Constants.TASK_GROUP_FIELD_COMPLETED, "0");
            groupData.put(Constants.TASK_GROUP_FIELD_SOURCE_AGENT, currentAgentType);
            redisOps.hsetAll(key, groupData);
            redisOps.expire(key, Constants.TASK_GROUP_TTL_SECONDS);

            for (Map<String, Object> req : requests) {
                String targetAgentType = (String) req.get(Constants.DispatchFields.AGENT_TYPE);
                String content = (String) req.getOrDefault(Constants.DispatchFields.CONTENT, "");
                Map<String, Object> payload = (Map<String, Object>) req.getOrDefault(Constants.DispatchFields.PAYLOAD, new HashMap<>());
                Map<String, Object> metadata = (Map<String, Object>) req.getOrDefault(Constants.DispatchFields.METADATA, new HashMap<>());

                if (waitForReply) {
                    payload = new HashMap<>(payload);
                    payload.put("wait_for_reply", true);
                }

                String msgId = Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
                String execId = Constants.EXECUTION_ID_PREFIX + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);
                AskAgentCommand command = AskAgentCommand.of(
                        MessageHeader.builder()
                                .messageId(msgId)
                                .sessionId(sessionId)
                                .traceId(traceId)
                                .sourceAgentType(waitForReply ? currentAgentType : "")
                                .targetAgentType(targetAgentType)
                                .parentMessageId(currentMessageId)
                                .taskGroupId(groupId)
                                .metadata(metadata)
                                .build(),
                        content,
                        waitForReply,
                        payload);

                Map<String, String> fields = new HashMap<>();
                fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(command));
                streamOps.xadd(Constants.QueueNames.ctrlStream(targetAgentType), fields, XAddOptions.noTrim());

                // Initialize execution tracking for each dispatched task
                try {
                    // Same three fields the single-call path records. A group
                    // member suspending is normal — it may call another agent or
                    // askUser itself — and without these it cannot recover its
                    // caller or its dispatch metadata when it resumes.
                    Map<String, Object> memberFields = new HashMap<>();
                    memberFields.put("source_agent_type", waitForReply ? currentAgentType : "");
                    memberFields.put("task_group_id", groupId);
                    memberFields.put("metadata", new HashMap<>(metadata));
                    workerRegistry.initializeExecution(execId, msgId, sessionId, targetAgentType,
                            currentMessageId, traceId, memberFields);
                } catch (Exception ex) {
                    log.warn("Failed to initialize execution tracking for group dispatch: {}", ex.getMessage());
                }

                dispatched.add(Map.of("message_id", msgId, "target_agent_type", targetAgentType));
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to dispatch group tasks", e);
        }

        Map<String, Object> result = new HashMap<>();
        result.put("status", "GROUP_QUEUED");
        result.put("task_group_id", groupId);
        result.put("dispatched_tasks", dispatched);
        return result;
    }

    /**
     * 收集任务组所有子任务的结果。
     * 当最后一个子任务完成后调用，返回所有子任务的结果列表。
     *
     * @param taskGroupId dispatchGroup 返回的 task_group_id
     * @param timeoutSeconds 等待所有结果的最大超时时间（秒），默认 30.0
     * @return 包含所有子任务结果的列表，每个元素包含 message_id, status, reply_data, content
     */
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
