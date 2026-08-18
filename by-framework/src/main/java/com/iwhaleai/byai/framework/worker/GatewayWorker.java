package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.ClusterRedisOps;
import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import com.iwhaleai.byai.framework.config.ConfigHolder;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.extensions.PluginRegistry;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.AgentTaskResult;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import com.iwhaleai.byai.framework.core.protocol.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class GatewayWorker {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayWorker.class);

    protected final String workerId;
    protected final RedisClient redisClient;
    private final RedisOps redisOps;
    private final RedisStreamOps streamOps;
    protected final WorkerRegistry registry;
    protected final PluginRegistry pluginRegistry;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    private ScheduledExecutorService heartbeatExecutor;

    /**
     * Called after each heartbeat with the latest admin lifecycle value ("active", "suspended", "evicted").
     * Set by WorkerRunner to update its internal state machine.
     */
    private Consumer<String> lifecycleCallback;

    /**
     * Called after each heartbeat with the set of denied agent types for this worker.
     * Set by WorkerRunner to update its denylist cache.
     */
    private Consumer<Set<String>> denylistRefresh;

    /**
     * Called before each heartbeat renewal. Returns false if the consumer loop is unhealthy.
     * When false, the heartbeat stops renewing the lease so the worker is evicted from routing.
     */
    private java.util.function.BooleanSupplier healthCheck;

    /**
     * Called when the health check fails. Allows the runner to initiate shutdown.
     */
    private Runnable onUnhealthy;

    public GatewayWorker(String workerId) {
        this(workerId, RedisClient.getInstance());
    }

    public GatewayWorker(String workerId, RedisClient redisClient) {
        this(workerId, redisClient, new WorkerRegistry(redisClient));
    }

    protected GatewayWorker(String workerId, RedisClient redisClient, WorkerRegistry registry) {
        this.workerId = workerId;
        this.redisClient = redisClient;
        this.redisOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisOps(redisClient.getJedisCluster())
                : new StandaloneRedisOps(redisClient);
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.registry = registry;
        this.pluginRegistry = new PluginRegistry();
    }

    public PluginRegistry getPluginRegistry() {
        return pluginRegistry;
    }

    public void setLifecycleCallback(Consumer<String> lifecycleCallback) {
        this.lifecycleCallback = lifecycleCallback;
    }

    public void setDenylistRefresh(Consumer<Set<String>> denylistRefresh) {
        this.denylistRefresh = denylistRefresh;
    }

    public void setHealthCheck(java.util.function.BooleanSupplier healthCheck) {
        this.healthCheck = healthCheck;
    }

    public void setOnUnhealthy(Runnable onUnhealthy) {
        this.onUnhealthy = onUnhealthy;
    }

    public abstract List<String> getAgentTypes();

    public abstract Object processCommand(GatewayCommand command, AgentContext context);

    public String getWorkerId() {
        return workerId;
    }

    /**
     * Get heartbeat interval in seconds from configuration.
     */
    public int getHeartbeatIntervalSeconds() {
        return ConfigHolder.getConfig().getWorker().getHeartbeatIntervalSeconds();
    }

    /**
     * Get heartbeat lease TTL in seconds from configuration.
     */
    public int getHeartbeatLeaseTtlSeconds() {
        return ConfigHolder.getConfig().getWorker().getHeartbeatLeaseTtlSeconds();
    }

    public void onCancelTask(CancelTaskCommand command) {
        LOG.info("[{}] Received cancel request for message: {}", workerId, command.targetMessageId());
    }

    public void startHeartbeat() {
        int leaseTtl = getHeartbeatLeaseTtlSeconds();
        int interval = getHeartbeatIntervalSeconds();

        // Read admin-controlled lifecycle BEFORE registering membership.
        // A worker that restarts while suspended must not re-join the
        // agent_type:members sets or start consuming until explicitly resumed.
        String startupLifecycle = "active";
        try {
            Map<String, String> adminState = registry.getWorkerAdminState(workerId);
            String lc = adminState.get("lifecycle");
            if (lc != null && !lc.isEmpty()) {
                startupLifecycle = lc;
            }
        } catch (Exception e) {
            LOG.warn("[{}] Failed to read admin state at startup: {}", workerId, e.getMessage());
        }

        if (!"active".equals(startupLifecycle)) {
            LOG.warn("[{}] Startup admin lifecycle is '{}'; skipping member registration — worker will not consume until resumed",
                    workerId, startupLifecycle);
            // Propagate the startup lifecycle to the runner immediately
            if (lifecycleCallback != null) {
                lifecycleCallback.accept(startupLifecycle);
            }
        }

        registry.heartbeatWorker(workerId, leaseTtl);
        pluginRegistry.onWorkerStartup(this);

        final String[] currentLifecycle = {startupLifecycle};

        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-" + workerId);
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                // Health check: if consumer loop is stalled, stop renewing the lease
                if (healthCheck != null && !healthCheck.getAsBoolean()) {
                    LOG.error("[{}] Heartbeat stopping: consumer loop is unhealthy", workerId);
                    heartbeatExecutor.shutdown();
                    if (onUnhealthy != null) onUnhealthy.run();
                    return;
                }

                registry.heartbeatWorker(workerId, leaseTtl);

                // Read admin state after each heartbeat
                Map<String, String> adminState = registry.getWorkerAdminState(workerId);
                String lc = adminState.get("lifecycle");
                currentLifecycle[0] = (lc != null && !lc.isEmpty()) ? lc : "active";

                if (lifecycleCallback != null) {
                    lifecycleCallback.accept(currentLifecycle[0]);
                }

                // Self-healing: re-register membership when active
                if ("active".equals(currentLifecycle[0])) {
                    registry.registerWorkerMembership(workerId, getAgentTypes());
                }

                // Refresh denylist for each agent type
                if (denylistRefresh != null) {
                    Set<String> denied = new HashSet<>();
                    for (String agentType : getAgentTypes()) {
                        if (registry.isWorkerDeniedForType(agentType, workerId)) {
                            denied.add(agentType);
                        }
                    }
                    denylistRefresh.accept(denied);
                }
            } catch (Exception e) {
                LOG.error("[{}] Heartbeat failed: {}", workerId, e.getMessage());
            }
        }, interval, interval, TimeUnit.SECONDS);
    }

    public void stopHeartbeat() {
        pluginRegistry.onWorkerShutdown(this);
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
            heartbeatExecutor = null;
        }
    }

    public void handleMessage(GatewayCommand command, String executionId) {
        if (command instanceof CancelTaskCommand cancelCmd) {
            onCancelTask(cancelCmd);
            return;
        }

        MessageHeader header = command.header();
        String traceId = header.traceId();
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }

        AgentContext context = new AgentContext(
                header.sessionId(),
                traceId,
                redisClient,
                header.targetAgentType(),
                header.messageId()
        );

        boolean isResume = command instanceof ResumeCommand;
        String sourceAgentType = header.sourceAgentType();
        boolean hasSourceAgent = sourceAgentType != null && !sourceAgentType.isBlank() && !isResume;

        LOG.info("[{}] Processing message: {} (exec: {})", workerId, header.messageId(), executionId);

        try {
            pluginRegistry.onTaskStart(context);

            if (isResume) {
                // Returns null when the group is not complete (or the reply was
                // discarded), otherwise the command to hand to processCommand —
                // which under protocol v2 is rebuilt to carry the aggregate.
                GatewayCommand resumed = handleTaskGroupResume(command, header, context);
                if (resumed == null) {
                    return;
                }
                command = resumed;
            }

            Object result = processCommand(command, context);
            // Only reached when processCommand returned normally. If it threw, callAgents
            // has already marked the Task Group aborted and these replies must NOT be
            // sent — the caller execution they would resume is the one that just failed.
            flushPendingGroupReplies(context);
            AgentTaskResult taskResult = AgentTaskResult.normalize(result);
            String finalStatus = taskResult.status();

            boolean permissionTransferred = false;
            if (hasSourceAgent) {
                permissionTransferred = true;
                enqueueAgentReturn(command, taskResult);
                context.emitState(AgentState.QUEUED + ": " + sourceAgentType);
            } else {
                context.emitState(AgentState.COMPLETED);
            }

            emitFinalAnswer(context, taskResult);

            // Emit APP_STREAM_RESPONSE if conditions are met
            boolean shouldEmitStreamEnd = !hasSourceAgent && AgentState.isTerminalState(finalStatus) && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                if (!context.isStreamFinished()) {
                    context.emitChunk("", EventType.APP_STREAM_RESPONSE.getValue());
                }
            }

            registry.markExecutionFinished(executionId, header.sessionId(), finalStatus);
            pluginRegistry.onTaskComplete(context, result);
        } catch (Exception e) {
            boolean isCancelled = e instanceof InterruptedException ||
                                 (e.getCause() instanceof InterruptedException) ||
                                 Thread.currentThread().isInterrupted();

            if (isCancelled) {
                String reason = e.getMessage() != null ? e.getMessage() : "Task was cancelled";
                LOG.info("[{}] Task cancellation detected for message: {}", workerId, header.messageId());
                context.emitState(AgentState.CANCELLING);

                boolean permissionTransferred = false;
                if (hasSourceAgent) {
                    // Check if parent execution has cancel_requested flag set.
                    // If so, skip the callback to avoid waking a cancelled parent.
                    boolean shouldCallback = true;
                    String parentMsgId = header.parentMessageId();
                    if (parentMsgId != null && !parentMsgId.isBlank()) {
                        try {
                            Map<String, Object> parentExec = registry.getExecutionByMessageId(parentMsgId, header.sessionId());
                            if (parentExec != null) {
                                Object cancelRequested = parentExec.get(Constants.ExecutionFields.CANCEL_REQUESTED);
                                if (Boolean.TRUE.equals(cancelRequested) || "true".equals(String.valueOf(cancelRequested))) {
                                    LOG.info("[{}] Parent execution is cancel-requested, skipping callback", workerId);
                                    shouldCallback = false;
                                }
                            }
                        } catch (Exception ex) {
                            LOG.warn("[{}] Failed to check parent cancel status: {}", workerId, ex.getMessage());
                        }
                    }

                    if (shouldCallback) {
                        enqueueAgentReturn(command, "CANCELLED", Map.of("reason", reason));
                        permissionTransferred = true;
                    }
                }
                context.emitState(AgentState.CANCELLED);

                boolean shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
                if (shouldEmitStreamEnd) {
                    if (!context.isStreamFinished()) {
                        context.emitChunk("", EventType.APP_STREAM_RESPONSE.getValue());
                    }
                }

                registry.markExecutionFinished(executionId, header.sessionId(), "CANCELLED");
                return;
            }

            LOG.error("[{}] Task failed: {}", workerId, e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            boolean permissionTransferred = false;
            if (hasSourceAgent) {
                enqueueAgentReturn(command, "FAILED", Map.of("error", e.getMessage() != null ? e.getMessage() : "Unknown error"));
                permissionTransferred = true;
            }
            context.emitState(AgentState.FAILED + ": " + e.getMessage());

            boolean shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                if (!context.isStreamFinished()) {
                    context.emitChunk("", EventType.APP_STREAM_RESPONSE.getValue());
                }
            }

            registry.markExecutionFinished(executionId, header.sessionId(), "FAILED");
            pluginRegistry.onTaskError(context, e);
        }
    }

    private GatewayCommand handleTaskGroupResume(GatewayCommand command, MessageHeader header, AgentContext context)
            throws Exception {
        GatewayCommand resumed = command;
        String taskGroupId = header.taskGroupId();
        if (taskGroupId != null && !taskGroupId.isBlank()) {
            String groupKey = Constants.RegistryKeys.taskGroup(taskGroupId);
            String totalStr = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL);
            if (totalStr != null) {
                String aborted = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_ABORTED);
                if (aborted != null && !aborted.isBlank()) {
                    LOG.warn("[{}] TaskGroup {} is aborted, discarding late reply from sub-task message_id={}",
                            workerId, taskGroupId, header.parentMessageId());
                    return null;
                }

                // Which Task Group contract this group was dispatched under. No stamp
                // means a pre-v2 dispatcher wrote it — possibly a worker still running
                // the old code mid-upgrade — so it must keep being joined the old way.
                String protocolVersion = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_PROTOCOL_VERSION);
                boolean isV2 = Constants.TASK_GROUP_PROTOCOL_V2.equals(protocolVersion);
                String resultsKey = Constants.RegistryKeys.taskGroupResults(taskGroupId);

                if (command instanceof ResumeCommand resumeCommand) {
                    Map<String, Object> resultData = new HashMap<>();
                    resultData.put("status", resumeCommand.status());
                    resultData.put("reply_data", resumeCommand.replyData());
                    resultData.put("content", resumeCommand.content());
                    // This reply flows FROM the sub-agent back TO the caller, so its
                    // sourceAgentType is the agent that produced the result.
                    resultData.put("target_agent_type", header.sourceAgentType());
                    resultData.put("metadata", resumeCommand.header().metadata());
                    resultData.put("extra_payload", resumeCommand.extraPayload());
                    // Under v2, parentMessageId is the sub-task's own dispatch id — unique
                    // per sibling. messageId is the caller's id, shared by every sibling,
                    // so keying by it lets them overwrite each other.
                    String resultField = isV2 ? header.parentMessageId() : header.messageId();
                    redisOps.hset(
                            resultsKey,
                            resultField,
                            objectMapper.writeValueAsString(resultData)
                    );
                    redisOps.expire(resultsKey, Constants.TASK_GROUP_TTL_SECONDS);
                }
                long completed = redisOps.hincrBy(groupKey, Constants.TASK_GROUP_FIELD_COMPLETED, 1);
                int total = Integer.parseInt(totalStr);
                if (completed < total) {
                    LOG.info("[{}] TaskGroup {} completed {}/{}, waiting...", workerId, taskGroupId, completed, total);
                    context.emitState(AgentState.QUEUED + ": waiting_for_group");
                    return null;
                }
                LOG.info("[{}] TaskGroup {} ALL COMPLETED ({})!", workerId, taskGroupId, total);

                if (isV2 && command instanceof ResumeCommand resumeCommand) {
                    List<Map<String, Object>> aggregated = aggregateTaskGroup(groupKey, resultsKey, taskGroupId, total);
                    // replyData is the single aggregation channel for a group resume;
                    // leaving content as whichever sibling replied last would give the
                    // caller two channels that disagree. ResumeCommand is a record, so
                    // the aggregate is delivered by rebuilding it.
                    resumed = ResumeCommand.of(
                            resumeCommand.header(),
                            "",
                            resumeCommand.status(),
                            aggregated,
                            new HashMap<>(resumeCommand.extraPayload())
                    );
                }
            }
        }
        context.emitState(AgentState.RESUMED);
        return resumed;
    }

    /**
     * Collect a completed Task Group's results in dispatch order.
     *
     * <p>Order comes from the group's {@code task_order} field, not from the Redis hash,
     * whose iteration order is unspecified — a caller fanning out to N agents needs to know
     * which result is which without matching by hand. Results present in Redis but absent
     * from {@code task_order} are appended rather than dropped, and a short result set is
     * logged loudly: silently returning fewer results than were dispatched is the failure
     * mode this SDK family rules out.
     */
    private List<Map<String, Object>> aggregateTaskGroup(String groupKey, String resultsKey, String taskGroupId,
            int total) {
        Map<String, String> rawResults = redisOps.hgetAll(resultsKey);
        if (rawResults == null) {
            rawResults = new HashMap<>();
        }
        List<String> order = new java.util.ArrayList<>();
        String rawOrder = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_TASK_ORDER);
        if (rawOrder != null && !rawOrder.isBlank()) {
            try {
                order = objectMapper.readValue(rawOrder, new TypeReference<List<String>>() { });
            } catch (Exception e) {
                LOG.error("[{}] TaskGroup {} has an unreadable {} field ({}); falling back to hash order",
                        workerId, taskGroupId, Constants.TASK_GROUP_FIELD_TASK_ORDER, rawOrder);
            }
        }

        List<Map<String, Object>> aggregated = new java.util.ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (String msgId : order) {
            String data = rawResults.get(msgId);
            if (data != null) {
                aggregated.add(toAggregateEntry(msgId, data));
                seen.add(msgId);
            }
        }
        for (Map.Entry<String, String> entry : rawResults.entrySet()) {
            if (!seen.contains(entry.getKey())) {
                aggregated.add(toAggregateEntry(entry.getKey(), entry.getValue()));
            }
        }

        if (aggregated.size() != total) {
            List<String> missing = new java.util.ArrayList<>();
            for (String msgId : order) {
                if (!rawResults.containsKey(msgId)) {
                    missing.add(msgId);
                }
            }
            LOG.error("[{}] TaskGroup {} aggregated {} result(s) but expected {}; missing sub-task "
                    + "message_ids={}. Resuming the caller with an incomplete result set.",
                    workerId, taskGroupId, aggregated.size(), total, missing.isEmpty() ? "unknown" : missing);
        }
        return aggregated;
    }

    private Map<String, Object> toAggregateEntry(String messageId, String rawData) {
        Map<String, Object> entry = new HashMap<>();
        entry.put("message_id", messageId);
        try {
            Map<String, Object> data = objectMapper.readValue(rawData, new TypeReference<Map<String, Object>>() { });
            entry.putAll(data);
            entry.put("message_id", messageId);
        } catch (Exception e) {
            LOG.error("[{}] Unreadable Task Group result for sub-task {}: {}", workerId, messageId, e.getMessage());
        }
        return entry;
    }

    /**
     * Deliver replies for Task Group sub-tasks that never reached a worker.
     *
     * <p>AgentContext.callAgents queues these instead of sending them inline: sending during
     * the dispatch loop would put a reply on the caller's control stream strictly before the
     * caller's processCommand returns, turning the pre-existing "a very fast sub-agent replies
     * before the caller suspends" race from unlikely into certain.
     *
     * <p>Failures are logged, never thrown: a Task Group that cannot be told about a dispatch
     * failure will time out, whereas throwing here would also destroy the caller's own result.
     */
    private void flushPendingGroupReplies(AgentContext context) {
        List<ResumeCommand> pending = context.drainPendingGroupReplies();
        for (ResumeCommand reply : pending) {
            try {
                Map<String, String> fields = new HashMap<>();
                fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(reply));
                streamOps.xadd(Constants.QueueNames.ctrlStream(reply.header().targetAgentType()),
                        fields, XAddOptions.noTrim());
            } catch (Exception e) {
                LOG.error("[{}] Failed to deliver Task Group {} dispatch-failure reply for sub-task {}: {}",
                        workerId, reply.header().taskGroupId(), reply.header().parentMessageId(), e.getMessage());
            }
        }
    }

    private void emitFinalAnswer(AgentContext context, AgentTaskResult taskResult) {
        String finalMessage = null;
        Object content = taskResult.content();
        if (content instanceof String s && !s.isEmpty()) {
            finalMessage = s;
        } else if (content instanceof List<?> l && !l.isEmpty()) {
            try {
                finalMessage = objectMapper.writeValueAsString(l);
            } catch (Exception e) {
                LOG.warn("Failed to serialize content for FINAL_ANSWER", e);
            }
        } else if (taskResult.replyData() instanceof String replyStr && !replyStr.isEmpty()) {
            finalMessage = replyStr;
        } else if (taskResult.replyData() != null) {
            try {
                finalMessage = objectMapper.writeValueAsString(taskResult.replyData());
            } catch (Exception e) {
                LOG.warn("Failed to serialize reply_data for FINAL_ANSWER", e);
            }
        }

        if (finalMessage != null) {
            context.emitChunk(finalMessage, EventType.FINAL_ANSWER.getValue());
        }
    }

    private void enqueueAgentReturn(GatewayCommand command, String status, Object replyData) {
        enqueueAgentReturn(command, new AgentTaskResult(status, "", replyData, Map.of(), Map.of()));
    }

    private void enqueueAgentReturn(GatewayCommand command, AgentTaskResult taskResult) {
        MessageHeader header = command.header();
        if (header.sourceAgentType() == null || header.sourceAgentType().isBlank()) {
            return;
        }
        Map<String, Object> mergedMetadata = new HashMap<>(header.metadata());
        mergedMetadata.putAll(taskResult.metadata());

        // messageId MUST be the caller's own message id (this dispatch's
        // parentMessageId). WorkerRunner reattaches the suspended caller execution
        // via getExecutionByMessageId(header.messageId()); a fresh id there makes
        // that lookup miss every time, so the reply mints a new execution and
        // orphans the one it was meant to continue. parentMessageId stays this
        // sub-task's own id — the only value unique per sibling, which is what
        // Group Join keys the result hash by under protocol v2.
        String callbackMessageId = header.parentMessageId() != null && !header.parentMessageId().isEmpty()
                ? header.parentMessageId()
                : "msg-" + UUID.randomUUID().toString().substring(0, 8);

        ResumeCommand callbackMsg = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId(callbackMessageId)
                        .sessionId(header.sessionId())
                        .traceId(header.traceId())
                        .sourceAgentType(header.targetAgentType() != null ? header.targetAgentType() : workerId)
                        .targetAgentType(header.sourceAgentType())
                        .parentMessageId(header.messageId())
                        .taskGroupId(header.taskGroupId() != null ? header.taskGroupId() : "")
                        .userCode(header.userCode())
                        .userName(header.userName())
                        .metadata(mergedMetadata)
                        .build(),
                taskResult.content(),
                taskResult.status(),
                taskResult.replyData(),
                new HashMap<>(taskResult.extraPayload())
        );

        try {
            Map<String, String> fields = new HashMap<>();
            fields.put("data", objectMapper.writeValueAsString(callbackMsg));
            streamOps.xadd(
                    Constants.QueueNames.ctrlStream(callbackMsg.header().targetAgentType()),
                    fields,
                    XAddOptions.noTrim()
            );
        } catch (Exception e) {
            LOG.error("Failed to enqueue callback: {}", e.getMessage());
        }
    }
}
