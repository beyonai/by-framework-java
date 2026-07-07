package com.iwhaleai.byai.framework.worker;

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
                if (!handleTaskGroupResume(command, header, context)) {
                    return;
                }
            }

            Object result = processCommand(command, context);
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

    private boolean handleTaskGroupResume(GatewayCommand command, MessageHeader header, AgentContext context) throws Exception {
        String taskGroupId = header.taskGroupId();
        if (taskGroupId != null && !taskGroupId.isBlank()) {
            String groupKey = Constants.RegistryKeys.taskGroup(taskGroupId);
            String totalStr = redisOps.hget(groupKey, "total");
            if (totalStr != null) {
                if (command instanceof ResumeCommand resumeCommand) {
                    String resultsKey = Constants.RegistryKeys.taskGroupResults(taskGroupId);
                    Map<String, Object> resultData = new HashMap<>();
                    resultData.put("status", resumeCommand.status());
                    resultData.put("reply_data", resumeCommand.replyData());
                    resultData.put("content", resumeCommand.content());
                    resultData.put("metadata", resumeCommand.header().metadata());
                    resultData.put("extra_payload", resumeCommand.extraPayload());
                    redisOps.hset(
                            resultsKey,
                            header.messageId(),
                            objectMapper.writeValueAsString(resultData)
                    );
                    redisOps.expire(resultsKey, Constants.TASK_GROUP_TTL_SECONDS);
                }
                long completed = redisOps.hincrBy(groupKey, "completed", 1);
                int total = Integer.parseInt(totalStr);
                if (completed < total) {
                    LOG.info("[{}] TaskGroup {} completed {}/{}, waiting...", workerId, taskGroupId, completed, total);
                    context.emitState(AgentState.QUEUED + ": waiting_for_group");
                    return false;
                }
                LOG.info("[{}] TaskGroup {} ALL COMPLETED ({})!", workerId, taskGroupId, total);
            }
        }
        context.emitState(AgentState.RESUMED);
        return true;
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

        ResumeCommand callbackMsg = ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-" + UUID.randomUUID().toString().substring(0, 8))
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
