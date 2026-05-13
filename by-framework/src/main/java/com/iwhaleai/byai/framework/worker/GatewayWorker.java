package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
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
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class GatewayWorker {
    private static final Logger LOG = LoggerFactory.getLogger(GatewayWorker.class);

    protected final String workerId;
    protected final RedisClient redisClient;
    protected final WorkerRegistry registry;
    protected final PluginRegistry pluginRegistry;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    private ScheduledExecutorService heartbeatExecutor;

    public GatewayWorker(String workerId) {
        this(workerId, RedisClient.getInstance());
    }

    public GatewayWorker(String workerId, RedisClient redisClient) {
        this(workerId, redisClient, new WorkerRegistry(redisClient));
    }

    protected GatewayWorker(String workerId, RedisClient redisClient, WorkerRegistry registry) {
        this.workerId = workerId;
        this.redisClient = redisClient;
        this.registry = registry;
        this.pluginRegistry = new PluginRegistry();
    }

    public PluginRegistry getPluginRegistry() {
        return pluginRegistry;
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

        registry.heartbeatWorker(workerId, leaseTtl);
        pluginRegistry.onWorkerStartup(this);

        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "heartbeat-" + workerId);
            t.setDaemon(true);
            return t;
        });

        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                registry.heartbeatWorker(workerId, leaseTtl);
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
                String taskGroupId = header.taskGroupId();
                if (taskGroupId != null && !taskGroupId.isBlank()) {
                    try (Jedis jedis = redisClient.getResource()) {
                        String groupKey = Constants.RegistryKeys.taskGroup(taskGroupId);
                        String totalStr = jedis.hget(groupKey, "total");
                        if (totalStr != null) {
                            if (command instanceof ResumeCommand resumeCommand) {
                                String resultsKey = Constants.RegistryKeys.taskGroupResults(taskGroupId);
                                Map<String, Object> resultData = new HashMap<>();
                                resultData.put("status", resumeCommand.status());
                                resultData.put("reply_data", resumeCommand.replyData());
                                resultData.put("content", resumeCommand.content());
                                resultData.put("metadata", resumeCommand.header().metadata());
                                resultData.put("extra_payload", resumeCommand.extraPayload());
                                jedis.hset(
                                        resultsKey,
                                        header.messageId(),
                                        objectMapper.writeValueAsString(resultData)
                                );
                                jedis.expire(resultsKey, Constants.TASK_GROUP_TTL_SECONDS);
                            }
                            long completed = jedis.hincrBy(groupKey, "completed", 1);
                            int total = Integer.parseInt(totalStr);
                            if (completed < total) {
                                LOG.info("[{}] TaskGroup {} completed {}/{}, waiting...", workerId, taskGroupId, completed, total);
                                context.emitState(AgentState.QUEUED + ": waiting_for_group");
                                return;
                            }
                            LOG.info("[{}] TaskGroup {} ALL COMPLETED ({})!", workerId, taskGroupId, total);
                        }
                    }
                }
                context.emitState(AgentState.RESUMED);
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

            // Extract final message and emit FINAL_ANSWER
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

        try (Jedis jedis = redisClient.getResource()) {
            Map<String, String> fields = new HashMap<>();
            fields.put("data", objectMapper.writeValueAsString(callbackMsg));
            jedis.xadd(
                    Constants.QueueNames.ctrlStream(callbackMsg.header().targetAgentType()),
                    (redis.clients.jedis.StreamEntryID) null,
                    fields
            );
        } catch (Exception e) {
            LOG.error("Failed to enqueue callback: {}", e.getMessage());
        }
    }
}
