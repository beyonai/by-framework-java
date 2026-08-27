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

    /**
     * Handle one business message.
     *
     * @param existingExecution the execution record this message resolved to, or
     *        {@code null} when there is none. A resumed execution's caller and its
     *        original dispatch metadata live here and nowhere else: the waking
     *        ResumeCommand's own header describes the hop that just finished, not
     *        the dispatch being answered. Mirrors Python's
     *        {@code RunningExecution.existing_data}.
     * @param isResumedExecution whether this execution has already been through a
     *        worker — a ResumeCommand, or a record whose status is past QUEUED.
     */
    public void handleMessage(GatewayCommand command, String executionId,
            Map<String, Object> existingExecution, boolean isResumedExecution) {
        if (command instanceof CancelTaskCommand cancelCmd) {
            onCancelTask(cancelCmd);
            return;
        }

        MessageHeader header = command.header();
        String traceId = header.traceId();
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }

        // Give a resumed handler its own dispatch metadata back. The mirror image
        // of resolveReplyCommand, and deliberately not the same rule: this agent
        // IS the addressee of the waking message, so that message's metadata is
        // real payload here rather than someone else's plumbing. Original
        // dispatch metadata is the base, the waking message wins collisions.
        //
        // Builds a NEW command — never mutates. resolveReplyCommand below reads
        // the raw one, so rewriting this header in place would leak the inbound
        // merge into the reply that goes out.
        GatewayCommand inboundCommand = restoreInboundMetadata(command, existingExecution);

        AgentContext context = new AgentContext(
                header.sessionId(),
                traceId,
                redisClient,
                header.targetAgentType(),
                header.messageId()
        );

        boolean isResume = command instanceof ResumeCommand;
        // NOT `sourceAgentType != null && !isResume`: a resume's header names the
        // sub-agent that just finished, so that predicate both denied every
        // resumed execution a reply AND would have addressed one to the callee.
        // resolveReplyCommand rebuilds the ORIGINAL dispatch header from the
        // execution record instead; the whole reply — routing included — must be
        // driven off that command, not off `command`.
        GatewayCommand replyCommand = resolveReplyCommand(command, existingExecution);
        boolean hasSourceAgent = replyCommand != null;
        String sourceAgentType = hasSourceAgent ? replyCommand.header().sourceAgentType() : "";

        LOG.info("[{}] Processing message: {} (exec: {})", workerId, header.messageId(), executionId);

        try {
            pluginRegistry.onTaskStart(context);

            if (isResume) {
                if (!handleTaskGroupResume(command, header, context)) {
                    return;
                }
            }

            // inboundCommand, not command: the handler reads what this execution
            // was originally dispatched with, merged under the waking message's
            // own metadata.
            Object result = processCommand(inboundCommand, context);
            AgentTaskResult taskResult = AgentTaskResult.normalize(result);
            String finalStatus = taskResult.status();

            // A suspended execution has no result yet — only the value the handler
            // returned so it could unwind. Forwarding that wakes the caller early
            // with a placeholder AND burns the single reply it was parked on; the
            // real result goes out when this execution resumes and finishes.
            //
            // The terminal-status exception is load-bearing: a handler that
            // reached COMPLETED/FAILED/CANCELLED after dispatching is finished and
            // will never be resumed to reply later, so it owes its caller a reply
            // NOW. Without it such an execution would record itself done and stay
            // silent, suspending its caller until a sweep bails it out.
            boolean isSuspended = context.isSuspended() && !AgentState.isTerminalState(finalStatus);

            boolean permissionTransferred = false;
            if (hasSourceAgent && !isSuspended) {
                permissionTransferred = true;
                enqueueAgentReturn(replyCommand, taskResult);
                context.emitState(AgentState.QUEUED + ": " + sourceAgentType);
            } else if (!hasSourceAgent) {
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
                        enqueueAgentReturn(replyCommand, "CANCELLED", Map.of("reason", reason));
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
                enqueueAgentReturn(replyCommand, "FAILED", Map.of("error", e.getMessage() != null ? e.getMessage() : "Unknown error"));
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
                    Map<String, Object> resultData = buildResultData(resumeCommand);
                    // Keyed by the SUB-TASK's own message id, not by header.messageId().
                    // A reply's header.messageId is the CALLER's id (the direction is
                    // reversed on the way back), so keying by it makes every sibling in
                    // a group write the same field and overwrite each other — the group
                    // then joins on one member's result repeated N times.
                    // header.parentMessageId is the sub-task's dispatch-time id, which
                    // is what Python and TS both key by.
                    redisOps.hset(
                            resultsKey,
                            header.parentMessageId(),
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

    /**
     * Return the command whose caller this execution owes a reply to, or
     * {@code null} when nobody is waiting.
     *
     * For a fresh dispatch the answer is the command itself: its header's
     * sourceAgentType is the caller.
     *
     * For a RESUME it is not. The ResumeCommand that woke us describes the hop
     * that finished — its sourceAgentType is our SUB-agent and its
     * parentMessageId is our sub-task — so replying against that header sends
     * our result back down to the sub-agent we just called. The caller is
     * whatever the ORIGINAL dispatch recorded in the execution registry, which
     * is why this rebuilds the dispatch header from the execution record.
     *
     * Two ways to have no caller, and both must be handled as "no caller"
     * rather than as an error:
     *   - a MISSING source_agent_type. Java's execution records historically do
     *     not write the field at all, so absence is the common case here, not a
     *     corruption signal.
     *   - the CLIENT_SOURCE_AGENT_TYPE sentinel, which is a marker and not an
     *     agent type. Treating it as a caller posts the result to a control
     *     stream nobody consumes AND suppresses the end-of-stream event the
     *     front end is waiting on.
     *
     * Mirrors Python worker.py's _resolve_reply_command.
     */
    static GatewayCommand resolveReplyCommand(GatewayCommand command, Map<String, Object> existingExecution) {
        MessageHeader header = command.header();
        if (!(command instanceof ResumeCommand)) {
            return header.sourceAgentType() != null && !header.sourceAgentType().isBlank() ? command : null;
        }

        Map<String, Object> snapshot = existingExecution != null ? existingExecution : Map.of();
        Object rawCaller = snapshot.get("source_agent_type");
        String callerAgentType = rawCaller != null ? String.valueOf(rawCaller) : "";
        if (callerAgentType.isBlank() || Constants.CLIENT_SOURCE_AGENT_TYPE.equals(callerAgentType)) {
            return null;
        }

        Object rawParent = snapshot.get("parent_message_id");
        Object rawGroup = snapshot.get("task_group_id");
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId(header.messageId())
                        .sessionId(header.sessionId())
                        .traceId(header.traceId())
                        .sourceAgentType(callerAgentType)
                        .targetAgentType(header.targetAgentType())
                        .parentMessageId(rawParent != null ? String.valueOf(rawParent) : "")
                        .taskGroupId(rawGroup != null ? String.valueOf(rawGroup) : "")
                        .userCode(header.userCode())
                        .userName(header.userName())
                        // REPLACEMENT, not a merge with header.metadata: that
                        // metadata belongs to whatever woke this execution up (an
                        // askUser answer, or a sub-call's reply), not to the
                        // original caller. Leaking it would let a transient hop
                        // overwrite the caller's own data instead of being layered
                        // under taskResult.metadata, which enqueueAgentReturn's
                        // merge already does correctly. A record with no stored
                        // metadata degrades to empty — never to the waking hop's.
                        .metadata(ResumeMetadata.storedMetadata(existingExecution))
                        .build(),
                ((ResumeCommand) command).content(),
                ((ResumeCommand) command).status(),
                ((ResumeCommand) command).replyData(),
                new HashMap<>(((ResumeCommand) command).extraPayload()));
    }

    /**
     * Give a resumed handler its own dispatch metadata back.
     *
     * <p>The mirror image of {@link #resolveReplyCommand}, and deliberately not
     * the same rule. That one rebuilds the header this execution <i>sends</i>;
     * this one rebuilds the header it <i>reads</i>. A resumed handler otherwise
     * sees only the metadata of whatever woke it up — an askUser answer's, or a
     * sub-call's reply — and everything the execution was originally dispatched
     * with is gone from the moment it first suspends.
     *
     * <p>Merged, not replaced (the opposite of the outbound direction): this
     * agent IS the addressee of the waking message, so its metadata is real
     * payload here rather than someone else's plumbing. See {@link ResumeMetadata}
     * for why the framework's per-hop trace keys are excluded from the base.
     *
     * <p>Returns a NEW command — never mutates. The caller keeps the raw one for
     * {@link #resolveReplyCommand}, so mutating here would leak the inbound merge
     * into the reply that goes out.
     *
     * <p>Mirrors Python worker.py's _restore_inbound_metadata.
     */
    static GatewayCommand restoreInboundMetadata(
            GatewayCommand command, Map<String, Object> existingExecution) {
        if (!(command instanceof ResumeCommand resume)) {
            return command;
        }
        MessageHeader header = command.header();
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId(header.messageId())
                        .sessionId(header.sessionId())
                        .traceId(header.traceId())
                        .sourceAgentType(header.sourceAgentType())
                        .targetAgentType(header.targetAgentType())
                        .parentMessageId(header.parentMessageId())
                        .taskGroupId(header.taskGroupId())
                        .userCode(header.userCode())
                        .userName(header.userName())
                        .metadata(ResumeMetadata.mergeResumeMetadata(
                                ResumeMetadata.storedMetadata(existingExecution),
                                header.metadata()))
                        .traceParentSpanId(header.traceParentSpanId())
                        .langfuseParentObservationId(header.langfuseParentObservationId())
                        .build(),
                resume.content(),
                resume.status(),
                resume.replyData(),
                new HashMap<>(resume.extraPayload()));
    }

    /**
     * The stored shape of one sub-task's result.
     *
     * <p>Kept isomorphic with what the single-call path persists, because the
     * same readers consume both — a recovery that can tell the two apart is a
     * recovery with two code paths.
     */
    private Map<String, Object> buildResultData(ResumeCommand reply) {
        Map<String, Object> resultData = new HashMap<>();
        resultData.put("status", reply.status());
        resultData.put("reply_data", reply.replyData());
        resultData.put("content", reply.content());
        // The sub-agent that produced this result: on a reply that is the
        // header's sourceAgentType, since the direction reverses on the way back.
        resultData.put("target_agent_type", reply.header().sourceAgentType());
        resultData.put("metadata", reply.header().metadata());
        resultData.put("extra_payload", reply.extraPayload());
        return resultData;
    }

    /**
     * Persist a single (non-group) call_agent result before replying.
     *
     * <p>A lost reply message used to lose the answer with it. A Task Group
     * already keeps full results; a single call reuses that exact storage as a
     * group of one (see TASK_GROUP_SINGLE_ID_PREFIX), which keeps recovery on one
     * code path. The reply message then carries nothing that is not recoverable
     * from Redis.
     *
     * <p>Fail-soft: losing the copy only costs recoverability, so an error here
     * must never stop the reply from going out.
     */
    private void persistSingleCallResult(MessageHeader dispatchHeader, ResumeCommand reply) {
        if (dispatchHeader.taskGroupId() != null && !dispatchHeader.taskGroupId().isBlank()) {
            return; // A real Task Group: the join path already stores it.
        }
        String childMessageId = dispatchHeader.messageId();
        if (childMessageId == null || childMessageId.isBlank()) {
            return;
        }
        try {
            String resultsKey = Constants.RegistryKeys.taskGroupResults(
                    Constants.TASK_GROUP_SINGLE_ID_PREFIX + childMessageId);
            redisOps.hset(resultsKey, childMessageId,
                    objectMapper.writeValueAsString(buildResultData(reply)));
            redisOps.expire(resultsKey, Constants.TASK_GROUP_TTL_SECONDS);
        } catch (Exception e) {
            LOG.warn("[{}] Failed to persist single-call result for message_id={}: {}",
                    workerId, childMessageId, e.getMessage());
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
                        // The caller reattaches its suspended execution by this
                        // id, so it must be the caller's OWN message id — this
                        // dispatch's parentMessageId. A freshly minted id resolves
                        // to no execution and orphans the caller.
                        .messageId(header.parentMessageId() != null && !header.parentMessageId().isBlank()
                                ? header.parentMessageId()
                                : Constants.MESSAGE_ID_PREFIX + UUID.randomUUID().toString().substring(0, 8))
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

        // Store the answer BEFORE sending it. If the reply message is lost in
        // transit, a sweep can still recover the result from Redis rather than
        // synthesising a fabricated failure for a sub-task that actually
        // succeeded.
        persistSingleCallResult(header, callbackMsg);

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
