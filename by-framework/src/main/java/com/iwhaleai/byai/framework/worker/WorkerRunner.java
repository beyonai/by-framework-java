package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.EvictWorkerCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommandFactory;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeWorkerCommand;
import com.iwhaleai.byai.framework.core.protocol.SuspendWorkerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.resps.StreamEntry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.UUID;

/**
 * Worker runner responsible for listening to Redis Stream and dispatching tasks
 * to GatewayWorker.
 * Uses virtual threads for high-throughput IO-bound task processing.
 */
public class WorkerRunner {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerRunner.class);

    private final GatewayWorker worker;
    private final RedisClient redisClient;
    private final RedisStreamOps streamOps;
    private final String groupName;
    private final String consumerName;
    private final ExecutorService taskExecutor;
    private final ConcurrentHashMap<String, Thread> activeExecutions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> messageToExecution = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean evictionHandled = new AtomicBoolean(false);
    private String lockToken;

    // Round-robin cursor for the agent_type phase-two blocking primary.
    // Only ever touched by the single agent_type loop thread.
    private int primaryCursor = 0;

    // Admin lifecycle state machine
    private volatile String adminLifecycle = "active";
    private volatile boolean evictForce = false;
    private final Set<String> deniedAgentTypes = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Consumer liveness tracking for health check
    private static final long CONSUMER_HEALTH_TIMEOUT_MS = 30_000L;
    private volatile long lastConsumerTick = 0L;

    public WorkerRunner(GatewayWorker worker) {
        this(worker, RedisClient.getInstance(), null);
    }

    public WorkerRunner(GatewayWorker worker, RedisClient redisClient, String groupName) {
        this.worker = worker;
        this.redisClient = redisClient;
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.groupName = groupName != null ? groupName : autoGroupName();
        this.consumerName = worker.workerId;
        // Use virtual threads for IO-bound task processing (Java 21+)
        this.taskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    private String autoGroupName() {
        List<String> agentTypes = new ArrayList<>(worker.getAgentTypes());
        Collections.sort(agentTypes);
        String payload = String.join(",", agentTypes);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-1");
            byte[] hash = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < Math.min(hash.length, Constants.GROUP_HASH_LENGTH); i++) {
                sb.append(String.format("%02x", hash[i]));
            }
            return Constants.ConsumerGroups.AGENT_ENGINES + ":" + sb;
        } catch (Exception e) {
            return Constants.ConsumerGroups.AGENT_ENGINES + ":default";
        }
    }

    private void setupStreams() {
        for (String agentType : worker.getAgentTypes()) {
            streamOps.xgroupCreateIfNotExists(Constants.QueueNames.ctrlStream(agentType), groupName);
        }
        streamOps.xgroupCreateIfNotExists(Constants.QueueNames.workerCtrlStream(worker.workerId), groupName);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("WorkerRunner already started");
        }

        setupStreams();

        this.lockToken = worker.registry.claimWorkerId(worker.workerId);

        // Wire lifecycle callback to update runner state from heartbeat thread
        worker.setLifecycleCallback(lifecycle -> {
            adminLifecycle = lifecycle;
            if ("evicted".equals(lifecycle)) {
                LOG.info("[{}] Eviction signaled via lifecycle callback", worker.workerId);
            }
        });

        // Wire denylist refresh callback
        worker.setDenylistRefresh(denied -> {
            deniedAgentTypes.clear();
            deniedAgentTypes.addAll(denied);
        });

        // Wire health check: if consumer tick hasn't updated within the timeout, the loop is stalled
        worker.setHealthCheck(() -> {
            if (lastConsumerTick == 0L) return true; // not yet started
            return (System.currentTimeMillis() - lastConsumerTick) < CONSUMER_HEALTH_TIMEOUT_MS;
        });
        worker.setOnUnhealthy(() -> {
            LOG.error("[{}] Consumer loop unhealthy; stopping runner", worker.workerId);
            running.set(false);
        });

        // startHeartbeat checks admin state first; if not active, it skips registerWorkerMembership
        // and calls the lifecycle callback with the startup lifecycle before the loop starts.
        worker.startHeartbeat();

        // Only register membership if active (startHeartbeat may have set adminLifecycle already)
        if ("active".equals(adminLifecycle)) {
            worker.registry.registerWorkerMembership(worker.workerId, worker.getAgentTypes());
        }

        LOG.info("[{}] Runner started, waiting for tasks...", worker.workerId);

        new Thread(this::runAgentTypeLoop, "runner-loop-" + worker.workerId).start();
        new Thread(this::runWorkerCtrlLoop, "runner-worker-ctrl-loop-" + worker.workerId).start();
    }

    /**
     * Reads every non-denied agent_type ctrl stream, combined into one
     * xreadGroup() call. Combining multiple agent_type streams together is
     * itself CROSSSLOT-prone under Cluster (each agent_type is its own,
     * untagged entity) - that redesign is the main Phase 4 issue's two-phase
     * scan+block plan, not this prefactor. This loop is only responsible for
     * no longer also pulling workerCtrlStream(workerId) into the same call.
     */
    private void runAgentTypeLoop() {
        while (running.get()) {
            try {
                lastConsumerTick = System.currentTimeMillis();

                // Lifecycle checks
                if ("suspended".equals(adminLifecycle)) {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                    continue;
                }
                if ("evicted".equals(adminLifecycle)) {
                    LOG.info("[{}] Eviction requested; stopping agent_type loop", worker.workerId);
                    break;
                }

                List<String> agentTypes = new ArrayList<>();
                for (String agentType : worker.getAgentTypes()) {
                    if (!deniedAgentTypes.contains(agentType)) {
                        agentTypes.add(agentType);
                    }
                }

                if (agentTypes.isEmpty()) {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                    continue;
                }

                List<Map.Entry<String, List<StreamEntry>>> results = fetchAgentTypeMessages(agentTypes);
                if (results != null) {
                    for (Map.Entry<String, List<StreamEntry>> entry : results) {
                        String streamName = entry.getKey();
                        for (StreamEntry streamEntry : entry.getValue()) {
                            processMessageAsync(streamName, streamEntry);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Agent_type loop interrupted, shutting down");
                break;
            } catch (Exception e) {
                LOG.error("Error in agent_type loop: {}", e.getMessage());
                try {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        handlePostEvictionShutdownIfNeeded();
    }

    /**
     * Two-phase, Cluster-safe read across every active agent_type stream.
     * Each agent_type is its own untagged entity/slot, so they can never be
     * combined into one xreadGroup() call.
     *
     * <p>Phase one: a concurrent, non-blocking single-stream read per active
     * agent_type. If any stream returned messages, return immediately -
     * this is what keeps the common case fast without ever blocking.
     *
     * <p>Phase two: only when every stream came back empty, one real
     * blocking read against a single "primary" stream, chosen by
     * round-robin across the declared agent_types so no agent_type is
     * permanently starved of the blocking slot. A message that arrives on
     * a non-primary stream mid-block waits at most until this block ends
     * (bounded by REDIS_BLOCK_TIMEOUT_MS), then gets picked up by the next
     * iteration's phase-one scan.
     */
    private List<Map.Entry<String, List<StreamEntry>>> fetchAgentTypeMessages(List<String> agentTypes)
            throws InterruptedException {
        List<String> streamNames = new ArrayList<>();
        for (String agentType : agentTypes) {
            streamNames.add(Constants.QueueNames.ctrlStream(agentType));
        }

        List<Future<List<Map.Entry<String, List<StreamEntry>>>>> futures = new ArrayList<>();
        for (String streamName : streamNames) {
            futures.add(taskExecutor.submit(() -> streamOps.xreadGroup(
                    streamName, groupName, consumerName, Constants.REDIS_READ_BATCH_SIZE, null)));
        }

        List<Map.Entry<String, List<StreamEntry>>> combined = new ArrayList<>();
        for (Future<List<Map.Entry<String, List<StreamEntry>>>> future : futures) {
            try {
                List<Map.Entry<String, List<StreamEntry>>> result = future.get();
                if (result != null) {
                    combined.addAll(result);
                }
            } catch (ExecutionException e) {
                LOG.error("Error in agent_type phase-one scan: {}",
                        e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            }
        }
        if (!combined.isEmpty()) {
            return combined;
        }

        String primary = streamNames.get(Math.floorMod(primaryCursor, streamNames.size()));
        primaryCursor++;
        return streamOps.xreadGroup(
                primary, groupName, consumerName, Constants.REDIS_READ_BATCH_SIZE, Constants.REDIS_BLOCK_TIMEOUT_MS);
    }

    /**
     * Reads only workerCtrlStream(workerId) (direct routing: lifecycle
     * commands, cancel requests targeted at this worker). Split out of the
     * combined loop since it belongs to the "worker" hash-tag group, a
     * different entity than the agent_type streams above - the two must
     * never share one xreadGroup() call under Cluster.
     */
    private void runWorkerCtrlLoop() {
        String workerCtrlStream = Constants.QueueNames.workerCtrlStream(worker.workerId);
        while (running.get()) {
            try {
                lastConsumerTick = System.currentTimeMillis();

                if ("suspended".equals(adminLifecycle)) {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                    continue;
                }
                if ("evicted".equals(adminLifecycle)) {
                    LOG.info("[{}] Eviction requested; stopping worker-ctrl loop", worker.workerId);
                    break;
                }

                List<Map.Entry<String, List<StreamEntry>>> results = streamOps.xreadGroup(
                        workerCtrlStream, groupName, consumerName,
                        Constants.REDIS_READ_BATCH_SIZE, Constants.REDIS_BLOCK_TIMEOUT_MS);

                if (results != null) {
                    for (Map.Entry<String, List<StreamEntry>> entry : results) {
                        String streamName = entry.getKey();
                        for (StreamEntry streamEntry : entry.getValue()) {
                            processMessageAsync(streamName, streamEntry);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Worker-ctrl loop interrupted, shutting down");
                break;
            } catch (Exception e) {
                LOG.error("Error in worker-ctrl loop: {}", e.getMessage());
                try {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        handlePostEvictionShutdownIfNeeded();
    }

    /** Runs once regardless of which loop observes the eviction first. */
    private void handlePostEvictionShutdownIfNeeded() {
        if (!"evicted".equals(adminLifecycle) || !evictionHandled.compareAndSet(false, true)) {
            return;
        }
        LOG.info("[{}] Worker evicted (force={}), initiating shutdown", worker.workerId, evictForce);
        if (evictForce) {
            // Force: interrupt all active executions immediately
            for (Thread t : activeExecutions.values()) {
                t.interrupt();
            }
        }
        // Signal the runner to stop (stop() does the actual cleanup)
        stop();
    }

    private void processMessageAsync(String streamName, StreamEntry entry) {
        taskExecutor.submit(() -> {
            try {
                String dataStr = entry.getFields().get(Constants.RedisFields.DATA);
                if (dataStr == null) {
                    return;
                }

                GatewayCommand command = GatewayCommandFactory.fromJson(dataStr);
                MessageHeader header = command.header();

                // 1. Handle Control Commands (e.g. CancelTaskCommand, lifecycle commands)
                if (command instanceof CancelTaskCommand cancelCmd) {
                    handleControlCommand(streamName, entry.getID(), cancelCmd);
                    return;
                }
                if (command instanceof SuspendWorkerCommand suspendCmd) {
                    handleSuspendWorker(streamName, entry.getID(), suspendCmd);
                    return;
                }
                if (command instanceof ResumeWorkerCommand resumeCmd) {
                    handleResumeWorker(streamName, entry.getID(), resumeCmd);
                    return;
                }
                if (command instanceof EvictWorkerCommand evictCmd) {
                    handleEvictWorker(streamName, entry.getID(), evictCmd);
                    return;
                }

                // 2. Business Replay protection (Idempotency)
                Map<String, Object> existing = worker.registry.getExecutionByMessageId(header.messageId(),
                        header.sessionId());
                if (existing != null) {
                    String status = String.valueOf(existing.get(Constants.ExecutionFields.STATUS));
                    if (Constants.TERMINAL_STATES.contains(status)) {
                        LOG.info("[{}] Skipping terminal replay: {} -> {}", worker.workerId, header.messageId(),
                                status);
                        streamOps.xack(streamName, groupName, entry.getID());
                        return;
                    }
                }

                // 3. Generate/Reuse Execution ID
                String executionId = existing != null
                        ? String.valueOf(existing.get(Constants.ExecutionFields.EXECUTION_ID))
                        : "exec-" + UUID.randomUUID().toString().substring(0, Constants.ID_SHORT_SUFFIX_LENGTH);

                // 4. Trace current thread
                activeExecutions.put(executionId, Thread.currentThread());
                messageToExecution.put(header.messageId(), executionId);

                try {
                    // 5. Update execution status to RUNNING with worker_id
                    if (existing != null) {
                        // Execution was pre-initialized (e.g. by client), update to RUNNING
                        worker.registry.updateExecutionStatus(executionId, header.sessionId(),
                                AgentState.RUNNING, Map.of(Constants.ExecutionFields.WORKER_ID, worker.workerId));
                    } else {
                        // No pre-existing execution, create a new one
                        Map<String, Object> execution = new HashMap<>();
                        execution.put(Constants.ExecutionFields.EXECUTION_ID, executionId);
                        execution.put(Constants.ExecutionFields.MESSAGE_ID, header.messageId());
                        execution.put(Constants.ExecutionFields.SESSION_ID, header.sessionId());
                        execution.put(Constants.ExecutionFields.WORKER_ID, worker.workerId);
                        execution.put(Constants.ExecutionFields.TARGET_AGENT_TYPE, header.targetAgentType());
                        execution.put(Constants.ExecutionFields.STATUS, AgentState.RUNNING);
                        execution.put(Constants.ExecutionFields.CREATED_AT, System.currentTimeMillis());
                        if (header.parentMessageId() != null && !header.parentMessageId().isEmpty()) {
                            execution.put("parent_message_id", header.parentMessageId());
                        }
                        worker.registry.saveExecution(execution);
                    }

                    // 6. Handle message
                    worker.handleMessage(command, executionId);

                    // 7. ACK
                    streamOps.xack(streamName, groupName, entry.getID());
                } finally {
                    activeExecutions.remove(executionId);
                    messageToExecution.remove(header.messageId());
                }
            } catch (Exception e) {
                LOG.error("Failed to process/ack message {}: {}", entry.getID(), e.getMessage());
            }
        });
    }

    private void handleControlCommand(String streamName, redis.clients.jedis.StreamEntryID streamId,
            CancelTaskCommand command) {
        try {
            String targetMsgId = command.body().targetMessageId();
            String targetExecId = command.body().targetExecutionId();
            String reason = command.body().reason();

            if (targetExecId == null || targetExecId.isBlank()) {
                targetExecId = messageToExecution.get(targetMsgId);
            }

            // Acknowledge the control command immediately
            streamOps.xack(streamName, groupName, streamId);

            if (targetExecId != null) {
                worker.registry.markExecutionCancelling(targetExecId, command.header().sessionId(), reason);
                Thread targetThread = activeExecutions.get(targetExecId);
                if (targetThread != null) {
                    LOG.info("[{}] Interrupting task: {} (message: {})", worker.workerId, targetExecId, targetMsgId);
                    worker.onCancelTask(command);
                    targetThread.interrupt();
                }
            }
        } catch (Exception e) {
            LOG.error("Error handling control command: {}", e.getMessage());
        }
    }

    private void handleSuspendWorker(String streamName, redis.clients.jedis.StreamEntryID streamId,
            SuspendWorkerCommand command) {
        try {
            LOG.info("[{}] Worker suspended by admin: {}", worker.workerId,
                    command.body() != null ? command.body().reason() : "");
            adminLifecycle = "suspended";
            streamOps.xack(streamName, groupName, streamId);
        } catch (Exception e) {
            LOG.error("[{}] Error handling SuspendWorkerCommand: {}", worker.workerId, e.getMessage());
        }
    }

    private void handleResumeWorker(String streamName, redis.clients.jedis.StreamEntryID streamId,
            ResumeWorkerCommand command) {
        try {
            LOG.info("[{}] Worker resumed by admin", worker.workerId);
            adminLifecycle = "active";
            streamOps.xack(streamName, groupName, streamId);
        } catch (Exception e) {
            LOG.error("[{}] Error handling ResumeWorkerCommand: {}", worker.workerId, e.getMessage());
        }
    }

    private void handleEvictWorker(String streamName, redis.clients.jedis.StreamEntryID streamId,
            EvictWorkerCommand command) {
        try {
            boolean force = command.body() != null && command.body().force();
            String reason = command.body() != null ? command.body().reason() : "";
            LOG.info("[{}] Worker evicted by admin (force={}, reason={})", worker.workerId, force, reason);
            evictForce = force;
            adminLifecycle = "evicted";
            streamOps.xack(streamName, groupName, streamId);
        } catch (Exception e) {
            LOG.error("[{}] Error handling EvictWorkerCommand: {}", worker.workerId, e.getMessage());
        }
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return; // Already stopped
        }

        worker.stopHeartbeat();

        boolean releasedWorkerId = false;
        try {
            if (lockToken != null) {
                releasedWorkerId = worker.registry.releaseWorkerId(worker.workerId, lockToken);
                lockToken = null;
            } else {
                releasedWorkerId = worker.registry.markWorkerInactive(worker.workerId);
            }
        } catch (Exception e) {
            LOG.warn("[{}] Failed to release worker presence: {}", worker.workerId, e.getMessage());
        }

        if (releasedWorkerId) {
            try {
                worker.registry.unregisterWorkerMembership(worker.workerId);
            } catch (Exception e) {
                LOG.warn("[{}] Failed to unregister worker membership: {}", worker.workerId, e.getMessage());
            }
        }

        taskExecutor.shutdown();
        try {
            if (!taskExecutor.awaitTermination(Constants.TERMINATION_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                taskExecutor.shutdownNow();
                if (!taskExecutor.awaitTermination(Constants.TERMINATION_GRACE_PERIOD_SEC, TimeUnit.SECONDS)) {
                    LOG.warn("Task executor did not terminate gracefully");
                }
            }
        } catch (InterruptedException e) {
            taskExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
