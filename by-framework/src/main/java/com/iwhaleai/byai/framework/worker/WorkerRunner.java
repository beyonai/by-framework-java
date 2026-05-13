package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommandFactory;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private final String groupName;
    private final String consumerName;
    private final ExecutorService taskExecutor;
    private final ConcurrentHashMap<String, Thread> activeExecutions = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> messageToExecution = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String lockToken;

    public WorkerRunner(GatewayWorker worker) {
        this(worker, RedisClient.getInstance(), null);
    }

    public WorkerRunner(GatewayWorker worker, RedisClient redisClient, String groupName) {
        this.worker = worker;
        this.redisClient = redisClient;
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
        try (Jedis jedis = redisClient.getResource()) {
            for (String agentType : worker.getAgentTypes()) {
                setupStreamGroup(jedis, Constants.QueueNames.ctrlStream(agentType));
            }
            setupStreamGroup(jedis, Constants.QueueNames.workerCtrlStream(worker.workerId));
        }
    }

    private void setupStreamGroup(Jedis jedis, String streamName) {
        try {
            jedis.xgroupCreate(streamName, groupName, redis.clients.jedis.StreamEntryID.LAST_ENTRY, true);
        } catch (Exception e) {
            if (!e.getMessage().contains("BUSYGROUP")) {
                LOG.warn("Warning setting up stream {}: {}", streamName, e.getMessage());
            }
        }
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("WorkerRunner already started");
        }

        setupStreams();

        this.lockToken = worker.registry.claimWorkerId(worker.workerId);

        worker.registry.registerWorkerMembership(worker.workerId, worker.getAgentTypes());

        worker.startHeartbeat();

        LOG.info("[{}] Runner started, waiting for tasks...", worker.workerId);

        new Thread(this::runLoop, "runner-loop-" + worker.workerId).start();
    }

    private void runLoop() {
        while (running.get()) {
            try {
                Map<String, redis.clients.jedis.StreamEntryID> streams = new HashMap<>();
                for (String agentType : worker.getAgentTypes()) {
                    streams.put(Constants.QueueNames.ctrlStream(agentType),
                            redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY);
                }
                streams.put(Constants.QueueNames.workerCtrlStream(worker.workerId),
                        redis.clients.jedis.StreamEntryID.UNRECEIVED_ENTRY);

                if (streams.isEmpty()) {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                    continue;
                }

                try (Jedis jedis = redisClient.getResource()) {
                    List<Map.Entry<String, List<StreamEntry>>> results = jedis.xreadGroup(
                            groupName,
                            consumerName,
                            XReadGroupParams.xReadGroupParams()
                                    .count(Constants.REDIS_READ_BATCH_SIZE)
                                    .block(Constants.REDIS_BLOCK_TIMEOUT_MS),
                            streams);

                    if (results != null) {
                        for (Map.Entry<String, List<StreamEntry>> entry : results) {
                            String streamName = entry.getKey();
                            for (StreamEntry streamEntry : entry.getValue()) {
                                processMessageAsync(streamName, streamEntry);
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Runner loop interrupted, shutting down");
                break;
            } catch (Exception e) {
                LOG.error("Error in runner loop: {}", e.getMessage());
                try {
                    Thread.sleep(Constants.LOOP_SLEEP_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
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

                // 1. Handle Control Commands (e.g. CancelTaskCommand)
                if (command instanceof CancelTaskCommand cancelCmd) {
                    handleControlCommand(streamName, entry.getID(), cancelCmd);
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
                        try (Jedis jedis = redisClient.getResource()) {
                            jedis.xack(streamName, groupName, entry.getID());
                        }
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
                    try (Jedis jedis = redisClient.getResource()) {
                        jedis.xack(streamName, groupName, entry.getID());
                    }
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
            try (Jedis jedis = redisClient.getResource()) {
                jedis.xack(streamName, groupName, streamId);
            }

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
