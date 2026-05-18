package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Core availability router that evaluates route policies when dispatching messages.
 *
 * <p>Replaces the simple require_online_worker boolean with a rich route_policy system.
 * When a target agent_type has no online worker, callers choose from 5 policies:
 * FAIL_FAST, SEND_ANYWAY, WAKE_AND_WAIT, WAKE_AND_QUEUE, QUEUE_ONLY.
 */
@Slf4j
public class AvailabilityRouter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int DEFAULT_TIMEOUT_MS = 30000;
    private static final int CIRCUIT_BREAKER_THRESHOLD = 5;
    private static final int CIRCUIT_BREAKER_WINDOW_SECONDS = 60;
    private static final int QUOTA_DEFAULT_LIMIT = 1000;

    private final RedisClient redisClient;
    private final WorkerRegistry workerRegistry;
    private final int defaultTimeoutMs;
    private final int circuitBreakerThreshold;
    private final int quotaDefaultLimit;

    public AvailabilityRouter(RedisClient redisClient) {
        this(redisClient, new WorkerRegistry(redisClient), DEFAULT_TIMEOUT_MS,
                CIRCUIT_BREAKER_THRESHOLD, QUOTA_DEFAULT_LIMIT);
    }

    public AvailabilityRouter(RedisClient redisClient, WorkerRegistry workerRegistry) {
        this(redisClient, workerRegistry, DEFAULT_TIMEOUT_MS,
                CIRCUIT_BREAKER_THRESHOLD, QUOTA_DEFAULT_LIMIT);
    }

    public AvailabilityRouter(RedisClient redisClient, WorkerRegistry workerRegistry,
                              int defaultTimeoutMs, int circuitBreakerThreshold, int quotaDefaultLimit) {
        this.redisClient = redisClient;
        this.workerRegistry = workerRegistry;
        this.defaultTimeoutMs = defaultTimeoutMs;
        this.circuitBreakerThreshold = circuitBreakerThreshold;
        this.quotaDefaultLimit = quotaDefaultLimit;
    }

    /**
     * Evaluate the delivery intent and return an availability result.
     * This is the main entry point for the availability control plane.
     *
     * @param intent The delivery intent
     * @return AvailabilityResult indicating how to proceed
     */
    public AvailabilityResult prepareDelivery(DeliveryIntent intent) {
        // Set defaults
        if (intent.getPolicy() == null || intent.getPolicy().isBlank()) {
            intent.setPolicy(RoutePolicy.FAIL_FAST);
        }
        if (intent.getTimeoutMs() <= 0) {
            intent.setTimeoutMs(defaultTimeoutMs);
        }

        // 1. Check control plane policy (circuit breaker & quota)
        String controlCheck = checkControlPlanePolicy(intent);
        if (controlCheck != null) {
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.REJECT)
                    .reason(controlCheck)
                    .errorCode(determineErrorCode(controlCheck))
                    .build();
        }

        // 2. SEND_ANYWAY bypasses all online checks
        if (RoutePolicy.SEND_ANYWAY.equals(intent.getPolicy())) {
            return buildDeliverNow(intent.getTargetAgentType());
        }

        // 3. Check online workers
        if (hasOnlineAgentType(intent.getTargetAgentType())) {
            return buildDeliverNow(intent.getTargetAgentType());
        }

        // 4. Try fallback routing
        String fallbackType = resolveConfiguredFallback(intent.getTargetAgentType());
        if (fallbackType != null && !fallbackType.isBlank()
                && !fallbackType.equals(intent.getTargetAgentType())
                && hasOnlineAgentType(fallbackType)) {
            log.info("Falling back from '{}' to '{}'", intent.getTargetAgentType(), fallbackType);
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE)
                    .selectedAgentType(fallbackType)
                    .streamName(Constants.QueueNames.ctrlStream(fallbackType))
                    .build();
        }

        // 5. Handle policy-based routing when no worker is online
        return handleNoOnlineWorker(intent);
    }

    // ---- Private helper methods ----

    private AvailabilityResult buildDeliverNow(String agentType) {
        return AvailabilityResult.builder()
                .status(AvailabilityStatus.DELIVER_NOW)
                .selectedAgentType(agentType)
                .streamName(Constants.QueueNames.ctrlStream(agentType))
                .build();
    }

    private AvailabilityResult handleNoOnlineWorker(DeliveryIntent intent) {
        switch (intent.getPolicy()) {
            case RoutePolicy.FAIL_FAST:
                return AvailabilityResult.builder()
                        .status(AvailabilityStatus.REJECT)
                        .reason("No online worker for agent_type '" + intent.getTargetAgentType() + "'")
                        .errorCode(ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE)
                        .build();

            case RoutePolicy.WAKE_AND_WAIT:
                return handleWakeAndWait(intent);

            case RoutePolicy.WAKE_AND_QUEUE:
                emitWakeup(intent);
                queuePendingDelivery(intent);
                return AvailabilityResult.builder()
                        .status(AvailabilityStatus.QUEUE_PENDING)
                        .selectedAgentType(intent.getTargetAgentType())
                        .build();

            case RoutePolicy.QUEUE_ONLY:
                queuePendingDelivery(intent);
                return AvailabilityResult.builder()
                        .status(AvailabilityStatus.QUEUE_PENDING)
                        .selectedAgentType(intent.getTargetAgentType())
                        .build();

            default:
                return AvailabilityResult.builder()
                        .status(AvailabilityStatus.REJECT)
                        .reason("Unknown policy: " + intent.getPolicy())
                        .build();
        }
    }

    private AvailabilityResult handleWakeAndWait(DeliveryIntent intent) {
        emitWakeup(intent);
        WakeupDecision decision = waitForWakeupDecision(intent);

        if (WakeupDecisionStatus.READY.equals(decision.getStatus())) {
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.WAIT_AND_DELIVER)
                    .selectedAgentType(intent.getTargetAgentType())
                    .streamName(Constants.QueueNames.ctrlStream(intent.getTargetAgentType()))
                    .targetWorkerId(decision.getWorkerId())
                    .build();
        }

        return AvailabilityResult.builder()
                .status(AvailabilityStatus.REJECT)
                .reason("Wakeup " + decision.getStatus() + ": " + decision.getReason())
                .errorCode(ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE)
                .build();
    }

    /**
     * Check circuit breaker and quota for the target agent type.
     *
     * @return null if OK, or a rejection reason string
     */
    String checkControlPlanePolicy(DeliveryIntent intent) {
        // Circuit breaker check
        if (isCircuitOpen(intent.getTargetAgentType())) {
            log.warn("Circuit breaker OPEN for agent_type='{}'", intent.getTargetAgentType());
            return "Circuit breaker open for agent_type '" + intent.getTargetAgentType() + "'";
        }

        // Quota check
        String tenantId = intent.getUserCode() != null ? intent.getUserCode() : "default";
        if (isQuotaExceeded(tenantId)) {
            log.warn("Quota exceeded for tenant='{}'", tenantId);
            return "Quota exceeded for tenant '" + tenantId + "'";
        }

        return null;
    }

    private String determineErrorCode(String reason) {
        if (reason != null && reason.contains("Circuit breaker")) {
            return ExecutionStatusAdapter.ERR_AGENT_CIRCUIT_OPEN;
        }
        if (reason != null && reason.contains("Quota")) {
            return ExecutionStatusAdapter.ERR_TENANT_QUOTA_EXCEEDED;
        }
        return ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE;
    }

    // ---- Circuit breaker ----

    boolean isCircuitOpen(String agentType) {
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.QueueNames.controlPlaneCircuitBreakerKey() + ":" + agentType;
            String value = jedis.get(key);
            if (value == null) {
                return false;
            }
            try {
                int failures = Integer.parseInt(value);
                return failures >= circuitBreakerThreshold;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    /**
     * Record a failure for circuit breaker tracking.
     */
    public void recordFailure(String agentType) {
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.QueueNames.controlPlaneCircuitBreakerKey() + ":" + agentType;
            jedis.incr(key);
            jedis.expire(key, CIRCUIT_BREAKER_WINDOW_SECONDS);
        }
    }

    // ---- Quota ----

    boolean isQuotaExceeded(String tenantId) {
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.QueueNames.controlPlaneQuotaKey(tenantId);
            String value = jedis.get(key);
            if (value == null) {
                return false;
            }
            try {
                int used = Integer.parseInt(value);
                return used >= quotaDefaultLimit;
            } catch (NumberFormatException e) {
                return false;
            }
        }
    }

    // ---- Online worker check ----

    boolean hasOnlineAgentType(String agentType) {
        WorkerRegistry.OnlineAgentCheckResult result = workerRegistry.hasOnlineAgentType(
                agentType, true, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
        return result.exists;
    }

    // ---- Fallback routing ----

    /**
     * Resolve a configured fallback agent type for a given agent type.
     * Override this method to provide custom fallback routing logic.
     *
     * @param agentType The original target agent type
     * @return A fallback agent type, or null if not configured
     */
    String resolveConfiguredFallback(String agentType) {
        // Default implementation: no fallback configured
        // Subclasses can override to provide fallback routing from config/Redis
        return null;
    }

    // ---- Wakeup ----

    void emitWakeup(DeliveryIntent intent) {
        try (Jedis jedis = redisClient.getResource()) {
            String managementStream = Constants.QueueNames.controlPlaneManagementStream();

            WakeupRequest request = WakeupRequest.builder()
                    .executionId(intent.getExecutionId())
                    .agentType(intent.getTargetAgentType())
                    .reason("No online worker available")
                    .priority(intent.getPriority() != null ? intent.getPriority() : "normal")
                    .requestedAt(System.currentTimeMillis())
                    .ttlMs(intent.getTimeoutMs())
                    .metadata(intent.getMetadata())
                    .build();

            jedis.xadd(managementStream, (StreamEntryID) null, request.toRedisPayload());
            log.info("Emitted wakeup for agent_type='{}' execution_id={}", intent.getTargetAgentType(), intent.getExecutionId());
        }
    }

    /**
     * Block and wait for a wakeup decision using Redis xread with block.
     * Since Jedis is synchronous (blocking), this uses Thread.sleep between polls.
     */
    WakeupDecision waitForWakeupDecision(DeliveryIntent intent) {
        long deadline = System.currentTimeMillis() + Math.max(intent.getTimeoutMs(), 100);
        String decisionStream = Constants.QueueNames.controlPlaneDecisionStream(intent.getExecutionId());

        while (System.currentTimeMillis() < deadline) {
            try (Jedis jedis = redisClient.getResource()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }

                int blockMs = (int) Math.min(remaining, 2000);

                XReadParams xReadParams = XReadParams.xReadParams().count(1).block(blockMs);
                Map<String, StreamEntryID> streamMap = Map.of(decisionStream, StreamEntryID.LAST_ENTRY);

                List<Map.Entry<String, List<StreamEntry>>> results =
                        jedis.xread(xReadParams, streamMap);

                if (results != null && !results.isEmpty()) {
                    for (Map.Entry<String, List<StreamEntry>> result : results) {
                        for (StreamEntry entry : result.getValue()) {
                            Map<String, String> fields = entry.getFields();
                            // Clean up the stream entry
                            jedis.xdel(decisionStream, entry.getID());
                            return WakeupDecision.fromDict(fields);
                        }
                    }
                }
            } catch (Exception e) {
                log.warn("Error waiting for wakeup decision: {}", e.getMessage());
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.warn("Timed out waiting for wakeup decision for execution_id={}", intent.getExecutionId());
        return WakeupDecision.builder()
                .executionId(intent.getExecutionId())
                .status(WakeupDecisionStatus.TIMEOUT)
                .reason("Timed out waiting for wakeup decision after " + intent.getTimeoutMs() + "ms")
                .build();
    }

    // ---- Pending delivery queue ----

    void queuePendingDelivery(DeliveryIntent intent) {
        try (Jedis jedis = redisClient.getResource()) {
            String pendingQueue = Constants.QueueNames.controlPlanePendingQueue(intent.getTargetAgentType());

            PendingDelivery pending = PendingDelivery.builder()
                    .executionId(intent.getExecutionId())
                    .messageId(intent.getMessageId())
                    .sessionId(intent.getSessionId())
                    .traceId(intent.getTraceId())
                    .source(intent.getSource())
                    .targetAgentType(intent.getTargetAgentType())
                    .userCode(intent.getUserCode())
                    .region(intent.getRegion())
                    .priority(intent.getPriority())
                    .policy(intent.getPolicy())
                    .queuedAt(System.currentTimeMillis())
                    .timeoutMs(intent.getTimeoutMs())
                    .commandPayload(intent.getCommandPayload())
                    .metadata(intent.getMetadata())
                    .build();

            jedis.xadd(pendingQueue, (StreamEntryID) null, pending.toRedisPayload());
            log.info("Queued pending delivery for agent_type='{}' execution_id={}", intent.getTargetAgentType(), intent.getExecutionId());
        }
    }

    // Expose for testing
    WorkerRegistry getWorkerRegistry() {
        return workerRegistry;
    }

    /**
     * Adapter to avoid circular dependency on ExecutionStatus constants.
     */
    private static final class ExecutionStatusAdapter {
        static final String ERR_AGENT_TYPE_UNAVAILABLE = "ERR_AGENT_TYPE_UNAVAILABLE";
        static final String ERR_AGENT_CIRCUIT_OPEN = "ERR_AGENT_CIRCUIT_OPEN";
        static final String ERR_TENANT_QUOTA_EXCEEDED = "ERR_TENANT_QUOTA_EXCEEDED";
    }
}
