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

        if (decision == null
                || WakeupDecisionStatus.TIMEOUT.equals(decision.getStatus())) {
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.REJECT)
                    .reason("Timed out waiting for wakeup decision after " + intent.getTimeoutMs() + "ms")
                    .errorCode(ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE)
                    .build();
        }

        if (WakeupDecisionStatus.FAILED.equals(decision.getStatus())
                || WakeupDecisionStatus.REJECTED.equals(decision.getStatus())) {
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.REJECT)
                    .reason(decision.getReason() != null && !decision.getReason().isEmpty()
                            ? decision.getReason()
                            : "Wakeup " + decision.getStatus())
                    .errorCode(ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE)
                    .build();
        }

        if (WakeupDecisionStatus.FALLBACK.equals(decision.getStatus())) {
            String targetAgent = decision.getSelectedAgentType() != null && !decision.getSelectedAgentType().isEmpty()
                    ? decision.getSelectedAgentType()
                    : decision.getReason();
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE)
                    .selectedAgentType(targetAgent)
                    .streamName(Constants.QueueNames.ctrlStream(targetAgent))
                    .build();
        }

        if (WakeupDecisionStatus.READY.equals(decision.getStatus())) {
            // Recheck: worker may still not be online even after READY signal
            if (hasOnlineAgentType(intent.getTargetAgentType())) {
                return AvailabilityResult.builder()
                        .status(AvailabilityStatus.WAIT_AND_DELIVER)
                        .selectedAgentType(intent.getTargetAgentType())
                        .streamName(Constants.QueueNames.ctrlStream(intent.getTargetAgentType()))
                        .targetWorkerId(decision.getWorkerId())
                        .build();
            }
            return AvailabilityResult.builder()
                    .status(AvailabilityStatus.REJECT)
                    .reason("Worker not online after wakeup READY for agent_type '"
                            + intent.getTargetAgentType() + "'")
                    .errorCode(ExecutionStatusAdapter.ERR_AGENT_TYPE_UNAVAILABLE)
                    .build();
        }

        return AvailabilityResult.builder()
                .status(AvailabilityStatus.REJECT)
                .reason("Unknown wakeup decision status: " + decision.getStatus())
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
            return getCircuitOpenReason(intent.getTargetAgentType());
        }

        // Quota check
        String tenantId = intent.getUserCode() != null ? intent.getUserCode() : "default";
        if (isQuotaExceeded(tenantId)) {
            log.warn("Quota exceeded for tenant='{}'", tenantId);
            return getQuotaExceededReason(tenantId);
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

    // ---- Circuit breaker & Quota JSON Helpers ----

    @SuppressWarnings("unchecked")
    private Map<String, Object> readJsonKey(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            String raw = jedis.get(key);
            if (raw == null || raw.isEmpty()) {
                return null;
            }
            return OBJECT_MAPPER.readValue(raw, Map.class);
        } catch (Exception e) {
            log.warn("Failed to read/parse JSON key {}: {}", key, e.getMessage());
            return null;
        }
    }

    private String getCircuitOpenReason(String agentType) {
        Map<String, Object> circuit = readJsonKey(Constants.QueueNames.controlPlaneCircuitBreakerKey(agentType));
        if (circuit != null && circuit.get("reason") != null) {
            return String.valueOf(circuit.get("reason"));
        }
        return "Circuit breaker open for agent_type '" + agentType + "'";
    }

    private String getQuotaExceededReason(String tenantId) {
        Map<String, Object> quota = readJsonKey(Constants.QueueNames.controlPlaneQuotaKey(tenantId));
        if (quota != null && quota.get("reason") != null) {
            return String.valueOf(quota.get("reason"));
        }
        return "Quota exceeded for tenant '" + tenantId + "'";
    }

    // ---- Circuit breaker ----

    boolean isCircuitOpen(String agentType) {
        // 1. Check control-plane JSON circuit key
        Map<String, Object> circuit = readJsonKey(Constants.QueueNames.controlPlaneCircuitBreakerKey(agentType));
        if (circuit != null) {
            return "OPEN".equalsIgnoreCase(String.valueOf(circuit.get("state")));
        }

        // 2. Fallback: check SDK-local failure count key
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.QueueNames.controlPlaneCircuitBreakerKey(agentType) + ":local";
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
            String key = Constants.QueueNames.controlPlaneCircuitBreakerKey(agentType) + ":local";
            jedis.incr(key);
            jedis.expire(key, CIRCUIT_BREAKER_WINDOW_SECONDS);
        }
    }

    // ---- Quota ----

    boolean isQuotaExceeded(String tenantId) {
        // 1. Check control-plane JSON quota key
        Map<String, Object> quota = readJsonKey(Constants.QueueNames.controlPlaneQuotaKey(tenantId));
        if (quota != null) {
            return Boolean.FALSE.equals(quota.get("available"));
        }

        // 2. Fallback: check SDK-local quota key
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.QueueNames.controlPlaneQuotaKey(tenantId) + ":local";
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
        Map<String, Object> fallback = readJsonKey(Constants.QueueNames.controlPlaneAgentFallback(agentType));
        if (fallback == null) {
            return null;
        }

        Object selectedObj = fallback.get("selected_agent_type");
        if (selectedObj == null) {
            selectedObj = fallback.get("agent_type");
        }
        if (selectedObj == null) {
            selectedObj = fallback.get("target_agent_type");
        }

        String selected = selectedObj != null ? String.valueOf(selectedObj).trim() : "";
        if (selected.isEmpty()) {
            return null;
        }

        if (hasOnlineAgentType(selected)) {
            return selected;
        }
        return null;
    }

    // ---- Wakeup ----

    void emitWakeup(DeliveryIntent intent) {
        try (Jedis jedis = redisClient.getResource()) {
            String managementStream = Constants.QueueNames.controlPlaneManagementStream();

            int priority = 0;
            if (intent.getPriority() != null) {
                try {
                    priority = Integer.parseInt(intent.getPriority());
                } catch (NumberFormatException ignored) {
                }
            }

            WakeupRequest request = WakeupRequest.builder()
                    .executionId(intent.getExecutionId())
                    .targetAgentType(intent.getTargetAgentType())
                    .sessionId(intent.getSessionId())
                    .traceId(intent.getTraceId())
                    .messageId(intent.getMessageId())
                    .source(intent.getSource() != null ? intent.getSource() : "client")
                    .policy(intent.getPolicy())
                    .timeoutMs(intent.getTimeoutMs())
                    .userCode(intent.getUserCode())
                    .region(intent.getRegion())
                    .priority(priority)
                    .metadata(intent.getMetadata())
                    .commandPayload(intent.getCommandPayload())
                    .build();

            jedis.xadd(managementStream, (StreamEntryID) null, request.toRedisPayload());
            log.info("Emitted wakeup for agent_type='{}' execution_id={}", intent.getTargetAgentType(), intent.getExecutionId());
        }
    }

    /**
     * Block and wait for a wakeup decision using Redis xread with block.
     * Mirrors Python's {@code _wait_for_wakeup_decision}.
     *
     * <ul>
     *   <li>Starts from 0-0 to avoid missing decisions already written by the controller</li>
     *   <li>Filters out STARTING / QUEUED intermediate states (continues waiting)</li>
     *   <li>Returns null on timeout (matching Python's None return)</li>
     * </ul>
     */
    WakeupDecision waitForWakeupDecision(DeliveryIntent intent) {
        long deadline = System.currentTimeMillis() + Math.max(intent.getTimeoutMs(), 100);
        String decisionStream = Constants.QueueNames.controlPlaneDecisionStream(intent.getExecutionId());

        // Start from 0-0 (not $) to avoid a race condition: the WakeupController
        // may have already written the decision before this xread call starts.
        StreamEntryID lastId = new StreamEntryID("0-0");

        while (System.currentTimeMillis() < deadline) {
            try (Jedis jedis = redisClient.getResource()) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    break;
                }

                int blockMs = (int) Math.min(remaining, 2000);

                XReadParams xReadParams = XReadParams.xReadParams().count(1).block(blockMs);
                Map<String, StreamEntryID> streamMap = Map.of(decisionStream, lastId);

                List<Map.Entry<String, List<StreamEntry>>> results =
                        jedis.xread(xReadParams, streamMap);

                if (results != null && !results.isEmpty()) {
                    for (Map.Entry<String, List<StreamEntry>> result : results) {
                        for (StreamEntry entry : result.getValue()) {
                            Map<String, String> fields = entry.getFields();
                            // Advance past consumed entries
                            lastId = entry.getID();
                            // Clean up the consumed entry
                            jedis.xdel(decisionStream, entry.getID());

                            WakeupDecision decision = WakeupDecision.fromDict(fields);

                            // Filter: skip intermediate states, keep waiting
                            if (WakeupDecisionStatus.STARTING.equals(decision.getStatus())
                                    || WakeupDecisionStatus.QUEUED.equals(decision.getStatus())) {
                                log.debug("Received intermediate wakeup status={}, continuing to wait",
                                        decision.getStatus());
                                break; // break inner loop, continue outer while loop
                            }

                            return decision;
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
        return null;
    }

    // ---- Pending delivery queue ----

    void queuePendingDelivery(DeliveryIntent intent) {
        try (Jedis jedis = redisClient.getResource()) {
            String pendingQueue = Constants.QueueNames.controlPlanePendingQueue();

            int priority = 0;
            if (intent.getPriority() != null) {
                try {
                    priority = Integer.parseInt(intent.getPriority());
                } catch (NumberFormatException ignored) {}
            }

            PendingDelivery pending = PendingDelivery.builder()
                    .executionId(intent.getExecutionId())
                    .messageId(intent.getMessageId())
                    .sessionId(intent.getSessionId())
                    .traceId(intent.getTraceId())
                    .source(intent.getSource())
                    .targetAgentType(intent.getTargetAgentType())
                    .deliveryStream(Constants.QueueNames.ctrlStream(intent.getTargetAgentType()))
                    .userCode(intent.getUserCode())
                    .region(intent.getRegion())
                    .priority(priority)
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
