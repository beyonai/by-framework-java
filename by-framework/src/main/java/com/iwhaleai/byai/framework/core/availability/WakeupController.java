package com.iwhaleai.byai.framework.core.availability;

import com.iwhaleai.byai.framework.common.ClusterRedisOps;
import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reference controller that processes wakeup requests from the management stream.
 * Reads wakeup requests, delegates to a WakeupProvider, and writes decisions back.
 */
@Slf4j
public class WakeupController {

    private final RedisOps redisOps;
    private final RedisStreamOps streamOps;
    private final WakeupProvider wakeupProvider;
    private final int dedupeTtlSeconds;
    private final int maxAttempts;

    public WakeupController(RedisClient redisClient, WakeupProvider wakeupProvider) {
        this(redisClient, wakeupProvider, 3600, 3);
    }

    public WakeupController(RedisClient redisClient, WakeupProvider wakeupProvider, int dedupeTtlSeconds, int maxAttempts) {
        this.redisOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisOps(redisClient.getJedisCluster())
                : new StandaloneRedisOps(redisClient);
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.wakeupProvider = wakeupProvider;
        this.dedupeTtlSeconds = dedupeTtlSeconds;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Process one wakeup request from the management stream.
     * Blocks up to blockMs milliseconds waiting for a new request.
     *
     * @param lastId  The last processed entry ID, or "0-0" to start from beginning
     * @param blockMs Max milliseconds to block waiting for a new entry
     * @return The stream entry ID of the processed request, or null if none processed
     */
    public StreamEntryID runOnce(StreamEntryID lastId, long blockMs) {
        String streamName = Constants.QueueNames.controlPlaneManagementStream();
        StreamEntryID startId = lastId != null ? lastId : new StreamEntryID("0-0");

        try {
            int blockMsInt = (int) Math.min(blockMs, Integer.MAX_VALUE);
            List<Map.Entry<String, List<StreamEntry>>> results =
                    streamOps.xread(streamName, startId, 1, blockMsInt);

            if (results == null || results.isEmpty()) {
                return null;
            }

            for (Map.Entry<String, List<StreamEntry>> result : results) {
                for (StreamEntry entry : result.getValue()) {
                    processWakeupEntry(entry);
                    return entry.getID();
                }
            }
        } catch (Exception e) {
            log.error("Error processing wakeup request: {}", e.getMessage(), e);
        }

        return null;
    }

    private void processWakeupEntry(StreamEntry entry) {
        Map<String, String> fields = entry.getFields();
        WakeupRequest request = WakeupRequest.fromDict(fields);
        if (request == null || request.getExecutionId() == null || request.getExecutionId().isEmpty()) {
            log.warn("Skipping malformed wakeup request: {}", entry.getID());
            return;
        }

        // Dedupe check (aligned with Python: SET NX with TTL)
        String dedupeKey = Constants.QueueNames.controlPlaneWakeupDedupe(
                request.getTargetAgentType(), request.getUserCode(), request.getRegion());
        String dedupeVal = redisOps.get(dedupeKey);
        int attempts = 0;
        if (dedupeVal != null) {
            try {
                attempts = Integer.parseInt(dedupeVal);
            } catch (NumberFormatException ignored) {
            }
        }
        if (attempts >= maxAttempts) {
            log.info("Wakeup request {} exceeded max attempts ({}), dropping", request.getExecutionId(), maxAttempts);
            return;
        }

        // Atomically claim the wakeup (SET NX with TTL, matching Python's SET EX NX)
        String result = redisOps.set(dedupeKey, request.getExecutionId(),
                SetParams.setParams().nx().ex(dedupeTtlSeconds));
        if (result == null) {
            // Key already exists — wakeup already claimed
            log.debug("Wakeup for execution_id={} already claimed, skipping", request.getExecutionId());
            return;
        }

        // Delegate to provider
        log.info("Processing wakeup for agent_type='{}' execution_id={}", request.getTargetAgentType(), request.getExecutionId());
        try {
            WakeupDecision decision = wakeupProvider.wakeup(request);
            writeDecision(decision, request);
        } catch (Exception e) {
            log.error("Wakeup provider failed for execution_id={}: {}", request.getExecutionId(), e.getMessage(), e);
            WakeupDecision failedDecision = WakeupDecision.builder()
                    .executionId(request.getExecutionId())
                    .targetAgentType(request.getTargetAgentType())
                    .status(WakeupDecisionStatus.FAILED)
                    .reason("Wakeup provider error: " + e.getMessage())
                    .workerId("")
                    .timestamp(System.currentTimeMillis())
                    .build();
            writeDecision(failedDecision, request);
        }
    }

    private void writeDecision(WakeupDecision decision, WakeupRequest request) {
        WakeupDecision.WakeupDecisionBuilder builder = WakeupDecision.builder()
                .executionId(decision.getExecutionId())
                .status(decision.getStatus())
                .reason(decision.getReason() != null ? decision.getReason() : "")
                .workerId(decision.getWorkerId() != null ? decision.getWorkerId() : "")
                .targetAgentType(decision.getTargetAgentType() != null && !decision.getTargetAgentType().isEmpty()
                        ? decision.getTargetAgentType() : request.getTargetAgentType())
                .selectedAgentType(decision.getSelectedAgentType())
                .workerIds(decision.getWorkerIds())
                .region(decision.getRegion() != null && !decision.getRegion().isEmpty()
                        ? decision.getRegion() : request.getRegion())
                .retryAfterMs(decision.getRetryAfterMs())
                .timestamp(decision.getTimestamp() > 0 ? decision.getTimestamp() : System.currentTimeMillis());

        WakeupDecision finalDecision = builder.build();
        String decisionStreamKey = Constants.QueueNames.controlPlaneDecisionStream(finalDecision.getExecutionId());
        Map<String, String> fieldMap = finalDecision.toDict();
        streamOps.xadd(decisionStreamKey, new HashMap<>(fieldMap), XAddOptions.noTrim());
        redisOps.expire(decisionStreamKey, 300); // 5-minute TTL for decision streams
    }
}
