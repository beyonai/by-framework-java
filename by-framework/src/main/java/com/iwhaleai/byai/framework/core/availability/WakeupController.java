package com.iwhaleai.byai.framework.core.availability;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
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

    private final RedisClient redisClient;
    private final WakeupProvider wakeupProvider;
    private final int dedupeTtlSeconds;
    private final int maxAttempts;

    public WakeupController(RedisClient redisClient, WakeupProvider wakeupProvider) {
        this(redisClient, wakeupProvider, 3600, 3);
    }

    public WakeupController(RedisClient redisClient, WakeupProvider wakeupProvider, int dedupeTtlSeconds, int maxAttempts) {
        this.redisClient = redisClient;
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

        try (Jedis jedis = redisClient.getResource()) {
            int blockMsInt = (int) Math.min(blockMs, Integer.MAX_VALUE);
            XReadParams xReadParams = XReadParams.xReadParams().count(1).block(blockMsInt);
            Map<String, StreamEntryID> streamMap = Map.of(streamName, startId);
            List<Map.Entry<String, List<StreamEntry>>> results = jedis.xread(xReadParams, streamMap);

            if (results == null || results.isEmpty()) {
                return null;
            }

            for (Map.Entry<String, List<StreamEntry>> result : results) {
                for (StreamEntry entry : result.getValue()) {
                    processWakeupEntry(jedis, entry);
                    return entry.getID();
                }
            }
        } catch (Exception e) {
            log.error("Error processing wakeup request: {}", e.getMessage(), e);
        }

        return null;
    }

    private void processWakeupEntry(Jedis jedis, StreamEntry entry) {
        Map<String, String> fields = entry.getFields();
        WakeupRequest request = WakeupRequest.fromDict(fields);
        if (request == null || request.getExecutionId() == null || request.getExecutionId().isEmpty()) {
            log.warn("Skipping malformed wakeup request: {}", entry.getID());
            return;
        }

        // Dedupe check
        String dedupeKey = Constants.REDIS_PREFIX + "control_plane:wakeup_dedupe:" + request.getExecutionId();
        String dedupeVal = jedis.get(dedupeKey);
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

        // Mark dedupe
        jedis.setex(dedupeKey, dedupeTtlSeconds, String.valueOf(attempts + 1));

        // Check TTL
        if (request.getTtlMs() > 0) {
            long age = System.currentTimeMillis() - request.getRequestedAt();
            if (age > request.getTtlMs()) {
                log.info("Wakeup request {} expired (age={}ms > ttl={}ms)", request.getExecutionId(), age, request.getTtlMs());
                writeDecision(jedis, request.getExecutionId(),
                        WakeupDecisionStatus.FAILED, "Request TTL expired", "");
                return;
            }
        }

        // Delegate to provider
        log.info("Processing wakeup for agent_type='{}' execution_id={}", request.getAgentType(), request.getExecutionId());
        try {
            WakeupDecision decision = wakeupProvider.wakeup(request);
            writeDecision(jedis, decision.getExecutionId(), decision.getStatus(),
                    decision.getReason(), decision.getWorkerId());
        } catch (Exception e) {
            log.error("Wakeup provider failed for execution_id={}: {}", request.getExecutionId(), e.getMessage(), e);
            writeDecision(jedis, request.getExecutionId(),
                    WakeupDecisionStatus.FAILED, "Wakeup provider error: " + e.getMessage(), "");
        }
    }

    private void writeDecision(Jedis jedis, String executionId, String status, String reason, String workerId) {
        String decisionStream = Constants.QueueNames.controlPlaneDecisionStream(executionId);
        WakeupDecision decision = WakeupDecision.builder()
                .executionId(executionId)
                .status(status)
                .reason(reason != null ? reason : "")
                .workerId(workerId != null ? workerId : "")
                .timestamp(System.currentTimeMillis())
                .build();

        Map<String, String> fieldMap = decision.toDict();
        jedis.xadd(decisionStream, (StreamEntryID) null, new HashMap<>(fieldMap));
        jedis.expire(decisionStream, 300); // 5-minute TTL for decision streams
    }
}
