package com.iwhaleai.byai.framework.core.availability;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Reference component for dispatching ready deliveries.
 * Reads pending deliveries and replays them to the target stream when a worker becomes available.
 */
@Slf4j
public class DeliveryGate {

    private final RedisClient redisClient;

    public DeliveryGate(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    /**
     * Dispatch all ready pending deliveries for a given execution ID.
     * Reads the pending delivery from the queue and replays it to the target agent stream.
     *
     * @param executionId The execution ID to dispatch
     * @return true if a pending delivery was dispatched
     */
    public boolean dispatchReady(String executionId) {
        // First, try to find the pending delivery by scanning the pending queues
        // Since we store by agent type, we need to look up the agent type from the execution
        try (Jedis jedis = redisClient.getResource()) {
            // We need to find the pending delivery. Check if we have the agent type info.
            // Scan pending queue keys pattern
            String pendingQueuePattern = Constants.REDIS_PREFIX + "control_plane:pending:*";
            for (String key : jedis.keys(pendingQueuePattern)) {
                List<StreamEntry> entries = jedis.xrange(key, (StreamEntryID) null, (StreamEntryID) null, 100);
                if (entries != null) {
                    for (StreamEntry entry : entries) {
                        Map<String, String> fields = entry.getFields();
                        PendingDelivery pending = PendingDelivery.fromDict(fields);
                        if (pending != null && executionId.equals(pending.getExecutionId())) {
                            // Replay to target stream
                            dispatchPending(entry, pending);
                            // Remove from pending queue
                            jedis.xdel(key, entry.getID());
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error dispatching ready delivery for execution_id={}: {}", executionId, e.getMessage(), e);
        }
        return false;
    }

    private void dispatchPending(StreamEntry entry, PendingDelivery pending) {
        try (Jedis jedis = redisClient.getResource()) {
            String targetStream = Constants.QueueNames.ctrlStream(pending.getTargetAgentType());
            jedis.xadd(targetStream, (StreamEntryID) null, pending.toRedisPayload());
            log.info("Dispatched pending delivery execution_id={} to stream={}", pending.getExecutionId(), targetStream);
        }
    }
}
