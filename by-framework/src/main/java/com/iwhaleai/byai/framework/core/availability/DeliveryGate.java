package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Reference component for dispatching ready deliveries.
 * Reads pending deliveries from the global pending stream and replays them
 * to the target agent's control stream when a worker becomes available.
 *
 * <p>Aligned with Python's DeliveryGate in delivery_gate.py.
 */
@Slf4j
public class DeliveryGate {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final RedisClient redisClient;

    public DeliveryGate(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    /**
     * Dispatch all ready pending deliveries for a given execution ID.
     * Reads from the global pending delivery stream, matches by execution_id,
     * sorts by priority (descending), and replays to the target stream.
     *
     * @param executionId The execution ID to dispatch
     * @param lastId      Cursor ID for xread (start with "0-0")
     * @param count       Max entries to read per batch
     * @return number of deliveries dispatched
     */
    public int dispatchReady(String executionId, String lastId, int count) {
        int dispatched = 0;
        try (Jedis jedis = redisClient.getResource()) {
            String pendingStream = Constants.QueueNames.controlPlanePendingQueue();
            Map<String, StreamEntryID> streamMap = Map.of(pendingStream, new StreamEntryID(lastId));
            XReadParams xReadParams = XReadParams.xReadParams().count(count);

            List<Map.Entry<String, List<StreamEntry>>> results =
                    jedis.xread(xReadParams, streamMap);

            if (results == null || results.isEmpty()) {
                return 0;
            }

            List<PendingDelivery> readyDeliveries = new ArrayList<>();
            for (Map.Entry<String, List<StreamEntry>> result : results) {
                for (StreamEntry entry : result.getValue()) {
                    Map<String, String> fields = entry.getFields();
                    PendingDelivery pending = PendingDelivery.fromDict(fields);
                    if (pending == null || !executionId.equals(pending.getExecutionId())) {
                        continue;
                    }
                    readyDeliveries.add(pending);
                }
            }

            // Sort by priority descending (matching Python)
            readyDeliveries.sort(Comparator.comparingInt(PendingDelivery::getPriority).reversed());

            for (PendingDelivery pending : readyDeliveries) {
                String targetStream = pending.getDeliveryStream();
                if (targetStream == null || targetStream.isEmpty()) {
                    targetStream = Constants.QueueNames.ctrlStream(pending.getTargetAgentType());
                }
                
                Map<String, String> payload = new java.util.HashMap<>();
                try {
                    String cmdJson = OBJECT_MAPPER.writeValueAsString(
                            pending.getCommandPayload() != null ? pending.getCommandPayload() : new java.util.HashMap<>());
                    payload.put("data", cmdJson);
                } catch (Exception ex) {
                    log.error("Failed to serialize command_payload for dispatch: {}", ex.getMessage());
                    continue;
                }

                jedis.xadd(targetStream, (StreamEntryID) null, payload);
                dispatched++;
                log.info("Dispatched pending delivery execution_id={} to stream={}",
                        pending.getExecutionId(), targetStream);
            }
        } catch (Exception e) {
            log.error("Error dispatching ready delivery for execution_id={}: {}", executionId, e.getMessage(), e);
        }
        return dispatched;
    }

    /**
     * Dispatch with defaults (lastId="0-0", count=100).
     */
    public int dispatchReady(String executionId) {
        return dispatchReady(executionId, "0-0", 100);
    }
}
