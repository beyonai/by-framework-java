package com.iwhaleai.byai.framework.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.ClusterRedisStreamOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.EvictWorkerCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeWorkerCommand;
import com.iwhaleai.byai.framework.core.protocol.SuspendWorkerCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Admin API for worker lifecycle and agent-type admission control.
 *
 * <p>Lifecycle commands are delivered via two channels:
 * <ol>
 *   <li>Push: XADD to byai_gateway:ctrl:worker:{worker_id} (immediate delivery).</li>
 *   <li>Pull: HSET to byai_gateway:registry:worker:admin:{worker_id} (durable fallback,
 *       read by the worker's heartbeat loop every heartbeat interval).</li>
 * </ol>
 */
public class WorkerManager {
    private static final Logger LOG = LoggerFactory.getLogger(WorkerManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RedisStreamOps streamOps;
    private final WorkerRegistry registry;

    public WorkerManager(RedisClient redisClient) {
        this(redisClient, new WorkerRegistry(redisClient));
    }

    public WorkerManager(RedisClient redisClient, WorkerRegistry registry) {
        this.streamOps = redisClient.getJedisCluster() != null
                ? new ClusterRedisStreamOps(redisClient.getJedisCluster())
                : new StandaloneRedisStreamOps(redisClient);
        this.registry = registry;
    }

    // ------------------------------------------------------------------
    // Lifecycle control
    // ------------------------------------------------------------------

    /**
     * Pause a running worker from consuming new tasks.
     *
     * <p>The worker finishes in-flight tasks but stops accepting new ones.
     * Immediately removes the worker from all agent_type:members sets so that
     * routing skips it at once; the worker re-adds itself on resume.
     *
     * @param workerId Worker ID
     * @param reason   Human-readable reason
     */
    public void suspendWorker(String workerId, String reason) {
        registry.setWorkerAdminState(workerId, "suspended", reason);
        registry.removeWorkerFromTypeMembers(workerId);

        SuspendWorkerCommand command = SuspendWorkerCommand.builder()
                .header(adminHeader())
                .body(SuspendWorkerCommand.SuspendWorkerBody.builder().reason(reason).build())
                .build();
        xaddCommand(Constants.QueueNames.workerCtrlStream(workerId), command);
        LOG.info("[WorkerManager] Suspended worker: {} reason={}", workerId, reason);
    }

    /**
     * Resume a previously suspended worker.
     *
     * <p>Re-adds the worker to all agent_type:members sets immediately (respecting the
     * denylist), so routing can reach it again without waiting for the next heartbeat cycle.
     *
     * @param workerId Worker ID
     */
    public void resumeWorker(String workerId) {
        registry.setWorkerAdminState(workerId, "active", "");
        registry.restoreWorkerToTypeMembers(workerId);

        ResumeWorkerCommand command = ResumeWorkerCommand.builder()
                .header(adminHeader())
                .build();
        xaddCommand(Constants.QueueNames.workerCtrlStream(workerId), command);
        LOG.info("[WorkerManager] Resumed worker: {}", workerId);
    }

    /**
     * Shut down a worker.
     *
     * <p>Immediately removes the worker from all agent_type:members sets so routing stops
     * sending new messages before the heartbeat TTL expires.
     *
     * @param workerId Worker ID
     * @param force    When true, cancels in-flight tasks immediately
     * @param reason   Human-readable eviction reason
     */
    public void evictWorker(String workerId, boolean force, String reason) {
        registry.setWorkerAdminState(workerId, "evicted", reason);
        registry.removeWorkerFromTypeMembers(workerId);

        EvictWorkerCommand command = EvictWorkerCommand.builder()
                .header(adminHeader())
                .body(EvictWorkerCommand.EvictWorkerBody.builder().reason(reason).force(force).build())
                .build();
        xaddCommand(Constants.QueueNames.workerCtrlStream(workerId), command);
        LOG.info("[WorkerManager] Evicted worker: {} force={} reason={}", workerId, force, reason);
    }

    // ------------------------------------------------------------------
    // Agent-type admission control
    // ------------------------------------------------------------------

    /**
     * Prevent worker_id from consuming the agent_type stream.
     *
     * @param agentType Agent type
     * @param workerId  Worker ID
     */
    public void denyWorkerForType(String agentType, String workerId) {
        registry.denyWorkerForType(agentType, workerId);
        LOG.info("[WorkerManager] Denied worker {} for agent type {}", workerId, agentType);
    }

    /**
     * Remove worker_id from the denylist for agent_type.
     *
     * @param agentType Agent type
     * @param workerId  Worker ID
     */
    public void allowWorkerForType(String agentType, String workerId) {
        registry.allowWorkerForType(agentType, workerId);
        LOG.info("[WorkerManager] Allowed worker {} for agent type {}", workerId, agentType);
    }

    /**
     * Return all worker_ids currently denied for agent_type.
     *
     * @param agentType Agent type
     * @return List of denied worker IDs
     */
    public List<String> getTypeDenylist(String agentType) {
        return registry.getAgentTypeDenylist(agentType);
    }

    // ------------------------------------------------------------------
    // Status queries
    // ------------------------------------------------------------------

    /**
     * Return the admin-controlled state for a worker.
     *
     * <p>Returns an empty map when no admin state has been set (default active).
     *
     * @param workerId Worker ID
     * @return Map with fields: lifecycle, reason, updated_at
     */
    public Map<String, String> getWorkerAdminState(String workerId) {
        return registry.getWorkerAdminState(workerId);
    }

    /**
     * Remove all admin state for a worker, restoring default-active behaviour.
     *
     * @param workerId Worker ID
     */
    public void clearWorkerAdminState(String workerId) {
        registry.clearWorkerAdminState(workerId);
        LOG.info("[WorkerManager] Cleared admin state for worker: {}", workerId);
    }

    // ------------------------------------------------------------------
    // Private helpers
    // ------------------------------------------------------------------

    private static MessageHeader adminHeader() {
        return MessageHeader.builder()
                .sessionId("admin")
                .traceId(UUID.randomUUID().toString().replace("-", ""))
                .messageId(UUID.randomUUID().toString().replace("-", ""))
                .build();
    }

    private void xaddCommand(String streamKey, Object command) {
        try {
            String json = OBJECT_MAPPER.writeValueAsString(command);
            Map<String, String> fields = new HashMap<>();
            fields.put(Constants.RedisFields.DATA, json);
            streamOps.xadd(streamKey, fields, XAddOptions.noTrim());
        } catch (Exception e) {
            LOG.error("[WorkerManager] Failed to push command to {}: {}", streamKey, e.getMessage());
        }
    }
}
