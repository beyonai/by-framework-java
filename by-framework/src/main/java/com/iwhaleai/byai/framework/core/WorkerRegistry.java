package com.iwhaleai.byai.framework.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.core.protocol.AgentState;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.SetParams;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Worker Registry for worker registration, discovery, and execution tracking.
 * Uses Redis sorted sets and hash structures for storage.
 */
public class WorkerRegistry {
    private final RedisClient redisClient;
    private final Map<String, String> lockTokens = new HashMap<>();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int PRESENCE_PAYLOAD_VERSION = 1;
    private static final TypeReference<Map<String, Object>> MAP_TYPE_REF = new TypeReference<>() {
    };
    private static final Random RANDOM = new Random();
    private static final String LOCAL_IP = getLocalIpAddress();

    private record WorkerPresence(String token, long lastSeen, boolean legacy, String ipAddress) {
        WorkerPresence(String token, long lastSeen, boolean legacy) {
            this(token, lastSeen, legacy, "");
        }
    }

    public WorkerRegistry(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    /**
     * Register worker membership for agent types.
     * This adds the worker to the agentTypeMembers set for each agent type.
     *
     * @param workerId Worker ID
     * @param agentTypes List of agent types this worker can handle
     */
    public synchronized void registerWorkerMembership(String workerId, List<String> agentTypes) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.sadd(Constants.RegistryKeys.knownWorkers(), workerId);
            if (agentTypes != null && !agentTypes.isEmpty()) {
                for (String agentType : agentTypes) {
                    jedis.sadd(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId), agentType);
                    jedis.sadd(Constants.RegistryKeys.agentTypeMembers(agentType), workerId);
                }
            }
        }
    }

    /**
     * Send heartbeat to maintain worker online lease.
     * Updates the token-owned workerOnlineLease key with TTL.
     *
     * @param workerId Worker ID
     * @param leaseTtlSeconds TTL for the lease key in seconds
     */
    public boolean heartbeatWorker(String workerId, int leaseTtlSeconds) {
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.RegistryKeys.workerOnlineLease(workerId);
            String token = lockTokens.get(workerId);
            String current = jedis.get(key);
            WorkerPresence currentPresence = decodeWorkerPresence(current);
            long now = System.currentTimeMillis();

            if (token != null) {
                if (current == null) {
                    String result = jedis.set(
                            key,
                            encodeWorkerPresence(token, now, LOCAL_IP),
                            SetParams.setParams().nx().ex(leaseTtlSeconds));
                    if (!"OK".equalsIgnoreCase(result)) {
                        return false;
                    }
                } else if (!token.equals(currentPresence.token())) {
                    return false;
                } else {
                    jedis.setex(key, leaseTtlSeconds, encodeWorkerPresence(token, now, LOCAL_IP));
                }
            } else {
                if (currentPresence.token() != null) {
                    return false;
                }
                jedis.setex(key, leaseTtlSeconds, encodeWorkerPresence(null, now, LOCAL_IP));
            }

            jedis.sadd(Constants.RegistryKeys.knownWorkers(), workerId);
            return true;
        }
    }

    public void heartbeatWorker(String workerId) {
        heartbeatWorker(workerId, Constants.WORKER_DEFAULT_LEASE_TTL_SECONDS);
    }

    /**
     * Unregister worker membership for all agent types.
     * This removes the worker from all agentTypeMembers sets.
     *
     * @param workerId Worker ID
     */
    public synchronized void unregisterWorkerMembership(String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> agentTypes = jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId));
            jedis.del(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId));
            jedis.srem(Constants.RegistryKeys.knownWorkers(), workerId);
            if (agentTypes != null) {
                for (String agentType : agentTypes) {
                    jedis.srem(Constants.RegistryKeys.agentTypeMembers(agentType), workerId);
                }
            }
        }
    }

    /**
     * Mark worker as inactive by removing its lease key.
     *
     * @param workerId Worker ID
     */
    public boolean markWorkerInactive(String workerId) {
        return markWorkerInactive(workerId, null);
    }

    public boolean markWorkerInactive(String workerId, String token) {
        String expected = token != null ? token : lockTokens.get(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            String key = Constants.RegistryKeys.workerOnlineLease(workerId);
            WorkerPresence currentPresence = decodeWorkerPresence(jedis.get(key));
            if (expected != null && !expected.equals(currentPresence.token())) {
                return false;
            }
            jedis.del(key);
            return true;
        }
    }

    /**
     * Check if a worker is online (alive).
     * Checks the workerOnlineLease presence payload.
     *
     * @param workerId Worker ID
     * @return true if worker is alive (lease key exists)
     */
    public boolean isWorkerOnline(String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            String leaseValue = jedis.get(Constants.RegistryKeys.workerOnlineLease(workerId));
            if (leaseValue == null) {
                return false;
            }
            WorkerPresence presence = decodeWorkerPresence(leaseValue);
            return presence.legacy() || presence.lastSeen() > 0;
        }
    }

    /**
     * Get online workers for an agent type.
     *
     * @param agentType Agent type identifier
     * @return List of online worker IDs
     */
    public List<String> getOnlineWorkers(String agentType) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> workers = jedis.smembers(Constants.RegistryKeys.agentTypeMembers(agentType));
            if (workers == null || workers.isEmpty()) {
                return Collections.emptyList();
            }

            List<String> onlineWorkers = new ArrayList<>();
            for (String workerId : workers) {
                if (isWorkerOnline(workerId)) {
                    onlineWorkers.add(workerId);
                }
            }
            return onlineWorkers;
        }
    }

    /**
     * Get a RANDOM online worker for an agent type.
     *
     * @param agentType Agent type identifier
     * @return Random online worker ID, or null if none available
     */
    public String getRandomOnlineWorker(String agentType) {
        List<String> onlineWorkers = getOnlineWorkers(agentType);
        if (onlineWorkers.isEmpty()) {
            return null;
        }
        return onlineWorkers.get(RANDOM.nextInt(onlineWorkers.size()));
    }

    /**
     * Get target worker for an agent type (alias for getRandomOnlineWorker).
     *
     * @param agentType Agent type identifier
     * @return Random online worker ID, or null if none available
     */
    public String getTargetWorker(String agentType) {
        return getRandomOnlineWorker(agentType);
    }

    public static class OnlineAgentCheckResult {
        public final boolean exists;
        public final List<String> workerIds;

        public OnlineAgentCheckResult(boolean exists, List<String> workerIds) {
            this.exists = exists;
            this.workerIds = workerIds;
        }
    }

    /**
     * Check if an agent type has any registered and alive workers.
     *
     * @param agentType Agent type identifier
     * @param checkActive Whether to check worker liveness (default true)
     * @return (exists, worker_ids_list)
     */
    public OnlineAgentCheckResult hasOnlineAgentType(String agentType, boolean checkActive) {
        return hasOnlineAgentType(agentType, checkActive, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
    }

    public OnlineAgentCheckResult hasOnlineAgentType(String agentType, boolean checkActive, long healthThresholdMs) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> workers = jedis.smembers(Constants.RegistryKeys.agentTypeMembers(agentType));
            if (workers == null || workers.isEmpty()) {
                return new OnlineAgentCheckResult(false, Collections.emptyList());
            }

            List<String> workerIds = new ArrayList<>(workers);

            if (checkActive) {
                List<String> onlineWorkerIds = new ArrayList<>();
                for (String workerId : workerIds) {
                    if (isWorkerOnline(workerId)) {
                        onlineWorkerIds.add(workerId);
                    }
                }
                workerIds = onlineWorkerIds;
            }

            return new OnlineAgentCheckResult(!workerIds.isEmpty(), workerIds);
        }
    }

    /**
     * Claim exclusive lock for worker ID.
     *
     * @param workerId Worker ID
     * @param ttlSeconds Lock TTL in seconds
     * @return Lock token
     * @throws RuntimeException if worker_id is already in use
     */
    public String claimWorkerId(String workerId, int ttlSeconds) {
        String presenceKey = Constants.RegistryKeys.workerOnlineLease(workerId);
        String token = java.util.UUID.randomUUID().toString();
        try (Jedis jedis = redisClient.getResource()) {
            String result = jedis.set(
                    presenceKey,
                    encodeWorkerPresence(token, 0, LOCAL_IP),
                    SetParams.setParams().nx().ex(ttlSeconds));
            if (!"OK".equalsIgnoreCase(result)) {
                throw new RuntimeException("worker_id already in use: " + workerId);
            }
            lockTokens.put(workerId, token);
            jedis.sadd(Constants.RegistryKeys.knownWorkers(), workerId);
            return token;
        }
    }

    public String claimWorkerId(String workerId) {
        return claimWorkerId(workerId, Constants.WORKER_LOCK_TTL_SECONDS);
    }

    /**
     * Refresh worker ID lock TTL.
     *
     * @param workerId Worker ID
     * @param ttlSeconds New TTL in seconds
     * @return true if refresh successful
     */
    public boolean refreshWorkerIdLock(String workerId, int ttlSeconds) {
        String token = lockTokens.get(workerId);
        if (token == null) {
            return false;
        }

        String presenceKey = Constants.RegistryKeys.workerOnlineLease(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            WorkerPresence presence = decodeWorkerPresence(jedis.get(presenceKey));
            if (token.equals(presence.token())) {
                return jedis.expire(presenceKey, ttlSeconds) > 0;
            }
            return false;
        }
    }

    public boolean refreshWorkerIdLock(String workerId) {
        return refreshWorkerIdLock(workerId, Constants.WORKER_LOCK_TTL_SECONDS);
    }

    /**
     * Release worker ID lock.
     *
     * @param workerId Worker ID
     * @param token Lock token
     * @return true if release successful
     */
    public boolean releaseWorkerId(String workerId, String token) {
        String expected = token != null ? token : lockTokens.get(workerId);
        if (expected == null) {
            return false;
        }

        String presenceKey = Constants.RegistryKeys.workerOnlineLease(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            WorkerPresence presence = decodeWorkerPresence(jedis.get(presenceKey));
            if (expected.equals(presence.token())) {
                jedis.del(presenceKey);
                lockTokens.remove(workerId);
                return true;
            }
            return false;
        }
    }

    public Map<String, Object> getWorker(String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            String rawPresence = jedis.get(Constants.RegistryKeys.workerOnlineLease(workerId));
            if (rawPresence == null) {
                return null;
            }
            WorkerPresence presence = decodeWorkerPresence(rawPresence);
            if (!presence.legacy() && presence.lastSeen() <= 0) {
                return null;
            }

            Set<String> agentTypes = jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId));
            Map<String, Object> result = new HashMap<>();
            result.put("agent_types", agentTypes);
            result.put("last_seen", presence.legacy() ? System.currentTimeMillis() : presence.lastSeen());
            result.put("ip_address", presence.ipAddress());
            return result;
        }
    }

    public Map<String, Map<String, Object>> getAllWorkers() {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> workerIds = jedis.smembers(Constants.RegistryKeys.knownWorkers());
            Map<String, Map<String, Object>> result = new HashMap<>();
            if (workerIds != null) {
                for (String id : workerIds) {
                    Map<String, Object> data = getWorker(id);
                    if (data != null) {
                        Map<String, String> adminState = getWorkerAdminState(id);
                        data.put("lifecycle", adminState.getOrDefault("lifecycle", "active"));
                        data.put("lifecycle_reason", adminState.getOrDefault("reason", ""));
                        result.put(id, data);
                    }
                }
            }
            return result;
        }
    }

    /**
     * Set admin-controlled lifecycle state for a worker.
     *
     * @param workerId  Worker ID
     * @param lifecycle One of "active", "suspended", "evicted"
     * @param reason    Human-readable reason for the state change
     */
    public synchronized void setWorkerAdminState(String workerId, String lifecycle, String reason) {
        String key = Constants.RegistryKeys.workerAdminState(workerId);
        long now = System.currentTimeMillis();
        try (Jedis jedis = redisClient.getResource()) {
            jedis.hset(key, "lifecycle", lifecycle);
            jedis.hset(key, "reason", reason != null ? reason : "");
            jedis.hset(key, "updated_at", String.valueOf(now));
        }
    }

    /**
     * Get admin-controlled state for a worker.
     *
     * @param workerId Worker ID
     * @return Map with fields: lifecycle, reason, updated_at (empty map if not set)
     */
    public Map<String, String> getWorkerAdminState(String workerId) {
        String key = Constants.RegistryKeys.workerAdminState(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            Map<String, String> raw = jedis.hgetAll(key);
            return raw != null ? raw : new HashMap<>();
        }
    }

    /**
     * Remove admin state for a worker, restoring default-active behaviour.
     *
     * @param workerId Worker ID
     */
    public synchronized void clearWorkerAdminState(String workerId) {
        String key = Constants.RegistryKeys.workerAdminState(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            jedis.del(key);
        }
    }

    /**
     * SREM worker_id from every agent_type:members set it currently belongs to.
     * Preserves the declared-agent-types key so membership can be restored later.
     * Used by suspend and evict to make the worker immediately invisible to routing.
     *
     * @param workerId Worker ID
     */
    public synchronized void removeWorkerFromTypeMembers(String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> agentTypes = jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId));
            if (agentTypes != null) {
                for (String agentType : agentTypes) {
                    jedis.srem(Constants.RegistryKeys.agentTypeMembers(agentType), workerId);
                }
            }
        }
    }

    /**
     * SADD worker_id back to every agent_type:members set it declared.
     * Used by resume to make the worker immediately visible to routing again.
     * Denylist is still respected — denied types are excluded.
     *
     * @param workerId Worker ID
     */
    public synchronized void restoreWorkerToTypeMembers(String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> agentTypes = jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes(workerId));
            if (agentTypes != null) {
                for (String agentType : agentTypes) {
                    if (!isWorkerDeniedForType(agentType, workerId)) {
                        jedis.sadd(Constants.RegistryKeys.agentTypeMembers(agentType), workerId);
                    }
                }
            }
        }
    }

    /**
     * Add worker_id to the denylist for agent_type.
     * The worker will stop being added to agent_type:workers and skip XREADGROUP for that stream.
     *
     * @param agentType Agent type
     * @param workerId  Worker ID
     */
    public synchronized void denyWorkerForType(String agentType, String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.sadd(Constants.RegistryKeys.agentTypeDenied(agentType), workerId);
            jedis.srem(Constants.RegistryKeys.agentTypeMembers(agentType), workerId);
        }
    }

    /**
     * Remove worker_id from the denylist for agent_type.
     *
     * @param agentType Agent type
     * @param workerId  Worker ID
     */
    public synchronized void allowWorkerForType(String agentType, String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.srem(Constants.RegistryKeys.agentTypeDenied(agentType), workerId);
        }
    }

    /**
     * Return true if worker_id is on the denylist for agent_type.
     *
     * @param agentType Agent type
     * @param workerId  Worker ID
     * @return true if denied
     */
    public boolean isWorkerDeniedForType(String agentType, String workerId) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.sismember(Constants.RegistryKeys.agentTypeDenied(agentType), workerId);
        }
    }

    /**
     * Return all worker_ids on the denylist for agent_type.
     *
     * @param agentType Agent type
     * @return List of denied worker IDs
     */
    public List<String> getAgentTypeDenylist(String agentType) {
        try (Jedis jedis = redisClient.getResource()) {
            Set<String> members = jedis.smembers(Constants.RegistryKeys.agentTypeDenied(agentType));
            return members != null ? new ArrayList<>(members) : Collections.emptyList();
        }
    }

    private static String encodeWorkerPresence(String token, long lastSeen, String ipAddress) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("version", PRESENCE_PAYLOAD_VERSION);
        payload.put("token", token);
        payload.put("last_seen", lastSeen);
        payload.put("ip_address", ipAddress != null ? ipAddress : "");
        try {
            return OBJECT_MAPPER.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize worker presence", e);
        }
    }

    private static WorkerPresence decodeWorkerPresence(String raw) {
        if (raw == null) {
            return new WorkerPresence(null, 0, false, "");
        }
        try {
            Map<String, Object> payload = OBJECT_MAPPER.readValue(raw, MAP_TYPE_REF);
            Object tokenValue = payload.get("token");
            Object lastSeenValue = payload.getOrDefault("last_seen", 0);
            String token = tokenValue == null ? null : String.valueOf(tokenValue);
            long lastSeen = lastSeenValue instanceof Number number
                    ? number.longValue()
                    : Long.parseLong(String.valueOf(lastSeenValue));
            String ipAddress = payload.get("ip_address") instanceof String s ? s : "";
            return new WorkerPresence(token, lastSeen, false, ipAddress);
        } catch (Exception ignored) {
            if ("1".equals(raw)) {
                return new WorkerPresence(null, 0, true, "");
            }
            return new WorkerPresence(raw, 0, true, "");
        }
    }

    private static String getLocalIpAddress() {
        try {
            // Try to find a non-loopback address via UDP trick (no actual connection made)
            try (java.net.DatagramSocket socket = new java.net.DatagramSocket()) {
                socket.connect(java.net.InetAddress.getByName("8.8.8.8"), 80);
                String addr = socket.getLocalAddress().getHostAddress();
                if (addr != null && !addr.isBlank() && !addr.startsWith("0.")) return addr;
            } catch (Exception ignored2) { /* fall through */ }
            java.net.InetAddress local = java.net.InetAddress.getLocalHost();
            if (!local.isLoopbackAddress()) return local.getHostAddress();
        } catch (Exception ignored) { /* best effort */ }
        return "";
    }

    public synchronized void saveExecution(Map<String, Object> execution) {
        String executionId = String.valueOf(execution.get(Constants.ExecutionFields.EXECUTION_ID));
        String messageId = String.valueOf(execution.get(Constants.ExecutionFields.MESSAGE_ID));
        String sessionId = String.valueOf(execution.get(Constants.ExecutionFields.SESSION_ID));

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        String encodedData;
        try {
            encodedData = OBJECT_MAPPER.writeValueAsString(execution);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize execution", e);
        }

        try (Jedis jedis = redisClient.getResource()) {
            try (Pipeline pipe = jedis.pipelined()) {
                pipe.hset(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId, encodedData);
                pipe.hset(regKey, Constants.RegistryFields.MSG_MAP_PREFIX + messageId, executionId);
                pipe.expire(regKey, Constants.DEFAULT_SESSION_TTL);
                pipe.sync();
            }
        }
    }

    public synchronized Map<String, Object> getExecution(String executionId, String sessionId) {
        if (sessionId == null || sessionId.isEmpty()) {
            return null;
        }

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            String data = jedis.hget(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId);
            if (data == null || data.isEmpty()) {
                return null;
            }
            try {
                return OBJECT_MAPPER.readValue(data, MAP_TYPE_REF);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to deserialize execution", e);
            }
        }
    }

    public synchronized Map<String, Object> getExecutionByMessageId(String messageId, String sessionId) {
        if (sessionId == null || sessionId.isEmpty()) {
            return null;
        }

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            String executionId = jedis.hget(regKey, Constants.RegistryFields.MSG_MAP_PREFIX + messageId);
            if (executionId == null || executionId.isEmpty()) {
                return null;
            }
            return getExecution(executionId, sessionId);
        }
    }

    /**
     * Initialize a new execution with QUEUED status and timestamps.
     * Called when a message is first dispatched (from client or agent context).
     *
     * @param executionId Execution ID
     * @param messageId Message ID
     * @param sessionId Session ID
     * @param targetAgentType Target agent type
     * @param parentMessageId Parent message ID (for tracking hierarchy)
     */
    public synchronized void initializeExecution(String executionId, String messageId, String sessionId,
                                                  String targetAgentType, String parentMessageId) {
        initializeExecution(executionId, messageId, sessionId, targetAgentType, parentMessageId, "");
    }

    /**
     * Initialize a new execution with QUEUED status, timestamps, and trace id.
     *
     * @param executionId Execution ID
     * @param messageId Message ID
     * @param sessionId Session ID
     * @param targetAgentType Target agent type
     * @param parentMessageId Parent message ID (for tracking hierarchy)
     * @param traceId Trace ID for cancellation and observability correlation
     */
    public synchronized void initializeExecution(String executionId, String messageId, String sessionId,
                                                  String targetAgentType, String parentMessageId, String traceId) {
        long now = System.currentTimeMillis();
        Map<String, Object> execution = new HashMap<>();
        execution.put(Constants.ExecutionFields.EXECUTION_ID, executionId);
        execution.put(Constants.ExecutionFields.MESSAGE_ID, messageId);
        execution.put(Constants.ExecutionFields.SESSION_ID, sessionId);
        execution.put(Constants.ExecutionFields.TRACE_ID, traceId != null ? traceId : "");
        execution.put(Constants.ExecutionFields.TARGET_AGENT_TYPE, targetAgentType != null ? targetAgentType : "");
        execution.put(Constants.ExecutionFields.STATUS, AgentState.QUEUED);
        execution.put(Constants.ExecutionFields.CREATED_AT, now);
        execution.put(Constants.ExecutionFields.UPDATED_AT, now);
        if (parentMessageId != null && !parentMessageId.isEmpty()) {
            execution.put("parent_message_id", parentMessageId);
        }

        List<Map<String, Object>> timeline = new ArrayList<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("status", AgentState.QUEUED);
        entry.put("timestamp", now);
        timeline.add(entry);
        execution.put("timeline", timeline);

        saveExecution(execution);
    }

    /**
     * Update execution status with timeline tracking.
     * Appends a new timeline entry and updates the status/updated_at fields.
     *
     * @param executionId Execution ID
     * @param sessionId Session ID
     * @param status New status
     * @param extraFields Additional fields to update (e.g., worker_id)
     */
    public synchronized void updateExecutionStatus(String executionId, String sessionId, String status,
                                                    Map<String, Object> extraFields) {
        Map<String, Object> current = getExecution(executionId, sessionId);
        if (current == null) {
            return;
        }

        long now = System.currentTimeMillis();
        current.put(Constants.ExecutionFields.STATUS, status);
        current.put(Constants.ExecutionFields.UPDATED_AT, now);

        if (extraFields != null) {
            current.putAll(extraFields);
        }

        // Append timeline entry
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> timeline = (List<Map<String, Object>>) current.getOrDefault("timeline", new ArrayList<>());
        Map<String, Object> entry = new HashMap<>();
        entry.put("status", status);
        entry.put("timestamp", now);
        timeline.add(entry);
        current.put("timeline", timeline);

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            try (Pipeline pipe = jedis.pipelined()) {
                pipe.hset(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId, OBJECT_MAPPER.writeValueAsString(current));
                pipe.expire(regKey, Constants.DEFAULT_SESSION_TTL);
                pipe.sync();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize execution", e);
            }
        }
    }

    public void updateExecutionStatus(String executionId, String sessionId, String status) {
        updateExecutionStatus(executionId, sessionId, status, null);
    }

    /**
     * Update execution status by looking up message ID first.
     *
     * @param messageId Message ID
     * @param sessionId Session ID
     * @param status New status
     * @param extraFields Additional fields to update
     */
    public synchronized void updateExecutionStatusByMessageId(String messageId, String sessionId, String status,
                                                               Map<String, Object> extraFields) {
        Map<String, Object> execution = getExecutionByMessageId(messageId, sessionId);
        if (execution == null) {
            return;
        }
        String executionId = String.valueOf(execution.get(Constants.ExecutionFields.EXECUTION_ID));
        updateExecutionStatus(executionId, sessionId, status, extraFields);
    }

    /**
     * Mark an execution as cancel-requested without changing its status.
     * Used to flag completed parent agents so cancelled children know not to callback.
     *
     * @param executionId Execution ID
     * @param sessionId Session ID
     * @param reason Cancel reason
     */
    public synchronized void markCancelRequested(String executionId, String sessionId, String reason) {
        Map<String, Object> current = getExecution(executionId, sessionId);
        if (current == null) {
            return;
        }

        current.put(Constants.ExecutionFields.CANCEL_REQUESTED, true);
        if (reason != null && !reason.isEmpty()) {
            current.put(Constants.ExecutionFields.CANCEL_REASON, reason);
        }
        current.put(Constants.ExecutionFields.UPDATED_AT, System.currentTimeMillis());

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            try (Pipeline pipe = jedis.pipelined()) {
                pipe.hset(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId, OBJECT_MAPPER.writeValueAsString(current));
                pipe.expire(regKey, Constants.DEFAULT_SESSION_TTL);
                pipe.sync();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize execution", e);
            }
        }
    }

    /**
     * Get all executions for a session.
     * Fetches all entries from the session registry hash that have the exec: prefix.
     *
     * @param sessionId Session ID
     * @return Map of executionId -> execution data
     */
    public synchronized Map<String, Map<String, Object>> getAllSessionExecutions(String sessionId) {
        if (sessionId == null || sessionId.isEmpty()) {
            return Collections.emptyMap();
        }

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        Map<String, Map<String, Object>> result = new HashMap<>();

        try (Jedis jedis = redisClient.getResource()) {
            Map<String, String> allFields = jedis.hgetAll(regKey);
            if (allFields == null) {
                return result;
            }

            for (Map.Entry<String, String> entry : allFields.entrySet()) {
                if (entry.getKey().startsWith(Constants.RegistryFields.EXEC_PREFIX)) {
                    try {
                        Map<String, Object> execData = OBJECT_MAPPER.readValue(entry.getValue(), MAP_TYPE_REF);
                        String execId = entry.getKey().substring(Constants.RegistryFields.EXEC_PREFIX.length());
                        result.put(execId, execData);
                    } catch (JsonProcessingException e) {
                        // skip malformed entries
                    }
                }
            }
        }

        return result;
    }

    public synchronized void markExecutionCancelling(String executionId, String sessionId, String reason) {
        Map<String, Object> current = getExecution(executionId, sessionId);
        if (current == null) {
            return;
        }

        current.put(Constants.ExecutionFields.STATUS, AgentState.CANCELLING);
        current.put(Constants.ExecutionFields.CANCEL_REQUESTED, true);
        current.put(Constants.ExecutionFields.CANCEL_REASON, reason);
        current.put(Constants.ExecutionFields.UPDATED_AT, System.currentTimeMillis());

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            try (Pipeline pipe = jedis.pipelined()) {
                pipe.hset(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId, OBJECT_MAPPER.writeValueAsString(current));
                pipe.expire(regKey, Constants.DEFAULT_SESSION_TTL);
                pipe.sync();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize execution", e);
            }
        }
    }

    public synchronized void markExecutionFinished(String executionId, String sessionId, String status) {
        Map<String, Object> current = getExecution(executionId, sessionId);
        if (current == null) {
            return;
        }

        long now = System.currentTimeMillis();
        current.put(Constants.ExecutionFields.STATUS, status);
        current.put(Constants.ExecutionFields.FINISHED_AT, now);
        current.put(Constants.ExecutionFields.UPDATED_AT, now);

        String regKey = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            try (Pipeline pipe = jedis.pipelined()) {
                pipe.hset(regKey, Constants.RegistryFields.EXEC_PREFIX + executionId, OBJECT_MAPPER.writeValueAsString(current));
                pipe.expire(regKey, Constants.DEFAULT_SESSION_TTL);
                pipe.sync();
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize execution", e);
            }
        }
    }
}
