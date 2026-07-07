package com.iwhaleai.byai.framework.common;

public class Constants {
    public static final String REDIS_PREFIX = "byai_gateway:";
    public static final String V2_PREFIX = "byai_gateway:v2:";
    public static final String SEPARATOR = ":";
    public static final int DEFAULT_SESSION_TTL = 7 * 24 * 3600;
    public static final int TRACE_TTL_SECONDS = 15 * 60;
    public static final java.util.Set<String> TERMINAL_STATES = java.util.Set.of("COMPLETED", "FAILED", "CANCELLED", "SUCCESS");

    /**
     * Return the configured Redis key schema version ("v1" or "v2").
     *
     * <p>Controlled by REDIS_KEY_SCHEMA_VERSION, defaulting to "v1" (the
     * current unprefixed key format). Cluster mode requires "v2" (see
     * RedisClient's fail-fast check).
     */
    public static String getKeySchemaVersion() {
        String version = com.iwhaleai.byai.framework.config.GatewayConfig.get(
                "REDIS_KEY_SCHEMA_VERSION", "v1");
        if (!"v1".equals(version) && !"v2".equals(version)) {
            throw new IllegalArgumentException(
                    "Invalid REDIS_KEY_SCHEMA_VERSION: " + version + " (must be 'v1' or 'v2')");
        }
        return version;
    }

    /**
     * Resolve a key according to REDIS_KEY_SCHEMA_VERSION.
     *
     * <p>v1 (default): returns v1Key unchanged, byte-for-byte. v2: returns
     * V2_PREFIX + v2Suffix, where v2Suffix already encodes any Cluster hash
     * tag needed for same-entity key groups.
     *
     * <p>Every key factory method routes through this one function so the
     * v1/v2 decision lives in exactly one place.
     */
    static String versioned(String v1Key, String v2Suffix) {
        if ("v2".equals(getKeySchemaVersion())) {
            return V2_PREFIX + v2Suffix;
        }
        return v1Key;
    }

    // Redis Stream field keys
    public static class RedisFields {
        public static final String DATA = "data";
        public static final String PAYLOAD = "payload";
        public static final String REPLY_DATA = "reply_data";
        public static final String STATUS = "status";
        public static final String CONTENT = "content";
        public static final String METADATA = "metadata";
        public static final String PAYLOAD_KEY = "payload";
    }

    // Execution map keys
    public static class ExecutionFields {
        public static final String EXECUTION_ID = "execution_id";
        public static final String MESSAGE_ID = "message_id";
        public static final String SESSION_ID = "session_id";
        public static final String TRACE_ID = "trace_id";
        public static final String WORKER_ID = "worker_id";
        public static final String STATUS = "status";
        public static final String TARGET_AGENT_TYPE = "target_agent_type";
        public static final String CREATED_AT = "created_at";
        public static final String FINISHED_AT = "finished_at";
        public static final String UPDATED_AT = "updated_at";
        public static final String CANCEL_REQUESTED = "cancel_requested";
        public static final String CANCEL_REASON = "cancel_reason";
    }

    // Dispatch group request keys
    public static class DispatchFields {
        public static final String AGENT_TYPE = "agent_type";
        public static final String CONTENT = "content";
        public static final String METADATA = "metadata";
        public static final String PAYLOAD = "payload";
    }

    // Session registry hash field prefixes
    public static class RegistryFields {
        public static final String EXEC_PREFIX = "exec:";
        public static final String MSG_MAP_PREFIX = "msg_map:";
    }

    // Timeout and limit constants
    public static final int LOOP_SLEEP_MS = 1000;
    public static final int REDIS_BLOCK_TIMEOUT_MS = 2000;
    public static final int REDIS_READ_BATCH_SIZE = 10;
    public static final int TERMINATION_TIMEOUT_SEC = 30;
    public static final int TERMINATION_GRACE_PERIOD_SEC = 10;
    // ID 前缀常量
public static final String MESSAGE_ID_PREFIX = "msg-";
public static final String EXECUTION_ID_PREFIX = "exec-";
public static final String TASK_GROUP_ID_PREFIX = "tg-";
public static final String CANCEL_MESSAGE_ID_PREFIX = "msg-cancel-";

// 任务组 Hash 字段
public static final String TASK_GROUP_FIELD_TOTAL = "total";
public static final String TASK_GROUP_FIELD_COMPLETED = "completed";
public static final String TASK_GROUP_FIELD_SOURCE_AGENT = "source_agent_type";

// 时间常量
public static final int WAIT_FOR_TASKS_TIMEOUT_SECONDS = 5;
public static final int TASK_GROUP_TTL_SECONDS = 86400;
public static final int FIRST_RETRY_WAIT_SECONDS = 1;
public static final int MAX_RETRY_COUNT = 3;
    public static final int WORKER_LOCK_TTL_SECONDS = 60;
    public static final int WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;
    public static final int WORKER_DEFAULT_LEASE_TTL_SECONDS = 15;
    public static final int SD_DEFAULT_HEALTH_THRESHOLD_MS = 30000;
    public static final int SD_NO_HEALTH_CHECK = -1;
    public static final int SD_NO_HEARTBEAT = 0;
    public static final int GROUP_HASH_LENGTH = 5;
    public static final int ID_SHORT_SUFFIX_LENGTH = 8;

    public static class QueueNames {

        /** Same-entity group with RegistryKeys.sessionRegistry: shares the session_id tag. */
        public static String sessionDataStream(String sessionId) {
            return versioned(
                    REDIS_PREFIX + String.format("session:%s:data_stream", sessionId),
                    String.format("session:{%s}:data_stream", sessionId));
        }

        public static String ctrlStream(String agentType) {
            return versioned(
                    REDIS_PREFIX + String.format("ctrl:agent_type:%s", agentType),
                    String.format("ctrl:agent_type:%s", agentType));
        }

        /** Same-entity group with the worker registry keys: shares the worker_id tag. */
        public static String workerCtrlStream(String workerId) {
            return versioned(
                    REDIS_PREFIX + "ctrl:worker:" + workerId,
                    "ctrl:worker:{" + workerId + "}");
        }

        // -- Control Plane (Agent Availability) --
        // Keys are aligned with Python's RedisKeys for cross-SDK compatibility.
        // Python ref: by_framework/common/constants.py

        /** Management stream for agent availability wakeup requests. */
        public static String controlPlaneManagementStream() {
            return versioned(REDIS_PREFIX + "control_plane:mgmt:wakeup", "control_plane:mgmt:wakeup");
        }

        /** Per-execution result stream for wakeup controller decisions. */
        public static String controlPlaneDecisionStream(String executionId) {
            return versioned(
                    REDIS_PREFIX + "control_plane:mgmt:wakeup:result:" + executionId,
                    "control_plane:mgmt:wakeup:result:" + executionId);
        }

        /** Pending delivery queue (single global stream, not per-agent-type). */
        public static String controlPlanePendingQueue() {
            return versioned(
                    REDIS_PREFIX + "control_plane:mgmt:delivery:pending", "control_plane:mgmt:delivery:pending");
        }

        /** Dead letter stream for failed control-plane work. */
        public static String controlPlaneDeadletterStream() {
            return versioned(REDIS_PREFIX + "control_plane:mgmt:deadletter", "control_plane:mgmt:deadletter");
        }

        /** Circuit-breaker state key for an agent type. */
        public static String controlPlaneCircuitBreakerKey(String agentType) {
            return versioned(
                    REDIS_PREFIX + "control_plane:circuit:agent_type:" + agentType,
                    "control_plane:circuit:agent_type:" + agentType);
        }

        /** User/tenant quota state key. */
        public static String controlPlaneQuotaKey(String userCode) {
            return versioned(
                    REDIS_PREFIX + "control_plane:quota:user:" + userCode, "control_plane:quota:user:" + userCode);
        }

        /** Availability state key for an agent type. */
        public static String controlPlaneAgentAvailability(String agentType) {
            return versioned(
                    REDIS_PREFIX + "control_plane:availability:agent_type:" + agentType,
                    "control_plane:availability:agent_type:" + agentType);
        }

        /** Fallback routing state key for an agent type. */
        public static String controlPlaneAgentFallback(String agentType) {
            return versioned(
                    REDIS_PREFIX + "control_plane:fallback:agent_type:" + agentType,
                    "control_plane:fallback:agent_type:" + agentType);
        }

        /** Deduplication key for concurrent wakeup requests. */
        public static String controlPlaneWakeupDedupe(String agentType, String userCode, String region) {
            String suffix = "control_plane:wakeup:dedupe:"
                    + agentType + ":" + (userCode != null ? userCode : "") + ":"
                    + (region != null ? region : "");
            return versioned(REDIS_PREFIX + suffix, suffix);
        }
    }

    public static class RegistryKeys {

        /** Global index spanning every worker entity: no hash tag, still version-prefixed. */
        public static String knownWorkers() {
            return versioned(REDIS_PREFIX + "registry:workers", "registry:workers");
        }

        /** Same-entity group with the other worker keys: shares the worker_id tag. */
        public static String workerDeclaredAgentTypes(String workerId) {
            return versioned(
                    REDIS_PREFIX + String.format("registry:worker:agent_types:%s", workerId),
                    String.format("registry:worker:{%s}:agent_types", workerId));
        }

        /**
         * Mandatory same-entity group with agentTypeDenied: WorkerRegistry's
         * denyWorkerForType() writes both keys together and must keep working
         * atomically under Cluster.
         */
        public static String agentTypeMembers(String agentType) {
            return versioned(
                    REDIS_PREFIX + String.format("registry:agent_type:workers:%s", agentType),
                    String.format("registry:agent_type:{%s}:workers", agentType));
        }

        /** Same-entity group with the other worker keys: shares the worker_id tag. */
        public static String workerLock(String workerId) {
            return versioned(
                    REDIS_PREFIX + String.format("registry:worker:lock:%s", workerId),
                    String.format("registry:worker:{%s}:lock", workerId));
        }

        /** Same-entity group with the other worker keys: shares the worker_id tag. */
        public static String workerOnlineLease(String workerId) {
            return versioned(
                    REDIS_PREFIX + String.format("registry:worker:online:%s", workerId),
                    String.format("registry:worker:{%s}:online", workerId));
        }

        /**
         * SCAN MATCH glob pattern matching every workerOnlineLease key.
         *
         * <p>Under v1 the worker_id is a trailing suffix (prefix + id). Under
         * v2 it's wrapped in a Cluster hash tag in the middle of the key
         * (prefix + "{" + id + "}" + suffix) - a bare "{prefix}*" pattern (or
         * a String.startsWith("{prefix}") check) would never match a real v2
         * key, since "{"/"}" are literal characters in Redis's glob matching
         * (only *, ?, [seq] are special), not wildcards.
         */
        public static String workerOnlineLeaseScanPattern() {
            if ("v2".equals(getKeySchemaVersion())) {
                return V2_PREFIX + "registry:worker:{*}:online";
            }
            return REDIS_PREFIX + "registry:worker:online:*";
        }

        /** Extract worker_id from a key returned by scanning with workerOnlineLeaseScanPattern(). */
        public static String workerIdFromOnlineLeaseKey(String key) {
            if ("v2".equals(getKeySchemaVersion())) {
                String prefix = V2_PREFIX + "registry:worker:{";
                String suffix = "}:online";
                if (key.startsWith(prefix) && key.endsWith(suffix)) {
                    return key.substring(prefix.length(), key.length() - suffix.length());
                }
                return null;
            }
            String prefix = REDIS_PREFIX + "registry:worker:online:";
            if (key.startsWith(prefix)) {
                return key.substring(prefix.length());
            }
            return null;
        }

        /**
         * 会话级聚合注册表 (Hash)。
         * <p>
         * 内部分为以下 Field 类别：
         * - exec:{execution_id} -> 存储具体的执行明细 JSON
         * - msg_map:{message_id} -> 存储消息 ID 到执行 ID 的映射关系
         *
         * <p>Same-entity group with sessionDataStream: shares the session_id tag.
         */
        public static String sessionRegistry(String sessionId) {
            return versioned(
                    REDIS_PREFIX + String.format("session:%s:registry", sessionId),
                    String.format("session:{%s}:registry", sessionId));
        }

        /** Same-entity group with taskGroupResults: shares the group_id tag. */
        public static String taskGroup(String groupId) {
            return versioned(REDIS_PREFIX + "task_group:" + groupId, "task_group:{" + groupId + "}");
        }

        /** Same-entity group with taskGroup: shares the group_id tag. */
        public static String taskGroupResults(String groupId) {
            return versioned(
                    REDIS_PREFIX + "task_group:" + groupId + ":results",
                    "task_group:{" + groupId + "}:results");
        }

        // --- 服务发现 (Service Discovery) ---
        public static final int SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10;

        /** Global index spanning every service name: no hash tag, still version-prefixed. */
        public static String sdServices() {
            return versioned(REDIS_PREFIX + "sd:services", "sd:services");
        }

        /** Same-entity group with sdInstanceDetails: shares the service_name tag. */
        public static String sdActiveInstances(String serviceName) {
            return versioned(
                    REDIS_PREFIX + String.format("sd:active:%s", serviceName),
                    String.format("sd:{%s}:active", serviceName));
        }

        /** Same-entity group with sdActiveInstances: shares the service_name tag. */
        public static String sdInstanceDetails(String serviceName) {
            return versioned(
                    REDIS_PREFIX + String.format("sd:instances:%s", serviceName),
                    String.format("sd:{%s}:instances", serviceName));
        }

        /** Same-entity group with the other worker keys: shares the worker_id tag. */
        public static String workerAdminState(String workerId) {
            return versioned(
                    REDIS_PREFIX + "registry:worker:admin:" + workerId,
                    "registry:worker:{" + workerId + "}:admin");
        }

        /** Mandatory same-entity group with agentTypeMembers (see there for why). */
        public static String agentTypeDenied(String agentType) {
            return versioned(
                    REDIS_PREFIX + "registry:agent_type:denied:" + agentType,
                    "registry:agent_type:{" + agentType + "}:denied");
        }
    }

    public static class TraceKeys {

        /**
         * v1 keeps Java's historical by_framework:trace:* namespace (byte-for-byte
         * identical to Python's old format). v2 unifies onto the shared
         * byai_gateway:v2:trace:{id} format used by all three language SDKs
         * (Python/Java previously shared by_framework:trace:*, TS used a
         * different byai_gateway:trace:* layout - v2 replaces both). Same-entity
         * group with traceSpans: shares the trace_id tag.
         */
        public static String traceMeta(String traceId) {
            return versioned("by_framework:trace:" + traceId, "trace:{" + traceId + "}");
        }

        /** Same-entity group with traceMeta: shares the trace_id tag. */
        public static String traceSpans(String traceId) {
            return versioned("by_framework:trace:spans:" + traceId, "trace:spans:{" + traceId + "}");
        }

        /**
         * Cross-entity relative to the trace group (meta/spans): deliberately
         * untagged, only the prefix moves under v2.
         */
        public static String traceIndexSession(String sessionId) {
            return versioned("by_framework:trace:idx:session:" + sessionId, "trace:idx:session:" + sessionId);
        }

        /** Cross-entity, untagged - see traceIndexSession. */
        public static String traceIndexWorker(String workerId) {
            return versioned("by_framework:trace:idx:worker:" + workerId, "trace:idx:worker:" + workerId);
        }

        /** Cross-entity, untagged - see traceIndexSession. */
        public static String traceIndexAgent(String agentType) {
            return versioned("by_framework:trace:idx:agent:" + agentType, "trace:idx:agent:" + agentType);
        }
    }

    public static class ConsumerGroups {
        public static final String AGENT_ENGINES = REDIS_PREFIX + "consumer_group:agent_engines";

        public static String autoGroupName(java.util.List<String> agentTypes) {
            java.util.List<String> sorted = new java.util.ArrayList<>(agentTypes);
            java.util.Collections.sort(sorted);
            String payload = String.join(",", sorted);
            try {
                java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-1");
                byte[] hash = digest.digest(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < Math.min(hash.length, 5); i++) {
                    sb.append(String.format("%02x", hash[i]));
                }
                return AGENT_ENGINES + ":" + sb.toString();
            } catch (Exception e) {
                return AGENT_ENGINES + ":default";
            }
        }
    }
}
