package com.iwhaleai.byai.framework.common;

public class Constants {
    public static final String REDIS_PREFIX = "byai_gateway:";
    public static final String SEPARATOR = ":";
    public static final int DEFAULT_SESSION_TTL = 7 * 24 * 3600;
    public static final java.util.Set<String> TERMINAL_STATES = java.util.Set.of("COMPLETED", "FAILED", "CANCELLED", "SUCCESS");

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

        public static String sessionDataStream(String sessionId) {
            return REDIS_PREFIX + String.format("session:%s:data_stream", sessionId);
        }

        public static String ctrlStream(String agentType) {
            return REDIS_PREFIX + String.format("ctrl:agent_type:%s", agentType);
        }

        public static String workerCtrlStream(String workerId) {
            return REDIS_PREFIX + "ctrl:worker:" + workerId;
        }

        // -- Control Plane (Agent Availability) --
        // Keys are aligned with Python's RedisKeys for cross-SDK compatibility.
        // Python ref: by_framework/common/constants.py

        /** Management stream for agent availability wakeup requests. */
        public static String controlPlaneManagementStream() {
            return REDIS_PREFIX + "control_plane:mgmt:wakeup";
        }

        /** Per-execution result stream for wakeup controller decisions. */
        public static String controlPlaneDecisionStream(String executionId) {
            return REDIS_PREFIX + "control_plane:mgmt:wakeup:result:" + executionId;
        }

        /** Pending delivery queue (single global stream, not per-agent-type). */
        public static String controlPlanePendingQueue() {
            return REDIS_PREFIX + "control_plane:mgmt:delivery:pending";
        }

        /** Dead letter stream for failed control-plane work. */
        public static String controlPlaneDeadletterStream() {
            return REDIS_PREFIX + "control_plane:mgmt:deadletter";
        }

        /** Circuit-breaker state key for an agent type. */
        public static String controlPlaneCircuitBreakerKey(String agentType) {
            return REDIS_PREFIX + "control_plane:circuit:agent_type:" + agentType;
        }

        /** User/tenant quota state key. */
        public static String controlPlaneQuotaKey(String userCode) {
            return REDIS_PREFIX + "control_plane:quota:user:" + userCode;
        }

        /** Availability state key for an agent type. */
        public static String controlPlaneAgentAvailability(String agentType) {
            return REDIS_PREFIX + "control_plane:availability:agent_type:" + agentType;
        }

        /** Fallback routing state key for an agent type. */
        public static String controlPlaneAgentFallback(String agentType) {
            return REDIS_PREFIX + "control_plane:fallback:agent_type:" + agentType;
        }

        /** Deduplication key for concurrent wakeup requests. */
        public static String controlPlaneWakeupDedupe(String agentType, String userCode, String region) {
            return REDIS_PREFIX + "control_plane:wakeup:dedupe:"
                    + agentType + ":" + (userCode != null ? userCode : "") + ":"
                    + (region != null ? region : "");
        }
    }

    public static class RegistryKeys {
        public static final String KNOWN_WORKERS = REDIS_PREFIX + "registry:workers";

        public static String workerDeclaredAgentTypes(String workerId) {
            return REDIS_PREFIX + String.format("registry:worker:agent_types:%s", workerId);
        }

        public static String agentTypeMembers(String agentType) {
            return REDIS_PREFIX + String.format("registry:agent_type:workers:%s", agentType);
        }

        public static String workerLock(String workerId) {
            return REDIS_PREFIX + String.format("registry:worker:lock:%s", workerId);
        }

        public static String workerOnlineLease(String workerId) {
            return REDIS_PREFIX + String.format("registry:worker:online:%s", workerId);
        }

        public static String executionDetail(String executionId) {
            return REDIS_PREFIX + String.format("registry:execution:detail:%s", executionId);
        }

        public static String executionByMessage(String messageId) {
            return REDIS_PREFIX + String.format("registry:execution:by_message:%s", messageId);
        }

        public static String sessionExecutions(String sessionId) {
            return REDIS_PREFIX + String.format("registry:session:executions:%s", sessionId);
        }

        /**
         * 会话级聚合注册表 (Hash)。
         * <p>
         * 内部分为以下 Field 类别：
         * - exec:{execution_id} -> 存储具体的执行明细 JSON
         * - msg_map:{message_id} -> 存储消息 ID 到执行 ID 的映射关系
         */
        public static String sessionRegistry(String sessionId) {
            return REDIS_PREFIX + String.format("session:%s:registry", sessionId);
        }

        public static String taskGroup(String groupId) {
            return REDIS_PREFIX + "task_group:" + groupId;
        }

        public static String taskGroupResults(String groupId) {
            return REDIS_PREFIX + "task_group:" + groupId + ":results";
        }

        // --- 服务发现 (Service Discovery) ---
        public static final String SD_SERVICES = REDIS_PREFIX + "sd:services";
        public static final int SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10;

        public static String sdActiveInstances(String serviceName) {
            return REDIS_PREFIX + String.format("sd:active:%s", serviceName);
        }

        public static String sdInstanceDetails(String serviceName) {
            return REDIS_PREFIX + String.format("sd:instances:%s", serviceName);
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
