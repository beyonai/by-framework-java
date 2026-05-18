package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Pending delivery stored in Redis while waiting for a worker.
 * Mirrors Python's PendingDelivery.
 */
@Data
@Builder
public class PendingDelivery {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String executionId;
    private String messageId;
    private String sessionId;
    private String traceId;
    private String source;
    private String targetAgentType;
    private String userCode;
    private String region;
    private String priority;
    private String policy;
    private long queuedAt;
    private long timeoutMs;
    private Map<String, Object> commandPayload;
    private Map<String, Object> metadata;

    /**
     * Serialize to a Redis stream payload.
     */
    public Map<String, String> toRedisPayload() {
        try {
            Map<String, Object> fields = new HashMap<>();
            fields.put("execution_id", executionId);
            fields.put("message_id", messageId);
            fields.put("session_id", sessionId);
            fields.put("trace_id", traceId);
            fields.put("source", source != null ? source : "");
            fields.put("target_agent_type", targetAgentType);
            fields.put("user_code", userCode != null ? userCode : "");
            fields.put("region", region != null ? region : "");
            fields.put("priority", priority != null ? priority : "");
            fields.put("policy", policy != null ? policy : RoutePolicy.FAIL_FAST);
            fields.put("queued_at", String.valueOf(queuedAt));
            fields.put("timeout_ms", String.valueOf(timeoutMs));
            fields.put("command_payload", OBJECT_MAPPER.writeValueAsString(commandPayload != null ? commandPayload : new HashMap<>()));
            fields.put("metadata", OBJECT_MAPPER.writeValueAsString(metadata != null ? metadata : new HashMap<>()));

            Map<String, String> payload = new HashMap<>();
            payload.put("data", OBJECT_MAPPER.writeValueAsString(fields));
            return payload;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize PendingDelivery", e);
        }
    }

    /**
     * Construct from a Redis stream field map.
     */
    @SuppressWarnings("unchecked")
    public static PendingDelivery fromDict(Map<String, String> dict) {
        try {
            String dataJson = dict.get("data");
            if (dataJson == null) {
                return null;
            }
            Map<String, Object> fields = OBJECT_MAPPER.readValue(dataJson, Map.class);
            Map<String, Object> commandPayload;
            Object cpVal = fields.get("command_payload");
            if (cpVal instanceof Map) {
                commandPayload = (Map<String, Object>) cpVal;
            } else if (cpVal instanceof String) {
                commandPayload = OBJECT_MAPPER.readValue((String) cpVal, Map.class);
            } else {
                commandPayload = new HashMap<>();
            }

            Map<String, Object> metadata;
            Object metaVal = fields.get("metadata");
            if (metaVal instanceof Map) {
                metadata = (Map<String, Object>) metaVal;
            } else if (metaVal instanceof String) {
                metadata = OBJECT_MAPPER.readValue((String) metaVal, Map.class);
            } else {
                metadata = new HashMap<>();
            }

            return PendingDelivery.builder()
                    .executionId(String.valueOf(fields.getOrDefault("execution_id", "")))
                    .messageId(String.valueOf(fields.getOrDefault("message_id", "")))
                    .sessionId(String.valueOf(fields.getOrDefault("session_id", "")))
                    .traceId(String.valueOf(fields.getOrDefault("trace_id", "")))
                    .source(String.valueOf(fields.getOrDefault("source", "")))
                    .targetAgentType(String.valueOf(fields.getOrDefault("target_agent_type", "")))
                    .userCode(String.valueOf(fields.getOrDefault("user_code", "")))
                    .region(String.valueOf(fields.getOrDefault("region", "")))
                    .priority(String.valueOf(fields.getOrDefault("priority", "")))
                    .policy(String.valueOf(fields.getOrDefault("policy", RoutePolicy.FAIL_FAST)))
                    .queuedAt(Long.parseLong(String.valueOf(fields.getOrDefault("queued_at", "0"))))
                    .timeoutMs(Long.parseLong(String.valueOf(fields.getOrDefault("timeout_ms", "0"))))
                    .commandPayload(commandPayload)
                    .metadata(metadata)
                    .build();
        } catch (JsonProcessingException | NumberFormatException e) {
            throw new RuntimeException("Failed to deserialize PendingDelivery", e);
        }
    }
}
