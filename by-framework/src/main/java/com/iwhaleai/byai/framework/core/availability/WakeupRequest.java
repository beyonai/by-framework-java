package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Wakeup request emitted to the management stream.
 * Field names and structure are aligned with Python's WakeupRequest for cross-SDK compatibility.
 */
@Data
@Builder
public class WakeupRequest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String executionId;
    private String targetAgentType;
    private String sessionId;
    private String traceId;
    private String messageId;
    private String source;
    private String policy;
    private long timeoutMs;
    private String userCode;
    private String region;
    private int priority;
    private Map<String, Object> metadata;
    private Map<String, Object> commandPayload;

    /**
     * Serialize to a Redis stream payload.
     * Matches Python's {@code {"data": json.dumps(asdict(request))}} format.
     */
    public Map<String, String> toRedisPayload() {
        try {
            Map<String, Object> fields = new HashMap<>();
            fields.put("execution_id", executionId);
            fields.put("target_agent_type", targetAgentType);
            fields.put("session_id", sessionId != null ? sessionId : "");
            fields.put("trace_id", traceId != null ? traceId : "");
            fields.put("message_id", messageId != null ? messageId : "");
            fields.put("source", source != null ? source : "");
            fields.put("policy", policy != null ? policy : "");
            fields.put("timeout_ms", timeoutMs);
            fields.put("user_code", userCode != null ? userCode : "");
            fields.put("region", region != null ? region : "");
            fields.put("priority", priority);
            fields.put("metadata", metadata != null ? metadata : new HashMap<>());
            fields.put("command_payload", commandPayload != null ? commandPayload : new HashMap<>());

            Map<String, String> payload = new HashMap<>();
            payload.put("data", OBJECT_MAPPER.writeValueAsString(fields));
            return payload;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize WakeupRequest", e);
        }
    }

    /**
     * Construct from a Redis stream field map.
     * Handles the {@code {"data": "{...}"}} wrapper used by Python.
     */
    @SuppressWarnings("unchecked")
    public static WakeupRequest fromDict(Map<String, String> dict) {
        try {
            String dataJson = dict.get("data");
            if (dataJson == null) {
                return null;
            }
            Map<String, Object> fields = OBJECT_MAPPER.readValue(dataJson, Map.class);

            // Handle target_agent_type (Python) or agent_type (legacy Java)
            String targetAgentType = String.valueOf(fields.getOrDefault("target_agent_type", ""));
            if (targetAgentType.isEmpty()) {
                targetAgentType = String.valueOf(fields.getOrDefault("agent_type", ""));
            }

            // Handle timeout_ms (Python) or ttl_ms (legacy Java)
            long timeoutMs = parseLongField(fields, "timeout_ms");
            if (timeoutMs == 0) {
                timeoutMs = parseLongField(fields, "ttl_ms");
            }

            // Handle priority: may be int or string
            int priority = 0;
            Object priVal = fields.get("priority");
            if (priVal instanceof Number) {
                priority = ((Number) priVal).intValue();
            } else if (priVal instanceof String && !((String) priVal).isEmpty()) {
                try {
                    priority = Integer.parseInt((String) priVal);
                } catch (NumberFormatException ignored) {
                }
            }

            // Handle metadata: may be Map or JSON string
            Map<String, Object> metadata;
            Object metaVal = fields.get("metadata");
            if (metaVal instanceof Map) {
                metadata = (Map<String, Object>) metaVal;
            } else if (metaVal instanceof String && !((String) metaVal).isEmpty()) {
                metadata = OBJECT_MAPPER.readValue((String) metaVal, Map.class);
            } else {
                metadata = new HashMap<>();
            }

            // Handle command_payload: may be Map or JSON string
            Map<String, Object> commandPayload;
            Object cpVal = fields.get("command_payload");
            if (cpVal instanceof Map) {
                commandPayload = (Map<String, Object>) cpVal;
            } else if (cpVal instanceof String && !((String) cpVal).isEmpty()) {
                commandPayload = OBJECT_MAPPER.readValue((String) cpVal, Map.class);
            } else {
                commandPayload = new HashMap<>();
            }

            return WakeupRequest.builder()
                    .executionId(String.valueOf(fields.getOrDefault("execution_id", "")))
                    .targetAgentType(targetAgentType)
                    .sessionId(String.valueOf(fields.getOrDefault("session_id", "")))
                    .traceId(String.valueOf(fields.getOrDefault("trace_id", "")))
                    .messageId(String.valueOf(fields.getOrDefault("message_id", "")))
                    .source(String.valueOf(fields.getOrDefault("source", "")))
                    .policy(String.valueOf(fields.getOrDefault("policy", "")))
                    .timeoutMs(timeoutMs)
                    .userCode(String.valueOf(fields.getOrDefault("user_code", "")))
                    .region(String.valueOf(fields.getOrDefault("region", "")))
                    .priority(priority)
                    .metadata(metadata)
                    .commandPayload(commandPayload)
                    .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize WakeupRequest", e);
        }
    }

    private static long parseLongField(Map<String, Object> fields, String key) {
        Object value = fields.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException ignored) {
            }
        }
        return 0;
    }
}
