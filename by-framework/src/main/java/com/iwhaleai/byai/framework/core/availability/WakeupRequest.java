package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Wakeup request emitted to the management stream.
 * Mirrors Python's WakeupRequest.
 */
@Data
@Builder
public class WakeupRequest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String executionId;
    private String agentType;
    private String reason;
    private String priority;
    private long requestedAt;
    private long ttlMs;
    private Map<String, Object> metadata;

    /**
     * Serialize to a Redis stream payload (Map of field -> JSON string).
     */
    public Map<String, String> toRedisPayload() {
        try {
            Map<String, Object> fields = new HashMap<>();
            fields.put("execution_id", executionId);
            fields.put("agent_type", agentType);
            fields.put("reason", reason != null ? reason : "");
            fields.put("priority", priority != null ? priority : "");
            fields.put("requested_at", String.valueOf(requestedAt));
            fields.put("ttl_ms", String.valueOf(ttlMs));
            fields.put("metadata", OBJECT_MAPPER.writeValueAsString(metadata != null ? metadata : new HashMap<>()));

            Map<String, String> payload = new HashMap<>();
            payload.put("data", OBJECT_MAPPER.writeValueAsString(fields));
            return payload;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize WakeupRequest", e);
        }
    }

    /**
     * Construct from a Redis stream field map.
     */
    @SuppressWarnings("unchecked")
    public static WakeupRequest fromDict(Map<String, String> dict) {
        try {
            String dataJson = dict.get("data");
            if (dataJson == null) {
                return null;
            }
            Map<String, Object> fields = OBJECT_MAPPER.readValue(dataJson, Map.class);
            Map<String, Object> metadata;
            Object metaVal = fields.get("metadata");
            if (metaVal instanceof Map) {
                metadata = (Map<String, Object>) metaVal;
            } else if (metaVal instanceof String) {
                metadata = OBJECT_MAPPER.readValue((String) metaVal, Map.class);
            } else {
                metadata = new HashMap<>();
            }

            return WakeupRequest.builder()
                    .executionId(String.valueOf(fields.getOrDefault("execution_id", "")))
                    .agentType(String.valueOf(fields.getOrDefault("agent_type", "")))
                    .reason(String.valueOf(fields.getOrDefault("reason", "")))
                    .priority(String.valueOf(fields.getOrDefault("priority", "")))
                    .requestedAt(Long.parseLong(String.valueOf(fields.getOrDefault("requested_at", "0"))))
                    .ttlMs(Long.parseLong(String.valueOf(fields.getOrDefault("ttl_ms", "0"))))
                    .metadata(metadata)
                    .build();
        } catch (JsonProcessingException | NumberFormatException e) {
            throw new RuntimeException("Failed to deserialize WakeupRequest", e);
        }
    }
}
