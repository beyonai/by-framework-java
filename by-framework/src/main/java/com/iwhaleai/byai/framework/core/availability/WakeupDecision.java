package com.iwhaleai.byai.framework.core.availability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Decision returned by the wakeup controller.
 * Mirrors Python's WakeupDecision.
 */
@Data
@Builder
public class WakeupDecision {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private String executionId;
    private String targetAgentType;
    private String selectedAgentType;
    private String status;
    private String reason;
    private String workerId;
    private java.util.List<String> workerIds;
    private String region;
    private Long retryAfterMs;
    private long timestamp;

    /**
     * Construct from a Redis stream field map.
     * Handles the nested "data" wrapper used by the Python WakeupController
     * ({@code {"data": "{\"execution_id\":...}"}}) as well as flat field maps.
     */
    @SuppressWarnings("unchecked")
    public static WakeupDecision fromDict(Map<String, String> dict) {
        try {
            Map<String, Object> fields;
            String dataJson = dict.get("data");
            if (dataJson != null && !dataJson.isEmpty()) {
                fields = OBJECT_MAPPER.readValue(dataJson, Map.class);
            } else {
                // Fallback: flat field map (backward compatibility)
                fields = (Map<String, Object>) (Map<?, ?>) dict;
            }

            String selectedAgentType = String.valueOf(fields.getOrDefault("selected_agent_type", ""));
            String targetAgentType = String.valueOf(fields.getOrDefault("target_agent_type", ""));
            if (targetAgentType.isEmpty()) {
                targetAgentType = String.valueOf(fields.getOrDefault("agent_type", ""));
            }
            String region = String.valueOf(fields.getOrDefault("region", ""));

            java.util.List<String> workerIds = new java.util.ArrayList<>();
            Object widsVal = fields.get("worker_ids");
            if (widsVal instanceof java.util.List) {
                for (Object o : (java.util.List<?>) widsVal) {
                    workerIds.add(String.valueOf(o));
                }
            } else if (widsVal instanceof String && !((String) widsVal).isEmpty()) {
                String[] parts = ((String) widsVal).split(",");
                for (String part : parts) {
                    workerIds.add(part.trim());
                }
            }

            String workerId = String.valueOf(fields.getOrDefault("worker_id", ""));
            if (workerId.isEmpty() && !workerIds.isEmpty()) {
                workerId = workerIds.get(0);
            }

            Long retryAfterMs = null;
            Object ramVal = fields.get("retry_after_ms");
            if (ramVal instanceof Number) {
                retryAfterMs = ((Number) ramVal).longValue();
            } else if (ramVal instanceof String && !((String) ramVal).isEmpty()) {
                try {
                    retryAfterMs = Long.parseLong((String) ramVal);
                } catch (NumberFormatException ignored) {
                }
            }

            return WakeupDecision.builder()
                    .executionId(String.valueOf(fields.getOrDefault("execution_id", "")))
                    .targetAgentType(targetAgentType)
                    .selectedAgentType(selectedAgentType)
                    .status(String.valueOf(fields.getOrDefault("status", WakeupDecisionStatus.FAILED)))
                    .reason(String.valueOf(fields.getOrDefault("reason", "")))
                    .workerId(workerId)
                    .workerIds(workerIds)
                    .region(region)
                    .retryAfterMs(retryAfterMs)
                    .timestamp(parseLong(fields.getOrDefault("timestamp", "0")))
                    .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize WakeupDecision", e);
        }
    }

    private static long parseLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    /**
     * Serialize to a Redis field map.
     */
    public Map<String, String> toDict() {
        try {
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("execution_id", executionId != null ? executionId : "");
            map.put("status", status != null ? status : "");
            map.put("reason", reason != null ? reason : "");
            map.put("worker_id", workerId != null ? workerId : "");
            map.put("timestamp", timestamp);
            map.put("target_agent_type", targetAgentType != null ? targetAgentType : "");
            map.put("selected_agent_type", selectedAgentType != null ? selectedAgentType : "");
            map.put("worker_ids", workerIds != null ? workerIds : new java.util.ArrayList<>());
            map.put("region", region != null ? region : "");
            map.put("retry_after_ms", retryAfterMs);

            return Map.of("data", OBJECT_MAPPER.writeValueAsString(map));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize WakeupDecision", e);
        }
    }
}
