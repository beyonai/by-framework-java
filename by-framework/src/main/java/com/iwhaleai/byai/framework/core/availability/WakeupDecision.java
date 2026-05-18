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
    private String status;
    private String reason;
    private String workerId;
    private long timestamp;

    /**
     * Construct from a Redis field map.
     */
    public static WakeupDecision fromDict(Map<String, String> dict) {
        return WakeupDecision.builder()
                .executionId(dict.getOrDefault("execution_id", ""))
                .status(dict.getOrDefault("status", WakeupDecisionStatus.FAILED))
                .reason(dict.getOrDefault("reason", ""))
                .workerId(dict.getOrDefault("worker_id", ""))
                .timestamp(Long.parseLong(dict.getOrDefault("timestamp", "0")))
                .build();
    }

    /**
     * Serialize to a Redis field map.
     */
    public Map<String, String> toDict() {
        try {
            Map<String, Object> map = Map.of(
                    "execution_id", executionId != null ? executionId : "",
                    "status", status != null ? status : "",
                    "reason", reason != null ? reason : "",
                    "worker_id", workerId != null ? workerId : "",
                    "timestamp", String.valueOf(timestamp));
            return Map.of("data", OBJECT_MAPPER.writeValueAsString(map));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize WakeupDecision", e);
        }
    }
}
