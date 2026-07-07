package com.iwhaleai.byai.framework.core.availability;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Delivery intent passed to the availability router.
 * Mirrors Python's DeliveryIntent dataclass.
 */
@Data
@Builder
public class DeliveryIntent {

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
    private long timeoutMs;
    private Map<String, Object> commandPayload;
    private Map<String, Object> metadata;
}
