package com.iwhaleai.byai.framework.core.availability;

import lombok.Builder;
import lombok.Data;

/**
 * Result returned by the AvailabilityRouter after evaluating route policy.
 * Mirrors Python's AvailabilityResult.
 */
@Data
@Builder
public class AvailabilityResult {

    private String status;
    private String selectedAgentType;
    private String streamName;
    private String targetWorkerId;
    private String reason;
    private String errorCode;
}
