package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response for cancel task operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CancelTaskResponse {
    private boolean success;
    private String messageId;
    private String executionId;
    private String workerId;
    private String status;
    private long timestamp;
    @Builder.Default
    private String error = "";
}
