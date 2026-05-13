package com.iwhaleai.byai.framework.core.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Response for send message operations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SendMessageResponse {
    private boolean success;
    private String messageId;
    private String traceId;
    private String targetWorkerId;
    private long timestamp;
    private String status;
}
