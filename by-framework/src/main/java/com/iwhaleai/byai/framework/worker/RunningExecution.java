package com.iwhaleai.byai.framework.worker;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Represents a running execution task.
 * Similar to Python's RunningExecution dataclass.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RunningExecution {

    private String executionId;
    private String messageId;
    private String sessionId;
    private String workerId;
    private String parentMessageId;
    private String cancelReason;
    private boolean isResumed;
    private Map<String, Object> existingData;

    // Cancellation state
    @Builder.Default
    private final AtomicBoolean cancelRequested = new AtomicBoolean(false);

    // Current state
    @Builder.Default
    private final AtomicReference<String> currentState = new AtomicReference<>("PENDING");

    /**
     * Check if cancellation has been requested.
     */
    public boolean isCancelRequested() {
        return cancelRequested.get();
    }

    /**
     * Request cancellation of this execution.
     */
    public void requestCancel(String reason) {
        cancelRequested.set(true);
        this.cancelReason = reason;
    }

    /**
     * Get the current state.
     */
    public String getState() {
        return currentState.get();
    }

    /**
     * Update the current state.
     */
    public void setState(String state) {
        currentState.set(state);
    }
}
