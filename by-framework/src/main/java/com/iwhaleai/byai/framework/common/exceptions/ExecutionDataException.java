package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when execution data cannot be parsed.
 */
public class ExecutionDataException extends FrameworkException {

    private final String executionId;

    public ExecutionDataException(String executionId) {
        super("Failed to parse execution data: " + executionId);
        this.executionId = executionId;
    }

    public ExecutionDataException(String executionId, Throwable cause) {
        super("Failed to parse execution data: " + executionId, cause);
        this.executionId = executionId;
    }

    public String getExecutionId() {
        return executionId;
    }
}
