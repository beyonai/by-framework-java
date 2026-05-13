package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when an execution record is not found.
 */
public class ExecutionNotFoundException extends FrameworkException {

    private final String executionId;
    private final String sessionId;

    public ExecutionNotFoundException(String executionId) {
        this(executionId, "");
    }

    public ExecutionNotFoundException(String executionId, String sessionId) {
        super("Execution not found: " + executionId + (sessionId != null && !sessionId.isEmpty() ? " (session: " + sessionId + ")" : ""));
        this.executionId = executionId;
        this.sessionId = sessionId;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getSessionId() {
        return sessionId;
    }
}
