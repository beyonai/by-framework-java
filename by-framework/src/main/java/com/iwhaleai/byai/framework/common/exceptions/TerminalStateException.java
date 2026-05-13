package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when an operation is attempted on an execution already in a terminal state.
 */
public class TerminalStateException extends FrameworkException {

    private final String executionId;
    private final String state;

    public TerminalStateException(String executionId, String state) {
        super("Execution " + executionId + " is already in terminal state: " + state);
        this.executionId = executionId;
        this.state = state;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getState() {
        return state;
    }
}
