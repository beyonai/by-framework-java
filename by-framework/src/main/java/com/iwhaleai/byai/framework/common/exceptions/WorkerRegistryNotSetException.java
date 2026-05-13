package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when WorkerRegistry is not set for an operation.
 */
public class WorkerRegistryNotSetException extends FrameworkException {

    private final String operation;

    public WorkerRegistryNotSetException(String operation) {
        super("WorkerRegistry not set for operation: " + operation);
        this.operation = operation;
    }

    public String getOperation() {
        return operation;
    }
}
