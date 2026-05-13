package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Exception thrown when worker lock acquisition fails.
 */
public class WorkerLockException extends FrameworkException {

    private final String workerId;

    public WorkerLockException(String workerId) {
        super("Failed to acquire lock for worker: " + workerId);
        this.workerId = workerId;
    }

    public String getWorkerId() {
        return workerId;
    }
}
