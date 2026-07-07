package com.iwhaleai.byai.framework.core.protocol;

/**
 * Execution status constants.
 */
public final class ExecutionStatus {

    public static final String SUCCESS = "SUCCESS";
    public static final String QUEUED = "QUEUED";
    public static final String CANCEL_REQUESTED = "CANCEL_REQUESTED";
    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String ALREADY_FINISHED = "ALREADY_FINISHED";
    public static final String FAILED = "FAILED";
    public static final String SESSION_MISMATCH = "SESSION_MISMATCH";

    // Error codes for error_code field
    public static final String ERR_WORKER_NOT_ONLINE = "ERR_WORKER_NOT_ONLINE";
    public static final String ERR_AGENT_TYPE_UNAVAILABLE = "ERR_AGENT_TYPE_UNAVAILABLE";
    public static final String ERR_AGENT_CIRCUIT_OPEN = "ERR_AGENT_CIRCUIT_OPEN";
    public static final String ERR_TENANT_QUOTA_EXCEEDED = "ERR_TENANT_QUOTA_EXCEEDED";

    private ExecutionStatus() {
        // Prevent instantiation
    }
}
