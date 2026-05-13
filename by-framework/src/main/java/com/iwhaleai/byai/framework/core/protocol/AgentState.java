package com.iwhaleai.byai.framework.core.protocol;

public class AgentState {
    public static final String STARTING = "STARTING";
    public static final String RUNNING = "RUNNING";
    public static final String WAITING_USER = "WAITING_USER";
    public static final String CALLING_AGENT = "CALLING_AGENT";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    public static final String CANCELLING = "CANCELLING";
    public static final String CANCELLED = "CANCELLED";
    public static final String RESUMED = "RESUMED";
    public static final String QUEUED = "QUEUED";
    public static final String NOT_FOUND = "NOT_FOUND";
    
    public static boolean isTerminalState(String state) {
        return COMPLETED.equals(state) || FAILED.equals(state) || CANCELLED.equals(state) || NOT_FOUND.equals(state);
    }
}
