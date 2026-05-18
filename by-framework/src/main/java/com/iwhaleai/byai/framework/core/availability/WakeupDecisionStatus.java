package com.iwhaleai.byai.framework.core.availability;

/**
 * Status constants for wakeup decisions.
 */
public final class WakeupDecisionStatus {

    public static final String READY = "READY";
    public static final String FAILED = "FAILED";
    public static final String REJECTED = "REJECTED";
    public static final String TIMEOUT = "TIMEOUT";

    private WakeupDecisionStatus() {
    }
}
