package com.iwhaleai.byai.framework.core.availability;

/**
 * Route policy constants for agent availability control plane.
 * Determines behavior when a target agent_type has no online worker.
 */
public final class RoutePolicy {

    public static final String FAIL_FAST = "FAIL_FAST";
    public static final String SEND_ANYWAY = "SEND_ANYWAY";
    public static final String WAKE_AND_WAIT = "WAKE_AND_WAIT";
    public static final String WAKE_AND_QUEUE = "WAKE_AND_QUEUE";
    public static final String QUEUE_ONLY = "QUEUE_ONLY";

    private RoutePolicy() {
    }
}
