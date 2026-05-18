package com.iwhaleai.byai.framework.core.availability;

/**
 * Status constants for availability results returned by the router.
 */
public final class AvailabilityStatus {

    public static final String DELIVER_NOW = "DELIVER_NOW";
    public static final String WAIT_AND_DELIVER = "WAIT_AND_DELIVER";
    public static final String QUEUE_PENDING = "QUEUE_PENDING";
    public static final String REJECT = "REJECT";
    public static final String FALLBACK_TO_OTHER_AGENT_TYPE = "FALLBACK_TO_OTHER_AGENT_TYPE";

    private AvailabilityStatus() {
    }
}
