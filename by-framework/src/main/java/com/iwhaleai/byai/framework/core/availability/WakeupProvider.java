package com.iwhaleai.byai.framework.core.availability;

/**
 * Interface for wakeup providers.
 * Java equivalent of Python's WakeupProvider Protocol.
 */
public interface WakeupProvider {

    /**
     * Wake up a worker for the given request.
     *
     * @param request The wakeup request
     * @return The wakeup decision
     */
    WakeupDecision wakeup(WakeupRequest request);
}
