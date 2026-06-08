package com.iwhaleai.byai.framework.client;

/**
 * Handle for a started client.dispatch observation.
 */
public interface ClientDispatchObservation {

    String id();

    void end(Object output, String error);
}
