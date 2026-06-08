package com.iwhaleai.byai.framework.client;

import java.util.Map;

/**
 * Creates the client.dispatch observation used as the root Langfuse parent.
 */
public interface ClientDispatchTracer {

    ClientDispatchObservation start(ClientDispatchRequest request);

    record ClientDispatchRequest(
            String traceId,
            String messageId,
            String targetAgentType,
            String sessionId,
            String userCode,
            String userName,
            Object content,
            Map<String, Object> metadata,
            String observationId
    ) { }
}
