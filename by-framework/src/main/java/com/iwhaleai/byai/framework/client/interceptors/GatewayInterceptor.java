package com.iwhaleai.byai.framework.client.interceptors;

/**
 * Base interface for SDK interceptors.
 * Interceptors can modify request arguments before the message is sent to Redis.
 */
public interface GatewayInterceptor {
    /**
     * Executed before the message is packaged and sent.
     * @param params Parameters containing agent type, session, content, etc.
     * @return Modified parameters.
     */
    SendMessageParams beforeSend(SendMessageParams params);
}
