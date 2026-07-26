package com.iwhaleai.byai.framework.worker;

import com.iwhaleai.byai.framework.common.RedisClient;

/**
 * AgentContext facade constructed for Byai workers, mirroring Python's ByaiAgentContext.
 *
 * <p>Unlike Python/TypeScript's {@code call_agent(content: Any/unknown)}, this SDK's
 * {@link AgentContext#callAgent} already takes a plain {@code String content} — the
 * narrowest possible type. There is nothing further to narrow for outbound dispatch, so
 * this class adds no overrides there. Its role is purely to be the context type
 * {@link ByaiWorker#createContext} hands to {@code processCommand}, so business logic can
 * distinguish a Byai-decoded execution via {@code instanceof ByaiAgentContext} the same
 * way it already distinguishes decoded inbound content via the command object itself
 * (see {@link ByaiWorker#prepareCommandForProcessing}).
 */
public class ByaiAgentContext extends AgentContext {

    public ByaiAgentContext(String sessionId, String traceId, RedisClient redisClient,
            String currentAgentType, String currentMessageId) {
        super(sessionId, traceId, redisClient, currentAgentType, currentMessageId);
    }
}
