package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registering a suspended caller in the wait index.
 *
 * <p>A caller that dispatches with {@code waitForReply} <b>ends its execution</b>
 * and is revived by a ResumeCommand. It has no live coroutine, so nothing can
 * time it out from the inside — the bound has to come from a separate index plus
 * a sweep.
 *
 * <p>Every write here is <b>fail-soft</b>. Losing a registration costs the
 * liveness guarantee for that one call, which is exactly the situation the whole
 * fleet was in before this subsystem existed. Letting a Redis blip on the index
 * abort the dispatch instead would be strictly worse.
 */
public final class WaitRegistration {

    private static final Logger LOG = LoggerFactory.getLogger(WaitRegistration.class);

    private WaitRegistration() {
    }

    /**
     * Register a caller waiting on a sub-agent's reply.
     *
     * <p>Must be called <b>next to</b> {@code initializeExecution} and BEFORE the
     * control message goes out. The window that must not exist is "dispatched but
     * nobody knows we are waiting". The opposite window — registered but the xadd
     * below fails — surfaces as a sweep finding a child that never started, which
     * is precisely what happened.
     *
     * @param childMessageId the sub-task's own message id, or {@code ""} for an
     *        askUser wait (contract section 4: an askUser member cannot be derived
     *        from the client's answer, so it is registered with an empty child id
     *        and recovered through the gate's second candidate)
     */
    public static void register(
            RedisOps redisOps,
            String sessionId,
            String parentMessageId,
            String childMessageId,
            String taskGroupId,
            long timeoutMs) {
        try {
            String member = WaitIndex.encodeMember(
                    sessionId, parentMessageId, childMessageId, taskGroupId);
            String key = Constants.RegistryKeys.waitIndex(WaitIndex.shardForSession(sessionId));
            double deadline = System.currentTimeMillis() + timeoutMs;
            redisOps.zadd(key, deadline, member);
        } catch (Exception e) {
            LOG.warn("Failed to register wait entry (session={}, parent={}, child={}): {}",
                    sessionId, parentMessageId, childMessageId, e.getMessage());
        }
    }
}
