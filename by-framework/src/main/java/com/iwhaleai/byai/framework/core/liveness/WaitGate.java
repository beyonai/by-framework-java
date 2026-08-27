package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Idempotency gate for replies that resume a suspended caller.
 *
 * <p>A suspended caller is woken by exactly one ResumeCommand. Once a sweep can
 * <i>synthesise</i> that reply — a callee whose worker died will never send one —
 * two copies can exist for the same wait: the synthesised one and the real one
 * arriving late. Waking the caller twice re-runs a finished execution and, in a
 * Task Group, pushes {@code completed} past {@code total} and aggregates twice.
 *
 * <p>This is the single place that decides which copy wins. It claims the wait
 * entry with a ZREM, and exactly one claimant can win because ZREM is atomic.
 *
 * <p><b>The hard part is what ZREM returning 0 means</b>, because it conflates
 * two opposite situations:
 *
 * <ul>
 *   <li>the entry existed and someone else already claimed it — a true duplicate,
 *       drop it;
 *   <li>the entry never existed — the reply belongs to a dispatch made before
 *       this version shipped, or to a wait whose entry expired. Dropping that
 *       silently loses a real reply, and during a rolling upgrade <i>every</i>
 *       in-flight reply looks like this.
 * </ul>
 *
 * <p>A short-lived consumed marker written by the winner separates them: a 0 with
 * a marker is a duplicate, a 0 without one is unregistered and must be let
 * through. When in doubt the gate lets the message through — a spurious extra
 * wake-up is recoverable, a dropped reply is permanent silence. The same rule
 * makes the gate fail <b>open</b>: any Redis error here allows the message.
 */
public final class WaitGate {

    private static final Logger LOG = LoggerFactory.getLogger(WaitGate.class);

    /** Claimed this wait; the reply is the one that wins. */
    public static final String ALLOW_CLAIMED = "claimed";
    /** No entry and no marker: nobody ever registered this wait. Let it through. */
    public static final String ALLOW_UNREGISTERED = "unregistered";
    /** The gate itself failed. Fail open. */
    public static final String ALLOW_GATE_ERROR = "gate_error";
    /** Provably a duplicate: the entry was claimed by someone else. */
    public static final String DENY_ALREADY_CONSUMED = "already_consumed";

    private WaitGate() {
    }

    /**
     * Outcome of the gate.
     *
     * @param allow whether the reply may be processed; false only when the wait
     *        it targets is provably already resolved
     * @param reason one of the ALLOW or DENY constants, carried so the caller can
     *        log it and put it on an orphaned-reply event
     * @param member the member the decision was made about, or "" if none matched
     */
    public record Decision(boolean allow, String reason, String member) {
    }

    /** Redis key of the "already consumed" marker for one member. */
    public static String consumedMarkerKey(String sessionId, String member) {
        return Constants.RegistryKeys.waitConsumed(sessionId, WaitIndex.memberDigest(member));
    }

    /**
     * Members this reply could be clearing, most-specific first.
     *
     * <p>Normally there is exactly one: the member rebuilt from the reply's own
     * header. The second covers askUser, which registers with an empty child
     * message id because it has no sub-task — while the matching reply comes from
     * a client free to put anything in {@code header.parentMessageId}, so that
     * member cannot be rebuilt exactly.
     *
     * <p><b>Order matters, and so does resolving each candidate fully before
     * trying the next.</b> A duplicate sub-agent reply must be caught by its OWN
     * marker rather than fall through and clear a live askUser wait that happens
     * to belong to the same caller. Reversing the order, or checking both entries
     * before either marker, reintroduces exactly that.
     *
     * <p>A reply carrying a task group id is a sub-agent reply by construction, so
     * the askUser variant is not even considered for it.
     */
    public static List<String> candidateMembers(GatewayCommand command) {
        MessageHeader header = command.header();
        List<String> members = new ArrayList<>();
        members.add(WaitIndex.memberFromResume(command));

        String taskGroupId = header.taskGroupId();
        if (taskGroupId == null || taskGroupId.isBlank()) {
            String askUserMember = WaitIndex.encodeMember(
                    header.sessionId(), header.messageId(), "", "");
            if (!members.contains(askUserMember)) {
                members.add(askUserMember);
            }
        }
        return members;
    }

    /**
     * Claim the wait a reply resolves and report whether it may be processed.
     *
     * <p>Call once per ResumeCommand, before the execution lookup and before Task
     * Group join accounting.
     */
    public static Decision consumeWaitEntry(RedisOps redisOps, GatewayCommand command) {
        String sessionId = command.header().sessionId();
        try {
            String indexKey = Constants.RegistryKeys.waitIndex(
                    WaitIndex.shardForSession(sessionId));

            for (String member : candidateMembers(command)) {
                long removed = redisOps.zrem(indexKey, member);
                if (removed > 0) {
                    markConsumed(redisOps, sessionId, member);
                    return new Decision(true, ALLOW_CLAIMED, member);
                }
                if (redisOps.exists(consumedMarkerKey(sessionId, member))) {
                    return new Decision(false, DENY_ALREADY_CONSUMED, member);
                }
            }
            // No entry, no marker: nobody ever registered this wait — a dispatch
            // from before this version, or an entry that outlived its index.
            // Unknown is NOT the same as duplicate. Let it through.
            return new Decision(true, ALLOW_UNREGISTERED, "");
        } catch (Exception e) {
            // Fail open. A gate that drops messages when Redis hiccups is worse
            // than the duplicate it was built to prevent.
            LOG.warn("Wait-index gate unavailable for session={}, allowing reply: {}",
                    sessionId, e.getMessage());
            return new Decision(true, ALLOW_GATE_ERROR, "");
        }
    }

    /**
     * Record that this wait was resolved, so a late twin can be recognised.
     *
     * <p>Fail-soft: losing the marker only means a much later duplicate would be
     * allowed through — one extra wake-up, which is the direction this whole class
     * errs in anyway.
     */
    private static void markConsumed(RedisOps redisOps, String sessionId, String member) {
        try {
            redisOps.setex(consumedMarkerKey(sessionId, member),
                    Constants.WAIT_CONSUMED_TTL_SECONDS, "1");
        } catch (Exception e) {
            LOG.warn("Failed to mark wait entry consumed (session={}): {}",
                    sessionId, e.getMessage());
        }
    }
}
