package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.common.RedisStreamOps;
import com.iwhaleai.byai.framework.common.XAddOptions;
import com.iwhaleai.byai.framework.core.WorkerRegistry;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import com.iwhaleai.byai.framework.worker.ResumeMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.params.SetParams;

/**
 * Background sweep over the wait index.
 *
 * <p>A caller suspended on a reply has no live coroutine — its execution
 * <i>finished</i> and will be recreated on resume — so nothing can time it out
 * from the inside. This is the outside.
 *
 * <p><b>Two switches, and they must stay separate.</b>
 *
 * <ul>
 *   <li><b>Pruning</b> is on by default and decides nothing. Nothing else ever
 *       removes a wait-index entry except the reply it was waiting for, so with
 *       compensation off every call whose reply never arrives — precisely the
 *       failures this subsystem exists for — leaves an entry behind forever, in
 *       a structure with no TTL of its own (the shard ZSET is shared by every
 *       session, so it cannot carry one).
 *   <li><b>Compensation</b> is off by default and is the rollback switch for the
 *       whole liveness feature. It is what synthesises replies, and turning it
 *       on is a behaviour change.
 * </ul>
 *
 * <p>Compensation triages a due entry against evidence that already exists —
 * the caller's record, the callee's record, and the callee's worker lease — and
 * synthesises the reply the callee would have sent when one can never arrive.
 * Ordering there is deliberate: everything establishing that somebody is still
 * waiting is checked before anything that could produce a reply, because a reply
 * nobody is waiting for re-enters a finished execution.
 *
 * <p>Per the cross-SDK contract the sweep is language-agnostic: all three SDKs
 * read and write the same ZSETs, so a Python or TypeScript sweeper compensates
 * waits registered by Java and vice versa.
 */
public class WaitSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(WaitSweeper.class);

    /** Outcome key for entries removed by a prune pass. */
    public static final String OUTCOME_PRUNED = "pruned";
    /** Outcome key for a shard whose sweep threw. */
    public static final String OUTCOME_ERROR = "error";

    // Triage outcomes. Every branch reports one, which is what makes a pass
    // observable in tests and in the logs.
    public static final String OUTCOME_MALFORMED = "malformed";
    public static final String OUTCOME_CALLER_MISSING = "caller_missing";
    public static final String OUTCOME_CALLER_TERMINAL = "caller_terminal";
    public static final String OUTCOME_CALLER_LOST = "caller_lost";
    public static final String OUTCOME_CALLER_NOT_SUSPENDED = "caller_not_suspended";
    public static final String OUTCOME_ASK_USER_SKIPPED = "ask_user_skipped";
    public static final String OUTCOME_GROUP_GONE = "group_gone";
    public static final String OUTCOME_GROUP_ABORTED = "group_aborted";
    public static final String OUTCOME_GROUP_ALREADY_JOINED = "group_already_joined";
    public static final String OUTCOME_CHILD_WAITING = "child_waiting";
    public static final String OUTCOME_CHILD_ALIVE = "child_alive";
    public static final String OUTCOME_RECOVERED = "recovered";
    public static final String OUTCOME_WORKER_LOST = "worker_lost";
    public static final String OUTCOME_NEVER_STARTED = "never_started";
    public static final String OUTCOME_TIMED_OUT = "timed_out";
    public static final String OUTCOME_UNROUTABLE = "unroutable";

    /** Marks a reply as produced by a sweep rather than by the callee. */
    static final String SYNTHESIZED_BY_SWEEPER = "wait_sweeper";

    /** Task-group field set when a fan-out failed partway through. */
    private static final String TASK_GROUP_FIELD_ABORTED = "aborted";

    /** Statuses that mean "this execution is parked on somebody else's reply". */
    private static final java.util.Set<String> SUSPENDED_STATES =
            java.util.Set.of(AgentState.WAITING_AGENT, AgentState.WAITING_USER,
                    AgentState.CALLING_AGENT);

    private final RedisOps redisOps;
    private final String workerId;
    private final boolean compensateEnabled;
    private final boolean pruneEnabled;
    private final int intervalSeconds;
    private final int pruneIntervalSeconds;
    private final int lockTtlSeconds;
    private final int renewMaxMultiple;
    private final boolean cancelOnTimeout;
    private final WorkerRegistry registry;
    private final RedisStreamOps streamOps;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ScheduledExecutorService executor;
    private int shardCursor = 0;
    private Long lastPruneNanos = null;

    public WaitSweeper(RedisOps redisOps, RedisStreamOps streamOps, WorkerRegistry registry,
            String workerId) {
        this(redisOps, streamOps, registry, workerId,
                envFlag("BY_FRAMEWORK_WAIT_SWEEPER_ENABLED", false),
                envFlag("BY_FRAMEWORK_WAIT_PRUNE_ENABLED", true),
                envInt("BY_FRAMEWORK_WAIT_SWEEP_INTERVAL_SECONDS",
                        Constants.WAIT_SWEEP_INTERVAL_SECONDS),
                envInt("BY_FRAMEWORK_WAIT_PRUNE_INTERVAL_SECONDS",
                        Constants.WAIT_PRUNE_INTERVAL_SECONDS),
                Constants.WAIT_SWEEP_LOCK_TTL_SECONDS,
                envInt("BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE",
                        Constants.WAIT_RENEW_MAX_MULTIPLE),
                envFlag("BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT", true));
    }

    public WaitSweeper(RedisOps redisOps, RedisStreamOps streamOps, WorkerRegistry registry,
            String workerId, boolean compensateEnabled, boolean pruneEnabled,
            int intervalSeconds, int pruneIntervalSeconds, int lockTtlSeconds,
            int renewMaxMultiple, boolean cancelOnTimeout) {
        this.streamOps = streamOps;
        this.registry = registry;
        this.renewMaxMultiple = renewMaxMultiple;
        this.cancelOnTimeout = cancelOnTimeout;
        this.redisOps = redisOps;
        this.workerId = workerId;
        this.compensateEnabled = compensateEnabled;
        this.pruneEnabled = pruneEnabled;
        this.intervalSeconds = intervalSeconds;
        this.pruneIntervalSeconds = pruneIntervalSeconds;
        this.lockTtlSeconds = lockTtlSeconds;
    }

    /**
     * How long the loop sleeps between passes.
     *
     * <p>Compensation is what needs a short cadence — a due entry should not
     * wait long for its triage. With it off the only work left is a garbage
     * collector with a multi-day horizon, so the loop drops to the prune cadence
     * rather than waking every 30 seconds to do nothing.
     */
    public int loopIntervalSeconds() {
        return compensateEnabled ? intervalSeconds : pruneIntervalSeconds;
    }

    /** Start the background loop, unless both halves are switched off. */
    public void start() {
        if (!compensateEnabled && !pruneEnabled) {
            LOG.debug("WaitSweeper fully disabled, not starting.");
            return;
        }
        LOG.info("WaitSweeper started (worker_id={}, interval={}s, compensate={}, prune={})",
                workerId, loopIntervalSeconds(), compensateEnabled, pruneEnabled);
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "wait-sweeper-" + workerId);
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(() -> {
            try {
                sweepOnce();
            } catch (Exception e) {
                // A sweep is a safety net; it must never take down its host.
                LOG.warn("Wait sweep pass failed: {}", e.getMessage());
            }
        }, loopIntervalSeconds(), loopIntervalSeconds(), TimeUnit.SECONDS);
    }

    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
            LOG.debug("WaitSweeper stopped (worker_id={})", workerId);
        }
    }

    /**
     * Run one pass over every shard this worker can claim right now.
     *
     * <p>Returns a count per outcome, which is also what makes a pass observable
     * in tests.
     */
    public Map<String, Integer> sweepOnce() {
        Map<String, Integer> outcomes = new HashMap<>();
        boolean pruning = pruneIsDue();
        int start = shardCursor;
        for (int offset = 0; offset < Constants.WAIT_INDEX_SHARDS; offset++) {
            int shard = (start + offset) % Constants.WAIT_INDEX_SHARDS;
            sweepShard(shard, pruning).forEach((k, v) -> outcomes.merge(k, v, Integer::sum));
        }
        // Rotate so the shard a busy worker starts on — and therefore claims
        // first — differs each cycle; nothing is pinned to one owner.
        shardCursor = (start + 1) % Constants.WAIT_INDEX_SHARDS;
        return outcomes;
    }

    /** Whether this pass should prune, on the coarser prune cadence. */
    private boolean pruneIsDue() {
        if (!pruneEnabled) {
            return false;
        }
        long now = System.nanoTime();
        if (lastPruneNanos != null
                && now - lastPruneNanos < pruneIntervalSeconds * 1_000_000_000L) {
            return false;
        }
        lastPruneNanos = now;
        return true;
    }

    /**
     * Claim one shard and prune it.
     *
     * <p>The lock is taken even though pruning is idempotent — ZREMRANGEBYSCORE
     * over a fixed range yields the same state however many workers run it.
     * Holding it just keeps a fleet from issuing the same command N times a
     * cycle. Correctness does not depend on it, which is why a lost or expired
     * claim costs nothing here.
     *
     * <p>Each shard is claimed independently: the shard keys deliberately carry
     * no hash tag (sharding exists to spread load), so under Cluster they live in
     * different slots and no multi-key atomic work is possible across them.
     */
    Map<String, Integer> sweepShard(int shard, boolean pruning) {
        if (!pruning && !compensateEnabled) {
            return Map.of();
        }
        String lockKey = Constants.RegistryKeys.waitSweepLock(shard);
        String token = UUID.randomUUID().toString().replace("-", "");
        boolean claimed;
        try {
            claimed = "OK".equals(redisOps.set(lockKey, token,
                    SetParams.setParams().nx().ex(lockTtlSeconds)));
        } catch (Exception e) {
            LOG.warn("Wait sweep could not claim shard {}: {}", shard, e.getMessage());
            return Map.of();
        }
        if (!claimed) {
            return Map.of();
        }

        Map<String, Integer> outcomes = new HashMap<>();
        try {
            if (pruning) {
                int pruned = pruneShard(shard);
                if (pruned > 0) {
                    outcomes.put(OUTCOME_PRUNED, pruned);
                }
            }
            if (compensateEnabled) {
                resolveDueEntries(shard).forEach((k, v) -> outcomes.merge(k, v, Integer::sum));
            }
            return outcomes;
        } catch (Exception e) {
            LOG.warn("Wait sweep failed on shard {}: {}", shard, e.getMessage());
            outcomes.merge(OUTCOME_ERROR, 1, Integer::sum);
            return outcomes;
        } finally {
            try {
                redisOps.del(lockKey);
            } catch (Exception e) {
                LOG.debug("Wait sweep lock release failed (shard {}): {}", shard, e.getMessage());
            }
        }
    }

    /**
     * Delete entries old enough that no interrogation could succeed.
     *
     * <p>Unlike triage this reads nothing: no member is decoded, no execution
     * record is fetched, no reply is produced. It is one ZREMRANGEBYSCORE over a
     * range whose upper bound is a <i>proof</i> rather than a guess.
     *
     * <p>Every writer of an entry sets its score to its own clock plus a
     * non-negative offset — registration adds the caller's timeout, a renewal
     * adds an increment — and only ever writes while the caller's execution
     * record exists. So a score more than {@code WAIT_PRUNE_AFTER_SECONDS} in the
     * past means the entry was last touched longer than a session TTL ago, hence
     * its session registry is gone and the only outcome triage could ever reach
     * for it is "caller missing", which deletes it anyway. That holds for renewed
     * entries too: a renewal <i>raises</i> the score, so an old score is evidence
     * about the most recent renewal, not about registration.
     *
     * <p>Because it decides nothing it needs no opt-in — and because the
     * threshold sits a day beyond the session TTL, the longest deadline anything
     * registers (the askUser timeout, which equals the session TTL exactly) is
     * never near it.
     */
    int pruneShard(int shard) {
        long cutoffMs = System.currentTimeMillis() - Constants.WAIT_PRUNE_AFTER_SECONDS * 1000L;
        long removed = redisOps.zremRangeByScore(
                Constants.RegistryKeys.waitIndex(shard), 0, cutoffMs);
        if (removed > 0) {
            LOG.info("Wait sweep pruned {} abandoned wait-index entries from shard {} "
                    + "(older than {}s)", removed, shard, Constants.WAIT_PRUNE_AFTER_SECONDS);
        }
        return (int) removed;
    }

    // ===== compensation =====================================================

    /** Wait entry that has come due, plus the score it came due with. */
    record DueEntry(String indexKey, String member, long deadlineMs, WaitIndex.Member entry) {
    }

    /**
     * Resolve every entry in this shard whose deadline has passed.
     *
     * <p>Scores come back with the members because the score IS the evidence for
     * how long this wait has already run: it is the caller's original deadline
     * until the first renewal replaces it, and that original is what the renewal
     * budget is measured from.
     */
    private Map<String, Integer> resolveDueEntries(int shard) {
        String indexKey = Constants.RegistryKeys.waitIndex(shard);
        long nowMs = System.currentTimeMillis();
        Map<String, Integer> outcomes = new HashMap<>();

        List<String> due = redisOps.zrangeByScore(indexKey, 0, nowMs,
                Constants.WAIT_SWEEP_BATCH_LIMIT);
        for (String member : due) {
            String outcome;
            try {
                // The score is re-read per member rather than fetched withscores:
                // Jedis' typed tuple API differs between standalone and cluster,
                // and the renewal origin key already holds the value that matters
                // once anything has renewed.
                Double score = redisOps.zscore(indexKey, member);
                outcome = resolveEntry(indexKey, member, score == null ? 0L : score.longValue());
            } catch (Exception e) {
                // One poisonous entry must not stop the rest of the shard.
                LOG.warn("Wait sweep could not resolve entry {}: {}", member, e.getMessage());
                outcome = OUTCOME_ERROR;
            }
            outcomes.merge(outcome, 1, Integer::sum);
        }
        return outcomes;
    }

    /**
     * Triage one due entry against evidence that already exists.
     *
     * <p><b>Ordering is deliberate</b>: everything that establishes <i>there is
     * still somebody waiting</i> is checked before anything that could produce a
     * reply. A reply nobody is waiting for is not free — it re-enters a finished
     * execution.
     */
    private String resolveEntry(String indexKey, String member, long deadlineMs) {
        WaitIndex.Member entry;
        try {
            entry = WaitIndex.decodeMember(member);
        } catch (IllegalArgumentException e) {
            LOG.warn("Dropping malformed wait-index member {}: {}", member, e.getMessage());
            redisOps.zrem(indexKey, member);
            return OUTCOME_MALFORMED;
        }

        DueEntry due = new DueEntry(indexKey, member, deadlineMs, entry);
        Map<String, Object> caller = registry.getExecutionByMessageId(
                entry.parentMessageId(), entry.sessionId());

        if (caller == null) {
            // The session registry expired out from under it: no reply,
            // synthesised or real, can reattach this caller any more.
            LOG.info("Wait sweep dropping entry for a caller with no execution record "
                    + "(session={}, caller={})", entry.sessionId(), entry.parentMessageId());
            redisOps.zrem(indexKey, member);
            return OUTCOME_CALLER_MISSING;
        }

        String callerStatus = String.valueOf(caller.getOrDefault("status", ""));
        if (AgentState.isTerminalState(callerStatus)) {
            // Reachable through a narrow but real window: the entry is registered
            // before the dispatch xadd, so an xadd that raises fails the caller
            // and leaves the entry behind. Waking a finished execution is exactly
            // what the idempotency work exists to prevent — clean up, synthesise
            // nothing.
            LOG.info("Wait sweep dropping entry for an already-{} caller "
                    + "(session={}, caller={})", callerStatus, entry.sessionId(),
                    entry.parentMessageId());
            redisOps.zrem(indexKey, member);
            return OUTCOME_CALLER_TERMINAL;
        }

        if (!SUSPENDED_STATES.contains(callerStatus)) {
            return resolveUnsuspendedCaller(due, caller);
        }

        if (entry.childMessageId().isEmpty()) {
            // askUser. "The human hasn't answered yet" is not a fault and has no
            // compensation; the entry stays purely so a repeated answer is
            // recognised as a duplicate by the gate.
            return OUTCOME_ASK_USER_SKIPPED;
        }

        if (!entry.taskGroupId().isEmpty()) {
            String blocked = groupBlocksCompensation(due);
            if (blocked != null) {
                return blocked;
            }
        }

        return triageChild(due, caller);
    }

    /**
     * Handle an entry whose caller is not (yet, or ever) suspended.
     *
     * <p>Two very different situations share this shape. The caller may still be
     * inside the handler that registered the wait, in which case a reply now
     * would run alongside it — so back off and look again. Or the caller's own
     * worker died before it could record its suspension, in which case no reply
     * can ever reattach it and the entry is garbage; that chain gets rescued one
     * level up, by the wait its own caller registered.
     */
    private String resolveUnsuspendedCaller(DueEntry due, Map<String, Object> caller) {
        String callerWorkerId = String.valueOf(caller.getOrDefault("worker_id", ""));
        if (!callerWorkerId.isEmpty() && !registry.isWorkerOnline(callerWorkerId)) {
            LOG.info("Wait sweep dropping entry whose caller was lost with worker {} "
                    + "(caller={})", callerWorkerId, due.entry().parentMessageId());
            redisOps.zrem(due.indexKey(), due.member());
            return OUTCOME_CALLER_LOST;
        }
        extendDeadline(due);
        return OUTCOME_CALLER_NOT_SUSPENDED;
    }

    /**
     * Reasons a Task Group orphan must be cleaned up rather than compensated.
     *
     * <p>A group orphan is otherwise compensated exactly like any other: the
     * synthesised reply carries the group id, so the worker's join stores it,
     * increments {@code completed}, and aggregates only if it was the last
     * sibling outstanding. <b>That is the whole point of routing it as a reply</b>
     * — writing the result and the counter from here would be a second
     * implementation of the group's accounting, and when THAT copy is the
     * increment reaching {@code total} there is no reply left to trigger the join
     * and the caller hangs forever.
     *
     * @return an outcome when the entry was resolved here, or null to let the
     *         normal triage run
     */
    private String groupBlocksCompensation(DueEntry due) {
        WaitIndex.Member entry = due.entry();
        String groupKey = Constants.RegistryKeys.taskGroup(entry.taskGroupId());

        String total = redisOps.hget(groupKey, Constants.TASK_GROUP_FIELD_TOTAL);
        if (total == null) {
            // The group tracker expired or was never written. A reply then finds
            // no group to join, so it falls through as a lone result and resumes
            // the caller with one sibling's payload where the aggregate belongs.
            LOG.info("Wait sweep dropping entry whose task group no longer exists "
                    + "(session={}, caller={}, group={})", entry.sessionId(),
                    entry.parentMessageId(), entry.taskGroupId());
            redisOps.zrem(due.indexKey(), due.member());
            return OUTCOME_GROUP_GONE;
        }

        if (redisOps.hget(groupKey, TASK_GROUP_FIELD_ABORTED) != null) {
            // Dispatch failed partway through the fan-out, so the caller was
            // already failed and every reply for this group is discarded on
            // arrival. Synthesising one more changes nothing and re-enters a
            // terminated execution on the way to being discarded.
            LOG.info("Wait sweep dropping entry for aborted task group "
                    + "(session={}, caller={}, group={})", entry.sessionId(),
                    entry.parentMessageId(), entry.taskGroupId());
            redisOps.zrem(due.indexKey(), due.member());
            return OUTCOME_GROUP_ABORTED;
        }

        String recorded = redisOps.hget(
                Constants.RegistryKeys.taskGroupResults(entry.taskGroupId()),
                entry.childMessageId());
        if (recorded != null) {
            // A result under this sub-task's id can only have been written by the
            // join, which means its reply already arrived and was counted. The
            // entry outliving that is a gate ZREM that did not land; a second
            // synthesised reply would be counted a second time.
            LOG.info("Wait sweep dropping entry already joined by its reply "
                    + "(session={}, caller={}, child={}, group={})", entry.sessionId(),
                    entry.parentMessageId(), entry.childMessageId(), entry.taskGroupId());
            redisOps.zrem(due.indexKey(), due.member());
            return OUTCOME_GROUP_ALREADY_JOINED;
        }

        return null;
    }

    /** Decide the callee's fate from its execution record and its lease. */
    private String triageChild(DueEntry due, Map<String, Object> caller) {
        WaitIndex.Member entry = due.entry();
        Map<String, Object> child = registry.getExecutionByMessageId(
                entry.childMessageId(), entry.sessionId());

        if (child == null) {
            return synthesizeFailure(due, caller, Map.of(),
                    Constants.LivenessErrorCode.CHILD_NEVER_STARTED,
                    "No execution was ever recorded for sub-task " + entry.childMessageId(),
                    OUTCOME_NEVER_STARTED, AgentState.FAILED);
        }

        String childStatus = String.valueOf(child.getOrDefault("status", ""));
        if (AgentState.isTerminalState(childStatus)) {
            return recoverFinishedChild(due, caller, child, childStatus);
        }

        if (SUSPENDED_STATES.contains(childStatus)) {
            // The callee is itself waiting on someone. Its own entry has a
            // deadline of its own and will fail first if that wait breaks;
            // killing this one now would collapse the whole chain at once and
            // report the wrong cause at every level.
            //
            // Deliberately exempt from the renewal ceiling below: this wait was
            // registered BEFORE the deeper one it is blocked on, so its ceiling
            // would be reached first and the chain would fail from the top down —
            // inverting the propagation order the whole design rests on. The
            // chain still terminates, because the deepest wait is blocked on real
            // work and IS subject to the ceiling.
            extendDeadline(due);
            return OUTCOME_CHILD_WAITING;
        }

        String childWorkerId = String.valueOf(child.getOrDefault("worker_id", ""));
        if (childWorkerId.isEmpty()) {
            return synthesizeFailure(due, caller, child,
                    Constants.LivenessErrorCode.CHILD_NEVER_STARTED,
                    "Sub-task " + entry.childMessageId()
                            + " was never picked up by a worker (status=" + childStatus + ")",
                    OUTCOME_NEVER_STARTED, AgentState.FAILED);
        }

        if (registry.isWorkerOnline(childWorkerId)) {
            // Running long is not the same as being dead, and the lease is the
            // only signal that tells them apart — so a live lease buys more time,
            // which is what keeps slow work from being killed.
            //
            // But only up to a ceiling. Renewing on a live lease alone answers
            // "is the process up", not "is the work progressing", so a callee
            // deadlocked or stuck in a call that never returns would be renewed
            // forever and its caller never resolved — precisely the hang this
            // subsystem exists to bound. No signal separates that from a
            // genuinely long call (both sit still), so the ceiling is
            // deliberately crude: generous, absolute, and therefore predictable.
            long limitMs = renewalCeilingMs(due, child);
            if (System.currentTimeMillis() < limitMs) {
                extendDeadline(due);
                return OUTCOME_CHILD_ALIVE;
            }
            String outcome = synthesizeFailure(due, caller, child,
                    Constants.LivenessErrorCode.CHILD_TIMEOUT,
                    "Sub-task " + entry.childMessageId() + " is still " + childStatus
                            + " on live worker " + childWorkerId + " but produced no reply within "
                            + renewMaxMultiple + "x its reply timeout",
                    OUTCOME_TIMED_OUT, AgentState.FAILED);
            // Strictly AFTER the caller has been resolved, and strictly
            // best-effort: this is the one branch with a live process on the
            // other end, so it is the only one where stopping the work is even
            // meaningful — and the caller's wake-up must not depend on it landing.
            cancelTimedOutChild(due, child);
            return outcome;
        }

        return synthesizeFailure(due, caller, child,
                Constants.LivenessErrorCode.CHILD_WORKER_LOST,
                "Worker " + childWorkerId + " running sub-task " + entry.childMessageId()
                        + " is no longer alive (status=" + childStatus + ")",
                OUTCOME_WORKER_LOST, AgentState.FAILED);
    }

    /**
     * A callee that finished but whose reply never arrived.
     *
     * <p>If it persisted a result before replying, that real result is recovered
     * and forwarded. Reporting a fabricated failure for a sub-task that actually
     * succeeded would be the worst outcome available here.
     *
     * <p>When the result is not there, the honest outcome IS a failure. An empty
     * COMPLETED would hand the caller a fabricated answer, which is worse than a
     * reported failure.
     */
    @SuppressWarnings("unchecked")
    private String recoverFinishedChild(DueEntry due, Map<String, Object> caller,
            Map<String, Object> child, String childStatus) {
        Map<String, Object> stored = loadStoredResult(due.entry().childMessageId());
        if (stored == null) {
            String status = AgentState.FAILED.equals(childStatus)
                    || AgentState.CANCELLED.equals(childStatus) ? childStatus : AgentState.FAILED;
            return synthesizeFailure(due, caller, child,
                    Constants.LivenessErrorCode.REPLY_LOST_RECOVERED,
                    "Sub-task " + due.entry().childMessageId() + " finished with " + childStatus
                            + " but neither its reply nor its stored result is available",
                    OUTCOME_RECOVERED, status);
        }

        Object storedStatus = stored.get("status");
        Map<String, Object> metadata = stored.get("metadata") instanceof Map
                ? new HashMap<>((Map<String, Object>) stored.get("metadata"))
                : new HashMap<>();
        Map<String, Object> extraPayload = stored.get("extra_payload") instanceof Map
                ? new HashMap<>((Map<String, Object>) stored.get("extra_payload"))
                : new HashMap<>();

        emitReply(due, caller, child,
                storedStatus != null ? String.valueOf(storedStatus) : childStatus,
                stored.getOrDefault("content", ""), stored.get("reply_data"),
                extraPayload, Constants.LivenessErrorCode.REPLY_LOST_RECOVERED, metadata);
        return OUTCOME_RECOVERED;
    }

    /** Read the callee's persisted result, or null if there is none. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> loadStoredResult(String childMessageId) {
        try {
            String raw = redisOps.hget(
                    Constants.RegistryKeys.taskGroupResults(
                            Constants.TASK_GROUP_SINGLE_ID_PREFIX + childMessageId),
                    childMessageId);
            if (raw == null || raw.isBlank()) {
                return null;
            }
            Object decoded = objectMapper.readValue(raw, Map.class);
            return decoded instanceof Map ? (Map<String, Object>) decoded : null;
        } catch (Exception e) {
            LOG.warn("Stored result for sub-task {} is unreadable: {}",
                    childMessageId, e.getMessage());
            return null;
        }
    }

    /**
     * Emit the failure reply the callee would have sent, had it lived.
     *
     * <p>{@code replyData} carries error/error_code because that is where a
     * dispatch-time failure already puts them: a caller must not be able to tell
     * a callee that failed from one that never got to fail, or the two shapes
     * drift apart and callers grow a second error path.
     */
    private String synthesizeFailure(DueEntry due, Map<String, Object> caller,
            Map<String, Object> child, String errorCode, String message, String outcome,
            String status) {
        LOG.warn("Wait sweep synthesizing {} reply for caller={} (child={}, session={}): {}",
                errorCode, due.entry().parentMessageId(), due.entry().childMessageId(),
                due.entry().sessionId(), message);

        Map<String, Object> replyData = new HashMap<>();
        replyData.put("error", message);
        replyData.put("error_code", errorCode);
        replyData.put("child_message_id", due.entry().childMessageId());

        // The caller's own dispatch metadata, read back off the callee's record —
        // so a caller that never gets a real reply still gets its own metadata
        // back, consistent with every other reply shape it can receive.
        Map<String, Object> metadata = ResumeMetadata.storedMetadata(child);

        boolean emitted = emitReply(due, caller, child, status, "", replyData,
                new HashMap<>(), errorCode, metadata);
        return emitted ? outcome : OUTCOME_UNROUTABLE;
    }

    /**
     * Put a stand-in reply on the caller's control stream.
     *
     * <p>Field-for-field it is what {@code GatewayWorker.enqueueAgentReturn}
     * produces — same id reversal ({@code header.messageId} is the caller's,
     * {@code header.parentMessageId} the sub-task's), same stream. That is not
     * tidiness: the runner reattaches the suspended execution by
     * {@code header.messageId}, and the gate rebuilds the wait-index member from
     * the header, so anything else either fails to resume the caller or fails to
     * clear its entry.
     *
     * <p><b>The wait-index entry is left in place on purpose.</b> Removing it
     * here would leave the synthesised reply as the only copy that must not be
     * gated — and a reply that bypasses the gate is a second wake-up path, which
     * is what makes double-resumes possible in the first place. Instead the
     * deadline is pushed out, so a caller whose control stream is not being
     * consumed gets at most one stand-in per renewal window instead of one per
     * sweep.
     *
     * <p>A reply for a Task Group member carries the group id like any other, so
     * it is stored and counted by the group's existing join and resolves the
     * caller only when it is the last sibling outstanding.
     */
    private boolean emitReply(DueEntry due, Map<String, Object> caller,
            Map<String, Object> child, String status, Object content, Object replyData,
            Map<String, Object> extraPayload, String errorCode, Map<String, Object> metadata) {
        WaitIndex.Member entry = due.entry();

        String callerAgentType = String.valueOf(child.getOrDefault("source_agent_type", ""));
        if (callerAgentType.isEmpty()) {
            callerAgentType = String.valueOf(caller.getOrDefault("target_agent_type", ""));
        }
        if (callerAgentType.isEmpty() || "null".equals(callerAgentType)) {
            LOG.warn("Wait sweep cannot route a reply for caller={} (session={}): the "
                    + "execution records name no caller agent type",
                    entry.parentMessageId(), entry.sessionId());
            extendDeadline(due);
            return false;
        }

        Map<String, Object> fullMetadata = new HashMap<>(metadata);
        fullMetadata.put("sweeper_worker_id", workerId);
        fullMetadata.put("error_code", errorCode);
        fullMetadata.put("synthesized_by", SYNTHESIZED_BY_SWEEPER);

        String traceId = String.valueOf(child.getOrDefault("trace_id", ""));
        if (traceId.isEmpty() || "null".equals(traceId)) {
            traceId = String.valueOf(caller.getOrDefault("trace_id", ""));
        }

        ResumeCommand reply = ResumeCommand.of(
                MessageHeader.builder()
                        // The caller reattaches by this id, so it must be the
                        // caller's own message id — not a freshly minted one.
                        .messageId(entry.parentMessageId())
                        .sessionId(entry.sessionId())
                        .traceId(traceId)
                        .sourceAgentType(String.valueOf(child.getOrDefault("target_agent_type", "")))
                        .targetAgentType(callerAgentType)
                        .parentMessageId(entry.childMessageId())
                        .taskGroupId(entry.taskGroupId())
                        .metadata(fullMetadata)
                        .build(),
                content, status, replyData, extraPayload);

        try {
            Map<String, String> fields = new HashMap<>();
            fields.put("data", objectMapper.writeValueAsString(reply));
            streamOps.xadd(Constants.QueueNames.ctrlStream(callerAgentType), fields,
                    XAddOptions.noTrim());
        } catch (Exception e) {
            LOG.warn("Wait sweep could not emit a stand-in reply for caller={}: {}",
                    entry.parentMessageId(), e.getMessage());
            extendDeadline(due);
            return false;
        }
        extendDeadline(due);
        return true;
    }

    /**
     * Absolute instant past which this wait stops being renewed.
     *
     * <p>Measured from the wait's ORIGINAL deadline, not from a renewal count:
     * renewals happen at a fixed increment that is a tunable, so counting them
     * would let a config change silently move the bound.
     *
     * <p>The caller's own timeout is recovered as the span between the sub-task's
     * {@code created_at} — written immediately before the wait is registered —
     * and that original deadline, so a caller that asked for ten minutes is not
     * held to the same budget as one that asked for four hours. When that span is
     * unusable the default is assumed, erring toward waiting longer: killing a
     * healthy callee is worse than resolving a dead one late.
     *
     * <p>Progress is deliberately NOT used as evidence. A callee's
     * {@code updated_at} stands just as still during a legitimate 20-minute model
     * call as during a deadlock, so a ceiling keyed on it would kill exactly the
     * work it is meant to protect.
     */
    private long renewalCeilingMs(DueEntry due, Map<String, Object> child) {
        long originMs = renewalOriginMs(due);
        long registeredMs = asLong(child.get("created_at"));
        long timeoutMs = originMs - registeredMs;
        if (registeredMs <= 0 || timeoutMs <= 0) {
            timeoutMs = Constants.DEFAULT_REPLY_TIMEOUT_MS;
        }
        // One renewal increment is the floor, so a degenerate (zero, or very
        // short) timeout still buys the callee one look.
        long graceMs = Math.max(timeoutMs * (renewMaxMultiple - 1),
                Constants.WAIT_RENEW_INCREMENT_MS);
        return originMs + graceMs;
    }

    /** The deadline this wait's renewal budget is measured from. */
    private long renewalOriginMs(DueEntry due) {
        try {
            String stored = redisOps.get(Constants.RegistryKeys.waitRenewOrigin(
                    due.entry().sessionId(), WaitIndex.memberDigest(due.member())));
            long parsed = asLong(stored);
            if (parsed > 0) {
                return parsed;
            }
        } catch (Exception e) {
            LOG.debug("Wait sweep could not read renewal origin: {}", e.getMessage());
        }
        // Nothing recorded means nothing has renewed this entry yet, so the score
        // it came due with still IS the original deadline.
        return due.deadlineMs();
    }

    /**
     * Push an entry's deadline out by a fixed increment.
     *
     * <p>Fixed rather than the caller's original timeout: the member has to stay
     * rebuildable from a reply alone, and a reply cannot know what timeout its
     * caller chose, so the timeout is not encoded in it.
     *
     * <p>Overwriting the score destroys the only record of the original deadline,
     * so the FIRST renewal saves it first (SET NX, so later renewals leave it
     * alone). Without that, the ceiling would re-measure from the deadline it just
     * pushed out and could never be reached. Fail-soft: if the write is lost the
     * budget merely restarts from the current deadline — bounded, just more
     * generous.
     */
    private void extendDeadline(DueEntry due) {
        try {
            redisOps.set(
                    Constants.RegistryKeys.waitRenewOrigin(
                            due.entry().sessionId(), WaitIndex.memberDigest(due.member())),
                    String.valueOf(due.deadlineMs()),
                    SetParams.setParams().nx().ex(Constants.TASK_GROUP_TTL_SECONDS));
        } catch (Exception e) {
            LOG.debug("Wait sweep could not record renewal origin: {}", e.getMessage());
        }
        long deadlineMs = System.currentTimeMillis() + Constants.WAIT_RENEW_INCREMENT_MS;
        redisOps.zadd(due.indexKey(), deadlineMs, due.member());
    }

    /**
     * Stop a callee that ran past its ceiling. Best-effort, by nature.
     *
     * <p>Only CHILD_TIMEOUT gets here. CHILD_WORKER_LOST and CHILD_NEVER_STARTED
     * have nothing on the other end to cancel, and a callee that already finished
     * has nothing left to stop.
     *
     * <p>Two things this deliberately does not do. It does <b>not silence the
     * callee</b> — the worker's cancellation branch still sends a CANCELLED
     * reply, and that copy is dropped by the idempotency gate, which is why
     * cancellation can never substitute for it. And it does <b>not affect the
     * caller</b>: the synthesised reply has already gone out above, and every
     * failure here is swallowed.
     */
    private boolean cancelTimedOutChild(DueEntry due, Map<String, Object> child) {
        if (!cancelOnTimeout) {
            return false;
        }
        if (Boolean.TRUE.equals(child.get("cancel_requested"))
                || "true".equals(String.valueOf(child.get("cancel_requested")))) {
            // A previous sweep already asked. Repeating it every renewal window
            // adds messages, not cancellation.
            return false;
        }
        try {
            registry.markExecutionCancelling(
                    String.valueOf(child.getOrDefault("execution_id", "")),
                    due.entry().sessionId(),
                    Constants.LivenessErrorCode.CHILD_TIMEOUT + ": no reply before deadline");
            LOG.info("Wait sweep requested cancellation of timed-out sub-task {} (session={})",
                    due.entry().childMessageId(), due.entry().sessionId());
            return true;
        } catch (Exception e) {
            LOG.warn("Wait sweep could not cancel timed-out sub-task {} (session={}): {}. "
                    + "The caller was resolved regardless.",
                    due.entry().childMessageId(), due.entry().sessionId(), e.getMessage());
            return false;
        }
    }

    private static long asLong(Object value) {
        if (value instanceof Number n) {
            return n.longValue();
        }
        try {
            return value == null ? 0L : Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    private static boolean envFlag(String name, boolean defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String v = raw.trim().toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("on");
    }

    private static int envInt(String name, int defaultValue) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
