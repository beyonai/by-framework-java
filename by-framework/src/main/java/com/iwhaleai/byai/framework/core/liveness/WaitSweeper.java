package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
 * <p>This class currently implements the loop and the pruning half.
 * Compensation — triage, synthesised replies, renewal ceiling, group orphans,
 * timeout cancellation — is the next stage; {@link #compensateEnabled} is
 * already read so the switch exists from the start.
 *
 * <p>Per the cross-SDK contract this half is language-agnostic: a Python or
 * TypeScript sweeper already compensates waits registered by Java, and vice
 * versa, because all three read the same ZSETs.
 */
public class WaitSweeper {

    private static final Logger LOG = LoggerFactory.getLogger(WaitSweeper.class);

    /** Outcome key for entries removed by a prune pass. */
    public static final String OUTCOME_PRUNED = "pruned";
    /** Outcome key for a shard whose sweep threw. */
    public static final String OUTCOME_ERROR = "error";

    private final RedisOps redisOps;
    private final String workerId;
    private final boolean compensateEnabled;
    private final boolean pruneEnabled;
    private final int intervalSeconds;
    private final int pruneIntervalSeconds;
    private final int lockTtlSeconds;

    private ScheduledExecutorService executor;
    private int shardCursor = 0;
    private Long lastPruneNanos = null;

    public WaitSweeper(RedisOps redisOps, String workerId) {
        this(redisOps, workerId,
                envFlag("BY_FRAMEWORK_WAIT_SWEEPER_ENABLED", false),
                envFlag("BY_FRAMEWORK_WAIT_PRUNE_ENABLED", true),
                envInt("BY_FRAMEWORK_WAIT_SWEEP_INTERVAL_SECONDS",
                        Constants.WAIT_SWEEP_INTERVAL_SECONDS),
                envInt("BY_FRAMEWORK_WAIT_PRUNE_INTERVAL_SECONDS",
                        Constants.WAIT_PRUNE_INTERVAL_SECONDS),
                Constants.WAIT_SWEEP_LOCK_TTL_SECONDS);
    }

    public WaitSweeper(RedisOps redisOps, String workerId, boolean compensateEnabled,
            boolean pruneEnabled, int intervalSeconds, int pruneIntervalSeconds,
            int lockTtlSeconds) {
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
            // Compensation lands in the next stage. The switch is read here from
            // the start so its default (off) is established before anything can
            // depend on it being on.
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
