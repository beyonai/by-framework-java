package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import redis.clients.jedis.params.SetParams;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WaitSweeperTest {

    @Mock
    private RedisOps redisOps;

    private WaitSweeper sweeper(boolean compensate, boolean prune) {
        return new WaitSweeper(redisOps, "worker-1", compensate, prune,
                Constants.WAIT_SWEEP_INTERVAL_SECONDS,
                Constants.WAIT_PRUNE_INTERVAL_SECONDS,
                Constants.WAIT_SWEEP_LOCK_TTL_SECONDS);
    }

    // --- the two switches are independent (contract section 9) --------------

    @Test
    void pruningRunsWithCompensationOff() {
        // The whole point of keeping the switches separate. Nothing else ever
        // removes a wait-index entry except the reply it was waiting for, so with
        // compensation off every call whose reply never arrives leaves an entry
        // behind forever — in a structure that has no TTL of its own.
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(3L);

        Map<String, Integer> outcomes = sweeper(false, true).sweepOnce();

        assertEquals(3 * Constants.WAIT_INDEX_SHARDS,
                outcomes.get(WaitSweeper.OUTCOME_PRUNED));
    }

    @Test
    void nothingRunsWhenBothSwitchesAreOff() {
        Map<String, Integer> outcomes = sweeper(false, false).sweepOnce();

        assertTrue(outcomes.isEmpty());
        verify(redisOps, never()).zremRangeByScore(anyString(), anyDouble(), anyDouble());
        verify(redisOps, never()).set(anyString(), anyString(), any(SetParams.class));
    }

    @Test
    void theLoopFallsBackToThePruneCadenceWhenCompensationIsOff() {
        // Compensation is what needs a short cadence. With it off the only work
        // left is a garbage collector with a multi-day horizon, so waking every
        // 30 seconds would just burn cycles doing nothing.
        assertEquals(Constants.WAIT_PRUNE_INTERVAL_SECONDS,
                sweeper(false, true).loopIntervalSeconds());
        assertEquals(Constants.WAIT_SWEEP_INTERVAL_SECONDS,
                sweeper(true, true).loopIntervalSeconds());
    }

    // --- prune's cutoff is a proof, not a guess -----------------------------

    @Test
    void pruneCutoffSitsAFullDayBeyondTheSessionTtl() {
        // The longest deadline anything registers is the askUser timeout, which
        // equals the session TTL exactly. The cutoff must be strictly beyond it,
        // or pruning would delete live waits.
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(0L);

        long before = System.currentTimeMillis();
        sweeper(false, true).pruneShard(0);

        ArgumentCaptor<Double> max = ArgumentCaptor.forClass(Double.class);
        verify(redisOps).zremRangeByScore(anyString(), eq(0.0), max.capture());

        long cutoff = max.getValue().longValue();
        long askUserDeadline = before + Constants.DEFAULT_ASK_USER_TIMEOUT_MS;
        assertTrue(cutoff < askUserDeadline,
                "the cutoff must never reach a live askUser deadline");
        assertTrue(Constants.WAIT_PRUNE_AFTER_SECONDS > Constants.DEFAULT_SESSION_TTL,
                "prune horizon must be strictly greater than the session TTL");
    }

    @Test
    void pruneReadsNothingAndDecidesNothing() {
        // Unlike triage: no member decoded, no execution record fetched, no reply
        // produced. That is why it needs no opt-in.
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(1L);

        sweeper(false, true).sweepShard(0, true);

        verify(redisOps, never()).zrangeByScore(anyString(), anyDouble(), anyDouble(), anyInt());
        verify(redisOps, never()).zrem(anyString(), anyString());
    }

    // --- shard claiming ------------------------------------------------------

    @Test
    void aShardAlreadyClaimedByAnotherWorkerIsSkipped() {
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn(null);

        Map<String, Integer> outcomes = sweeper(false, true).sweepShard(0, true);

        assertTrue(outcomes.isEmpty());
        verify(redisOps, never()).zremRangeByScore(anyString(), anyDouble(), anyDouble());
    }

    @Test
    void theClaimIsAlwaysReleased() {
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble()))
                .thenThrow(new RuntimeException("redis down"));

        Map<String, Integer> outcomes = sweeper(false, true).sweepShard(0, true);

        // A sweep is a safety net; a failing shard is recorded, not raised.
        assertEquals(1, outcomes.get(WaitSweeper.OUTCOME_ERROR));
        verify(redisOps).del(Constants.RegistryKeys.waitSweepLock(0));
    }

    @Test
    void aFailedClaimNeverBlocksThePass() {
        // Correctness does not depend on the lock — it only stops a fleet from
        // issuing the same idempotent command N times a cycle.
        when(redisOps.set(anyString(), anyString(), any(SetParams.class)))
                .thenThrow(new RuntimeException("redis down"));

        assertDoesNotThrow(() -> sweeper(false, true).sweepOnce());
    }

    @Test
    void everyShardIsVisitedInAPass() {
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(0L);

        sweeper(false, true).sweepOnce();

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        verify(redisOps, times(Constants.WAIT_INDEX_SHARDS))
                .zremRangeByScore(keys.capture(), anyDouble(), anyDouble());
        assertEquals(Constants.WAIT_INDEX_SHARDS,
                keys.getAllValues().stream().distinct().count(),
                "each shard must be swept exactly once per pass");
    }

    @Test
    void theStartingShardRotatesBetweenPasses() {
        // Nothing is pinned to one owner: a busy worker that always claims first
        // would otherwise monopolise the same shard every cycle.
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(0L);
        WaitSweeper s = sweeper(false, true);

        s.sweepOnce();
        ArgumentCaptor<String> first = ArgumentCaptor.forClass(String.class);
        verify(redisOps, atLeastOnce()).zremRangeByScore(first.capture(), anyDouble(), anyDouble());
        String firstShardOfPassOne = first.getAllValues().get(0);

        clearInvocations(redisOps);
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zremRangeByScore(anyString(), anyDouble(), anyDouble())).thenReturn(0L);
        // Force the prune cadence to elapse for the second pass.
        s.sweepShard(1, true);
        ArgumentCaptor<String> second = ArgumentCaptor.forClass(String.class);
        verify(redisOps, atLeastOnce()).zremRangeByScore(second.capture(), anyDouble(), anyDouble());

        assertNotEquals(firstShardOfPassOne, second.getAllValues().get(0));
    }
}
