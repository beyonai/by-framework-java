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

import java.util.List;
import java.util.Map;

import com.iwhaleai.byai.framework.core.protocol.AgentState;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WaitSweeperTest {

    @Mock
    private RedisOps redisOps;

    @Mock
    private com.iwhaleai.byai.framework.common.RedisStreamOps streamOps;

    @Mock
    private com.iwhaleai.byai.framework.core.WorkerRegistry registry;

    private WaitSweeper sweeper(boolean compensate, boolean prune) {
        return new WaitSweeper(redisOps, streamOps, registry, "worker-1", compensate, prune,
                Constants.WAIT_SWEEP_INTERVAL_SECONDS,
                Constants.WAIT_PRUNE_INTERVAL_SECONDS,
                Constants.WAIT_SWEEP_LOCK_TTL_SECONDS,
                Constants.WAIT_RENEW_MAX_MULTIPLE,
                true);
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

    // --- compensation triage ------------------------------------------------

    private static final String SESSION = "sess-1";
    private static final String CALLER_MSG = "msg-caller";
    private static final String CHILD_MSG = "msg-child";

    private String member(String childMessageId, String groupId) {
        return WaitIndex.encodeMember(SESSION, CALLER_MSG, childMessageId, groupId);
    }

    /** Arms one due entry in shard N with the given member. */
    private WaitSweeper armedSweeper(String member) {
        int shard = WaitIndex.shardForSession(SESSION);
        when(redisOps.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(redisOps.zrangeByScore(eq(Constants.RegistryKeys.waitIndex(shard)),
                anyDouble(), anyDouble(), anyInt())).thenReturn(List.of(member));
        when(redisOps.zscore(anyString(), anyString()))
                .thenReturn((double) (System.currentTimeMillis() - 1000));
        return sweeper(true, false);
    }

    private static Map<String, Object> execution(String status, String workerId) {
        Map<String, Object> m = new java.util.HashMap<>();
        m.put("status", status);
        m.put("worker_id", workerId);
        m.put("execution_id", "exec-x");
        m.put("target_agent_type", "agent-child");
        m.put("source_agent_type", "agent-caller");
        m.put("created_at", System.currentTimeMillis() - 60_000);
        return m;
    }

    @Test
    void aMalformedMemberIsDroppedRatherThanRetriedForever() {
        WaitSweeper s = armedSweeper("not|enough|fields");

        Map<String, Integer> outcomes = s.sweepOnce();

        assertEquals(1, outcomes.get(WaitSweeper.OUTCOME_MALFORMED));
        verify(redisOps).zrem(anyString(), eq("not|enough|fields"));
    }

    @Test
    void anEntryWhoseCallerHasNoRecordIsDropped() {
        // The session registry expired: no reply, synthesised or real, can
        // reattach this caller any more.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION))).thenReturn(null);

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_CALLER_MISSING));
        verify(redisOps).zrem(anyString(), eq(member(CHILD_MSG, "")));
    }

    @Test
    void anAlreadyTerminalCallerIsNeverWokenAgain() {
        // Waking a finished execution is exactly what the idempotency work
        // exists to prevent — clean up, synthesise nothing.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.COMPLETED, "worker-a"));

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_CALLER_TERMINAL));
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
    }

    @Test
    void anAskUserWaitIsSkippedRatherThanCompensated() {
        // "The human hasn't answered yet" is not a fault and has no
        // compensation; the entry stays so a repeated answer is still
        // recognised as a duplicate by the gate.
        WaitSweeper s = armedSweeper(member("", ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_USER, "worker-a"));

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_ASK_USER_SKIPPED));
        verify(redisOps, never()).zrem(anyString(), anyString());
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
    }

    @Test
    void aCalleeThatIsItselfWaitingIsRenewedNotFailed() {
        // Its own entry has a deadline of its own and will fail first if that
        // wait breaks. Killing this one now would collapse the whole chain at
        // once and report the wrong cause at every level.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-b"));

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_CHILD_WAITING));
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
        // Renewed, not removed.
        verify(redisOps).zadd(anyString(), anyDouble(), eq(member(CHILD_MSG, "")));
    }

    @Test
    void aCalleeWhoseWorkerIsGoneGetsASynthesisedFailure() {
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.RUNNING, "worker-dead"));
        when(registry.isWorkerOnline("worker-dead")).thenReturn(false);

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_WORKER_LOST));
        verify(streamOps).xadd(anyString(), anyMap(), any());
    }

    @Test
    void aCalleeOnALiveWorkerBuysMoreTimeUpToTheCeiling() {
        // Running long is not the same as being dead, and the lease is the only
        // signal that tells them apart.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.RUNNING, "worker-live"));
        when(registry.isWorkerOnline("worker-live")).thenReturn(true);

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_CHILD_ALIVE));
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
    }

    @Test
    void theSweeperNeverWritesGroupAccountingItself() {
        // Writing the result and the counter here would be a second
        // implementation of the group's accounting, and when THAT copy is the
        // increment reaching total there is no reply left to trigger the join.
        // The synthesised reply carries the group id so the existing join does it.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, "tg-1"));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.RUNNING, "worker-dead"));
        when(registry.isWorkerOnline("worker-dead")).thenReturn(false);
        when(redisOps.hget(anyString(), eq(Constants.TASK_GROUP_FIELD_TOTAL))).thenReturn("2");

        s.sweepOnce();

        verify(redisOps, never()).hincrBy(anyString(), eq("completed"), anyLong());
        verify(streamOps).xadd(anyString(), anyMap(), any());
    }

    @Test
    void aGroupMemberAlreadyJoinedByItsReplyIsDropped() {
        // A result under this sub-task's id can only have been written by the
        // join, so its reply already arrived and was counted. A second
        // synthesised reply would be counted a second time.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, "tg-1"));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(redisOps.hget(anyString(), eq(Constants.TASK_GROUP_FIELD_TOTAL))).thenReturn("2");
        when(redisOps.hget(anyString(), eq(CHILD_MSG))).thenReturn("{\"status\":\"COMPLETED\"}");

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_GROUP_ALREADY_JOINED));
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
    }

    @Test
    void aVanishedTaskGroupIsCleanedUpNotCompensated() {
        // A reply would find no group to join, so it would resume the caller
        // with one sibling's payload where the aggregate belongs.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, "tg-1"));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(redisOps.hget(anyString(), eq(Constants.TASK_GROUP_FIELD_TOTAL))).thenReturn(null);

        assertEquals(1, s.sweepOnce().get(WaitSweeper.OUTCOME_GROUP_GONE));
        verify(streamOps, never()).xadd(anyString(), anyMap(), any());
    }

    @Test
    void theWaitEntryIsRenewedRatherThanRemovedAfterSynthesising() {
        // Removing it would leave the synthesised reply as the only copy that
        // must not be gated — and a reply that bypasses the gate is a second
        // wake-up path, which is what makes double-resumes possible at all.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.RUNNING, "worker-dead"));
        when(registry.isWorkerOnline("worker-dead")).thenReturn(false);

        s.sweepOnce();

        verify(redisOps).zadd(anyString(), anyDouble(), eq(member(CHILD_MSG, "")));
        verify(redisOps, never()).zrem(anyString(), eq(member(CHILD_MSG, "")));
    }

    @Test
    void theFirstRenewalRecordsTheOriginalDeadlineWithNx() {
        // Overwriting the score destroys the only record of the original
        // deadline; without saving it, the ceiling would re-measure from the
        // deadline it just pushed out and could never be reached.
        WaitSweeper s = armedSweeper(member(CHILD_MSG, ""));
        when(registry.getExecutionByMessageId(eq(CALLER_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-a"));
        when(registry.getExecutionByMessageId(eq(CHILD_MSG), eq(SESSION)))
                .thenReturn(execution(AgentState.WAITING_AGENT, "worker-b"));

        s.sweepOnce();

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        verify(redisOps, atLeastOnce()).set(keys.capture(), anyString(), any(SetParams.class));
        assertTrue(keys.getAllValues().stream().anyMatch(k -> k.contains("wait_renew_origin")),
                "the first renewal must save the original deadline");
    }
}
