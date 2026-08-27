package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.core.protocol.AgentState;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ResumeCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class WaitGateTest {

    @Mock
    private RedisOps redisOps;

    private static ResumeCommand subAgentReply() {
        // A sub-agent's reply: the ids are reversed on the way back, so
        // messageId is the CALLER's and parentMessageId is the sub-task's.
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-caller").sessionId("sess-1").traceId("t-1")
                        .sourceAgentType("agent-child").targetAgentType("agent-caller")
                        .parentMessageId("msg-child").build(),
                "done", AgentState.COMPLETED, Map.of(), Map.of());
    }

    private static ResumeCommand groupMemberReply() {
        return ResumeCommand.of(
                MessageHeader.builder()
                        .messageId("msg-caller").sessionId("sess-1").traceId("t-1")
                        .sourceAgentType("agent-child").targetAgentType("agent-caller")
                        .parentMessageId("msg-child").taskGroupId("tg-1").build(),
                "done", AgentState.COMPLETED, Map.of(), Map.of());
    }

    // --- candidate order (contract section 4) -------------------------------

    @Test
    void theDerivedMemberIsTriedFirstAndTheAskUserVariantSecond() {
        List<String> candidates = WaitGate.candidateMembers(subAgentReply());
        assertEquals(2, candidates.size());
        assertEquals(WaitIndex.encodeMember("sess-1", "msg-caller", "msg-child", ""),
                candidates.get(0));
        // The askUser variant has an empty child id, because askUser has no sub-task.
        assertEquals(WaitIndex.encodeMember("sess-1", "msg-caller", "", ""),
                candidates.get(1));
    }

    @Test
    void aGroupReplyNeverConsidersTheAskUserVariant() {
        // A reply carrying a task group id is a sub-agent reply by construction.
        List<String> candidates = WaitGate.candidateMembers(groupMemberReply());
        assertEquals(1, candidates.size());
    }

    @Test
    void aDuplicateSubAgentReplyMustNotClearALiveAskUserWait() {
        // THE case the candidate order exists for. Both waits belong to the same
        // caller: an askUser that is still live, and a sub-agent reply that was
        // already consumed. Resolving candidates in the wrong order — or checking
        // both entries before either marker — would let the duplicate fall through
        // to the askUser candidate and clear a wait that is still waiting.
        String derived = WaitIndex.encodeMember("sess-1", "msg-caller", "msg-child", "");
        String askUser = WaitIndex.encodeMember("sess-1", "msg-caller", "", "");
        String indexKey = Constants.RegistryKeys.waitIndex(WaitIndex.shardForSession("sess-1"));

        // The sub-agent entry is gone (already claimed) but its marker is set.
        when(redisOps.zrem(eq(indexKey), eq(derived))).thenReturn(0L);
        when(redisOps.exists(eq(WaitGate.consumedMarkerKey("sess-1", derived)))).thenReturn(true);
        // The askUser entry is still live.
        when(redisOps.zrem(eq(indexKey), eq(askUser))).thenReturn(1L);

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertFalse(decision.allow(), "a duplicate must be dropped by its own marker");
        assertEquals(WaitGate.DENY_ALREADY_CONSUMED, decision.reason());
        // And crucially, the live askUser entry must be untouched.
        verify(redisOps, never()).zrem(eq(indexKey), eq(askUser));
    }

    // --- the two meanings of ZREM 0 (contract section 5) ---------------------

    @Test
    void claimingTheEntryAllowsTheReplyAndMarksItConsumed() {
        when(redisOps.zrem(anyString(), anyString())).thenReturn(1L);

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertTrue(decision.allow());
        assertEquals(WaitGate.ALLOW_CLAIMED, decision.reason());
        verify(redisOps).setex(anyString(), eq(Constants.WAIT_CONSUMED_TTL_SECONDS), eq("1"));
    }

    @Test
    void zeroWithoutAMarkerMeansUnregisteredAndMustBeLetThrough() {
        // The rolling-upgrade case: every in-flight reply from before this version
        // shipped looks exactly like this. Treating it as a duplicate would drop a
        // real reply on every deploy.
        when(redisOps.zrem(anyString(), anyString())).thenReturn(0L);
        when(redisOps.exists(anyString())).thenReturn(false);

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertTrue(decision.allow(), "unknown is not the same as duplicate");
        assertEquals(WaitGate.ALLOW_UNREGISTERED, decision.reason());
    }

    @Test
    void zeroWithAMarkerMeansDuplicateAndIsDropped() {
        when(redisOps.zrem(anyString(), anyString())).thenReturn(0L);
        when(redisOps.exists(anyString())).thenReturn(true);

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertFalse(decision.allow());
        assertEquals(WaitGate.DENY_ALREADY_CONSUMED, decision.reason());
    }

    @Test
    void theGateFailsOpenOnAnyRedisError() {
        // A gate that drops messages when Redis hiccups is worse than the
        // duplicate it was built to prevent.
        when(redisOps.zrem(anyString(), anyString()))
                .thenThrow(new RuntimeException("redis down"));

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertTrue(decision.allow());
        assertEquals(WaitGate.ALLOW_GATE_ERROR, decision.reason());
    }

    @Test
    void aFailedConsumedMarkerDoesNotBlockTheClaim() {
        // Losing the marker costs one extra wake-up much later, which is the
        // direction this whole class errs in.
        when(redisOps.zrem(anyString(), anyString())).thenReturn(1L);
        doThrow(new RuntimeException("redis down"))
                .when(redisOps).setex(anyString(), anyInt(), anyString());

        WaitGate.Decision decision = WaitGate.consumeWaitEntry(redisOps, subAgentReply());

        assertTrue(decision.allow());
        assertEquals(WaitGate.ALLOW_CLAIMED, decision.reason());
    }

    // --- digest (cross-SDK contract) ----------------------------------------

    @Test
    void memberDigestMatchesThePythonReferenceValues() {
        // Key names are derived from this, so a divergence means the two SDKs
        // write and read different marker keys.
        assertEquals("641ff61cb0ea3832e467f7a2fc3c1f9ca7d34c6d",
                WaitIndex.memberDigest("sess-1|msg-a|msg-b|tg-1"));
        assertEquals("4fbd0c114399780e9cf033d4362bc8c6063bae1b",
                WaitIndex.memberDigest("a\\|b|c\\\\d||"));
    }
}
