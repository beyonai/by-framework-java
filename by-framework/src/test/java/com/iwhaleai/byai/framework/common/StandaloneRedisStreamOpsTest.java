package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for StandaloneRedisStreamOps: a thin RedisStreamOps adapter over a
 * pooled standalone Jedis connection, borrowed and closed per call.
 */
@ExtendWith(MockitoExtension.class)
class StandaloneRedisStreamOpsTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    @Test
    void xgroupCreateIfNotExistsCreatesGroupFromLastEntry() {
        when(redisClient.getResource()).thenReturn(jedis);
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xgroupCreateIfNotExists("stream-a", "group-a");

        verify(jedis).xgroupCreate("stream-a", "group-a", StreamEntryID.LAST_ENTRY, true);
    }

    @Test
    void xgroupCreateIfNotExistsSwallowsBusygroupError() {
        when(redisClient.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException("BUSYGROUP Consumer Group name already exists"))
                .when(jedis).xgroupCreate(anyString(), anyString(), any(), anyBoolean());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        assertDoesNotThrow(() -> ops.xgroupCreateIfNotExists("stream-a", "group-a"));
    }

    @Test
    void xgroupCreateIfNotExistsSwallowsOtherErrorsToo() {
        when(redisClient.getResource()).thenReturn(jedis);
        doThrow(new RuntimeException("connection refused"))
                .when(jedis).xgroupCreate(anyString(), anyString(), any(), anyBoolean());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        assertDoesNotThrow(() -> ops.xgroupCreateIfNotExists("stream-a", "group-a"));
    }

    @Test
    void xackDelegatesToJedis() {
        when(redisClient.getResource()).thenReturn(jedis);
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);
        StreamEntryID id = new StreamEntryID("123-0");

        ops.xack("stream-a", "group-a", id);

        verify(jedis).xack("stream-a", "group-a", id);
    }

    @Test
    void xreadGroupReadsOnlyTheGivenStream() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, 2000);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xreadGroup(eq("group-a"), eq("consumer-a"), any(), streamsCaptor.capture());
        assertEquals(Map.of("stream-a", StreamEntryID.UNRECEIVED_ENTRY), streamsCaptor.getValue());
    }

    @Test
    void xreadGroupOmitsBlockWhenBlockMsIsNull() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, null);

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        verify(jedis).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), anyMap());
        CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
        paramsCaptor.getValue().addParams(args);
        assertFalse(args.isBlocking(), "a null blockMs must not issue a BLOCK 0 (block-forever) read");
    }

    @Test
    void xreadGroupSetsBlockWhenBlockMsIsProvided() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, 2000);

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        verify(jedis).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), anyMap());
        CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
        paramsCaptor.getValue().addParams(args);
        assertTrue(args.isBlocking());
    }

    @Test
    void xaddNoTrimDoesNotSetMaxLen() {
        when(redisClient.getResource()).thenReturn(jedis);
        StreamEntryID generatedId = new StreamEntryID("1-0");
        when(jedis.xadd(eq("stream-a"), any(XAddParams.class), anyMap())).thenReturn(generatedId);
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        StreamEntryID result = ops.xadd("stream-a", Map.of("k", "v"), XAddOptions.noTrim());

        assertEquals(generatedId, result);
        ArgumentCaptor<XAddParams> paramsCaptor = ArgumentCaptor.forClass(XAddParams.class);
        verify(jedis).xadd(eq("stream-a"), paramsCaptor.capture(), eq(Map.of("k", "v")));
        CommandArguments args = new CommandArguments(Protocol.Command.XADD);
        paramsCaptor.getValue().addParams(args);
        assertFalse(containsKeyword(args, "MAXLEN"));
    }

    @Test
    void xaddTrimToUsesApproximateMaxLen() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xadd(eq("stream-a"), any(XAddParams.class), anyMap())).thenReturn(new StreamEntryID("1-0"));
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xadd("stream-a", Map.of("k", "v"), XAddOptions.trimTo(1000));

        ArgumentCaptor<XAddParams> paramsCaptor = ArgumentCaptor.forClass(XAddParams.class);
        verify(jedis).xadd(eq("stream-a"), paramsCaptor.capture(), eq(Map.of("k", "v")));
        CommandArguments args = new CommandArguments(Protocol.Command.XADD);
        paramsCaptor.getValue().addParams(args);
        assertTrue(containsKeyword(args, "MAXLEN"));
        assertTrue(containsKeyword(args, "~"), "trimTo must always use approximate (~) trimming, never exact");
        assertFalse(containsExactValue(args, "1000") && containsKeywordImmediatelyBefore(args, "1000", "="),
                "trimTo must never request exact (=) trimming");
    }

    private static boolean containsKeyword(CommandArguments args, String keyword) {
        for (var rawable : args) {
            if (keyword.equals(new String(rawable.getRaw()))) {
                return true;
            }
        }
        return false;
    }

    private static boolean containsExactValue(CommandArguments args, String value) {
        return containsKeyword(args, value);
    }

    private static boolean containsKeywordImmediatelyBefore(CommandArguments args, String value, String keyword) {
        List<String> tokens = new ArrayList<>();
        for (var rawable : args) {
            tokens.add(new String(rawable.getRaw()));
        }
        int idx = tokens.indexOf(value);
        return idx > 0 && keyword.equals(tokens.get(idx - 1));
    }

    @Test
    void xpendingRangeScansFromTheBeginningOfTheStream() {
        when(redisClient.getResource()).thenReturn(jedis);
        StreamPendingEntry entry = new StreamPendingEntry(new StreamEntryID("1-0"), "consumer-a", 100L, 1);
        when(jedis.xpending(eq("stream-a"), eq("group-a"), any(XPendingParams.class)))
                .thenReturn(List.of(entry));
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        List<StreamPendingEntry> result = ops.xpendingRange("stream-a", "group-a", 50);

        assertEquals(List.of(entry), result);
        verify(jedis).xpending(eq("stream-a"), eq("group-a"), any(XPendingParams.class));
    }

    @Test
    void xclaimReturnsActualClaimedEntriesNotJustACount() {
        when(redisClient.getResource()).thenReturn(jedis);
        StreamEntryID id = new StreamEntryID("1-0");
        redis.clients.jedis.resps.StreamEntry claimed =
                new redis.clients.jedis.resps.StreamEntry(id, Map.of("k", "v"));
        when(jedis.xclaim(eq("stream-a"), eq("group-a"), eq("consumer-a"), eq(5000L), any(), eq(id)))
                .thenReturn(List.of(claimed));
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        List<redis.clients.jedis.resps.StreamEntry> result =
                ops.xclaim("stream-a", "group-a", "consumer-a", 5000L, id);

        assertEquals(List.of(claimed), result);
    }

    @Test
    void xreadNonGroupReadsOnlyTheGivenStream() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(List.of());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xread("stream-a", StreamEntryID.LAST_ENTRY, 10, null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis).xread(any(XReadParams.class), streamsCaptor.capture());
        assertEquals(Map.of("stream-a", StreamEntryID.LAST_ENTRY), streamsCaptor.getValue());
    }

    @Test
    void xreadOmitsBlockWhenBlockMsIsNull() {
        when(redisClient.getResource()).thenReturn(jedis);
        when(jedis.xread(any(XReadParams.class), anyMap())).thenReturn(List.of());
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);

        ops.xread("stream-a", StreamEntryID.LAST_ENTRY, 10, null);

        ArgumentCaptor<XReadParams> paramsCaptor = ArgumentCaptor.forClass(XReadParams.class);
        verify(jedis).xread(paramsCaptor.capture(), anyMap());
        CommandArguments args = new CommandArguments(Protocol.Command.XREAD);
        paramsCaptor.getValue().addParams(args);
        assertFalse(containsKeyword(args, "BLOCK"));
    }

    @Test
    void xdelDelegatesToJedis() {
        when(redisClient.getResource()).thenReturn(jedis);
        StandaloneRedisStreamOps ops = new StandaloneRedisStreamOps(redisClient);
        StreamEntryID id = new StreamEntryID("1-0");
        when(jedis.xdel("stream-a", id)).thenReturn(1L);

        long result = ops.xdel("stream-a", id);

        assertEquals(1L, result);
    }
}
