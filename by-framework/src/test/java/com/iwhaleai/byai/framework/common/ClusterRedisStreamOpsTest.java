package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for ClusterRedisStreamOps: a thin RedisStreamOps adapter directly over
 * the shared JedisCluster instance - unlike the standalone adapter, there is
 * no per-call borrow/close since JedisCluster is a long-lived client.
 */
@ExtendWith(MockitoExtension.class)
class ClusterRedisStreamOpsTest {

    @Mock
    private JedisCluster jedisCluster;

    @Test
    void xgroupCreateIfNotExistsCreatesGroupFromLastEntry() {
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xgroupCreateIfNotExists("stream-a", "group-a");

        verify(jedisCluster).xgroupCreate("stream-a", "group-a", StreamEntryID.LAST_ENTRY, true);
    }

    @Test
    void xgroupCreateIfNotExistsSwallowsBusygroupError() {
        doThrow(new RuntimeException("BUSYGROUP Consumer Group name already exists"))
                .when(jedisCluster).xgroupCreate(anyString(), anyString(), any(), anyBoolean());
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        assertDoesNotThrow(() -> ops.xgroupCreateIfNotExists("stream-a", "group-a"));
    }

    @Test
    void xackDelegatesToJedisCluster() {
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);
        StreamEntryID id = new StreamEntryID("123-0");

        ops.xack("stream-a", "group-a", id);

        verify(jedisCluster).xack("stream-a", "group-a", id);
    }

    @Test
    void xreadGroupReadsOnlyTheGivenStream() {
        when(jedisCluster.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, 2000);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedisCluster).xreadGroup(eq("group-a"), eq("consumer-a"), any(), streamsCaptor.capture());
        assertEquals(Map.of("stream-a", StreamEntryID.UNRECEIVED_ENTRY), streamsCaptor.getValue());
    }

    @Test
    void xreadGroupOmitsBlockWhenBlockMsIsNull() {
        when(jedisCluster.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, null);

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        verify(jedisCluster).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), anyMap());
        CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
        paramsCaptor.getValue().addParams(args);
        assertFalse(args.isBlocking(), "a null blockMs must not issue a BLOCK 0 (block-forever) read");
    }

    @Test
    void xreadGroupSetsBlockWhenBlockMsIsProvided() {
        when(jedisCluster.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(List.of());
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xreadGroup("stream-a", "group-a", "consumer-a", 10, 2000);

        ArgumentCaptor<XReadGroupParams> paramsCaptor = ArgumentCaptor.forClass(XReadGroupParams.class);
        verify(jedisCluster).xreadGroup(anyString(), anyString(), paramsCaptor.capture(), anyMap());
        CommandArguments args = new CommandArguments(Protocol.Command.XREADGROUP);
        paramsCaptor.getValue().addParams(args);
        assertTrue(args.isBlocking());
    }

    private static boolean containsKeyword(CommandArguments args, String keyword) {
        for (var rawable : args) {
            if (keyword.equals(new String(rawable.getRaw()))) {
                return true;
            }
        }
        return false;
    }

    @Test
    void xaddNoTrimDoesNotSetMaxLen() {
        when(jedisCluster.xadd(eq("stream-a"), any(XAddParams.class), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xadd("stream-a", Map.of("k", "v"), XAddOptions.noTrim());

        ArgumentCaptor<XAddParams> paramsCaptor = ArgumentCaptor.forClass(XAddParams.class);
        verify(jedisCluster).xadd(eq("stream-a"), paramsCaptor.capture(), eq(Map.of("k", "v")));
        CommandArguments args = new CommandArguments(Protocol.Command.XADD);
        paramsCaptor.getValue().addParams(args);
        assertFalse(containsKeyword(args, "MAXLEN"));
    }

    @Test
    void xaddTrimToUsesApproximateMaxLen() {
        when(jedisCluster.xadd(eq("stream-a"), any(XAddParams.class), anyMap()))
                .thenReturn(new StreamEntryID("1-0"));
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xadd("stream-a", Map.of("k", "v"), XAddOptions.trimTo(1000));

        ArgumentCaptor<XAddParams> paramsCaptor = ArgumentCaptor.forClass(XAddParams.class);
        verify(jedisCluster).xadd(eq("stream-a"), paramsCaptor.capture(), eq(Map.of("k", "v")));
        CommandArguments args = new CommandArguments(Protocol.Command.XADD);
        paramsCaptor.getValue().addParams(args);
        assertTrue(containsKeyword(args, "MAXLEN"));
        assertTrue(containsKeyword(args, "~"), "trimTo must always use approximate (~) trimming, never exact");
    }

    @Test
    void xpendingRangeScansFromTheBeginningOfTheStream() {
        StreamPendingEntry entry = new StreamPendingEntry(new StreamEntryID("1-0"), "consumer-a", 100L, 1);
        when(jedisCluster.xpending(eq("stream-a"), eq("group-a"), any(XPendingParams.class)))
                .thenReturn(List.of(entry));
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        List<StreamPendingEntry> result = ops.xpendingRange("stream-a", "group-a", 50);

        assertEquals(List.of(entry), result);
    }

    @Test
    void xclaimReturnsActualClaimedEntriesNotJustACount() {
        StreamEntryID id = new StreamEntryID("1-0");
        StreamEntry claimed = new StreamEntry(id, Map.of("k", "v"));
        when(jedisCluster.xclaim(eq("stream-a"), eq("group-a"), eq("consumer-a"), eq(5000L), any(), eq(id)))
                .thenReturn(List.of(claimed));
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        List<StreamEntry> result = ops.xclaim("stream-a", "group-a", "consumer-a", 5000L, id);

        assertEquals(List.of(claimed), result);
    }

    @Test
    void xreadNonGroupReadsOnlyTheGivenStream() {
        when(jedisCluster.xread(any(XReadParams.class), anyMap())).thenReturn(List.of());
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);

        ops.xread("stream-a", StreamEntryID.LAST_ENTRY, 10, null);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, StreamEntryID>> streamsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedisCluster).xread(any(XReadParams.class), streamsCaptor.capture());
        assertEquals(Map.of("stream-a", StreamEntryID.LAST_ENTRY), streamsCaptor.getValue());
    }

    @Test
    void xdelDelegatesToJedisCluster() {
        ClusterRedisStreamOps ops = new ClusterRedisStreamOps(jedisCluster);
        StreamEntryID id = new StreamEntryID("1-0");
        when(jedisCluster.xdel("stream-a", id)).thenReturn(1L);

        long result = ops.xdel("stream-a", id);

        assertEquals(1L, result);
    }
}
