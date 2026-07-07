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
import redis.clients.jedis.params.XReadGroupParams;

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
}
