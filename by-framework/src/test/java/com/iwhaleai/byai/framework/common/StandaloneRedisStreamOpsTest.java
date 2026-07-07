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
import redis.clients.jedis.params.XReadGroupParams;

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
}
