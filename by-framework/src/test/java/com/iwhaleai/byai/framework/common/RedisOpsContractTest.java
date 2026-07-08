package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Shared contract asserting StandaloneRedisOps and ClusterRedisOps expose
 * identical business semantics. Both Jedis and JedisCluster implement
 * JedisCommands, so the same assertions run against whichever concrete mock
 * each subclass wires up.
 */
abstract class RedisOpsContractTest {

    protected abstract RedisOps ops();

    protected abstract JedisCommands redisCommandsMock();

    @Test
    void getDelegates() {
        when(redisCommandsMock().get("k")).thenReturn("v");
        assertEquals("v", ops().get("k"));
    }

    @Test
    void setDelegatesWithParams() {
        SetParams params = SetParams.setParams().nx().ex(60);
        when(redisCommandsMock().set("k", "v", params)).thenReturn("OK");
        assertEquals("OK", ops().set("k", "v", params));
    }

    @Test
    void setexDelegates() {
        ops().setex("k", 60, "v");
        verify(redisCommandsMock()).setex("k", 60, "v");
    }

    @Test
    void delDelegates() {
        when(redisCommandsMock().del("k")).thenReturn(1L);
        assertEquals(1L, ops().del("k"));
    }

    @Test
    void expireDelegates() {
        when(redisCommandsMock().expire("k", 60)).thenReturn(1L);
        assertEquals(1L, ops().expire("k", 60));
    }

    @Test
    void incrDelegates() {
        when(redisCommandsMock().incr("k")).thenReturn(2L);
        assertEquals(2L, ops().incr("k"));
    }

    @Test
    void sismemberDelegates() {
        when(redisCommandsMock().sismember("k", "m")).thenReturn(true);
        assertTrue(ops().sismember("k", "m"));
    }

    @Test
    void hgetDelegates() {
        when(redisCommandsMock().hget("k", "f")).thenReturn("v");
        assertEquals("v", ops().hget("k", "f"));
    }

    @Test
    void hgetAllDelegates() {
        when(redisCommandsMock().hgetAll("k")).thenReturn(Map.of("f", "v"));
        assertEquals(Map.of("f", "v"), ops().hgetAll("k"));
    }

    @Test
    void hsetDelegates() {
        ops().hset("k", "f", "v");
        verify(redisCommandsMock()).hset("k", "f", "v");
    }

    @Test
    void hsetAllDelegates() {
        when(redisCommandsMock().hset("k", Map.of("f1", "v1", "f2", "v2"))).thenReturn(2L);
        assertEquals(2L, ops().hsetAll("k", Map.of("f1", "v1", "f2", "v2")));
    }

    @Test
    void hincrByDelegates() {
        when(redisCommandsMock().hincrBy("k", "f", 1L)).thenReturn(5L);
        assertEquals(5L, ops().hincrBy("k", "f", 1L));
    }

    @Test
    void saddDelegates() {
        when(redisCommandsMock().sadd("k", "a", "b")).thenReturn(2L);
        assertEquals(2L, ops().sadd("k", "a", "b"));
    }

    @Test
    void sremDelegates() {
        when(redisCommandsMock().srem("k", "a")).thenReturn(1L);
        assertEquals(1L, ops().srem("k", "a"));
    }

    @Test
    void smembersDelegates() {
        when(redisCommandsMock().smembers("k")).thenReturn(Set.of("a", "b"));
        assertEquals(Set.of("a", "b"), ops().smembers("k"));
    }

    @Test
    void saveSessionExecutionHsetsEachFieldAndRefreshesTtl() {
        String key = Constants.RegistryKeys.sessionRegistry("sess-1");

        ops().saveSessionExecution("sess-1", Map.of("exec:e1", "{}"), 600);

        verify(redisCommandsMock()).hset(key, "exec:e1", "{}");
        verify(redisCommandsMock()).expire(key, 600);
    }

    @Test
    void updateWorkerAdminStateWritesAllThreeFields() {
        String key = Constants.RegistryKeys.workerAdminState("w1");

        ops().updateWorkerAdminState("w1", "suspended", "reason-x", 123L);

        verify(redisCommandsMock()).hset(key, "lifecycle", "suspended");
        verify(redisCommandsMock()).hset(key, "reason", "reason-x");
        verify(redisCommandsMock()).hset(key, "updated_at", "123");
    }

    @Test
    void batchGetWorkerAdminStatesFansOutPerWorker() {
        when(redisCommandsMock().hgetAll(Constants.RegistryKeys.workerAdminState("w1")))
                .thenReturn(Map.of("lifecycle", "active"));
        when(redisCommandsMock().hgetAll(Constants.RegistryKeys.workerAdminState("w2")))
                .thenReturn(Map.of());

        Map<String, Map<String, String>> result = ops().batchGetWorkerAdminStates(List.of("w1", "w2"));

        assertEquals(Map.of("lifecycle", "active"), result.get("w1"));
        assertEquals(Map.of(), result.get("w2"));
    }

    @Test
    void registerServiceInstanceWritesDetailsAndActiveIndex() {
        String detailsKey = Constants.RegistryKeys.sdInstanceDetails("svc");
        String activeKey = Constants.RegistryKeys.sdActiveInstances("svc");

        ops().registerServiceInstance("svc", "svc:1", "{\"id\":\"svc:1\"}", 1000L);

        verify(redisCommandsMock()).hset(detailsKey, "svc:1", "{\"id\":\"svc:1\"}");
        verify(redisCommandsMock()).zadd(activeKey, 1000d, "svc:1");
    }

    @Test
    void heartbeatServiceInstanceRefreshesActiveIndexScore() {
        String activeKey = Constants.RegistryKeys.sdActiveInstances("svc");

        ops().heartbeatServiceInstance("svc", "svc:1", 2000L);

        verify(redisCommandsMock()).zadd(activeKey, 2000d, "svc:1");
    }

    @Test
    void unregisterServiceInstanceRemovesDetailsAndActiveIndexEntry() {
        String detailsKey = Constants.RegistryKeys.sdInstanceDetails("svc");
        String activeKey = Constants.RegistryKeys.sdActiveInstances("svc");

        ops().unregisterServiceInstance("svc", "svc:1");

        verify(redisCommandsMock()).hdel(detailsKey, "svc:1");
        verify(redisCommandsMock()).zrem(activeKey, "svc:1");
    }

    @Test
    void fetchActiveServiceInstancesReturnsPayloadAndHeartbeatPerInstance() {
        String activeKey = Constants.RegistryKeys.sdActiveInstances("svc");
        String detailsKey = Constants.RegistryKeys.sdInstanceDetails("svc");

        Tuple tuple = mock(Tuple.class);
        when(tuple.getElement()).thenReturn("svc:1");
        when(tuple.getScore()).thenReturn(1500d);
        when(redisCommandsMock().zrangeWithScores(activeKey, 0, -1)).thenReturn(List.of(tuple));
        when(redisCommandsMock().hmget(detailsKey, "svc:1")).thenReturn(List.of("{\"id\":\"svc:1\"}"));

        Map<String, RedisOps.ActiveInstanceRecord> result = ops().fetchActiveServiceInstances("svc");

        assertEquals(1, result.size());
        RedisOps.ActiveInstanceRecord record = result.get("svc:1");
        assertEquals("{\"id\":\"svc:1\"}", record.instanceJson());
        assertEquals(1500L, record.lastHeartbeatMs());
    }

    @Test
    void rpushDelegates() {
        when(redisCommandsMock().rpush("k", "v")).thenReturn(1L);
        assertEquals(1L, ops().rpush("k", "v"));
    }

    @Test
    void zaddDelegates() {
        when(redisCommandsMock().zadd("k", 1.5d, "m")).thenReturn(1L);
        assertEquals(1L, ops().zadd("k", 1.5d, "m"));
    }
}
