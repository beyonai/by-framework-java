package com.iwhaleai.byai.framework.common;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.List;
import java.util.Map;

/**
 * RedisStreamOps backed directly by the shared JedisCluster instance.
 * JedisCluster manages its own per-slot connections internally, so unlike
 * StandaloneRedisStreamOps there is no per-call borrow/close - the instance
 * is held for the lifetime of this class and never closed here.
 */
@Slf4j
public class ClusterRedisStreamOps implements RedisStreamOps {
    private final JedisCluster jedisCluster;

    public ClusterRedisStreamOps(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public void xgroupCreateIfNotExists(String streamName, String groupName) {
        try {
            jedisCluster.xgroupCreate(streamName, groupName, StreamEntryID.LAST_ENTRY, true);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                log.warn("Warning setting up stream {}: {}", streamName, e.getMessage());
            }
        }
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xreadGroup(
            String streamName, String groupName, String consumerName, int count, Integer blockMs) {
        XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);
        if (blockMs != null) {
            params.block(blockMs);
        }
        return jedisCluster.xreadGroup(groupName, consumerName, params,
                Map.of(streamName, StreamEntryID.UNRECEIVED_ENTRY));
    }

    @Override
    public void xack(String streamName, String groupName, StreamEntryID id) {
        jedisCluster.xack(streamName, groupName, id);
    }

    @Override
    public StreamEntryID xadd(String streamName, Map<String, String> fields, XAddOptions options) {
        return jedisCluster.xadd(streamName, options.toParams(), fields);
    }

    @Override
    public List<StreamPendingEntry> xpendingRange(String streamName, String groupName, int count) {
        XPendingParams params = XPendingParams.xPendingParams(
                StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, count);
        return jedisCluster.xpending(streamName, groupName, params);
    }

    @Override
    public List<StreamEntry> xclaim(
            String streamName, String groupName, String consumerName, long minIdleTimeMs, StreamEntryID... ids) {
        return jedisCluster.xclaim(streamName, groupName, consumerName, minIdleTimeMs, XClaimParams.xClaimParams(),
                ids);
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xread(
            String streamName, StreamEntryID afterId, int count, Integer blockMs) {
        XReadParams params = XReadParams.xReadParams().count(count);
        if (blockMs != null) {
            params.block(blockMs);
        }
        return jedisCluster.xread(params, Map.of(streamName, afterId));
    }

    @Override
    public long xdel(String streamName, StreamEntryID... ids) {
        return jedisCluster.xdel(streamName, ids);
    }
}
