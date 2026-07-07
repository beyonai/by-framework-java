package com.iwhaleai.byai.framework.common;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.List;
import java.util.Map;

/**
 * RedisStreamOps backed by a pooled standalone Jedis connection, borrowed
 * and closed on every call (matching the rest of the codebase's pattern for
 * RedisClient.getResource()).
 */
@Slf4j
public class StandaloneRedisStreamOps implements RedisStreamOps {
    private final RedisClient redisClient;

    public StandaloneRedisStreamOps(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public void xgroupCreateIfNotExists(String streamName, String groupName) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.xgroupCreate(streamName, groupName, StreamEntryID.LAST_ENTRY, true);
        } catch (Exception e) {
            if (e.getMessage() == null || !e.getMessage().contains("BUSYGROUP")) {
                log.warn("Warning setting up stream {}: {}", streamName, e.getMessage());
            }
        }
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xreadGroup(
            String streamName, String groupName, String consumerName, int count, Integer blockMs) {
        try (Jedis jedis = redisClient.getResource()) {
            XReadGroupParams params = XReadGroupParams.xReadGroupParams().count(count);
            if (blockMs != null) {
                params.block(blockMs);
            }
            return jedis.xreadGroup(groupName, consumerName, params,
                    Map.of(streamName, StreamEntryID.UNRECEIVED_ENTRY));
        }
    }

    @Override
    public void xack(String streamName, String groupName, StreamEntryID id) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.xack(streamName, groupName, id);
        }
    }

    @Override
    public StreamEntryID xadd(String streamName, Map<String, String> fields, XAddOptions options) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.xadd(streamName, options.toParams(), fields);
        }
    }

    @Override
    public List<StreamPendingEntry> xpendingRange(String streamName, String groupName, int count) {
        try (Jedis jedis = redisClient.getResource()) {
            XPendingParams params = XPendingParams.xPendingParams(
                    StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, count);
            return jedis.xpending(streamName, groupName, params);
        }
    }

    @Override
    public List<StreamEntry> xclaim(
            String streamName, String groupName, String consumerName, long minIdleTimeMs, StreamEntryID... ids) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.xclaim(streamName, groupName, consumerName, minIdleTimeMs, XClaimParams.xClaimParams(), ids);
        }
    }

    @Override
    public List<Map.Entry<String, List<StreamEntry>>> xread(
            String streamName, StreamEntryID afterId, int count, Integer blockMs) {
        try (Jedis jedis = redisClient.getResource()) {
            XReadParams params = XReadParams.xReadParams().count(count);
            if (blockMs != null) {
                params.block(blockMs);
            }
            return jedis.xread(params, Map.of(streamName, afterId));
        }
    }

    @Override
    public long xdel(String streamName, StreamEntryID... ids) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.xdel(streamName, ids);
        }
    }
}
