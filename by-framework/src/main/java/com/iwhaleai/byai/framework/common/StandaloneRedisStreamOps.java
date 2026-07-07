package com.iwhaleai.byai.framework.common;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

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
}
