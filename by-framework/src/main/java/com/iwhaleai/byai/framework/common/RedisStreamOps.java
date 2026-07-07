package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

/**
 * Minimal Redis Stream operations needed to run WorkerRunner's agent_type and
 * worker-ctrl read loops as independent, Cluster-safe paths. xadd,
 * xpendingRange, and xclaim are out of scope here - they land in the fuller
 * adapter added by the Phase 5 issue.
 */
public interface RedisStreamOps {

    /** Idempotent consumer-group creation: silently swallows BUSYGROUP (group already exists). */
    void xgroupCreateIfNotExists(String streamName, String groupName);

    /**
     * Read a single stream as a consumer-group member.
     *
     * @param blockMs null for a non-blocking read that returns immediately;
     *                a positive value to block for up to that many milliseconds
     */
    List<Map.Entry<String, List<StreamEntry>>> xreadGroup(
            String streamName, String groupName, String consumerName, int count, Integer blockMs);

    void xack(String streamName, String groupName, StreamEntryID id);
}
