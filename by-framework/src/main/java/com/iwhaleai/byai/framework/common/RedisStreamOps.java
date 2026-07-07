package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.resps.StreamEntry;
import redis.clients.jedis.resps.StreamPendingEntry;

import java.util.List;
import java.util.Map;

/**
 * Redis Stream operations needed by business classes without depending on
 * Jedis/JedisCluster directly. Originally added minimally in the Phase 4
 * prefactor (#12) for WorkerRunner's split read loops; extended here (Phase 5,
 * #14) with xadd/xpendingRange/xclaim/xread/xdel to cover every other
 * business class's stream usage.
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

    /** Append an entry with an auto-generated ID, applying the given trimming policy. */
    StreamEntryID xadd(String streamName, Map<String, String> fields, XAddOptions options);

    /**
     * Read the pending-entries-list for a consumer group, from the very
     * beginning of the stream up to count entries. Used to find stuck/
     * unacked messages for reclaim (xclaim).
     */
    List<StreamPendingEntry> xpendingRange(String streamName, String groupName, int count);

    /**
     * Claim ownership of pending entries for this consumer, returning the
     * actual claimed entries (not just a count) so the caller can redispatch
     * their contents.
     */
    List<StreamEntry> xclaim(
            String streamName, String groupName, String consumerName, long minIdleTimeMs, StreamEntryID... ids);

    /** Plain (non-consumer-group) read of a single stream, used for direct polling. */
    List<Map.Entry<String, List<StreamEntry>>> xread(
            String streamName, StreamEntryID afterId, int count, Integer blockMs);

    long xdel(String streamName, StreamEntryID... ids);
}
