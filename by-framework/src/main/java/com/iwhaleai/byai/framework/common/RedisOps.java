package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis operations needed by business classes (WorkerRegistry, AgentContext,
 * GatewayClient, GatewayWorker, WorkerManager, WakeupController,
 * AvailabilityRouter) without depending on Jedis/JedisCluster/Pipeline
 * directly. A raw Pipeline-exposing interface doesn't work here -
 * JedisCluster's pipeline capability and standalone Pipeline aren't the same
 * abstraction, and Phase 3 already established that cross-entity operations
 * get split into independent business-level calls rather than generic
 * pipelines. Batch operations here are named for what they do, not how.
 */
public interface RedisOps {

    boolean isClusterMode();

    // --- Single-key passthroughs ---

    String get(String key);

    String set(String key, String value, SetParams params);

    void setex(String key, int seconds, String value);

    long del(String key);

    long expire(String key, int seconds);

    long incr(String key);

    boolean sismember(String key, String member);

    String hget(String key, String field);

    Map<String, String> hgetAll(String key);

    void hset(String key, String field, String value);

    long hincrBy(String key, String field, long value);

    long sadd(String key, String... members);

    long srem(String key, String... members);

    Set<String> smembers(String key);

    // --- Business-level batch operations ---

    /**
     * Write one or more fields into a session's registry hash and refresh its
     * TTL in one round trip. Every field always belongs to the same
     * sessionRegistry(sessionId) key, so this is always single-key regardless
     * of Cluster/standalone mode.
     */
    void saveSessionExecution(String sessionId, Map<String, String> fields, int ttlSeconds);

    /**
     * Write a worker's admin lifecycle/reason/updated_at fields in one round
     * trip instead of three sequential HSETs.
     */
    void updateWorkerAdminState(String workerId, String lifecycle, String reason, long updatedAt);

    /**
     * Fetch admin state for multiple workers at once. Each worker_admin(id)
     * key is untagged/independent (Phase 2a), so this always fans out into
     * one HGETALL per worker - the business value is having one call site
     * instead of duplicating the fan-out loop at every caller.
     */
    Map<String, Map<String, String>> batchGetWorkerAdminStates(List<String> workerIds);

    /**
     * SCAN for keys matching pattern, up to limit results (0 = unlimited).
     * Cluster-aware: standalone scans the single node; Cluster iterates every
     * cluster node and merges results, since JedisCluster has no built-in
     * cross-node SCAN.
     */
    List<String> scanKeys(String pattern, int limit);
}
