package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RedisOps backed directly by the shared JedisCluster instance. JedisCluster
 * manages its own per-slot connections internally, so unlike
 * StandaloneRedisOps there is no per-call borrow/close - the instance is
 * held for the lifetime of this class and never closed here.
 */
public class ClusterRedisOps implements RedisOps {
    private final JedisCluster jedisCluster;

    public ClusterRedisOps(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public boolean isClusterMode() {
        return true;
    }

    @Override
    public String get(String key) {
        return jedisCluster.get(key);
    }

    @Override
    public String set(String key, String value, SetParams params) {
        return jedisCluster.set(key, value, params);
    }

    @Override
    public void setex(String key, int seconds, String value) {
        jedisCluster.setex(key, seconds, value);
    }

    @Override
    public long del(String key) {
        return jedisCluster.del(key);
    }

    @Override
    public long expire(String key, int seconds) {
        return jedisCluster.expire(key, seconds);
    }

    @Override
    public long incr(String key) {
        return jedisCluster.incr(key);
    }

    @Override
    public boolean sismember(String key, String member) {
        return jedisCluster.sismember(key, member);
    }

    @Override
    public String hget(String key, String field) {
        return jedisCluster.hget(key, field);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        return jedisCluster.hgetAll(key);
    }

    @Override
    public void hset(String key, String field, String value) {
        jedisCluster.hset(key, field, value);
    }

    @Override
    public long hsetAll(String key, Map<String, String> fields) {
        return jedisCluster.hset(key, fields);
    }

    @Override
    public long hincrBy(String key, String field, long value) {
        return jedisCluster.hincrBy(key, field, value);
    }

    @Override
    public long sadd(String key, String... members) {
        return jedisCluster.sadd(key, members);
    }

    @Override
    public long srem(String key, String... members) {
        return jedisCluster.srem(key, members);
    }

    @Override
    public Set<String> smembers(String key) {
        return jedisCluster.smembers(key);
    }

    @Override
    public void saveSessionExecution(String sessionId, Map<String, String> fields, int ttlSeconds) {
        String key = Constants.RegistryKeys.sessionRegistry(sessionId);
        for (Map.Entry<String, String> field : fields.entrySet()) {
            jedisCluster.hset(key, field.getKey(), field.getValue());
        }
        jedisCluster.expire(key, ttlSeconds);
    }

    @Override
    public void updateWorkerAdminState(String workerId, String lifecycle, String reason, long updatedAt) {
        String key = Constants.RegistryKeys.workerAdminState(workerId);
        jedisCluster.hset(key, "lifecycle", lifecycle);
        jedisCluster.hset(key, "reason", reason != null ? reason : "");
        jedisCluster.hset(key, "updated_at", String.valueOf(updatedAt));
    }

    @Override
    public Map<String, Map<String, String>> batchGetWorkerAdminStates(List<String> workerIds) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (String workerId : workerIds) {
            result.put(workerId, jedisCluster.hgetAll(Constants.RegistryKeys.workerAdminState(workerId)));
        }
        return result;
    }

    /**
     * JedisCluster has no built-in cross-node SCAN, so this iterates every
     * node's own connection pool and merges results, deduplicating in case
     * the node map ever includes both a master and its replica for the same
     * keyspace slice.
     */
    @Override
    public void registerServiceInstance(String serviceName, String instanceId, String instanceJson, long timestampMs) {
        jedisCluster.hset(Constants.RegistryKeys.sdInstanceDetails(serviceName), instanceId, instanceJson);
        jedisCluster.zadd(Constants.RegistryKeys.sdActiveInstances(serviceName), timestampMs, instanceId);
    }

    @Override
    public void heartbeatServiceInstance(String serviceName, String instanceId, long timestampMs) {
        jedisCluster.zadd(Constants.RegistryKeys.sdActiveInstances(serviceName), timestampMs, instanceId);
    }

    @Override
    public void unregisterServiceInstance(String serviceName, String instanceId) {
        jedisCluster.hdel(Constants.RegistryKeys.sdInstanceDetails(serviceName), instanceId);
        jedisCluster.zrem(Constants.RegistryKeys.sdActiveInstances(serviceName), instanceId);
    }

    @Override
    public Map<String, ActiveInstanceRecord> fetchActiveServiceInstances(String serviceName) {
        List<Tuple> active = jedisCluster.zrangeWithScores(
                Constants.RegistryKeys.sdActiveInstances(serviceName), 0, -1);
        if (active.isEmpty()) {
            return Map.of();
        }

        List<String> instanceIds = active.stream().map(Tuple::getElement).toList();
        List<String> payloads = jedisCluster.hmget(
                Constants.RegistryKeys.sdInstanceDetails(serviceName), instanceIds.toArray(new String[0]));

        Map<String, ActiveInstanceRecord> result = new LinkedHashMap<>();
        for (int i = 0; i < active.size(); i++) {
            String payload = payloads.get(i);
            if (payload != null && !payload.isEmpty()) {
                result.put(instanceIds.get(i),
                        new ActiveInstanceRecord(payload, (long) active.get(i).getScore()));
            }
        }
        return result;
    }

    @Override
    public List<String> scanKeys(String pattern, int limit) {
        Set<String> result = new LinkedHashSet<>();
        ScanParams params = new ScanParams().match(pattern).count(100);
        for (ConnectionPool pool : jedisCluster.getClusterNodes().values()) {
            try (Jedis nodeJedis = new Jedis(pool.getResource())) {
                String cursor = ScanParams.SCAN_POINTER_START;
                do {
                    ScanResult<String> scanResult = nodeJedis.scan(cursor, params);
                    cursor = scanResult.getCursor();
                    for (String key : scanResult.getResult()) {
                        result.add(key);
                        if (limit > 0 && result.size() >= limit) {
                            return new ArrayList<>(result);
                        }
                    }
                } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
            }
        }
        return new ArrayList<>(result);
    }
}
