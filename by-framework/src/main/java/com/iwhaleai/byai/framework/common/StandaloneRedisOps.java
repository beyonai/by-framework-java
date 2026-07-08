package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RedisOps backed by a pooled standalone Jedis connection, borrowed and
 * closed on every call (matching StandaloneRedisStreamOps and the rest of
 * the codebase's pattern for RedisClient.getResource()).
 */
public class StandaloneRedisOps implements RedisOps {
    private final RedisClient redisClient;

    public StandaloneRedisOps(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    @Override
    public boolean isClusterMode() {
        return false;
    }

    @Override
    public String get(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.get(key);
        }
    }

    @Override
    public String set(String key, String value, SetParams params) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.set(key, value, params);
        }
    }

    @Override
    public void setex(String key, int seconds, String value) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.setex(key, seconds, value);
        }
    }

    @Override
    public long del(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.del(key);
        }
    }

    @Override
    public long expire(String key, int seconds) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.expire(key, seconds);
        }
    }

    @Override
    public long incr(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.incr(key);
        }
    }

    @Override
    public boolean sismember(String key, String member) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.sismember(key, member);
        }
    }

    @Override
    public String hget(String key, String field) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.hget(key, field);
        }
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.hgetAll(key);
        }
    }

    @Override
    public void hset(String key, String field, String value) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.hset(key, field, value);
        }
    }

    @Override
    public long hsetAll(String key, Map<String, String> fields) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.hset(key, fields);
        }
    }

    @Override
    public long hincrBy(String key, String field, long value) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.hincrBy(key, field, value);
        }
    }

    @Override
    public long sadd(String key, String... members) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.sadd(key, members);
        }
    }

    @Override
    public long srem(String key, String... members) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.srem(key, members);
        }
    }

    @Override
    public Set<String> smembers(String key) {
        try (Jedis jedis = redisClient.getResource()) {
            return jedis.smembers(key);
        }
    }

    @Override
    public void saveSessionExecution(String sessionId, Map<String, String> fields, int ttlSeconds) {
        String key = Constants.RegistryKeys.sessionRegistry(sessionId);
        try (Jedis jedis = redisClient.getResource()) {
            for (Map.Entry<String, String> field : fields.entrySet()) {
                jedis.hset(key, field.getKey(), field.getValue());
            }
            jedis.expire(key, ttlSeconds);
        }
    }

    @Override
    public void updateWorkerAdminState(String workerId, String lifecycle, String reason, long updatedAt) {
        String key = Constants.RegistryKeys.workerAdminState(workerId);
        try (Jedis jedis = redisClient.getResource()) {
            jedis.hset(key, "lifecycle", lifecycle);
            jedis.hset(key, "reason", reason != null ? reason : "");
            jedis.hset(key, "updated_at", String.valueOf(updatedAt));
        }
    }

    @Override
    public Map<String, Map<String, String>> batchGetWorkerAdminStates(List<String> workerIds) {
        Map<String, Map<String, String>> result = new HashMap<>();
        try (Jedis jedis = redisClient.getResource()) {
            for (String workerId : workerIds) {
                result.put(workerId, jedis.hgetAll(Constants.RegistryKeys.workerAdminState(workerId)));
            }
        }
        return result;
    }

    @Override
    public void registerServiceInstance(String serviceName, String instanceId, String instanceJson, long timestampMs) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.hset(Constants.RegistryKeys.sdInstanceDetails(serviceName), instanceId, instanceJson);
            jedis.zadd(Constants.RegistryKeys.sdActiveInstances(serviceName), timestampMs, instanceId);
        }
    }

    @Override
    public void heartbeatServiceInstance(String serviceName, String instanceId, long timestampMs) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.zadd(Constants.RegistryKeys.sdActiveInstances(serviceName), timestampMs, instanceId);
        }
    }

    @Override
    public void unregisterServiceInstance(String serviceName, String instanceId) {
        try (Jedis jedis = redisClient.getResource()) {
            jedis.hdel(Constants.RegistryKeys.sdInstanceDetails(serviceName), instanceId);
            jedis.zrem(Constants.RegistryKeys.sdActiveInstances(serviceName), instanceId);
        }
    }

    @Override
    public Map<String, ActiveInstanceRecord> fetchActiveServiceInstances(String serviceName) {
        try (Jedis jedis = redisClient.getResource()) {
            List<Tuple> active = jedis.zrangeWithScores(
                    Constants.RegistryKeys.sdActiveInstances(serviceName), 0, -1);
            if (active.isEmpty()) {
                return Map.of();
            }

            List<String> instanceIds = active.stream().map(Tuple::getElement).toList();
            List<String> payloads = jedis.hmget(
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
    }

    @Override
    public List<String> scanKeys(String pattern, int limit) {
        List<String> result = new ArrayList<>();
        ScanParams params = new ScanParams().match(pattern).count(100);
        try (Jedis jedis = redisClient.getResource()) {
            String cursor = ScanParams.SCAN_POINTER_START;
            do {
                ScanResult<String> scanResult = jedis.scan(cursor, params);
                cursor = scanResult.getCursor();
                for (String key : scanResult.getResult()) {
                    result.add(key);
                    if (limit > 0 && result.size() >= limit) {
                        return result;
                    }
                }
            } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        }
        return result;
    }
}
