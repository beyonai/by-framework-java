package com.iwhaleai.byai.framework.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
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
