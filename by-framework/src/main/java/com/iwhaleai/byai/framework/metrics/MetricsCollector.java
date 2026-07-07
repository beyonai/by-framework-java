package com.iwhaleai.byai.framework.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background metrics collector with distributed Redis lock.
 *
 * <p>Only one worker process at a time writes history points. The lock is a
 * Redis SET NX with an expiry renewed on every successful collection cycle,
 * so the lock outlives individual iteration failures.
 *
 * <p>Configuration via environment variables:
 * <ul>
 *   <li>BY_FRAMEWORK_METRICS_HISTORY_ENABLED — default: true</li>
 *   <li>BY_FRAMEWORK_METRICS_HISTORY_INTERVAL_SECONDS — default: 5</li>
 *   <li>BY_FRAMEWORK_OBSERVABILITY_ENABLED — default: true</li>
 * </ul>
 */
public class MetricsCollector {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

    static final String COLLECTOR_LOCK_KEY = "by_framework:obs:collector_lock";
    static final String REDIS_HISTORY_KEY = "by_framework:obs:history";
    static final long REDIS_HISTORY_TTL_MS = 2L * 60 * 60 * 1000; // 2 hours
    private static final int LOCK_TTL_MULTIPLIER = 3;

    private final RedisClient redisClient;
    private final String workerId;
    private final int intervalSeconds;
    private final boolean enabled;
    private final int lockTtlSeconds;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService executor;

    public MetricsCollector() {
        this(RedisClient.getInstance(), null, null, null);
    }

    public MetricsCollector(RedisClient redisClient, String workerId, Integer intervalSeconds, Boolean enabled) {
        this.redisClient = redisClient;
        this.workerId = workerId != null ? workerId : "collector-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        this.intervalSeconds = intervalSeconds != null ? intervalSeconds : envInt("BY_FRAMEWORK_METRICS_HISTORY_INTERVAL_SECONDS", 5);
        this.enabled = enabled != null ? enabled
                : envBool("BY_FRAMEWORK_METRICS_HISTORY_ENABLED", true)
                  && envBool("BY_FRAMEWORK_OBSERVABILITY_ENABLED", true);
        this.lockTtlSeconds = Math.max(this.intervalSeconds * LOCK_TTL_MULTIPLIER, 15);
    }

    public void start() {
        if (!enabled || !running.compareAndSet(false, true)) return;
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-collector-" + workerId);
            t.setDaemon(true);
            return t;
        });
        executor.scheduleAtFixedRate(this::collectOnce, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
        LOG.debug("MetricsCollector started (workerId={}, interval={}s)", workerId, intervalSeconds);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;
        if (executor != null) {
            executor.shutdown();
        }
        try (Jedis jedis = redisClient.getResource()) {
            releaseLock(jedis);
        } catch (Exception e) {
            LOG.debug("MetricsCollector lock release failed on stop: {}", e.getMessage());
        }
        LOG.debug("MetricsCollector stopped (workerId={})", workerId);
    }

    private void collectOnce() {
        if (!enabled || !running.get()) return;
        try (Jedis jedis = redisClient.getResource()) {
            if (!acquireOrRenewLock(jedis)) return;
            Map<String, Object> snapshot = buildSnapshot(jedis);
            Map<String, Long> point = buildHistoryPoint(snapshot);
            saveHistoryPoint(jedis, point);
        } catch (Exception e) {
            LOG.debug("MetricsCollector snapshot failed: {}", e.getMessage());
        }
    }

    private boolean acquireOrRenewLock(Jedis jedis) {
        try {
            String result = jedis.set(COLLECTOR_LOCK_KEY, workerId, new redis.clients.jedis.params.SetParams()
                    .nx().ex(lockTtlSeconds));
            if ("OK".equals(result)) return true;
            String current = jedis.get(COLLECTOR_LOCK_KEY);
            if (workerId.equals(current)) {
                jedis.expire(COLLECTOR_LOCK_KEY, lockTtlSeconds);
                return true;
            }
            return false;
        } catch (Exception e) {
            LOG.debug("MetricsCollector lock acquire failed: {}", e.getMessage());
            return false;
        }
    }

    private void releaseLock(Jedis jedis) {
        try {
            String current = jedis.get(COLLECTOR_LOCK_KEY);
            if (workerId.equals(current)) {
                jedis.del(COLLECTOR_LOCK_KEY);
            }
        } catch (Exception e) {
            LOG.debug("MetricsCollector lock release failed: {}", e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> buildSnapshot(Jedis jedis) {
        List<String> workerIds = scanOnlineWorkerIds(jedis, 300);

        // Collect all agent types declared by online workers
        Set<String> agentTypeSet = new HashSet<>();
        for (String wid : workerIds) {
            try {
                Set<String> types = jedis.smembers(Constants.RegistryKeys.workerDeclaredAgentTypes(wid));
                agentTypeSet.addAll(types);
            } catch (Exception ignored) { }
        }

        // Collect queue depths
        long queueDepthTotal = 0;
        for (String agentType : agentTypeSet) {
            try {
                long depth = jedis.xlen(Constants.QueueNames.ctrlStream(agentType));
                queueDepthTotal += depth;
            } catch (Exception ignored) { }
        }

        Map<String, Object> totals = new HashMap<>();
        totals.put("workers_online", workerIds.size());
        totals.put("agent_types", agentTypeSet.size());
        totals.put("active_executions", 0);

        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("generated_at", Instant.now().toEpochMilli());
        snapshot.put("totals", totals);
        snapshot.put("status_counts", new HashMap<String, Integer>());
        snapshot.put("queue_depth_total", queueDepthTotal);
        return snapshot;
    }

    @SuppressWarnings("unchecked")
    Map<String, Long> buildHistoryPoint(Map<String, Object> snapshot) {
        Map<String, Object> totals = (Map<String, Object>) snapshot.getOrDefault("totals", Map.of());
        Map<String, Object> statusCounts = (Map<String, Object>) snapshot.getOrDefault("status_counts", Map.of());
        Map<String, Long> point = new HashMap<>();
        point.put("generated_at", toLong(snapshot.get("generated_at")));
        point.put("workers_online", toLong(totals.get("workers_online")));
        point.put("active_executions", toLong(totals.get("active_executions")));
        point.put("queued_executions", toLong(statusCounts.get("QUEUED")));
        point.put("failed_executions", toLong(statusCounts.get("FAILED")));
        point.put("queue_depth_total", toLong(snapshot.get("queue_depth_total")));
        return point;
    }

    void saveHistoryPoint(Jedis jedis, Map<String, Long> point) {
        long score = point.getOrDefault("generated_at", 0L);
        if (score == 0) return;
        try {
            String json = objectMapper.writeValueAsString(point);
            jedis.zadd(REDIS_HISTORY_KEY, score, json);
            long cutoff = score - REDIS_HISTORY_TTL_MS;
            jedis.zremrangeByScore(REDIS_HISTORY_KEY, Double.NEGATIVE_INFINITY, cutoff);
        } catch (Exception e) {
            LOG.debug("MetricsCollector saveHistoryPoint failed: {}", e.getMessage());
        }
    }

    private List<String> scanOnlineWorkerIds(Jedis jedis, int limit) {
        String pattern = Constants.RegistryKeys.workerOnlineLeaseScanPattern();
        ScanParams params = new ScanParams().match(pattern).count(100);
        Set<String> result = new HashSet<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        do {
            ScanResult<String> scanResult = jedis.scan(cursor, params);
            cursor = scanResult.getCursor();
            for (String key : scanResult.getResult()) {
                String workerId = Constants.RegistryKeys.workerIdFromOnlineLeaseKey(key);
                if (workerId != null) {
                    result.add(workerId);
                    if (limit > 0 && result.size() >= limit) {
                        return new ArrayList<>(result);
                    }
                }
            }
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        return new ArrayList<>(result);
    }

    private static long toLong(Object val) {
        if (val == null) return 0L;
        if (val instanceof Number) return ((Number) val).longValue();
        try { return Long.parseLong(val.toString()); } catch (Exception ignored) { return 0L; }
    }

    private static int envInt(String name, int defaultVal) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) return defaultVal;
        try { return Integer.parseInt(raw.trim()); } catch (Exception ignored) { return defaultVal; }
    }

    private static boolean envBool(String name, boolean defaultVal) {
        String raw = System.getenv(name);
        if (raw == null || raw.isBlank()) return defaultVal;
        raw = raw.trim().toLowerCase();
        if (List.of("1", "true", "yes", "on", "enabled").contains(raw)) return true;
        if (List.of("0", "false", "no", "off", "disabled").contains(raw)) return false;
        return defaultVal;
    }

    public Map<String, Object> snapshot() {
        return Map.of(
            "worker_id", workerId,
            "enabled", enabled,
            "interval_seconds", intervalSeconds,
            "lock_key", COLLECTOR_LOCK_KEY
        );
    }
}
