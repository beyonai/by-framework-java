package com.iwhaleai.byai.framework.core.discovery;

import com.iwhaleai.byai.framework.common.ClusterRedisOps;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.common.RedisOps;
import com.iwhaleai.byai.framework.common.StandaloneRedisOps;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 带本地缓存的高效服务发现客户端。
 * <p>
 * 供消费者使用。通过内存缓存及后台刷新机制降低对 Redis 的访问频率。
 */
@Slf4j
public class DiscoveryClient {
    private final RedisOps redisOps;
    private final int cacheIntervalSeconds;
    private final Map<String, List<ServiceInstance>> cache = new ConcurrentHashMap<>();
    private final Map<String, Long> lastRefresh = new ConcurrentHashMap<>();
    private final Set<String> watchedServices = new CopyOnWriteArraySet<>();
    private final Map<String, AtomicInteger> rrCounters = new ConcurrentHashMap<>();
    private final Random random = new Random();
    private ScheduledExecutorService scheduler;

    public DiscoveryClient(RedisClient redisClient) {
        this(redisClient, 5);
    }

    public DiscoveryClient(RedisClient redisClient, int cacheIntervalSeconds) {
        this(redisClient.getJedisCluster() != null
                ? new ClusterRedisOps(redisClient.getJedisCluster())
                : new StandaloneRedisOps(redisClient),
                cacheIntervalSeconds);
    }

    public DiscoveryClient(RedisOps redisOps) {
        this(redisOps, 5);
    }

    public DiscoveryClient(RedisOps redisOps, int cacheIntervalSeconds) {
        this.redisOps = redisOps;
        this.cacheIntervalSeconds = cacheIntervalSeconds;
    }

    /**
     * 获取服务实例。优先使用缓存。
     */
    public List<ServiceInstance> getInstances(String serviceName) {
        return getInstances(serviceName, false, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
    }

    public List<ServiceInstance> getInstances(String serviceName, boolean forceRefresh, long healthThresholdMs) {
        long now = System.currentTimeMillis();
        long last = lastRefresh.getOrDefault(serviceName, 0L);

        boolean isStale = (now - last) > (cacheIntervalSeconds * 1000L);

        if (forceRefresh || isStale || !cache.containsKey(serviceName)) {
            refreshService(serviceName, Constants.SD_NO_HEALTH_CHECK);
        }

        List<ServiceInstance> instances = cache.getOrDefault(serviceName, Collections.emptyList());
        if (healthThresholdMs == Constants.SD_NO_HEALTH_CHECK) {
            return instances;
        }

        long minScore = now - healthThresholdMs;
        return instances.stream()
                .filter(i -> i.getLastHeartbeat() >= minScore)
                .collect(Collectors.toList());
    }

    /**
     * 从 Redis 同步实例列表并更新缓存。
     */
    public synchronized void refreshService(String serviceName, long healthThresholdMs) {
        try {
            // 获取所有活跃实例的详情 JSON 及心跳时间戳
            Map<String, RedisOps.ActiveInstanceRecord> active = redisOps.fetchActiveServiceInstances(serviceName);

            if (active.isEmpty()) {
                cache.put(serviceName, Collections.emptyList());
                lastRefresh.put(serviceName, System.currentTimeMillis());
                return;
            }

            List<ServiceInstance> instances = new ArrayList<>();
            for (RedisOps.ActiveInstanceRecord record : active.values()) {
                ServiceInstance instance = ServiceInstance.fromJson(record.instanceJson());
                instance.setLastHeartbeat(record.lastHeartbeatMs());
                instances.add(instance);
            }

            cache.put(serviceName, instances);
            lastRefresh.put(serviceName, System.currentTimeMillis());
        } catch (Exception e) {
            log.error("Failed to refresh service instances for: {}", serviceName, e);
        }
    }

    /**
     * 将服务加入后台自动刷新列表。
     */
    public synchronized void watch(String serviceName) {
        watchedServices.add(serviceName);
        if (scheduler == null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "discovery-client-refresh");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(
                    this::refreshAllWatched,
                    cacheIntervalSeconds,
                    cacheIntervalSeconds,
                    TimeUnit.SECONDS
            );
        }
    }

    private void refreshAllWatched() {
        for (String serviceName : watchedServices) {
            refreshService(serviceName, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
        }
    }

    /**
     * 进行负载均衡发现。
     */
    public Optional<ServiceInstance> discover(String serviceName) {
        return discover(serviceName, "random", Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
    }

    public Optional<ServiceInstance> discover(String serviceName, long healthThresholdMs) {
        return discover(serviceName, "random", healthThresholdMs);
    }

    public Optional<ServiceInstance> discover(String serviceName, String strategy) {
        return discover(serviceName, strategy, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
    }

    public Optional<ServiceInstance> discover(String serviceName, String strategy, long healthThresholdMs) {
        List<ServiceInstance> instances = getInstances(serviceName, false, healthThresholdMs);
        if (instances == null || instances.isEmpty()) {
            return Optional.empty();
        }

        if ("round-robin".equalsIgnoreCase(strategy)) {
            AtomicInteger counter = rrCounters.computeIfAbsent(serviceName, k -> new AtomicInteger(0));
            int index = Math.abs(counter.getAndIncrement() % instances.size());
            return Optional.of(instances.get(index));
        }

        // Default or "random"
        return Optional.of(instances.get(random.nextInt(instances.size())));
    }

    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }
}
