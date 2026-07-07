package com.iwhaleai.byai.framework.core.discovery;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的服务注册中心 SDK。
 * <p>
 * 供服务端使用，负责服务注册、自动心跳维护及注销。
 */
@Slf4j
public class ServiceRegistry {
    private final RedisClient redisClient;
    private ScheduledExecutorService scheduler;
    private ServiceInstance currentInstance;
    private String currentServiceName;

    public ServiceRegistry(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    /**
     * 注册当前服务实例并开启后台心跳。
     */
    public synchronized void register(
            String serviceName,
            String host,
            int port,
            int weight,
            Map<String, Object> metadata,
            int heartbeatIntervalSeconds) {
        register(serviceName, "http", host, port, null, weight, metadata, heartbeatIntervalSeconds);
    }

    /**
     * 注册当前服务实例并开启后台心跳（支持路径前缀）。
     */
    public synchronized void register(
            String serviceName,
            String host,
            int port,
            String pathPrefix,
            int weight,
            Map<String, Object> metadata,
            int heartbeatIntervalSeconds) {
        register(serviceName, "http", host, port, pathPrefix, weight, metadata, heartbeatIntervalSeconds);
    }

    /**
     * 注册当前服务实例并开启后台心跳（支持协议和路径前缀）。
     */
    public synchronized void register(
            String serviceName,
            String protocol,
            String host,
            int port,
            String pathPrefix,
            int weight,
            Map<String, Object> metadata,
            int heartbeatIntervalSeconds) {
        if (host == null) {
            host = DiscoveryUtils.getLocalIp(redisClient.getHost(), redisClient.getPort());
        }

        String instanceId = serviceName + ":" + UUID.randomUUID().toString().substring(0, 8);
        this.currentInstance = ServiceInstance.builder()
                .id(instanceId)
                .protocol(protocol != null ? protocol : "http")
                .host(host)
                .port(port)
                .pathPrefix(pathPrefix)
                .weight(weight)
                .metadata(metadata)
                .build();
        this.currentServiceName = serviceName;

        try (Jedis jedis = redisClient.getResource()) {
            // 1. 写入实例详情
            jedis.hset(
                    Constants.RegistryKeys.sdInstanceDetails(serviceName),
                    instanceId,
                    currentInstance.toJson()
            );
            // 2. 写入活跃实例索引 (即使不发心跳，也需写入一次以保证可见性)
            jedis.zadd(
                    Constants.RegistryKeys.sdActiveInstances(serviceName),
                    System.currentTimeMillis(),
                    instanceId
            );
            // 3. 将服务名加入全局索引
            jedis.sadd(Constants.RegistryKeys.sdServices(), serviceName);
        }

        // 3. 立即触发首次心跳并启动循环（如果间隔 > 0）
        if (heartbeatIntervalSeconds > 0) {
            sendHeartbeat();

            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "service-discovery-heartbeat-" + serviceName);
                t.setDaemon(true);
                return t;
            });

            scheduler.scheduleAtFixedRate(
                    this::sendHeartbeat,
                    heartbeatIntervalSeconds,
                    heartbeatIntervalSeconds,
                    TimeUnit.SECONDS
            );
        } else {
            log.info("Service registration without heartbeats enabled for: {}", serviceName);
        }
        
        log.info("Registered service instance: {} at {}://{}:{}{}", instanceId, 
            currentInstance.getProtocol(), host, port, 
            (pathPrefix != null && !pathPrefix.isEmpty()) ? " with path prefix: " + pathPrefix : "");
    }

    public void register(String serviceName, int port) {
        register(serviceName, "http", null, port, null, 1, null, Constants.RegistryKeys.SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS);
    }

    public void register(String serviceName, int port, String pathPrefix) {
        register(serviceName, "http", null, port, pathPrefix, 1, null, Constants.RegistryKeys.SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS);
    }

    public void register(String serviceName, String protocol, int port, String pathPrefix) {
        register(serviceName, protocol, null, port, pathPrefix, 1, null, Constants.RegistryKeys.SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS);
    }

    /**
     * 仅注册服务实例，不发送心跳。
     */
    public void registerOnly(String serviceName, int port) {
        registerOnly(serviceName, "http", null, port, null, 1, null);
    }

    /**
     * 仅注册服务实例，不发送心跳（支持路径前缀）。
     */
    public void registerOnly(String serviceName, int port, String pathPrefix) {
        registerOnly(serviceName, "http", null, port, pathPrefix, 1, null);
    }

    /**
     * 仅注册服务实例，不发送心跳（支持协议和路径前缀）。
     */
    public void registerOnly(String serviceName, String protocol, int port, String pathPrefix) {
        registerOnly(serviceName, protocol, null, port, pathPrefix, 1, null);
    }

    /**
     * 仅注册服务实例，不发送心跳（支持协议、主机、端口和路径前缀）。
     */
    public void registerOnly(String serviceName, String protocol, String host, int port, String pathPrefix) {
        registerOnly(serviceName, protocol, host, port, pathPrefix, 1, null);
    }

    /**
     * 仅注册服务实例，不发送心跳（完整参数支持）。
     */
    public void registerOnly(
            String serviceName,
            String protocol,
            String host,
            int port,
            String pathPrefix,
            int weight,
            Map<String, Object> metadata) {
        register(serviceName, protocol, host, port, pathPrefix, weight, metadata, Constants.SD_NO_HEARTBEAT);
    }

    /**
     * 注销当前服务实例并停止心跳。
     */
    public synchronized void unregister() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }

        if (currentInstance != null && currentServiceName != null) {
            try (Jedis jedis = redisClient.getResource()) {
                try (Pipeline pipe = jedis.pipelined()) {
                    pipe.hdel(
                            Constants.RegistryKeys.sdInstanceDetails(currentServiceName),
                            currentInstance.getId()
                    );
                    pipe.zrem(
                            Constants.RegistryKeys.sdActiveInstances(currentServiceName),
                            currentInstance.getId()
                    );
                    pipe.sync();
                }
            }
            log.info("Unregistered service instance: {}", currentInstance.getId());
            currentInstance = null;
            currentServiceName = null;
        }
    }

    private void sendHeartbeat() {
        if (currentInstance != null && currentServiceName != null) {
            try (Jedis jedis = redisClient.getResource()) {
                long now = System.currentTimeMillis();
                jedis.zadd(
                        Constants.RegistryKeys.sdActiveInstances(currentServiceName),
                        now,
                        currentInstance.getId()
                );
            } catch (Exception e) {
                log.error("Failed to send heartbeat for service: {}", currentServiceName, e);
            }
        }
    }

    public ServiceInstance getCurrentInstance() {
        return currentInstance;
    }
}
