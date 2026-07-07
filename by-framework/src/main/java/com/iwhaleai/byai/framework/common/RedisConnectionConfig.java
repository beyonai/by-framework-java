package com.iwhaleai.byai.framework.common;

import com.iwhaleai.byai.framework.config.GatewayConfig;
import lombok.Getter;
import lombok.Setter;
import redis.clients.jedis.HostAndPort;

import java.util.ArrayList;
import java.util.List;

/**
 * Unified Redis connection configuration, supporting both standalone and
 * Cluster deployment modes. Selected via config/env vars; standalone
 * remains the default with no behavior change when cluster mode isn't
 * configured.
 */
@Getter
@Setter
public class RedisConnectionConfig {

    /** Deployment mode. Reserves space for a future SENTINEL value, not
     * implemented in this phase. */
    public enum Mode {
        STANDALONE,
        CLUSTER
    }

    private Mode mode = Mode.STANDALONE;
    private String host = "localhost";
    private int port = 6379;
    private int db = 0;
    private String username;
    private String password;
    private int timeout = 5000;
    private List<HostAndPort> clusterNodes = new ArrayList<>();

    /**
     * Load connection config from REDIS_MODE/REDIS_HOST/REDIS_PORT/REDIS_DB/
     * REDIS_USERNAME/REDIS_PASSWORD/REDIS_CLUSTER_NODES, via GatewayConfig
     * (system property checked before the real env var, then config file).
     */
    public static RedisConnectionConfig fromEnv() {
        RedisConnectionConfig config = new RedisConnectionConfig();
        String modeStr = GatewayConfig.get("REDIS_MODE", "standalone");
        config.mode = "cluster".equalsIgnoreCase(modeStr) ? Mode.CLUSTER : Mode.STANDALONE;
        config.host = GatewayConfig.get("REDIS_HOST", "localhost");
        config.port = GatewayConfig.getInt("REDIS_PORT", 6379);
        config.db = GatewayConfig.getInt("REDIS_DB", 0);
        config.username = GatewayConfig.get("REDIS_USERNAME");
        config.password = GatewayConfig.get("REDIS_PASSWORD");
        config.clusterNodes = parseClusterNodes(GatewayConfig.get("REDIS_CLUSTER_NODES"));
        return config;
    }

    private static List<HostAndPort> parseClusterNodes(String nodesStr) {
        List<HostAndPort> nodes = new ArrayList<>();
        if (nodesStr == null || nodesStr.isEmpty()) {
            return nodes;
        }
        for (String pair : nodesStr.split(",")) {
            String trimmed = pair.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            int idx = trimmed.lastIndexOf(':');
            String nodeHost = trimmed.substring(0, idx);
            int nodePort = Integer.parseInt(trimmed.substring(idx + 1));
            nodes.add(new HostAndPort(nodeHost, nodePort));
        }
        return nodes;
    }
}
