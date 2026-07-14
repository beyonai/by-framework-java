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
     * Load connection config from REDIS_MODE/REDIS_HOST/REDIS_PORT/
     * REDIS_DATABASE/REDIS_USERNAME/REDIS_PASSWORD/REDIS_CLUSTER_NODES, via
     * GatewayConfig (system property checked before the real env var, then
     * config file).
     *
     * REDIS_CLUSTER_HOST (comma-separated "host:port" list) is the preferred
     * way to opt into Cluster mode: setting it alone is enough to switch to
     * Cluster, no separate REDIS_MODE=cluster needed. It takes precedence
     * over REDIS_CLUSTER_NODES/REDIS_MODE when REDIS_MODE isn't set
     * explicitly, so the legacy explicit-mode configuration keeps working.
     *
     * REDIS_DATABASE replaces REDIS_DB (which still works as a deprecated
     * fallback, logging a warning, during the transition period).
     */
    public static RedisConnectionConfig fromEnv() {
        RedisConnectionConfig config = new RedisConnectionConfig();
        String modeStr = GatewayConfig.get("REDIS_MODE");
        String clusterHost = GatewayConfig.get("REDIS_CLUSTER_HOST");
        boolean hasClusterHost = clusterHost != null && !clusterHost.isEmpty();
        if (modeStr == null) {
            modeStr = hasClusterHost ? "cluster" : "standalone";
        }
        config.mode = "cluster".equalsIgnoreCase(modeStr) ? Mode.CLUSTER : Mode.STANDALONE;
        config.host = GatewayConfig.get("REDIS_HOST", "localhost");
        config.port = GatewayConfig.getInt("REDIS_PORT", 6379);
        config.db = GatewayConfig.getIntWithDeprecatedFallback("REDIS_DATABASE", "REDIS_DB", 0);
        config.username = GatewayConfig.get("REDIS_USERNAME");
        config.password = GatewayConfig.get("REDIS_PASSWORD");
        String clusterNodesStr = hasClusterHost ? clusterHost : GatewayConfig.get("REDIS_CLUSTER_NODES");
        config.clusterNodes = parseClusterNodes(clusterNodesStr);
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
