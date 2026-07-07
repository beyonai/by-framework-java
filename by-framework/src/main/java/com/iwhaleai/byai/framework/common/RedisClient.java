package com.iwhaleai.byai.framework.common;

import com.iwhaleai.byai.framework.config.GatewayConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.commands.JedisCommands;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class RedisClient {
    private final JedisPool jedisPool;
    private final JedisCluster jedisCluster;
    private static volatile RedisClient instance;

    private final String host;
    private final int port;
    private final int db;
    private final String username;
    private final String password;
    private final int timeout;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public RedisClient(String host, int port) {
        this(host, port, 0, null, null, 5000);
    }

    public RedisClient(String host, int port, int db, String username, String password) {
        this(host, port, db, username, password, 5000);
    }

    public RedisClient(String host, int port, int db, String username, String password, int timeout) {
        this.host = host;
        this.port = port;
        this.db = db;
        this.username = username;
        this.password = password;
        this.timeout = timeout;
        this.jedisCluster = null;

        log.info("Initializing Redis connection: host={}, port={}, db={}, username={}, timeout={}ms",
                 host, port, db, username != null ? username : "default", timeout);

        this.jedisPool = buildJedisPool(host, port, db, username, password, timeout);
    }

    /**
     * Cluster-aware constructor. Fails fast, synchronously, with no network
     * I/O attempted, if mode=CLUSTER and the key schema version isn't v2 -
     * v1 keys have no Cluster hash tags and will hit CROSSSLOT errors under
     * Cluster. Standalone mode (the default) behaves exactly like the
     * legacy constructors above.
     */
    public RedisClient(RedisConnectionConfig config) {
        this.host = config.getHost();
        this.port = config.getPort();
        this.db = config.getDb();
        this.username = config.getUsername();
        this.password = config.getPassword();
        this.timeout = config.getTimeout();

        if (config.getMode() == RedisConnectionConfig.Mode.CLUSTER) {
            String schemaVersion = Constants.getKeySchemaVersion();
            if (!"v2".equals(schemaVersion)) {
                throw new IllegalStateException(
                        "REDIS_MODE=cluster requires REDIS_KEY_SCHEMA_VERSION=v2 "
                                + "(v1 key format has no hash tags and will hit CROSSSLOT "
                                + "errors under Cluster). Set REDIS_KEY_SCHEMA_VERSION=v2 "
                                + "and complete the key migration first.");
            }
            log.info("Initializing Redis Cluster connection: nodes={}", config.getClusterNodes());
            this.jedisPool = null;
            this.jedisCluster = buildJedisCluster(config);
        } else {
            log.info("Initializing Redis connection: host={}, port={}, db={}, username={}, timeout={}ms",
                    host, port, db, username != null ? username : "default", timeout);
            this.jedisCluster = null;
            this.jedisPool = buildJedisPool(host, port, db, username, password, timeout);
        }
    }

    private static JedisPool buildJedisPool(
            String host, int port, int db, String username, String password, int timeout) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(5);

        // Jedis 5.x 构造函数依然兼容老的参数模式，但建议显式指定超时
        if (password != null && !password.isEmpty()) {
            if (username != null && !username.isEmpty()) {
                // JedisPool(poolConfig, host, port, timeout, user, password, database)
                return new JedisPool(poolConfig, host, port, timeout, username, password, db);
            }
            // JedisPool(poolConfig, host, port, timeout, password, database)
            return new JedisPool(poolConfig, host, port, timeout, password, db);
        }
        return new JedisPool(poolConfig, host, port, timeout, null, db);
    }

    private static JedisCluster buildJedisCluster(RedisConnectionConfig config) {
        Set<HostAndPort> nodes = new HashSet<>(config.getClusterNodes());
        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder()
                .timeoutMillis(config.getTimeout());
        if (config.getUsername() != null && !config.getUsername().isEmpty()) {
            clientConfigBuilder.user(config.getUsername());
        }
        if (config.getPassword() != null && !config.getPassword().isEmpty()) {
            clientConfigBuilder.password(config.getPassword());
        }
        JedisClientConfig clientConfig = clientConfigBuilder.build();
        return new JedisCluster(nodes, clientConfig);
    }

    public static RedisClient getInstance() {
        if (instance == null) {
            synchronized (RedisClient.class) {
                if (instance == null) {
                    // Use GatewayConfig to load from various sources
                    String host = GatewayConfig.get("gateway.redis.host", "localhost");
                    int port = GatewayConfig.getInt("gateway.redis.port", 6379);
                    int db = GatewayConfig.getInt("gateway.redis.db", 0);
                    String user = GatewayConfig.get("gateway.redis.username");
                    String pass = GatewayConfig.get("gateway.redis.password");
                    int timeout = GatewayConfig.getInt("gateway.redis.timeout", 5000);
                    instance = new RedisClient(host, port, db, user, pass, timeout);
                }
            }
        }
        return instance;
    }

    /**
     * 向后兼容的初始化方法。
     */
    public static void init(String host, int port, int db, String username, String password) {
        init(host, port, db, username, password, 5000);
    }

    public static void init(String host, int port, int db, String username, String password, int timeout) {
        synchronized (RedisClient.class) {
            if (instance != null) {
                instance.close();
            }
            instance = new RedisClient(host, port, db, username, password, timeout);
        }
    }

    public Jedis getResource() {
        return jedisPool.getResource();
    }

    /**
     * Mode-agnostic accessor: a JedisCommands-typed handle that works for
     * both standalone (a borrowed Jedis from the pool) and Cluster (the
     * JedisCluster instance itself, which manages per-slot connections
     * internally and needs no separate "borrow" step). getResource() above
     * remains standalone-only and unchanged for existing callers.
     */
    public JedisCommands getCommands() {
        return jedisCluster != null ? jedisCluster : jedisPool.getResource();
    }

    /**
     * Raw JedisCluster accessor, null in standalone mode. Unlike getCommands(),
     * this is typed so callers building a long-lived Cluster-specific adapter
     * (see ClusterRedisStreamOps) can hold onto it without ever risking a
     * close() call meant for a pooled, per-borrow Jedis.
     */
    public JedisCluster getJedisCluster() {
        return jedisCluster;
    }

    public void close() {
        if (jedisCluster != null) {
            jedisCluster.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
