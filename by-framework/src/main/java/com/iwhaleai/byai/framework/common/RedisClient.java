package com.iwhaleai.byai.framework.common;

import com.iwhaleai.byai.framework.config.GatewayConfig;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

@Slf4j
public class RedisClient {
    private final JedisPool jedisPool;
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
        
        log.info("Initializing Redis connection: host={}, port={}, db={}, username={}, timeout={}ms", 
                 host, port, db, username != null ? username : "default", timeout);
        
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(50);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(5);
        
        // Jedis 5.x 构造函数依然兼容老的参数模式，但建议显式指定超时
        if (password != null && !password.isEmpty()) {
            if (username != null && !username.isEmpty()) {
                // JedisPool(poolConfig, host, port, timeout, user, password, database)
                this.jedisPool = new JedisPool(poolConfig, host, port, timeout, username, password, db);
            } else {
                // JedisPool(poolConfig, host, port, timeout, password, database)
                this.jedisPool = new JedisPool(poolConfig, host, port, timeout, password, db);
            }
        } else {
            this.jedisPool = new JedisPool(poolConfig, host, port, timeout, null, db);
        }
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

    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}
