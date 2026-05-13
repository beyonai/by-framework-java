package com.iwhaleai.byai.framework.config;

import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Gateway SDK 配置管理类
 * 支持从系统属性、环境变量和配置文件 (gateway-config.properties) 加载
 * 优先级：系统属性 > 环境变量 > 配置文件
 */
@Slf4j
public class GatewayConfig {
    private static final String DEFAULT_CONFIG_FILE = "gateway-config.PROPERTIES";
    private static final Properties PROPERTIES = new Properties();

    static {
        loadFromResources();
    }

    private static void loadFromResources() {
        try (InputStream is = GatewayConfig.class.getClassLoader().getResourceAsStream(DEFAULT_CONFIG_FILE)) {
            if (is != null) {
                PROPERTIES.load(is);
                log.info("Loaded configuration from {}", DEFAULT_CONFIG_FILE);
            } else {
                log.warn("{} not found in classpath resources, relying on env or system properties", DEFAULT_CONFIG_FILE);
            }
        } catch (IOException e) {
            log.error("Failed to load {}", DEFAULT_CONFIG_FILE, e);
        }
    }

    public static String get(String key) {
        return get(key, null);
    }

    public static String get(String key, String defaultValue) {
        // 1. 系统属性 (JVM -D 参数)
        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }

        // 2. 环境变量 (将 . 转换为 _ 并转大写，例如 gateway.redis.host -> GATEWAY_REDIS_HOST)
        String envKey = key.replace('.', '_').toUpperCase();
        value = System.getenv(envKey);
        if (value != null) {
            return value;
        }

        // 特殊处理一些通用的环境变量名
        if ("gateway.redis.host".equals(key)) {
            value = System.getenv("REDIS_HOST");
            if (value != null) {
                return value;
            }
        } else if ("gateway.redis.port".equals(key)) {
            value = System.getenv("REDIS_PORT");
            if (value != null) {
                return value;
            }
        } else if ("gateway.redis.db".equals(key)) {
            value = System.getenv("REDIS_DB");
            if (value != null) {
                return value;
            }
        } else if ("gateway.redis.username".equals(key)) {
            value = System.getenv("REDIS_USERNAME");
            if (value != null) {
                return value;
            }
        } else if ("gateway.redis.password".equals(key)) {
            value = System.getenv("REDIS_PASSWORD");
            if (value != null) {
                return value;
            }
        }

        // 3. 配置文件
        return PROPERTIES.getProperty(key, defaultValue);
    }

    public static int getInt(String key, int defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.warn("Failed to parse config {} as int: {}", key, value);
            return defaultValue;
        }
    }
}
