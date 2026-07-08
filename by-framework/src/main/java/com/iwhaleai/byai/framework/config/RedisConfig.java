package com.iwhaleai.byai.framework.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Redis connection configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class RedisConfig {

    @Builder.Default
    private String host = "localhost";

    @Builder.Default
    private int port = 6379;

    @Builder.Default
    private int db = 0;

    @Builder.Default
    private String password = "";

    private String username;

    @Builder.Default
    private boolean decodeResponses = true;

    private Integer maxConnections;

    /**
     * Create configuration from environment variables.
     *
     * REDIS_DATABASE replaces REDIS_DB (which still works as a deprecated
     * fallback, logging a warning, during the transition period).
     */
    public static RedisConfig fromEnv() {
        RedisConfigBuilder builder = RedisConfig.builder()
                .host(getEnv("REDIS_HOST", "localhost"))
                .port(getEnvInt("REDIS_PORT", 6379))
                .db(getEnvIntWithDeprecatedFallback("REDIS_DATABASE", "REDIS_DB", 0))
                .password(getEnv("REDIS_PASSWORD", ""))
                .username(getEnv("REDIS_USERNAME", null));

        String maxConn = getEnv("REDIS_MAX_CONNECTIONS", null);
        if (maxConn != null && !maxConn.isEmpty()) {
            builder.maxConnections(Integer.parseInt(maxConn));
        }

        return builder.build();
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    private static int getEnvInt(String key, int defaultValue) {
        String value = System.getenv(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return defaultValue;
    }

    private static int getEnvIntWithDeprecatedFallback(String key, String deprecatedKey, int defaultValue) {
        String value = System.getenv(key);
        if (value == null) {
            value = System.getenv(deprecatedKey);
            if (value != null) {
                log.warn("{} is deprecated, use {} instead", deprecatedKey, key);
            }
        }
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            // ignore
        }
        return defaultValue;
    }
}
