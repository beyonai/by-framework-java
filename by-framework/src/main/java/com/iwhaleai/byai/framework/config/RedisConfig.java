package com.iwhaleai.byai.framework.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Redis connection configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
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
     */
    public static RedisConfig fromEnv() {
        RedisConfigBuilder builder = RedisConfig.builder()
                .host(getEnv("REDIS_HOST", "localhost"))
                .port(getEnvInt("REDIS_PORT", 6379))
                .db(getEnvInt("REDIS_DB", 0))
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
}
