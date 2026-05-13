package com.iwhaleai.byai.framework.config;

import com.iwhaleai.byai.framework.common.Constants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Worker runner configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkerConfig {

    @Builder.Default
    private int maxConcurrency = 50;

    @Builder.Default
    private int fetchCount = 10;

    @Builder.Default
    private int heartbeatIntervalSeconds = Constants.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    @Builder.Default
    private int heartbeatLeaseTtlSeconds = Constants.WORKER_DEFAULT_LEASE_TTL_SECONDS;

    @Builder.Default
    private int lockTtlSeconds = 60;

    @Builder.Default
    private int streamBlockMs = 2000;

    /**
     * Create configuration from environment variables.
     */
    public static WorkerConfig fromEnv() {
        return WorkerConfig.builder()
                .maxConcurrency(getEnvInt("WORKER_MAX_CONCURRENCY", 50))
                .fetchCount(getEnvInt("WORKER_FETCH_COUNT", 10))
                .heartbeatIntervalSeconds(getEnvInt("WORKER_HEARTBEAT_INTERVAL_SECONDS",
                        Constants.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS))
                .heartbeatLeaseTtlSeconds(getEnvInt("WORKER_HEARTBEAT_LEASE_TTL_SECONDS",
                        Constants.WORKER_DEFAULT_LEASE_TTL_SECONDS))
                .lockTtlSeconds(getEnvInt("WORKER_LOCK_TTL_SECONDS", 60))
                .streamBlockMs(getEnvInt("WORKER_STREAM_BLOCK_MS", 2000))
                .build();
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
