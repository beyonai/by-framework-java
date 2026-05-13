package com.iwhaleai.byai.framework.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Main framework configuration combining all sub-configs.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FrameworkConfig {

    @Builder.Default
    private RedisConfig redis = new RedisConfig();

    @Builder.Default
    private WorkerConfig worker = new WorkerConfig();

    @Builder.Default
    private LoggingConfig logging = new LoggingConfig();

    /**
     * Create configuration from environment variables.
     */
    public static FrameworkConfig fromEnv() {
        return FrameworkConfig.builder()
                .redis(RedisConfig.fromEnv())
                .worker(WorkerConfig.fromEnv())
                .logging(LoggingConfig.fromEnv())
                .build();
    }
}
