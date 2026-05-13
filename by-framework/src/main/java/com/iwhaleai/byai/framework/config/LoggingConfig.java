package com.iwhaleai.byai.framework.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Logging configuration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LoggingConfig {

    @Builder.Default
    private String level = "INFO";

    @Builder.Default
    private boolean useJson = false;

    @Builder.Default
    private String logFile = "by-framework.log";

    /**
     * Create configuration from environment variables.
     */
    public static LoggingConfig fromEnv() {
        return LoggingConfig.builder()
                .level(getEnv("LOG_LEVEL", "INFO"))
                .useJson(Boolean.parseBoolean(getEnv("LOG_USE_JSON", "false")))
                .logFile(getEnv("LOG_FILE", null))
                .build();
    }

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
