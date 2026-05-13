package com.iwhaleai.byai.framework.util.http;

import lombok.Builder;
import lombok.Data;

import java.util.Set;

/**
 * Configuration for retry behavior.
 */
@Data
@Builder
public class RetryConfig {

    @Builder.Default
    private int maxAttempts = 3;

    @Builder.Default
    private double initialDelay = 0.5;

    @Builder.Default
    private double maxDelay = 30.0;

    @Builder.Default
    private double backoffMultiplier = 2.0;

    @Builder.Default
    private Set<Integer> retryOnStatusCodes = Set.of(429, 500, 502, 503, 504);

    /**
     * Create a config that disables retries.
     */
    public static RetryConfig noRetry() {
        return RetryConfig.builder()
                .maxAttempts(1)
                .retryOnStatusCodes(Set.of())
                .build();
    }

    /**
     * Calculate delay for given attempt using exponential backoff.
     */
    public static double calculateDelay(int attempt, RetryConfig config) {
        double delay = config.getInitialDelay() * Math.pow(config.getBackoffMultiplier(), attempt - 1);
        return Math.min(delay, config.getMaxDelay());
    }
}
