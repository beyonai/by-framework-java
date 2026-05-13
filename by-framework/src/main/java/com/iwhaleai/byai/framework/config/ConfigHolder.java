package com.iwhaleai.byai.framework.config;

/**
 * Global config holder for framework configuration.
 */
public class ConfigHolder {

    private static volatile FrameworkConfig config;

    /**
     * Get the global framework configuration.
     */
    public static FrameworkConfig getConfig() {
        if (config == null) {
            synchronized (ConfigHolder.class) {
                if (config == null) {
                    config = FrameworkConfig.fromEnv();
                }
            }
        }
        return config;
    }

    /**
     * Initialize global configuration (for testing or custom config).
     */
    public static void init(FrameworkConfig newConfig) {
        synchronized (ConfigHolder.class) {
            config = newConfig;
        }
    }

    private ConfigHolder() {
        // Prevent instantiation
    }
}
