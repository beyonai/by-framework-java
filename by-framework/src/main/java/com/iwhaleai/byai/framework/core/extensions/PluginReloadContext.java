package com.iwhaleai.byai.framework.core.extensions;

import lombok.Getter;

import java.util.Collections;
import java.util.List;

/**
 * Context passed to plugin reload hooks. currentAgentConfigs is the working config list for
 * the current reload stage; plugins transform it and return the next full config version.
 */
public class PluginReloadContext {
    @Getter
    private final String pluginId;
    @Getter
    private final String reloadId;
    @Getter
    private final String reason;
    @Getter
    private final List<AgentConfig> currentAgentConfigs;
    @Getter
    private final List<AgentConfig> previousStableAgentConfigs;
    @Getter
    private final int currentVersion;

    public PluginReloadContext(String pluginId, String reloadId, String reason,
            List<AgentConfig> currentAgentConfigs, List<AgentConfig> previousStableAgentConfigs,
            int currentVersion) {
        this.pluginId = pluginId;
        this.reloadId = reloadId;
        this.reason = reason;
        this.currentAgentConfigs = Collections.unmodifiableList(currentAgentConfigs);
        this.previousStableAgentConfigs = Collections.unmodifiableList(previousStableAgentConfigs);
        this.currentVersion = currentVersion;
    }
}
