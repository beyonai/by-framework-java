package com.iwhaleai.byai.framework.core.extensions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PluginBuildContext {
    private List<AgentConfig> agentConfigs;
    private List<AgentConfig> prevAgentConfigs;

    public PluginBuildContext() {
        this.agentConfigs = new ArrayList<>();
        this.prevAgentConfigs = new ArrayList<>();
    }

    public PluginBuildContext(List<AgentConfig> agentConfigs) {
        this.agentConfigs = new ArrayList<>(agentConfigs);
        this.prevAgentConfigs = new ArrayList<>();
    }

    public void setAgentConfigs(List<AgentConfig> newConfigs) {
        this.agentConfigs = new ArrayList<>(newConfigs);
    }

    public List<AgentConfig> listAgentConfigs() {
        return new ArrayList<>(this.agentConfigs);
    }

    public void freezePrevAgentConfigs() {
        this.prevAgentConfigs = new ArrayList<>(this.agentConfigs);
    }

    public List<AgentConfig> getPrevAgentConfigs() {
        return Collections.unmodifiableList(this.prevAgentConfigs);
    }
}
