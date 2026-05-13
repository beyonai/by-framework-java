package com.iwhaleai.byai.framework.core.extensions;

import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;

import java.util.ArrayList;
import java.util.List;

public abstract class Plugin {
    private static final List<Class<? extends Plugin>> REGISTERED_PLUGINS = new ArrayList<>();

    protected final PluginManifest manifest;
    protected final String name;
    protected final String pluginId;
    protected final String version;
    protected Integer hookTimeoutSeconds;

    public Plugin(PluginManifest manifest) {
        this.manifest = manifest;
        this.name = manifest.getPluginId();
        this.pluginId = manifest.getPluginId();
        this.version = manifest.getVersion();
    }

    public Plugin(PluginManifest manifest, Integer hookTimeoutSeconds) {
        this(manifest);
        this.hookTimeoutSeconds = hookTimeoutSeconds;
    }

    public static synchronized void registerPluginClass(Class<? extends Plugin> pluginClass) {
        if (!REGISTERED_PLUGINS.contains(pluginClass)) {
            REGISTERED_PLUGINS.add(pluginClass);
        }
    }

    public static List<Class<? extends Plugin>> getRegisteredPlugins() {
        return new ArrayList<>(REGISTERED_PLUGINS);
    }

    public abstract List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext);

    public void onWorkerStartup(GatewayWorker worker) {
    }

    public void onWorkerShutdown(GatewayWorker worker) {
    }

    public void onTaskStart(AgentContext context) {
    }

    public void onTaskComplete(AgentContext context, Object result) {
    }

    public void onTaskError(AgentContext context, Throwable error) {
    }

    public void onTaskCancel(AgentContext context, CancelTaskCommand command) {
    }
}
