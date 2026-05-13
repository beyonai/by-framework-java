package com.iwhaleai.byai.framework.core.extensions;

import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import java.util.ArrayList;
import java.util.List;

public class TestPlugin extends Plugin {

    public static final List<String> hookCallLog = new ArrayList<>();

    public TestPlugin() {
        super(PluginManifest.builder()
                .pluginId("test-plugin")
                .version("1.0.0")
                .priority(0)
                .enabled(true)
                .build());
        hookCallLog.clear();
    }

    @Override
    public List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext) {
        hookCallLog.add("registerAgentConfigs");
        return new ArrayList<>();
    }

    @Override
    public void onWorkerStartup(GatewayWorker worker) {
        hookCallLog.add("onWorkerStartup:" + worker.getWorkerId());
    }

    @Override
    public void onWorkerShutdown(GatewayWorker worker) {
        hookCallLog.add("onWorkerShutdown:" + worker.getWorkerId());
    }

    @Override
    public void onTaskStart(AgentContext context) {
        hookCallLog.add("onTaskStart:" + context.getSessionId());
    }

    @Override
    public void onTaskComplete(AgentContext context, Object result) {
        hookCallLog.add("onTaskComplete:" + context.getSessionId() + ":" + result);
    }

    @Override
    public void onTaskError(AgentContext context, Throwable error) {
        hookCallLog.add("onTaskError:" + context.getSessionId() + ":" + error.getMessage());
    }

    @Override
    public void onTaskCancel(AgentContext context, CancelTaskCommand command) {
        hookCallLog.add("onTaskCancel:" + context.getSessionId() + ":" + command.targetMessageId());
    }
}
