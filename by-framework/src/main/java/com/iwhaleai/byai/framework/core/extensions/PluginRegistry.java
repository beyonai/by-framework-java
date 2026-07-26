package com.iwhaleai.byai.framework.core.extensions;

import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PluginRegistry {
    /** Shared virtual-thread executor used to race plugin hooks against their configured timeout. */
    private static final ExecutorService HOOK_EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

    private final List<Plugin> plugins = new ArrayList<>();
    private boolean logHookStatsOnShutdown = true;
    private final List<AgentConfig> agentConfigs = new ArrayList<>();
    private final Set<Plugin> initializedPlugins = new HashSet<>();
    private final Map<String, Map<String, HookStats>> hookStats = new HashMap<>();
    private int agentConfigsVersion = 0;
    private final Set<String> processedReloadIds = new HashSet<>();
    private final Map<String, ReloadStatus> reloadStatusById = new HashMap<>();

    public static class HookStats {
        int success = 0;
        int failure = 0;
        int timeout = 0;
        long totalMs = 0;
        String lastError = "";
    }

    /** Immutable view of a stable AgentConfig collection version. */
    public static class AgentConfigsSnapshot {
        @Getter
        private final int version;
        @Getter
        private final List<AgentConfig> configs;

        public AgentConfigsSnapshot(int version, List<AgentConfig> configs) {
            this.version = version;
            this.configs = Collections.unmodifiableList(new ArrayList<>(configs));
        }
    }

    @Getter
    public static class ReloadStatus {
        private final String status;
        private final String reason;
        private final int versionBefore;
        private final int versionAfter;
        private final String error;

        public ReloadStatus(String status, String reason, int versionBefore, int versionAfter, String error) {
            this.status = status;
            this.reason = reason;
            this.versionBefore = versionBefore;
            this.versionAfter = versionAfter;
            this.error = error;
        }
    }

    public List<AgentConfig> getAgentConfigs() {
        return new ArrayList<>(agentConfigs);
    }

    public int getAgentConfigsVersion() {
        return agentConfigsVersion;
    }

    public AgentConfigsSnapshot getAgentConfigsSnapshot() {
        return new AgentConfigsSnapshot(agentConfigsVersion, agentConfigs);
    }

    public ReloadStatus getReloadStatus(String reloadId) {
        return reloadStatusById.get(reloadId);
    }

    public AgentConfig getAgentConfig(String agentId) {
        return agentConfigs.stream()
                .filter(config -> config.getAgentId().equals(agentId))
                .findFirst()
                .orElse(null);
    }

    public void registerBundle(Plugin plugin) {
        if (!plugins.contains(plugin)) {
            if (plugins.stream().anyMatch(existing -> existing.name.equals(plugin.name))) {
                System.err.println("Duplicate plugin name detected: " + plugin.name);
            }
            plugins.add(plugin);
        }
    }

    public void registerBundles(List<Plugin> plugins) {
        for (Plugin plugin : plugins) {
            registerBundle(plugin);
        }
    }

    public List<Plugin> getActivePlugins() {
        List<Plugin> activePlugins = new ArrayList<>();
        for (Plugin plugin : plugins) {
            if (plugin.manifest.isEnabled()) {
                activePlugins.add(plugin);
            }
        }
        activePlugins.sort(Comparator.comparingInt((Plugin p) -> p.manifest.getPriority())
                .thenComparing(p -> p.name));
        return activePlugins;
    }

    public Plugin getPlugin(String pluginId) {
        return plugins.stream()
                .filter(p -> p.pluginId.equals(pluginId))
                .findFirst()
                .orElse(null);
    }

    private HookStats ensureHookStats(String pluginName, String hookName) {
        Map<String, HookStats> pluginStats = hookStats.computeIfAbsent(pluginName, k -> new HashMap<>());
        return pluginStats.computeIfAbsent(hookName, k -> new HookStats());
    }

    private void executeHook(Plugin plugin, String hookName, Runnable hook, String sessionId, String traceId, String workerId) {
        HookStats stat = ensureHookStats(plugin.name, hookName);
        long start = System.currentTimeMillis();
        Integer timeoutSeconds = plugin.hookTimeoutSeconds;

        try {
            if (timeoutSeconds != null && timeoutSeconds > 0) {
                Future<?> future = HOOK_EXECUTOR.submit(hook);
                try {
                    future.get(timeoutSeconds, TimeUnit.SECONDS);
                    stat.success++;
                } catch (TimeoutException e) {
                    stat.timeout++;
                    stat.lastError = "hook timeout (" + timeoutSeconds + "s)";
                    future.cancel(true);
                    System.err.println("Plugin " + plugin.name + " " + hookName + " timed out after " + timeoutSeconds + "s");
                } catch (ExecutionException e) {
                    stat.failure++;
                    Throwable cause = e.getCause() != null ? e.getCause() : e;
                    stat.lastError = cause.getMessage() != null ? cause.getMessage() : "";
                    System.err.println("Plugin " + plugin.name + " " + hookName + " failed: " + cause.getMessage());
                }
            } else {
                hook.run();
                stat.success++;
            }
        } catch (Exception e) {
            stat.failure++;
            stat.lastError = e.getMessage() != null ? e.getMessage() : "";
            System.err.println("Plugin " + plugin.name + " " + hookName + " failed: " + e.getMessage());
        } finally {
            stat.totalMs += System.currentTimeMillis() - start;
        }
    }

    public Map<String, Map<String, Object>> getHookStats() {
        Map<String, Map<String, Object>> snapshot = new HashMap<>();
        for (Map.Entry<String, Map<String, HookStats>> entry : hookStats.entrySet()) {
            Map<String, Object> pluginSnapshot = new HashMap<>();
            for (Map.Entry<String, HookStats> hookEntry : entry.getValue().entrySet()) {
                HookStats stat = hookEntry.getValue();
                int totalRuns = stat.success + stat.failure;
                double avgMs = totalRuns > 0 ? (double) stat.totalMs / totalRuns : 0.0;

                Map<String, Object> hookSnapshot = new HashMap<>();
                hookSnapshot.put("success", stat.success);
                hookSnapshot.put("failure", stat.failure);
                hookSnapshot.put("timeout", stat.timeout);
                hookSnapshot.put("totalMs", stat.totalMs);
                hookSnapshot.put("avgMs", avgMs);
                hookSnapshot.put("totalRuns", totalRuns);
                hookSnapshot.put("lastError", stat.lastError);
                pluginSnapshot.put(hookEntry.getKey(), hookSnapshot);
            }
            snapshot.put(entry.getKey(), pluginSnapshot);
        }
        return snapshot;
    }

    public void logHookStats() {
        Map<String, Map<String, Object>> stats = getHookStats();
        if (stats.isEmpty()) {
            System.out.println("Plugin hook stats: no data");
            return;
        }

        for (Map.Entry<String, Map<String, Object>> entry : stats.entrySet()) {
            for (Map.Entry<String, Object> hookEntry : entry.getValue().entrySet()) {
                @SuppressWarnings("unchecked")
                Map<String, Object> stat = (Map<String, Object>) hookEntry.getValue();
                System.out.println("Plugin hook stats: plugin=" + entry.getKey() +
                        " hook=" + hookEntry.getKey() +
                        " total_runs=" + stat.get("totalRuns") +
                        " success=" + stat.get("success") +
                        " failure=" + stat.get("failure") +
                        " timeout=" + stat.get("timeout") +
                        " avg_ms=" + String.format("%.2f", stat.get("avgMs")) +
                        " last_error=" + stat.get("lastError"));
            }
        }
    }

    public void resetHookStats(String pluginName, String hookName) {
        if (pluginName == null || pluginName.isEmpty()) {
            hookStats.clear();
            return;
        }

        Map<String, HookStats> pluginStats = hookStats.get(pluginName);
        if (pluginStats == null) {
            return;
        }

        if (hookName == null || hookName.isEmpty()) {
            hookStats.remove(pluginName);
            return;
        }

        pluginStats.remove(hookName);
        if (pluginStats.isEmpty()) {
            hookStats.remove(pluginName);
        }
    }

    public void resetHookStats() {
        resetHookStats(null, null);
    }

    public void resetHookStats(String pluginName) {
        resetHookStats(pluginName, null);
    }

    private void validateAgentConfig(AgentConfig config) {
        if (config.getAgentId() == null || config.getAgentId().isEmpty()) {
            throw new IllegalArgumentException("AgentConfig.agentId must not be empty");
        }
    }

    private void registerPluginAgentConfigs(Plugin plugin, PluginBuildContext buildContext) {
        buildContext.freezePrevAgentConfigs();
        List<AgentConfig> newConfigs = plugin.registerAgentConfigs(buildContext);
        if (newConfigs != null) {
            buildContext.setAgentConfigs(newConfigs);
        }

        List<AgentConfig> configs = buildContext.listAgentConfigs();
        if (configs.isEmpty()) {
            return;
        }

        for (AgentConfig config : configs) {
            validateAgentConfig(config);
            String agentId = config.getAgentId();
            AgentConfig existing = getAgentConfig(agentId);
            if (existing != null) {
                if (existing == config) {
                    continue;
                }
                ConflictStrategy strategy = config.getOnConflict();
                if (strategy == ConflictStrategy.ERROR) {
                    throw new IllegalArgumentException("agent_config '" + agentId + "' is already registered");
                }
                if (strategy == ConflictStrategy.SKIP) {
                    System.err.println("Skip duplicate agent_config registration: " + agentId);
                    continue;
                }
                System.err.println("Overwrite duplicate agent_config registration: " + agentId);
                agentConfigs.remove(existing);
            }
            agentConfigs.add(config);
        }
    }

    public void discoverPlugins() {
        for (Class<? extends Plugin> cls : Plugin.getRegisteredPlugins()) {
            if (plugins.stream().anyMatch(p -> cls.isInstance(p))) {
                continue;
            }
            try {
                Plugin plugin = cls.getDeclaredConstructor().newInstance();
                registerBundle(plugin);
                System.out.println("Auto-discovered and registered plugin: " + plugin.pluginId);
            } catch (Exception e) {
                System.err.println("Failed to auto-instantiate plugin class " + cls.getName() + ": " + e.getMessage());
            }
        }
    }

    public void initializePlugins(PluginBuildContext buildContext) {
        final PluginBuildContext finalBuildContext = buildContext != null ? buildContext : new PluginBuildContext(new ArrayList<>(agentConfigs));

        for (Plugin plugin : getActivePlugins()) {
            if (initializedPlugins.contains(plugin)) {
                continue;
            }

            HookStats statBefore = ensureHookStats(plugin.name, "registerAgentConfigs");
            int successBefore = statBefore.success;
            executeHook(plugin, "registerAgentConfigs", () -> registerPluginAgentConfigs(plugin, finalBuildContext), "", "", "");
            HookStats statAfter = ensureHookStats(plugin.name, "registerAgentConfigs");
            if (statAfter.success > successBefore) {
                initializedPlugins.add(plugin);
            }
        }
    }

    public void initializePlugins() {
        initializePlugins(null);
    }

    private static List<AgentConfig> extractReloadConfigs(List<AgentConfig> result, List<AgentConfig> fallback) {
        return result != null ? new ArrayList<>(result) : new ArrayList<>(fallback);
    }

    private List<AgentConfig> replayReloadChain(AgentConfigsSnapshot baseSnapshot, String reloadId, String reason) {
        List<AgentConfig> stableConfigs = baseSnapshot.getConfigs();
        List<AgentConfig> workingConfigs = new ArrayList<>(stableConfigs);

        for (Plugin plugin : getActivePlugins()) {
            PluginReloadContext reloadContext = new PluginReloadContext(
                    plugin.pluginId,
                    reloadId,
                    reason,
                    new ArrayList<>(workingConfigs),
                    stableConfigs,
                    baseSnapshot.getVersion()
            );
            List<AgentConfig> result = plugin.reload(reloadContext);
            workingConfigs = extractReloadConfigs(result, workingConfigs);
            for (AgentConfig config : workingConfigs) {
                validateAgentConfig(config);
            }
        }

        return workingConfigs;
    }

    /**
     * Sequentially replay plugin reload hooks over the current stable version. Each plugin
     * receives the current working AgentConfig list and may return the next full version. The
     * stable version is only replaced after the whole chain succeeds (ADR-0001-adjacent
     * hot-reload invariant).
     */
    public AgentConfigsSnapshot reloadPlugins(String reloadId, String reason) {
        if (processedReloadIds.contains(reloadId)) {
            return getAgentConfigsSnapshot();
        }

        AgentConfigsSnapshot baseSnapshot = getAgentConfigsSnapshot();
        try {
            List<AgentConfig> nextConfigs = replayReloadChain(baseSnapshot, reloadId, reason);
            agentConfigs.clear();
            agentConfigs.addAll(nextConfigs);
            agentConfigsVersion += 1;
            AgentConfigsSnapshot nextSnapshot = getAgentConfigsSnapshot();
            processedReloadIds.add(reloadId);
            reloadStatusById.put(reloadId, new ReloadStatus(
                    "success", reason, baseSnapshot.getVersion(), nextSnapshot.getVersion(), ""));
            return nextSnapshot;
        } catch (RuntimeException e) {
            processedReloadIds.add(reloadId);
            reloadStatusById.put(reloadId, new ReloadStatus(
                    "failure", reason, baseSnapshot.getVersion(), agentConfigsVersion,
                    e.getMessage() != null ? e.getMessage() : e.getClass().getName()));
            throw e;
        }
    }

    public void applyDefaultHookTimeout(int timeoutSeconds) {
        if (timeoutSeconds <= 0) {
            return;
        }
        for (Plugin plugin : plugins) {
            if (plugin.hookTimeoutSeconds == null) {
                plugin.hookTimeoutSeconds = timeoutSeconds;
            }
        }
    }

    public void onWorkerStartup(GatewayWorker worker) {
        discoverPlugins();
        initializePlugins();

        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onWorkerStartup", () -> plugin.onWorkerStartup(worker), "", "", worker.getWorkerId());
        }
    }

    public void onWorkerShutdown(GatewayWorker worker) {
        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onWorkerShutdown", () -> plugin.onWorkerShutdown(worker), "", "", worker.getWorkerId());
        }
    }

    public void onTaskStart(AgentContext context) {
        String sessionId = context.getSessionId() != null ? context.getSessionId() : "";
        String traceId = context.getTraceId() != null ? context.getTraceId() : "";
        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onTaskStart", () -> plugin.onTaskStart(context), sessionId, traceId, "");
        }
    }

    public void onTaskComplete(AgentContext context, Object result) {
        String sessionId = context.getSessionId() != null ? context.getSessionId() : "";
        String traceId = context.getTraceId() != null ? context.getTraceId() : "";
        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onTaskComplete", () -> plugin.onTaskComplete(context, result), sessionId, traceId, "");
        }
    }

    public void onTaskError(AgentContext context, Throwable error) {
        String sessionId = context.getSessionId() != null ? context.getSessionId() : "";
        String traceId = context.getTraceId() != null ? context.getTraceId() : "";
        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onTaskError", () -> plugin.onTaskError(context, error), sessionId, traceId, "");
        }
    }

    public void onTaskCancel(AgentContext context, CancelTaskCommand command) {
        String sessionId = context.getSessionId() != null ? context.getSessionId() : "";
        String traceId = context.getTraceId() != null ? context.getTraceId() : "";
        for (Plugin plugin : getActivePlugins()) {
            executeHook(plugin, "onTaskCancel", () -> plugin.onTaskCancel(context, command), sessionId, traceId, "");
        }
    }
}
