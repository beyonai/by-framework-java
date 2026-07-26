package com.iwhaleai.byai.framework.core.extensions;

import com.iwhaleai.byai.framework.worker.GatewayWorker;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class PluginRegistryTest {

    @BeforeEach
    void setUp() {
        TestPlugin.hookCallLog.clear();
    }

    @Test
    void testPluginRegistryBasicFunctionality() {
        // 创建插件注册表
        PluginRegistry registry = new PluginRegistry();
        assertNotNull(registry);

        // 初始状态下没有插件
        assertTrue(registry.getActivePlugins().isEmpty());
        assertTrue(registry.getAgentConfigs().isEmpty());
    }

    @Test
    void testPluginRegistration() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();

        // 注册插件
        registry.registerBundle(testPlugin);

        // 验证插件已注册
        List<Plugin> activePlugins = registry.getActivePlugins();
        assertEquals(1, activePlugins.size());
        assertEquals("test-plugin", activePlugins.get(0).pluginId);
        assertTrue(activePlugins.get(0).manifest.isEnabled());
    }

    @Test
    void testDiscoverPlugins() {
        PluginRegistry registry = new PluginRegistry();

        // 注册插件类
        Plugin.registerPluginClass(TestPlugin.class);

        // 发现插件
        registry.discoverPlugins();

        // 验证插件已发现并注册
        List<Plugin> activePlugins = registry.getActivePlugins();
        assertTrue(activePlugins.size() >= 1);
        assertTrue(activePlugins.stream().anyMatch(p -> "test-plugin".equals(p.pluginId)));
    }

    @Test
    void testPluginLifecycleHooks() {
        // 创建测试插件
        TestPlugin testPlugin = new TestPlugin();
        PluginRegistry registry = new PluginRegistry();
        registry.registerBundle(testPlugin);

        // 创建模拟的 Worker 和 Context
        GatewayWorker worker = new GatewayWorker("test-worker") {
            @Override
            public List<String> getAgentTypes() {
                return new ArrayList<>();
            }

            @Override
            public Object processCommand(GatewayCommand command, AgentContext context) {
                return null;
            }
        };

        AgentContext context = new AgentContext("test-session", "test-trace", RedisClient.getInstance(), "test-agent",
                "test-message");

        // 调用各种钩子
        registry.onWorkerStartup(worker);
        registry.onTaskStart(context);
        registry.onTaskComplete(context, "success-result");
        registry.onWorkerShutdown(worker);

        // 验证所有钩子都被正确调用
        List<String> callLog = TestPlugin.hookCallLog;
        assertTrue(callLog.contains("onWorkerStartup:test-worker"));
        assertTrue(callLog.contains("onTaskStart:test-session"));
        assertTrue(callLog.contains("onTaskComplete:test-session:success-result"));
        assertTrue(callLog.contains("onWorkerShutdown:test-worker"));
    }

    @Test
    void testPluginInitialization() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        // 初始化插件
        registry.initializePlugins();

        // 验证插件已初始化
        // TODO: 添加验证逻辑
        assertFalse(registry.getActivePlugins().isEmpty());
    }

    @Test
    void testCancelTaskHook() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        AgentContext context = new AgentContext("test-session", "test-trace", RedisClient.getInstance(), "test-agent",
                "test-message");
        CancelTaskCommand command = CancelTaskCommand.builder()
                .header(MessageHeader.builder()
                        .messageId("cancel-123")
                        .sessionId("test-session")
                        .traceId("test-trace")
                        .build())
                .body(CancelTaskCommand.CancelTaskBody.builder()
                        .targetMessageId("target-message-456")
                        .build())
                .build();

        registry.onTaskCancel(context, command);

        assertTrue(TestPlugin.hookCallLog.contains("onTaskCancel:test-session:target-message-456"));
    }

    private static AgentConfig agentConfig(String agentId, String description) {
        return AgentConfig.builder().agentId(agentId).description(description).build();
    }

    private static class ReloadablePlugin extends Plugin {
        final List<PluginReloadContext> reloadCalls = new ArrayList<>();
        private final AgentConfig config;
        private final String nextDescription;

        ReloadablePlugin(AgentConfig config, String nextDescription) {
            super(PluginManifest.builder().pluginId(config.getAgentId()).enabled(true).build());
            this.config = config;
            this.nextDescription = nextDescription;
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext) {
            List<AgentConfig> configs = new ArrayList<>(buildContext.listAgentConfigs());
            configs.add(config);
            return configs;
        }

        @Override
        public List<AgentConfig> reload(PluginReloadContext context) {
            reloadCalls.add(context);
            List<AgentConfig> next = new ArrayList<>();
            for (AgentConfig c : context.getCurrentAgentConfigs()) {
                if (c.getAgentId().equals(pluginId)) {
                    next.add(agentConfig(c.getAgentId(), nextDescription));
                } else {
                    next.add(c);
                }
            }
            return next;
        }
    }

    private static class ThrowingReloadPlugin extends Plugin {
        ThrowingReloadPlugin(AgentConfig config) {
            super(PluginManifest.builder().pluginId(config.getAgentId()).enabled(true).build());
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext) {
            List<AgentConfig> configs = new ArrayList<>(buildContext.listAgentConfigs());
            configs.add(agentConfig(pluginId, "v1"));
            return configs;
        }

        @Override
        public List<AgentConfig> reload(PluginReloadContext context) {
            throw new RuntimeException("reload boom");
        }
    }

    private PluginRegistry buildRegistry(Plugin plugin) {
        PluginRegistry registry = new PluginRegistry();
        registry.registerBundle(plugin);
        registry.initializePlugins();
        return registry;
    }

    @Test
    void reloadDefaultsToNoOpPassthroughOfCurrentAgentConfigs() {
        TestPlugin plugin = new TestPlugin();
        List<AgentConfig> current = List.of(agentConfig("noop-agent", "v1"));
        PluginReloadContext context = new PluginReloadContext(
                "test-plugin", "r-1", "", current, List.of(), 0);

        List<AgentConfig> result = plugin.reload(context);

        assertEquals(current, result);
    }

    @Test
    void reloadPluginsReplaysEachActivePluginAndCommitsOnlyAfterTheWholeChainSucceeds() {
        ReloadablePlugin plugin = new ReloadablePlugin(agentConfig("a", "v1"), "v2");
        PluginRegistry registry = buildRegistry(plugin);
        int versionBefore = registry.getAgentConfigsVersion();

        PluginRegistry.AgentConfigsSnapshot snapshot = registry.reloadPlugins("reload-1", "ship v2 prompt");

        assertEquals(1, plugin.reloadCalls.size());
        assertEquals("reload-1", plugin.reloadCalls.get(0).getReloadId());
        assertEquals("ship v2 prompt", plugin.reloadCalls.get(0).getReason());
        assertEquals(versionBefore + 1, snapshot.getVersion());
        assertEquals("v2", registry.getAgentConfig("a").getDescription());
        assertEquals(versionBefore + 1, registry.getAgentConfigsVersion());
    }

    @Test
    void reloadPluginsIsIdempotentPerReloadId() {
        ReloadablePlugin plugin = new ReloadablePlugin(agentConfig("a", "v1"), "v2");
        PluginRegistry registry = buildRegistry(plugin);

        registry.reloadPlugins("reload-2", "first");
        int versionAfterFirst = registry.getAgentConfigsVersion();
        assertEquals(1, plugin.reloadCalls.size());

        PluginRegistry.AgentConfigsSnapshot second = registry.reloadPlugins("reload-2", "first");

        assertEquals(1, plugin.reloadCalls.size());
        assertEquals(versionAfterFirst, registry.getAgentConfigsVersion());
        assertEquals(versionAfterFirst, second.getVersion());
    }

    @Test
    void aFailingPluginReloadLeavesConfigsAndVersionUnchanged() {
        ThrowingReloadPlugin plugin = new ThrowingReloadPlugin(agentConfig("b", "v1"));
        PluginRegistry registry = buildRegistry(plugin);
        int versionBefore = registry.getAgentConfigsVersion();
        List<AgentConfig> configsBefore = registry.getAgentConfigs();

        assertThrows(RuntimeException.class, () -> registry.reloadPlugins("reload-3", "bad change"));

        assertEquals(versionBefore, registry.getAgentConfigsVersion());
        assertEquals(configsBefore, registry.getAgentConfigs());
        assertEquals("failure", registry.getReloadStatus("reload-3").getStatus());
        assertTrue(registry.getReloadStatus("reload-3").getError().contains("reload boom"));
    }

    private static class SlowHookPlugin extends Plugin {
        final long sleepMs;

        SlowHookPlugin(int hookTimeoutSeconds, long sleepMs) {
            super(PluginManifest.builder().pluginId("slow-plugin").enabled(true).build(), hookTimeoutSeconds);
            this.sleepMs = sleepMs;
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext) {
            return new ArrayList<>();
        }

        @Override
        public void onTaskStart(AgentContext context) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void aHookExceedingItsTimeoutIsRecordedAsTimedOutAndDoesNotBlockTheCallerPastTheTimeout() {
        PluginRegistry registry = new PluginRegistry();
        SlowHookPlugin plugin = new SlowHookPlugin(1, 5000); // 1s timeout, hook sleeps 5s
        registry.registerBundle(plugin);

        AgentContext context = new AgentContext("sess-slow", "trace-slow", RedisClient.getInstance(), "test-agent", "msg-slow");

        long start = System.currentTimeMillis();
        registry.onTaskStart(context);
        long elapsedMs = System.currentTimeMillis() - start;

        assertTrue(elapsedMs < 4000, "caller should not be blocked for the hook's full 5s sleep; took " + elapsedMs + "ms");

        Map<String, Map<String, Object>> stats = registry.getHookStats();
        @SuppressWarnings("unchecked")
        Map<String, Object> hookStat = (Map<String, Object>) stats.get("slow-plugin").get("onTaskStart");
        assertEquals(1, hookStat.get("timeout"));
        assertEquals(0, hookStat.get("success"));
        assertEquals(0, hookStat.get("failure"));
    }

    @Test
    void reloadPluginsRecordsASuccessStatusWithVersionBeforeAndAfter() {
        ReloadablePlugin plugin = new ReloadablePlugin(agentConfig("c", "v1"), "v2");
        PluginRegistry registry = buildRegistry(plugin);
        int versionBefore = registry.getAgentConfigsVersion();

        registry.reloadPlugins("reload-4", "ok");

        PluginRegistry.ReloadStatus status = registry.getReloadStatus("reload-4");
        assertEquals("success", status.getStatus());
        assertEquals(versionBefore, status.getVersionBefore());
        assertEquals(versionBefore + 1, status.getVersionAfter());
    }
}
