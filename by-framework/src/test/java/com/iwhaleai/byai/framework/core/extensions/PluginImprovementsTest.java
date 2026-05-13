package com.iwhaleai.byai.framework.core.extensions;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.protocol.CancelTaskCommand;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.worker.AgentContext;
import com.iwhaleai.byai.framework.worker.GatewayWorker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Plugin 系统完善测试，对标 Python test_plugin_improvements.py
 */
class PluginImprovementsTest {

    @BeforeEach
    void setUp() {
        TestPlugin.hookCallLog.clear();
    }

    // --- Priority Plugin for ordering tests ---
    static class LowPriorityPlugin extends Plugin {
        public LowPriorityPlugin() {
            super(PluginManifest.builder()
                    .pluginId("low-priority")
                    .priority(10)
                    .enabled(true)
                    .build());
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
            return new ArrayList<>();
        }

        @Override
        public void onWorkerStartup(GatewayWorker worker) {
            TestPlugin.hookCallLog.add("low-priority:startup");
        }
    }

    static class HighPriorityPlugin extends Plugin {
        public HighPriorityPlugin() {
            super(PluginManifest.builder()
                    .pluginId("high-priority")
                    .priority(1)
                    .enabled(true)
                    .build());
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
            return new ArrayList<>();
        }

        @Override
        public void onWorkerStartup(GatewayWorker worker) {
            TestPlugin.hookCallLog.add("high-priority:startup");
        }
    }

    static class DisabledPlugin extends Plugin {
        public DisabledPlugin() {
            super(PluginManifest.builder()
                    .pluginId("disabled-plugin")
                    .enabled(false)
                    .build());
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
            TestPlugin.hookCallLog.add("disabled:registerAgentConfigs");
            return new ArrayList<>();
        }

        @Override
        public void onWorkerShutdown(GatewayWorker worker) {
            TestPlugin.hookCallLog.add("disabled:shutdown");
        }
    }

    static class FailingPlugin extends Plugin {
        public FailingPlugin() {
            super(PluginManifest.builder()
                    .pluginId("failing-plugin")
                    .enabled(true)
                    .build());
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
            return new ArrayList<>();
        }

        @Override
        public void onTaskStart(AgentContext context) {
            throw new RuntimeException("plugin failed");
        }
    }

    private GatewayWorker createMockWorker(String workerId) {
        return new GatewayWorker(workerId) {
            @Override
            public List<String> getAgentTypes() {
                return new ArrayList<>();
            }

            @Override
            public Object processCommand(GatewayCommand command, AgentContext context) {
                return null;
            }
        };
    }

    private AgentContext createMockContext(String sessionId) {
        return new AgentContext(sessionId, "trace-1", RedisClient.getInstance(), "agent-1", "msg-1");
    }

    @Test
    void initializePluginsIsIdempotent() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin plugin = new TestPlugin();
        registry.registerBundle(plugin);

        registry.initializePlugins();
        registry.initializePlugins(); // second call should be no-op

        long registerCount = TestPlugin.hookCallLog.stream()
                .filter(s -> s.equals("registerAgentConfigs"))
                .count();
        assertEquals(1, registerCount, "registerAgentConfigs should be called exactly once");
    }

    @Test
    void disabledPluginExcludedFromActivePlugins() {
        PluginRegistry registry = new PluginRegistry();
        registry.registerBundle(new TestPlugin());
        registry.registerBundle(new DisabledPlugin());

        List<Plugin> active = registry.getActivePlugins();
        assertEquals(1, active.size());
        assertEquals("test-plugin", active.get(0).pluginId);
    }

    @Test
    void lifecycleSkipsDisabledPlugins() {
        PluginRegistry registry = new PluginRegistry();
        registry.registerBundle(new DisabledPlugin());

        GatewayWorker worker = createMockWorker("w1");
        registry.onWorkerShutdown(worker);

        assertFalse(TestPlugin.hookCallLog.contains("disabled:shutdown"));
    }

    @Test
    void lifecycleOrdersByPriorityThenName() {
        PluginRegistry registry = new PluginRegistry();
        registry.registerBundle(new LowPriorityPlugin());
        registry.registerBundle(new HighPriorityPlugin());

        GatewayWorker worker = createMockWorker("w1");
        // Manually call onWorkerStartup hooks (skipping discover/init to keep test
        // focused)
        for (Plugin p : registry.getActivePlugins()) {
            p.onWorkerStartup(worker);
        }

        // high-priority (priority=1) should come before low-priority (priority=10)
        int highIdx = TestPlugin.hookCallLog.indexOf("high-priority:startup");
        int lowIdx = TestPlugin.hookCallLog.indexOf("low-priority:startup");
        assertTrue(highIdx >= 0, "high-priority hook should be called");
        assertTrue(lowIdx >= 0, "low-priority hook should be called");
        assertTrue(highIdx < lowIdx, "high-priority should run before low-priority");
    }

    @Test
    void agentConfigConflictStrategyError() {
        PluginRegistry registry = new PluginRegistry();

        // First plugin registers agent config
        Plugin firstPlugin = new Plugin(PluginManifest.builder().pluginId("first").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("First")
                        .onConflict(ConflictStrategy.ERROR)
                        .build());
            }
        };

        Plugin secondPlugin = new Plugin(PluginManifest.builder().pluginId("second").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("Second")
                        .onConflict(ConflictStrategy.ERROR)
                        .build());
            }
        };

        registry.registerBundle(firstPlugin);
        registry.registerBundle(secondPlugin);

        // initializePlugins wraps the error in executeHook, so it records failure in
        // stats
        registry.initializePlugins();

        // The second plugin's registerAgentConfigs should have failed
        Map<String, Map<String, Object>> stats = registry.getHookStats();
        Map<String, Object> secondStats = (Map<String, Object>) stats.get("second").get("registerAgentConfigs");
        assertEquals(1, secondStats.get("failure"));

        // Only the first config should be registered
        assertEquals(1, registry.getAgentConfigs().size());
        assertEquals("First", registry.getAgentConfig("shared-agent").getName());
    }

    @Test
    void agentConfigConflictStrategySkip() {
        PluginRegistry registry = new PluginRegistry();

        Plugin firstPlugin = new Plugin(PluginManifest.builder().pluginId("first").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("First")
                        .onConflict(ConflictStrategy.ERROR)
                        .build());
            }
        };

        Plugin skipPlugin = new Plugin(PluginManifest.builder().pluginId("skip-plugin").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("Skip")
                        .onConflict(ConflictStrategy.SKIP)
                        .build());
            }
        };

        registry.registerBundle(firstPlugin);
        registry.registerBundle(skipPlugin);
        registry.initializePlugins();

        AgentConfig config = registry.getAgentConfig("shared-agent");
        assertNotNull(config);
        assertEquals("First", config.getName(), "Original config should be preserved with SKIP strategy");
    }

    @Test
    void agentConfigConflictStrategyOverwrite() {
        PluginRegistry registry = new PluginRegistry();

        Plugin firstPlugin = new Plugin(PluginManifest.builder().pluginId("first").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("First")
                        .onConflict(ConflictStrategy.ERROR)
                        .build());
            }
        };

        Plugin overwritePlugin = new Plugin(
                PluginManifest.builder().pluginId("overwrite-plugin").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("shared-agent")
                        .name("Overwritten")
                        .onConflict(ConflictStrategy.OVERWRITE)
                        .build());
            }
        };

        registry.registerBundle(firstPlugin);
        registry.registerBundle(overwritePlugin);
        registry.initializePlugins();

        AgentConfig config = registry.getAgentConfig("shared-agent");
        assertNotNull(config);
        assertEquals("Overwritten", config.getName(), "Config should be overwritten with OVERWRITE strategy");
    }

    @Test
    void hookStatsRecordSuccessAndFailure() {
        PluginRegistry registry = new PluginRegistry();
        FailingPlugin failingPlugin = new FailingPlugin();
        registry.registerBundle(failingPlugin);

        AgentContext context = createMockContext("sess-1");
        registry.onTaskStart(context); // this will fail

        Map<String, Map<String, Object>> stats = registry.getHookStats();
        assertNotNull(stats.get("failing-plugin"));
        Map<String, Object> taskStartStats = (Map<String, Object>) stats.get("failing-plugin").get("onTaskStart");
        assertNotNull(taskStartStats);
        assertEquals(0, taskStartStats.get("success"));
        assertEquals(1, taskStartStats.get("failure"));
        assertEquals("plugin failed", taskStartStats.get("lastError"));
    }

    @Test
    void hookStatsRecordSuccess() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        AgentContext context = createMockContext("sess-1");
        registry.onTaskStart(context);
        registry.onTaskStart(context);

        Map<String, Map<String, Object>> stats = registry.getHookStats();
        Map<String, Object> taskStartStats = (Map<String, Object>) stats.get("test-plugin").get("onTaskStart");
        assertEquals(2, taskStartStats.get("success"));
        assertEquals(0, taskStartStats.get("failure"));
    }

    @Test
    void resetHookStatsGlobal() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        AgentContext context = createMockContext("sess-1");
        registry.onTaskStart(context);

        assertFalse(registry.getHookStats().isEmpty());

        registry.resetHookStats();

        assertTrue(registry.getHookStats().isEmpty());
    }

    @Test
    void resetHookStatsForSpecificPlugin() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        AgentContext context = createMockContext("sess-1");
        registry.onTaskStart(context);
        registry.onTaskComplete(context, "result");

        registry.resetHookStats("test-plugin", "onTaskStart");

        Map<String, Map<String, Object>> stats = registry.getHookStats();
        // onTaskStart should be gone, onTaskComplete should remain
        Map<String, Object> pluginStats = stats.get("test-plugin");
        assertNotNull(pluginStats);
        assertNull(pluginStats.get("onTaskStart"));
        assertNotNull(pluginStats.get("onTaskComplete"));
    }

    @Test
    void resetHookStatsForEntirePlugin() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        AgentContext context = createMockContext("sess-1");
        registry.onTaskStart(context);

        registry.resetHookStats("test-plugin");

        assertFalse(registry.getHookStats().containsKey("test-plugin"));
    }

    @Test
    void applyDefaultHookTimeoutOnlyFillsUnsetValues() {
        PluginRegistry registry = new PluginRegistry();

        Plugin pluginWithTimeout = new Plugin(
                PluginManifest.builder().pluginId("with-timeout").enabled(true).build(),
                5) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return new ArrayList<>();
            }
        };

        Plugin pluginWithoutTimeout = new Plugin(
                PluginManifest.builder().pluginId("without-timeout").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return new ArrayList<>();
            }
        };

        registry.registerBundle(pluginWithTimeout);
        registry.registerBundle(pluginWithoutTimeout);

        registry.applyDefaultHookTimeout(10);

        // Plugin with pre-set timeout should retain its original value
        assertEquals(5, pluginWithTimeout.hookTimeoutSeconds);
        // Plugin without timeout should get the default
        assertEquals(10, pluginWithoutTimeout.hookTimeoutSeconds);
    }

    @Test
    void agentConfigRegistrationWithToolsAndPrompts() {
        PluginRegistry registry = new PluginRegistry();

        Plugin bundlePlugin = new Plugin(PluginManifest.builder().pluginId("bundle-plugin").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("bundle-agent")
                        .name("Bundle Agent")
                        .tools(Map.of("search", Map.of("type", "web")))
                        .prompts(Map.of("system", "You are helpful"))
                        .skills(Map.of("summarize", Map.of("model", "gpt-4")))
                        .build());
            }
        };

        registry.registerBundle(bundlePlugin);
        registry.initializePlugins();

        AgentConfig config = registry.getAgentConfig("bundle-agent");
        assertNotNull(config);
        assertEquals("Bundle Agent", config.getName());
        assertNotNull(config.getTools().get("search"));
        assertEquals("You are helpful", config.getPrompts().get("system"));
        assertNotNull(config.getSkills().get("summarize"));
    }

    @Test
    void emptyAgentIdThrowsValidationError() {
        PluginRegistry registry = new PluginRegistry();

        Plugin invalidPlugin = new Plugin(PluginManifest.builder().pluginId("invalid").enabled(true).build()) {
            @Override
            public List<AgentConfig> registerAgentConfigs(PluginBuildContext ctx) {
                return List.of(AgentConfig.builder()
                        .agentId("")
                        .name("Invalid")
                        .build());
            }
        };

        registry.registerBundle(invalidPlugin);

        // initializePlugins wraps the error, so we check stats for failure
        registry.initializePlugins();
        Map<String, Map<String, Object>> stats = registry.getHookStats();
        Map<String, Object> regStats = (Map<String, Object>) stats.get("invalid").get("registerAgentConfigs");
        assertEquals(1, regStats.get("failure"));
    }

    @Test
    void getPluginById() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin testPlugin = new TestPlugin();
        registry.registerBundle(testPlugin);

        Plugin found = registry.getPlugin("test-plugin");
        assertNotNull(found);
        assertEquals("test-plugin", found.pluginId);

        Plugin notFound = registry.getPlugin("nonexistent");
        assertNull(notFound);
    }

    @Test
    void duplicatePluginRegistrationIgnored() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin plugin = new TestPlugin();

        registry.registerBundle(plugin);
        registry.registerBundle(plugin); // same reference

        assertEquals(1, registry.getActivePlugins().size());
    }

    @Test
    void onTaskErrorHookDispatches() {
        PluginRegistry registry = new PluginRegistry();
        TestPlugin plugin = new TestPlugin();
        registry.registerBundle(plugin);

        AgentContext context = createMockContext("sess-err");
        registry.onTaskError(context, new RuntimeException("test error"));

        assertTrue(TestPlugin.hookCallLog.contains("onTaskError:sess-err:test error"));
    }

    @Test
    void pluginBuildContextFreezesAndRestores() {
        AgentConfig config1 = AgentConfig.builder().agentId("agent-1").name("Agent 1").build();
        PluginBuildContext ctx = new PluginBuildContext(List.of(config1));

        ctx.freezePrevAgentConfigs();
        List<AgentConfig> prev = ctx.getPrevAgentConfigs();
        assertEquals(1, prev.size());
        assertEquals("agent-1", prev.get(0).getAgentId());

        // Prev should be unmodifiable
        assertThrows(UnsupportedOperationException.class, () -> prev.add(
                AgentConfig.builder().agentId("rogue").build()));
    }
}
