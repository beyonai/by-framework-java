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
}
