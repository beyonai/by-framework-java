package com.iwhaleai.byai.framework.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.extensions.AgentConfig;
import com.iwhaleai.byai.framework.core.extensions.Plugin;
import com.iwhaleai.byai.framework.core.extensions.PluginBuildContext;
import com.iwhaleai.byai.framework.core.extensions.PluginManifest;
import com.iwhaleai.byai.framework.core.extensions.PluginReloadContext;
import com.iwhaleai.byai.framework.core.extensions.PluginRegistry;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;
import com.iwhaleai.byai.framework.core.protocol.ReloadPluginsCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WorkerRunnerReloadPluginsTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static class ReloadDemoPlugin extends Plugin {
        private final String nextDescription;

        ReloadDemoPlugin(String nextDescription) {
            super(PluginManifest.builder().pluginId("demo-agent").enabled(true).build());
            this.nextDescription = nextDescription;
        }

        @Override
        public List<AgentConfig> registerAgentConfigs(PluginBuildContext buildContext) {
            List<AgentConfig> configs = new ArrayList<>(buildContext.listAgentConfigs());
            configs.add(AgentConfig.builder().agentId("demo-agent").description("v1").build());
            return configs;
        }

        @Override
        public List<AgentConfig> reload(PluginReloadContext context) {
            List<AgentConfig> next = new ArrayList<>();
            for (AgentConfig config : context.getCurrentAgentConfigs()) {
                if (config.getAgentId().equals("demo-agent")) {
                    next.add(AgentConfig.builder().agentId("demo-agent").description(nextDescription).build());
                } else {
                    next.add(config);
                }
            }
            return next;
        }
    }

    private static class SimpleWorker extends GatewayWorker {
        SimpleWorker(String workerId, RedisClient redisClient) {
            super(workerId, redisClient);
        }

        @Override
        public List<String> getAgentTypes() {
            return List.of("reload-agent");
        }

        @Override
        public Object processCommand(GatewayCommand command, AgentContext context) {
            return "ok";
        }
    }

    @Test
    void reloadPluginsCommandOnTheWorkerCtrlStreamReloadsPluginsAndPublishesAnAck() throws Exception {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        SimpleWorker worker = new SimpleWorker("worker-reload", redisClient);
        worker.getPluginRegistry().registerBundle(new ReloadDemoPlugin("v2"));
        worker.getPluginRegistry().initializePlugins();
        assertEquals("v1", worker.getPluginRegistry().getAgentConfig("demo-agent").getDescription());

        WorkerRunner runner = new WorkerRunner(worker, redisClient, "test-group");

        when(jedis.set(anyString(), anyString(), any(SetParams.class))).thenReturn("OK");
        when(jedis.xgroupCreate(anyString(), anyString(), any(), anyBoolean())).thenReturn("OK");

        ReloadPluginsCommand command = ReloadPluginsCommand.of(
                MessageHeader.builder()
                        .messageId("msg-reload-1")
                        .sessionId("reload:reload-agent")
                        .traceId("trace-reload-1")
                        .targetAgentType("reload-agent")
                        .build(),
                "reload-1",
                "ship v2"
        );
        Map<String, String> fields = new HashMap<>();
        fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(command));
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), fields);

        String workerCtrlStream = Constants.QueueNames.workerCtrlStream("worker-reload");
        lenient().when(jedis.xreadGroup(anyString(), anyString(), any(), anyMap())).thenReturn(null);
        when(jedis.xreadGroup(anyString(), anyString(), any(), eq(Map.of(workerCtrlStream, StreamEntryID.UNRECEIVED_ENTRY))))
                .thenReturn(List.of(Map.entry(workerCtrlStream, List.of(entry))));

        runner.start();
        Thread.sleep(300);
        runner.stop();

        assertEquals("v2", worker.getPluginRegistry().getAgentConfig("demo-agent").getDescription());

        ArgumentCaptor<Map<String, String>> ackFieldsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(jedis, atLeastOnce()).xadd(
                eq(Constants.RegistryKeys.pluginReloadAckStream("reload-1")),
                any(XAddParams.class),
                ackFieldsCaptor.capture());

        @SuppressWarnings("unchecked")
        Map<String, Object> ackPayload = objectMapper.readValue(ackFieldsCaptor.getValue().get(Constants.RedisFields.DATA), Map.class);
        assertEquals("success", ackPayload.get("status"));
        assertEquals("worker-reload", ackPayload.get("worker_id"));
        assertEquals("reload-1", ackPayload.get("reload_id"));

        verify(jedis, atLeastOnce()).xack(eq(workerCtrlStream), anyString(), eq(new StreamEntryID("1-0")));
    }
}
