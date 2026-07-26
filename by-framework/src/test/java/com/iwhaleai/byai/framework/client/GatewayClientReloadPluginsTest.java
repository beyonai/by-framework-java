package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GatewayClientReloadPluginsTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
    }

    @Test
    void reloadPluginsForAgentTypeFansOutToEveryOnlineWorker() {
        when(jedis.smembers(Constants.RegistryKeys.agentTypeMembers("reload-agent")))
                .thenReturn(Set.of("worker-1"));
        when(jedis.get(Constants.RegistryKeys.workerOnlineLease("worker-1"))).thenReturn("1");

        GatewayClient<Object> client = new GatewayClient<>(redisClient);
        Map<String, Object> result = client.reloadPluginsForAgentType("reload-agent", "ship v2", null);

        assertEquals(1, result.get("dispatched_count"));
        assertEquals(List.of("worker-1"), result.get("worker_ids"));
        assertNotNull(result.get("reload_id"));

        verify(jedis).xadd(eq(Constants.QueueNames.workerCtrlStream("worker-1")), any(XAddParams.class), anyMap());
    }

    @Test
    void reloadPluginsForAgentTypeWithNoOnlineWorkersIsANoOp() {
        when(jedis.smembers(Constants.RegistryKeys.agentTypeMembers("ghost-agent"))).thenReturn(Set.of());

        GatewayClient<Object> client = new GatewayClient<>(redisClient);
        Map<String, Object> result = client.reloadPluginsForAgentType("ghost-agent", "", null);

        assertEquals(0, result.get("dispatched_count"));
        assertEquals(List.of(), result.get("worker_ids"));
        verify(jedis, never()).xadd(anyString(), any(XAddParams.class), anyMap());
    }

    @Test
    void collectReloadAcksReadsPayloadsFromTheAckStream() throws Exception {
        String reloadId = "reload-xyz";
        Map<String, Object> ackPayload = new HashMap<>();
        ackPayload.put("reload_id", reloadId);
        ackPayload.put("worker_id", "worker-1");
        ackPayload.put("status", "success");

        Map<String, String> fields = new HashMap<>();
        fields.put(Constants.RedisFields.DATA, objectMapper.writeValueAsString(ackPayload));
        StreamEntry entry = new StreamEntry(new StreamEntryID("1-0"), fields);
        String stream = Constants.RegistryKeys.pluginReloadAckStream(reloadId);

        when(jedis.xread(any(redis.clients.jedis.params.XReadParams.class), anyMap()))
                .thenReturn(List.of(Map.entry(stream, List.of(entry))));

        GatewayClient<Object> client = new GatewayClient<>(redisClient);
        List<Map<String, Object>> acks = client.collectReloadAcks(reloadId, "0-0", 0, 100);

        assertEquals(1, acks.size());
        assertEquals("success", acks.get(0).get("status"));
        assertEquals("worker-1", acks.get(0).get("worker_id"));
    }
}
