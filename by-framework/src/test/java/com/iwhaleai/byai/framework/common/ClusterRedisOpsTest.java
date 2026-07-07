package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * ClusterRedisOps: inherits the shared RedisOpsContractTest suite, plus
 * Cluster-specific coverage of the cross-node scanKeys merge (JedisCluster
 * has no built-in cross-node SCAN, unlike a single standalone node).
 */
@ExtendWith(MockitoExtension.class)
class ClusterRedisOpsTest extends RedisOpsContractTest {

    @Mock
    private JedisCluster jedisCluster;

    private RedisOps ops;

    @BeforeEach
    void setUp() {
        ops = new ClusterRedisOps(jedisCluster);
    }

    @Override
    protected RedisOps ops() {
        return ops;
    }

    @Override
    protected JedisCommands redisCommandsMock() {
        return jedisCluster;
    }

    @Test
    void isClusterModeIsTrue() {
        assertTrue(ops.isClusterMode());
    }

    @Test
    void scanKeysMergesResultsAcrossEveryClusterNode(@org.mockito.Mock ConnectionPool poolA,
            @org.mockito.Mock ConnectionPool poolB,
            @org.mockito.Mock redis.clients.jedis.Connection connA,
            @org.mockito.Mock redis.clients.jedis.Connection connB) {
        when(poolA.getResource()).thenReturn(connA);
        when(poolB.getResource()).thenReturn(connB);
        Map<String, ConnectionPool> nodes = new LinkedHashMap<>();
        nodes.put("node-a:6379", poolA);
        nodes.put("node-b:6379", poolB);
        when(jedisCluster.getClusterNodes()).thenReturn(nodes);

        try (var mockedJedisCtor = mockConstruction(Jedis.class, (mockJedis, context) -> {
            Object connectionArg = context.arguments().get(0);
            if (connectionArg == connA) {
                when(mockJedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                        .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key-a1", "key-a2")));
            } else {
                when(mockJedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                        .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key-b1")));
            }
        })) {
            List<String> result = ops.scanKeys("prefix:*", 0);

            assertEquals(3, result.size());
            assertTrue(result.containsAll(List.of("key-a1", "key-a2", "key-b1")));
        }
    }

    @Test
    void scanKeysStopsAtLimit() {
        ConnectionPool pool = mock(ConnectionPool.class);
        when(jedisCluster.getClusterNodes()).thenReturn(Map.of("node-a:6379", pool));

        try (var mockedJedisCtor = mockConstruction(Jedis.class, (mockJedis, context) ->
                when(mockJedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                        .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START,
                                List.of("key-1", "key-2", "key-3"))))) {
            List<String> result = ops.scanKeys("prefix:*", 2);

            assertEquals(2, result.size());
        }
    }
}
