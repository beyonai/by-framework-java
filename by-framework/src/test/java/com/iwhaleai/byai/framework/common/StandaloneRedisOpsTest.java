package com.iwhaleai.byai.framework.common;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * StandaloneRedisOps: inherits the shared RedisOpsContractTest suite, plus
 * standalone-specific coverage of the per-call borrow/close pattern.
 */
@ExtendWith(MockitoExtension.class)
class StandaloneRedisOpsTest extends RedisOpsContractTest {

    @Mock
    private RedisClient redisClient;

    @Mock
    private Jedis jedis;

    private RedisOps ops;

    @BeforeEach
    void setUp() {
        lenient().when(redisClient.getResource()).thenReturn(jedis);
        ops = new StandaloneRedisOps(redisClient);
    }

    @Override
    protected RedisOps ops() {
        return ops;
    }

    @Override
    protected JedisCommands redisCommandsMock() {
        return jedis;
    }

    @Test
    void isClusterModeIsFalse() {
        assertFalse(ops.isClusterMode());
    }

    @Test
    void getBorrowsAndClosesAJedisConnectionPerCall() {
        when(jedis.get("k")).thenReturn("v");

        ops.get("k");

        verify(redisClient).getResource();
        verify(jedis).close();
    }

    @Test
    void scanKeysScansTheSingleNode() {
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key-1", "key-2")));

        List<String> result = ops.scanKeys("prefix:*", 0);

        assertEquals(List.of("key-1", "key-2"), result);
    }

    @Test
    void scanKeysStopsAtLimit() {
        when(jedis.scan(eq(ScanParams.SCAN_POINTER_START), any(ScanParams.class)))
                .thenReturn(new ScanResult<>(ScanParams.SCAN_POINTER_START, List.of("key-1", "key-2", "key-3")));

        List<String> result = ops.scanKeys("prefix:*", 2);

        assertEquals(2, result.size());
    }
}
