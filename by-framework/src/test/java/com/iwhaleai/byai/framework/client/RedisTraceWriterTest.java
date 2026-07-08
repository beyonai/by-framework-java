package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisOps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * RedisTraceWriter tested against the RedisOps seam (Cluster-compatible by
 * construction) instead of a raw Jedis mock, per the client dispatch trace
 * Cluster compatibility work.
 */
@ExtendWith(MockitoExtension.class)
class RedisTraceWriterTest {

    @Mock
    private RedisOps redisOps;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void recordClientDispatchWritesMetaSpanTtlsAndIndexes() {
        RedisTraceWriter writer = new RedisTraceWriter(redisOps, objectMapper);

        writer.recordClientDispatch(new RedisTraceWriter.ClientDispatchRecord(
                "trace-1", "msg-1", "sess-1", null, "demo-agent", "worker-1",
                "AGENT_TYPE", "DELIVERED", 1000L, 1500L));

        String traceKey = Constants.TraceKeys.traceMeta("trace-1");
        String spanKey = Constants.TraceKeys.traceSpans("trace-1");

        verify(redisOps).hsetAll(eq(traceKey), any(Map.class));
        verify(redisOps).rpush(eq(spanKey), anyString());
        verify(redisOps).expire(traceKey, Constants.TRACE_TTL_SECONDS);
        verify(redisOps).expire(spanKey, Constants.TRACE_TTL_SECONDS);

        verify(redisOps).zadd(eq(Constants.TraceKeys.traceIndexSession("sess-1")), anyDouble(), eq("trace-1"));
        verify(redisOps).zadd(eq(Constants.TraceKeys.traceIndexWorker("worker-1")), anyDouble(), eq("trace-1"));
        verify(redisOps).zadd(eq(Constants.TraceKeys.traceIndexAgent("demo-agent")), anyDouble(), eq("trace-1"));
    }

    @Test
    void traceMetaWriteFailureSkipsIndexWrites() {
        RedisTraceWriter writer = new RedisTraceWriter(redisOps, objectMapper);
        doThrow(new RuntimeException("cluster down")).when(redisOps).hsetAll(anyString(), any(Map.class));

        writer.recordClientDispatch(new RedisTraceWriter.ClientDispatchRecord(
                "trace-1", "msg-1", "sess-1", null, "demo-agent", "worker-1",
                "AGENT_TYPE", "DELIVERED", 1000L, 1500L));

        verify(redisOps, never()).zadd(anyString(), anyDouble(), anyString());
    }

    @Test
    void indexWriteFailureDoesNotBlockSiblingIndexesOrTtls() {
        RedisTraceWriter writer = new RedisTraceWriter(redisOps, objectMapper);
        String traceKey = Constants.TraceKeys.traceMeta("trace-1");
        String spanKey = Constants.TraceKeys.traceSpans("trace-1");
        String sessionIndexKey = Constants.TraceKeys.traceIndexSession("sess-1");
        doThrow(new RuntimeException("cluster index down"))
                .when(redisOps).zadd(eq(sessionIndexKey), anyDouble(), anyString());

        writer.recordClientDispatch(new RedisTraceWriter.ClientDispatchRecord(
                "trace-1", "msg-1", "sess-1", null, "demo-agent", "worker-1",
                "AGENT_TYPE", "DELIVERED", 1000L, 1500L));

        verify(redisOps).expire(traceKey, Constants.TRACE_TTL_SECONDS);
        verify(redisOps).expire(spanKey, Constants.TRACE_TTL_SECONDS);
        verify(redisOps).zadd(eq(Constants.TraceKeys.traceIndexAgent("demo-agent")), anyDouble(), eq("trace-1"));
    }
}
