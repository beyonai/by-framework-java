package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.RedisClient;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

@Slf4j
class RedisTraceWriter {
    private static final String CLIENT_DISPATCH = "client.dispatch";

    private final RedisClient redisClient;
    private final ObjectMapper objectMapper;

    RedisTraceWriter(RedisClient redisClient, ObjectMapper objectMapper) {
        this.redisClient = redisClient;
        this.objectMapper = objectMapper;
    }

    void recordClientDispatch(ClientDispatchRecord record) {
        if (record == null || isBlank(record.traceId()) || isBlank(record.messageId())) {
            return;
        }

        try (Jedis jedis = redisClient.getResource()) {
            String traceKey = Constants.TraceKeys.traceMeta(record.traceId());
            String spanKey = Constants.TraceKeys.traceSpans(record.traceId());
            double score = record.startTs();

            jedis.hset(traceKey, traceMeta(record));
            jedis.rpush(spanKey, objectMapper.writeValueAsString(span(record)));
            indexTrace(jedis, Constants.TraceKeys.traceIndexSession(record.sessionId()), score, record.traceId());
            indexTrace(jedis, Constants.TraceKeys.traceIndexWorker(record.workerId()), score, record.traceId());
            indexTrace(jedis, Constants.TraceKeys.traceIndexAgent(record.targetAgentType()), score, record.traceId());
            expireTraceKeys(jedis, traceKey, spanKey, record);
        } catch (Exception e) {
            log.warn("Redis client.dispatch trace write skipped: {}", e.getMessage());
        }
    }

    private Map<String, String> traceMeta(ClientDispatchRecord record) {
        Map<String, String> meta = new HashMap<>();
        putString(meta, "trace_id", record.traceId());
        putString(meta, "session_id", record.sessionId());
        putString(meta, "name", CLIENT_DISPATCH + ":" + record.targetAgentType());
        putString(meta, "status", "COMPLETED");
        putString(meta, "root_agent_type", record.targetAgentType());
        putString(meta, "root_message_id", record.messageId());
        putString(meta, "start_ts", String.valueOf(record.startTs()));
        putString(meta, "updated_at", String.valueOf(record.endTs()));
        return meta;
    }

    private Map<String, Object> span(ClientDispatchRecord record) {
        Map<String, Object> span = new HashMap<>();
        putObject(span, "trace_id", record.traceId());
        putObject(span, "span_id", record.messageId() + ":" + CLIENT_DISPATCH);
        putObject(span, "parent_span_id", "");
        putObject(span, "name", CLIENT_DISPATCH + ":" + record.targetAgentType());
        putObject(span, "operation", CLIENT_DISPATCH);
        putObject(span, "component", "client");
        putObject(span, "kind", "span");
        putObject(span, "status", "COMPLETED");
        putObject(span, "source", "redis");
        putObject(span, "session_id", record.sessionId());
        putObject(span, "message_id", record.messageId());
        putObject(span, "parent_message_id", record.parentMessageId());
        putObject(span, "worker_id", record.workerId());
        putObject(span, "source_agent_type", "client");
        putObject(span, "target_agent_type", record.targetAgentType());
        putObject(span, "route_policy", record.routePolicy());
        putObject(span, "route_status", record.routeStatus());
        span.put("start_ts", record.startTs());
        span.put("end_ts", record.endTs());
        span.put("duration_ms", Math.max(0L, record.endTs() - record.startTs()));
        return span;
    }

    private void indexTrace(Jedis jedis, String key, double score, String traceId) {
        if (isBlank(key) || isBlank(traceId) || key.endsWith(":")) {
            return;
        }
        jedis.zadd(key, score, traceId);
    }

    private void expireTraceKeys(Jedis jedis, String traceKey, String spanKey, ClientDispatchRecord record) {
        jedis.expire(traceKey, Constants.TRACE_TTL_SECONDS);
        jedis.expire(spanKey, Constants.TRACE_TTL_SECONDS);
        expireIfPresent(jedis, Constants.TraceKeys.traceIndexSession(record.sessionId()));
        expireIfPresent(jedis, Constants.TraceKeys.traceIndexWorker(record.workerId()));
        expireIfPresent(jedis, Constants.TraceKeys.traceIndexAgent(record.targetAgentType()));
    }

    private void expireIfPresent(Jedis jedis, String key) {
        if (!isBlank(key) && !key.endsWith(":")) {
            jedis.expire(key, Constants.TRACE_TTL_SECONDS);
        }
    }

    private static void putString(Map<String, String> map, String key, String value) {
        if (!isBlank(value)) {
            map.put(key, value);
        }
    }

    private static void putObject(Map<String, Object> map, String key, String value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    record ClientDispatchRecord(
            String traceId,
            String messageId,
            String sessionId,
            String parentMessageId,
            String targetAgentType,
            String workerId,
            String routePolicy,
            String routeStatus,
            long startTs,
            long endTs) {
    }
}
