package com.iwhaleai.byai.framework.core.protocol;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Structured return value for GatewayWorker.processCommand.
 *
 * <p>Fields mirror ResumeCommand body fields. metadata is merged into
 * ResumeCommand.header.metadata when a child agent returns to its caller.
 */
public record AgentTaskResult(
        String status,
        Object content,
        Object replyData,
        Map<String, Object> metadata,
        Map<String, Object> extraPayload
) {
    private static final Set<String> RESULT_FIELDS = Set.of(
            "status",
            "content",
            "replyData",
            "reply_data",
            "metadata",
            "extraPayload",
            "extra_payload"
    );

    public AgentTaskResult {
        status = status != null ? status : AgentState.COMPLETED;
        content = content != null ? content : "";
        replyData = ensureJsonSerializable(replyData, "replyData");
        metadata = ensureJsonObject(metadata != null ? metadata : Map.of(), "metadata");
        extraPayload = ensureJsonObject(
                extraPayload != null ? extraPayload : Map.of(),
                "extraPayload"
        );
        ensureWireContent(content, "content");
    }

    public static AgentTaskResult completed(Object replyData) {
        return new AgentTaskResult(AgentState.COMPLETED, "", replyData, Map.of(), Map.of());
    }

    public static AgentTaskResult normalize(Object result) {
        if (result instanceof AgentTaskResult taskResult) {
            return taskResult;
        }
        if (result instanceof String value && isAgentState(value)) {
            return new AgentTaskResult(value, "", null, Map.of(), Map.of());
        }
        if (result instanceof Map<?, ?> map) {
            Map<String, Object> stringMap = ensureJsonObject(map, "replyData");
            Map<String, Object> metadata = stringMap.containsKey("metadata")
                    ? ensureJsonObject(stringMap.get("metadata"), "metadata")
                    : Map.of();
            if (isStructuredResultMap(stringMap)) {
                return new AgentTaskResult(
                        stringValue(stringMap.get("status"), AgentState.COMPLETED),
                        stringMap.getOrDefault("content", ""),
                        firstPresent(stringMap, "replyData", "reply_data"),
                        metadata,
                        ensureJsonObject(
                                firstPresent(stringMap, "extraPayload", "extra_payload", Map.of()),
                                "extraPayload"
                        )
                );
            }
            return new AgentTaskResult(
                    stringValue(stringMap.get("status"), AgentState.COMPLETED),
                    "",
                    stringMap,
                    metadata,
                    Map.of()
            );
        }
        return new AgentTaskResult(AgentState.COMPLETED, "", result, Map.of(), Map.of());
    }

    public static Object ensureJsonSerializable(Object value, String path) {
        if (value == null
                || value instanceof String
                || value instanceof Number
                || value instanceof Boolean) {
            return value;
        }
        if (value instanceof List<?> list) {
            List<Object> serialized = new ArrayList<>();
            for (int index = 0; index < list.size(); index++) {
                serialized.add(
                        ensureJsonSerializable(list.get(index), path + "[" + index + "]")
                );
            }
            return serialized;
        }
        if (value instanceof Map<?, ?> map) {
            return ensureJsonObject(map, path);
        }
        throw new IllegalArgumentException(
                "processCommand return value must be JSON serializable; got "
                        + value.getClass().getSimpleName() + " at " + path
        );
    }

    public static Map<String, Object> ensureJsonObject(Object value, String path) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException(
                    "processCommand return metadata fields must be JSON objects; got "
                            + (value == null ? "null" : value.getClass().getSimpleName())
                            + " at " + path
            );
        }
        Map<String, Object> serialized = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();
            if (!(key instanceof String stringKey)) {
                throw new IllegalArgumentException(
                        "processCommand return value must be JSON serializable; got non-string key "
                                + key + " at " + path
                );
            }
            serialized.put(
                    stringKey,
                    ensureJsonSerializable(entry.getValue(), path + "." + stringKey)
            );
        }
        return serialized;
    }

    private static void ensureWireContent(Object value, String path) {
        if (value instanceof String) {
            return;
        }
        if (value instanceof List<?> list) {
            for (int index = 0; index < list.size(); index++) {
                if (!(list.get(index) instanceof Map<?, ?>)) {
                    throw new IllegalArgumentException(
                            "processCommand return content must be a string or list of maps; got "
                                    + list.get(index).getClass().getSimpleName()
                                    + " at " + path + "[" + index + "]"
                    );
                }
                ensureJsonSerializable(list.get(index), path + "[" + index + "]");
            }
            return;
        }
        throw new IllegalArgumentException(
                "processCommand return content must be a string or list of maps; got "
                        + (value == null ? "null" : value.getClass().getSimpleName())
                        + " at " + path
        );
    }

    private static boolean isStructuredResultMap(Map<String, Object> map) {
        return map.containsKey("replyData")
                || map.containsKey("reply_data")
                || map.containsKey("content")
                || (!map.isEmpty() && map.keySet().stream().allMatch(RESULT_FIELDS::contains));
    }

    private static boolean isAgentState(String value) {
        return Set.of(
                AgentState.STARTING,
                AgentState.RUNNING,
                AgentState.WAITING_USER,
                AgentState.CALLING_AGENT,
                AgentState.COMPLETED,
                AgentState.FAILED,
                AgentState.CANCELLING,
                AgentState.CANCELLED,
                AgentState.RESUMED,
                AgentState.QUEUED,
                AgentState.NOT_FOUND
        ).contains(value);
    }

    private static Object firstPresent(Map<String, Object> map, String firstKey, String secondKey) {
        return firstPresent(map, firstKey, secondKey, null);
    }

    private static Object firstPresent(
            Map<String, Object> map,
            String firstKey,
            String secondKey,
            Object defaultValue
    ) {
        if (map.containsKey(firstKey)) {
            return map.get(firstKey);
        }
        if (map.containsKey(secondKey)) {
            return map.get(secondKey);
        }
        return defaultValue;
    }

    private static String stringValue(Object value, String defaultValue) {
        return value != null ? String.valueOf(value) : defaultValue;
    }
}
