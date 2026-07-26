package com.iwhaleai.byai.framework.core.protocol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Content codec implementation for BaiYing domain objects, mirroring the Python/TypeScript
 * ByaiContentCodec: a string passes through unchanged; a single-message wire array is
 * auto-unwrapped to a scalar BaiYingMessage; a multi-message array stays a list of
 * BaiYingMessage; anything that isn't shaped like a wire message list passes through as-is.
 */
public class ByaiContentCodec implements ContentCodec {

    @Override
    public Object serialize(Object content) {
        return serializeByaiContent(content);
    }

    @Override
    public Object deserialize(Object content) {
        return deserializeByaiContent(content);
    }

    public static Object serializeByaiContent(Object content) {
        if (content instanceof String) {
            return content;
        }
        if (content instanceof List<?> list) {
            List<Object> result = new ArrayList<>();
            for (Object item : list) {
                result.add(isWireMessage(item) ? serializeMessage((Map<?, ?>) item) : item);
            }
            return result;
        }
        if (content instanceof BaiYingMessage message) {
            List<Object> result = new ArrayList<>();
            result.add(serializeMessage(message));
            return result;
        }
        return content;
    }

    @SuppressWarnings("unchecked")
    public static Object deserializeByaiContent(Object content) {
        if (content instanceof String) {
            return content;
        }
        if (!(content instanceof List<?> list) || list.isEmpty()) {
            return content;
        }
        for (Object item : list) {
            if (!isWireMessage(item)) {
                return content;
            }
        }

        List<BaiYingMessage> messages = new ArrayList<>();
        for (Object item : list) {
            messages.add(deserializeMessage((Map<String, Object>) item));
        }
        return messages.size() == 1 ? messages.get(0) : messages;
    }

    private static boolean isWireMessage(Object item) {
        return item instanceof Map<?, ?> map && map.containsKey("role") && map.containsKey("content");
    }

    private static Map<String, Object> serializeMessage(Map<?, ?> item) {
        Map<String, Object> map = new HashMap<>();
        map.put("role", item.get("role"));
        map.put("content", item.get("content"));
        return map;
    }

    private static Map<String, Object> serializeMessage(BaiYingMessage message) {
        Map<String, Object> map = new HashMap<>();
        map.put("role", message.getRole());
        Object content = message.getContent();
        if (content instanceof BaiYingMessage.MessageContent mc) {
            Map<String, Object> contentMap = new HashMap<>();
            contentMap.put("text", mc.getText());
            contentMap.put("files", mc.getFiles() != null ? mc.getFiles() : List.of());
            contentMap.put("resources", mc.getResources() != null ? mc.getResources() : List.of());
            map.put("content", contentMap);
        } else {
            map.put("content", content);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private static BaiYingMessage deserializeMessage(Map<String, Object> item) {
        String role = String.valueOf(item.get("role"));
        Object payload = item.get("content");

        if (payload instanceof Map<?, ?> payloadMap) {
            Map<String, Object> p = (Map<String, Object>) payloadMap;
            BaiYingMessage.MessageContent content = BaiYingMessage.MessageContent.builder()
                    .text(p.get("text") != null ? String.valueOf(p.get("text")) : "")
                    .files(toMessageFiles(p.get("files")))
                    .resources(toResources(p.get("resources")))
                    .build();
            return BaiYingMessage.builder().role(role).content(content).build();
        }
        return BaiYingMessage.builder().role(role).content(payload).build();
    }

    @SuppressWarnings("unchecked")
    private static List<BaiYingMessage.MessageFile> toMessageFiles(Object raw) {
        List<BaiYingMessage.MessageFile> files = new ArrayList<>();
        if (!(raw instanceof List<?> list)) {
            return files;
        }
        for (Object entry : list) {
            if (!(entry instanceof Map<?, ?> map)) {
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) map;
            files.add(BaiYingMessage.MessageFile.builder()
                    .fileId(stringOrNull(m.get("fileId")))
                    .fileUrl(stringOrNull(m.get("fileUrl")))
                    .fileType(stringOrNull(m.get("fileType")))
                    .fileName(stringOrNull(m.get("fileName")))
                    .build());
        }
        return files;
    }

    @SuppressWarnings("unchecked")
    private static List<BaiYingMessage.Resource> toResources(Object raw) {
        List<BaiYingMessage.Resource> resources = new ArrayList<>();
        if (!(raw instanceof List<?> list)) {
            return resources;
        }
        for (Object entry : list) {
            if (!(entry instanceof Map<?, ?> map)) {
                continue;
            }
            Map<String, Object> m = (Map<String, Object>) map;
            resources.add(BaiYingMessage.Resource.builder()
                    .resourceId(stringOrNull(m.get("resourceId")))
                    .resourceName(stringOrNull(m.get("resourceName")))
                    .resourceType(stringOrNull(m.get("resourceType")))
                    .id(stringOrNull(m.get("id")))
                    .path(stringOrNull(m.get("path")))
                    .resourceDesc(stringOrNull(m.get("resourceDesc")))
                    .build());
        }
        return resources;
    }

    private static String stringOrNull(Object value) {
        return value != null ? String.valueOf(value) : null;
    }
}
