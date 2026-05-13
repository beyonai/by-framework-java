package com.iwhaleai.byai.framework.core.runtime.history;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory history backend (default).
 * Suitable for development and testing. Data is lost on process restart.
 */
@Slf4j
public class InMemoryHistoryBackend extends BaseHistoryBackend {

    private final Map<String, List<Map<String, Object>>> storage = new ConcurrentHashMap<>();

    @Override
    public List<Map<String, Object>> getHistory(String sessionId, int limit) {
        List<Map<String, Object>> messages = storage.getOrDefault(sessionId, new ArrayList<>());
        // Return last N messages in chronological order
        int size = messages.size();
        if (size <= limit) {
            return new ArrayList<>(messages);
        }
        return new ArrayList<>(messages.subList(size - limit, size));
    }

    @Override
    public void saveMessage(String sessionId, String role, String content, Map<String, Object> metadata) {
        Map<String, Object> message = new HashMap<>();
        message.put("role", role);
        message.put("content", content);
        message.put("metadata", metadata != null ? metadata : Collections.emptyMap());

        storage.computeIfAbsent(sessionId, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(message);

        log.debug("Saved message to session {}: role={}", sessionId, role);
    }
}
