package com.iwhaleai.byai.framework.core.runtime.history;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * Session history manager with pluggable backend.
 * Provides a high-level API for session message history.
 */
@Slf4j
public class HistoryManager {

    private static BaseHistoryBackend defaultBackend = new InMemoryHistoryBackend();

    private final String sessionId;
    private final BaseHistoryBackend backend;

    public HistoryManager(String sessionId) {
        this(sessionId, null);
    }

    public HistoryManager(String sessionId, BaseHistoryBackend backend) {
        this.sessionId = sessionId;
        this.backend = backend != null ? backend : defaultBackend;
    }

    /**
     * Set the default backend for all new HistoryManager instances.
     */
    public static void setDefaultBackend(BaseHistoryBackend backend) {
        defaultBackend = backend;
    }

    /**
     * Get message history for this session.
     *
     * @param limit maximum number of messages to return (default: 10)
     * @return list of messages in chronological order
     */
    public List<Map<String, Object>> getHistory(int limit) {
        return backend.getHistory(sessionId, limit);
    }

    /**
     * Get message history for this session (default limit: 10).
     */
    public List<Map<String, Object>> getHistory() {
        return getHistory(10);
    }

    /**
     * Save a message to session history.
     *
     * @param role      the message role (user, assistant, system, tool)
     * @param content   the message content
     * @param metadata  additional metadata (optional)
     */
    public void saveMessage(String role, String content, Map<String, Object> metadata) {
        backend.saveMessage(sessionId, role, content, metadata);
    }

    /**
     * Save a message with no metadata.
     */
    public void saveMessage(String role, String content) {
        saveMessage(role, content, null);
    }
}
