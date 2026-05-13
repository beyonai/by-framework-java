package com.iwhaleai.byai.framework.core.runtime.history;

import java.util.List;
import java.util.Map;

/**
 * Abstract base class for history storage backends.
 * Implementations provide persistent or in-memory storage for session message history.
 */
public abstract class BaseHistoryBackend {

    /**
     * Get message history for a session.
     *
     * @param sessionId the session ID
     * @param limit     maximum number of messages to return
     * @return list of message dicts in chronological order (oldest first)
     *         Each message has: role (String), content (String), metadata (Map)
     */
    public abstract List<Map<String, Object>> getHistory(String sessionId, int limit);

    /**
     * Save a message to session history.
     *
     * @param sessionId the session ID
     * @param role      the message role (user, assistant, system, tool)
     * @param content   the message content
     * @param metadata  additional metadata (optional, can be null)
     */
    public abstract void saveMessage(String sessionId, String role, String content, Map<String, Object> metadata);
}
