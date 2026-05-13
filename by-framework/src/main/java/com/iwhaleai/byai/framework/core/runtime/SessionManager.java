package com.iwhaleai.byai.framework.core.runtime;

import com.iwhaleai.byai.framework.core.runtime.filestore.FileStorage;
import com.iwhaleai.byai.framework.core.runtime.history.HistoryManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Session manager for coordinating session-level resources.
 * Provides access to file management and history for a session.
 */
@Slf4j
public class SessionManager {

    @Getter
    private final String sessionId;

    @Getter
    private final String userCode;

    @Getter
    private final String userName;

    @Getter
    private final FileManager fileManager;

    @Getter
    private final HistoryManager history;

    public SessionManager(String sessionId, String userCode, String userName, FileStorage storage, String workspaceDir) {
        this.sessionId = sessionId;
        this.userCode = userCode;
        this.userName = userName;
        this.fileManager = new FileManager(sessionId, userCode, storage, workspaceDir);
        this.history = new HistoryManager(sessionId);

        log.info("SessionManager created for session: {} (user: {})", sessionId, userCode);
    }

    public SessionManager(String sessionId, String userCode, String userName, FileStorage storage) {
        this(sessionId, userCode, userName, storage, null);
    }

    public SessionManager(String sessionId, String userCode, String userName) {
        this(sessionId, userCode, userName, null, null);
    }

    public SessionManager(String sessionId) {
        this(sessionId, null, null, null, null);
    }

    /**
     * Initialize session resources.
     */
    public void initialize() {
        fileManager.initialize();
        log.info("SessionManager initialized for session: {}", sessionId);
    }

    /**
     * Shutdown session resources.
     */
    public void shutdown() {
        fileManager.shutdown();
        log.info("SessionManager shutdown for session: {}", sessionId);
    }

    /**
     * Get the number of messages in session history.
     */
    public int getMessageCount() {
        return history.getHistory(Integer.MAX_VALUE).size();
    }
}
