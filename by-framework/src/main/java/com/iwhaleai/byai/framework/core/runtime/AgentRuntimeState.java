package com.iwhaleai.byai.framework.core.runtime;

import com.iwhaleai.byai.framework.core.extensions.AgentConfigManager;
import com.iwhaleai.byai.framework.core.runtime.filestore.FileStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Unified state container providing single entry point for session management and agent configuration.
 */
@Slf4j
public class AgentRuntimeState {

    @Getter
    private final String sessionId;

    @Getter
    private final String userCode;

    @Getter
    private final String userName;

    @Getter
    private final SessionManager sessionManager;

    @Getter
    private final AgentConfigManager configManager;

    public AgentRuntimeState(String sessionId, String userCode, String userName, FileStorage storage, String workspaceDir) {
        this.sessionId = sessionId;
        this.userCode = userCode;
        this.userName = userName;
        this.sessionManager = new SessionManager(sessionId, userCode, userName, storage, workspaceDir);
        this.configManager = new AgentConfigManager();
    }

    public AgentRuntimeState(String sessionId, String userCode, String userName, FileStorage storage) {
        this(sessionId, userCode, userName, storage, null);
    }

    public AgentRuntimeState(String sessionId, String userCode, String userName) {
        this(sessionId, userCode, userName, null, null);
    }

    public AgentRuntimeState(String sessionId) {
        this(sessionId, null, null, null, null);
    }

    /**
     * Initialize the runtime state.
     */
    public void initialize() {
        sessionManager.initialize();
        log.info("AgentRuntimeState initialized for session: {}", sessionId);
    }

    /**
     * Shutdown the runtime state and release resources.
     */
    public void shutdown() {
        sessionManager.shutdown();
        log.info("AgentRuntimeState shutdown for session: {}", sessionId);
    }
}
