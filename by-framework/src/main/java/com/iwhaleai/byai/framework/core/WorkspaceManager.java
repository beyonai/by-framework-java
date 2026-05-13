package com.iwhaleai.byai.framework.core;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Workspace manager for setting up and cleaning up task workspaces.
 * Provides public/private directory structure for session and task isolation.
 */
@Slf4j
public class WorkspaceManager {

    private final String baseDir;

    public WorkspaceManager(String baseDir) {
        this.baseDir = baseDir != null ? baseDir : "/tmp/workspace";
    }

    public WorkspaceManager() {
        this("/tmp/workspace");
    }

    /**
     * Set up workspace directories for a task.
     *
     * @param sessionId the session ID
     * @param taskId the task ID
     * @return map containing workspace paths: root, public, private, historyDb
     */
    public Map<String, String> setupWorkspace(String sessionId, String taskId) throws IOException {
        Map<String, String> paths = new HashMap<>();

        String root = Paths.get(baseDir, "session_" + sessionId).toString();
        String publicDir = Paths.get(root, "public").toString();
        String privateDir = Paths.get(root, "private", taskId).toString();

        paths.put("root", root);
        paths.put("public", publicDir);
        paths.put("private", privateDir);
        paths.put("historyDb", Paths.get(publicDir, "memory", "local_db").toString());

        // Create directory structure
        // Public directories (session-level, persist across tasks)
        createDirectories(
                Paths.get(publicDir, "session"),
                Paths.get(publicDir, "memory", "local_db"),
                Paths.get(publicDir, "agent_skills")
        );

        // Private directories (task-level, cleaned up after task)
        createDirectories(
                Paths.get(privateDir, "input"),
                Paths.get(privateDir, "temp"),
                Paths.get(privateDir, "output"),
                Paths.get(privateDir, "system")
        );

        log.info("Workspace setup complete for session={}, task={}: {}", sessionId, taskId, paths);
        return paths;
    }

    /**
     * Clean up task-specific workspace directories.
     *
     * @param sessionId the session ID
     * @param taskId the task ID
     */
    public void cleanupTask(String sessionId, String taskId) throws IOException {
        Path privateDir = Paths.get(baseDir, "session_" + sessionId, "private", taskId);

        if (Files.exists(privateDir)) {
            // Use nio for efficient directory deletion
            try (var stream = Files.walk(privateDir)) {
                stream.sorted((a, b) -> b.compareTo(a)) // Delete files before directories
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.warn("Failed to delete: {}", path, e);
                            }
                        });
            }
            log.info("Task workspace cleaned up: session={}, task={}", sessionId, taskId);
        }
    }

    /**
     * Clean up entire session workspace.
     *
     * @param sessionId the session ID
     */
    public void cleanupSession(String sessionId) throws IOException {
        Path sessionDir = Paths.get(baseDir, "session_" + sessionId);

        if (Files.exists(sessionDir)) {
            try (var stream = Files.walk(sessionDir)) {
                stream.sorted((a, b) -> b.compareTo(a))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                log.warn("Failed to delete: {}", path, e);
                            }
                        });
            }
            log.info("Session workspace cleaned up: session={}", sessionId);
        }
    }

    private void createDirectories(Path... paths) throws IOException {
        for (Path path : paths) {
            Files.createDirectories(path);
        }
    }

    /**
     * Get the public directory for a session.
     */
    public String getPublicDir(String sessionId) {
        return Paths.get(baseDir, "session_" + sessionId, "public").toString();
    }

    /**
     * Get the private directory for a task.
     */
    public String getPrivateDir(String sessionId, String taskId) {
        return Paths.get(baseDir, "session_" + sessionId, "private", taskId).toString();
    }
}
