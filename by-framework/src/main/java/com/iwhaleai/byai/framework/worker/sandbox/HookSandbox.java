package com.iwhaleai.byai.framework.worker.sandbox;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Sandbox for restricting file access to the current workspace directory.
 * Uses a delegated file access approach instead of patching JVM builtins.
 */
@Slf4j
public class HookSandbox {

    private final String baseWorkspace;
    private Path workspacePath;

    public HookSandbox(String baseWorkspace) {
        this.baseWorkspace = baseWorkspace != null ? baseWorkspace : "/tmp/workspace";
    }

    /**
     * Set up the sandbox for a specific task workspace.
     */
    public void setup(String sessionId, String taskId) {
        this.workspacePath = Paths.get(baseWorkspace, "session_" + sessionId, "private", taskId).normalize();
        ActiveWorkspace.set(workspacePath.toString());
        log.info("HookSandbox activated for workspace: {}", workspacePath);
    }

    /**
     * Clean up the sandbox.
     */
    public void cleanup() {
        ActiveWorkspace.clear();
        this.workspacePath = null;
        log.info("HookSandbox deactivated");
    }

    /**
     * Validate that a path is within the allowed workspace.
     *
     * @throws SecurityException if the path is outside workspace
     */
    public void validatePath(String path) throws SecurityException {
        if (workspacePath == null) {
            return; // Sandbox not active, allow access
        }

        Path targetPath = Paths.get(path).isAbsolute()
                ? Paths.get(path).normalize()
                : workspacePath.resolve(path).normalize();

        // Check if the resolved path is within workspace
        if (!targetPath.startsWith(workspacePath)) {
            // Allow access to standard library paths
            String pathStr = targetPath.toString();
            if (pathStr.contains("lib/python") || pathStr.contains("site-packages")
                    || pathStr.contains(".jar!") || pathStr.contains("java.base")) {
                return;
            }
            throw new SecurityException("File access denied: " + path + " is outside workspace: " + workspacePath);
        }
    }

    /**
     * Validate and resolve a path to an actual file within workspace.
     *
     * @throws SecurityException if the path is outside workspace
     * @throws IOException if the path doesn't exist
     */
    public Path resolveAndValidate(String relativePath) throws IOException {
        validatePath(relativePath);
        Path resolved = workspacePath.resolve(relativePath).normalize();

        if (!Files.exists(resolved)) {
            throw new IOException("File not found: " + relativePath);
        }

        if (!resolved.startsWith(workspacePath)) {
            throw new SecurityException("Path traversal detected: " + relativePath);
        }

        return resolved;
    }

    /**
     * Read a file within the sandbox.
     *
     * @throws SecurityException if access is denied
     * @throws IOException if reading fails
     */
    public byte[] readFile(String relativePath) throws IOException {
        Path path = resolveAndValidate(relativePath);
        return Files.readAllBytes(path);
    }

    /**
     * Read a file as string within the sandbox.
     */
    public String readFileAsString(String relativePath, String encoding) throws IOException {
        byte[] data = readFile(relativePath);
        return new String(data, encoding != null ? encoding : "utf-8");
    }

    /**
     * Write a file within the sandbox.
     *
     * @throws SecurityException if access is denied
     */
    public void writeFile(String relativePath, byte[] content) throws IOException {
        validatePath(relativePath);
        Path resolved = workspacePath.resolve(relativePath).normalize();

        if (!resolved.startsWith(workspacePath)) {
            throw new SecurityException("Path traversal detected: " + relativePath);
        }

        // Create parent directories if needed
        Files.createDirectories(resolved.getParent());
        Files.write(resolved, content);
    }

    /**
     * Write a string file within the sandbox.
     */
    public void writeFile(String relativePath, String content, String encoding) throws IOException {
        writeFile(relativePath, content.getBytes(encoding != null ? encoding : "utf-8"));
    }

    /**
     * Check if a file exists within the sandbox.
     */
    public boolean exists(String relativePath) {
        try {
            Path path = workspacePath.resolve(relativePath).normalize();
            if (!path.startsWith(workspacePath)) {
                return false;
            }
            return Files.exists(path);
        } catch (Exception e) {
            return false;
        }
    }
}
