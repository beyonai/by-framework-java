package com.iwhaleai.byai.framework.core.runtime;

import com.iwhaleai.byai.framework.core.runtime.filestore.FileStorage;
import com.iwhaleai.byai.framework.core.runtime.filestore.LocalFileStorage;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * File manager for session-level file operations.
 * Provides a high-level API for reading, writing, and managing files within a session workspace.
 */
@Slf4j
public class FileManager {

    private static final String DEFAULT_WORKSPACE_DIR = "workspace";

    private final String sessionId;
    private final String userCode;
    private final FileStorage storage;
    private final String workspaceDir;

    public FileManager(String sessionId, String userCode, FileStorage storage, String workspaceDir) {
        this.sessionId = sessionId;
        this.userCode = userCode != null ? userCode : "default";
        this.storage = storage != null ? storage : createDefaultStorage(workspaceDir);
        this.workspaceDir = this.userCode + "/" + sessionId;
    }

    public FileManager(String sessionId, String userCode, FileStorage storage) {
        this(sessionId, userCode, storage, DEFAULT_WORKSPACE_DIR);
    }

    private LocalFileStorage createDefaultStorage(String workspaceDir) {
        String basePath = workspaceDir != null ? workspaceDir : DEFAULT_WORKSPACE_DIR;
        LocalFileStorage storage = new LocalFileStorage(basePath + "/" + userCode + "/" + sessionId);
        storage.initialize();
        return storage;
    }

    /**
     * Get the underlying storage backend.
     */
    public FileStorage getStorage() {
        return storage;
    }

    /**
     * Get the workspace directory name for this session.
     */
    public String getWorkspaceDir() {
        return workspaceDir;
    }

    /**
     * Initialize the file manager.
     */
    public void initialize() {
        if (storage instanceof LocalFileStorage) {
            storage.initialize();
        }
    }

    /**
     * Shutdown the file manager and release resources.
     */
    public void shutdown() {
        storage.shutdown();
    }

    /**
     * Read a file.
     *
     * @param filename the filename relative to workspace
     * @param encoding the file encoding (default: utf-8)
     * @return file content as string
     */
    public String readFile(String filename, String encoding) {
        String effectiveEncoding = encoding != null ? encoding : "utf-8";
        Object content = storage.read(filename, effectiveEncoding);
        if (content instanceof byte[]) {
            return new String((byte[]) content);
        }
        return (String) content;
    }

    /**
     * Read a file with default encoding (utf-8).
     */
    public String readFile(String filename) {
        return readFile(filename, "utf-8");
    }

    /**
     * Write content to a file.
     *
     * @param filename  the filename relative to workspace
     * @param content   the content to write
     * @param encoding  the file encoding (default: utf-8)
     * @param overwrite whether to overwrite existing file (default: true)
     */
    public void writeFile(String filename, String content, String encoding, boolean overwrite) {
        storage.write(filename, content, encoding);
    }

    /**
     * Write content with default encoding (utf-8) and overwrite=true.
     */
    public void writeFile(String filename, String content) {
        writeFile(filename, content, "utf-8", true);
    }

    /**
     * Check if a file exists.
     */
    public boolean exists(String filename) {
        return storage.exists(filename);
    }

    /**
     * Check if path is a file.
     */
    public boolean isFile(String filename) {
        return storage.isFile(filename);
    }

    /**
     * Check if path is a directory.
     */
    public boolean isDir(String filename) {
        return storage.isDir(filename);
    }

    /**
     * List files in a directory.
     *
     * @param directory the directory name (empty string for workspace root)
     * @return list of file/directory names
     */
    public List<String> listFiles(String directory) {
        return storage.list(directory);
    }

    /**
     * List files at workspace root.
     */
    public List<String> listFiles() {
        return listFiles("");
    }

    /**
     * Get a URL for accessing a file.
     *
     * @param filename the filename
     * @param expires  URL expiration in seconds (for presigned URLs)
     * @return the file URL
     */
    public String getFileUrl(String filename, int expires) {
        return storage.getUrl(filename, expires);
    }

    /**
     * Get a URL with default expiration (3600 seconds).
     */
    public String getFileUrl(String filename) {
        return getFileUrl(filename, 3600);
    }
}
