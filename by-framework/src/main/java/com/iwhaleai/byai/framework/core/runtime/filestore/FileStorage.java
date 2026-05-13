package com.iwhaleai.byai.framework.core.runtime.filestore;

import java.util.List;

/**
 * Interface for file storage backends.
 * Provides a common abstraction for local filesystem and cloud storage (MinIO/S3) implementations.
 */
public interface FileStorage {

    /**
     * Initialize the storage backend.
     */
    void initialize();

    /**
     * Shutdown the storage backend and release resources.
     */
    void shutdown();

    /**
     * Write content to a file.
     *
     * @param path     the file path relative to the storage root
     * @param content  the content to write (String or byte[])
     * @param encoding the encoding for string content (default: utf-8)
     */
    void write(String path, Object content, String encoding);

    /**
     * Read content from a file.
     *
     * @param path     the file path relative to the storage root
     * @param encoding the encoding for string content (default: utf-8)
     * @return the file content as String or byte[]
     */
    Object read(String path, String encoding);

    /**
     * Delete a file or directory.
     *
     * @param path the file or directory path
     */
    void delete(String path);

    /**
     * Check if a path exists.
     *
     * @param path the path to check
     * @return true if exists
     */
    boolean exists(String path);

    /**
     * Check if a path is a file.
     *
     * @param path the path to check
     * @return true if it's a file
     */
    boolean isFile(String path);

    /**
     * Check if a path is a directory.
     *
     * @param path the path to check
     * @return true if it's a directory
     */
    boolean isDir(String path);

    /**
     * List files in a directory.
     *
     * @param path the directory path (empty string for root)
     * @return list of file/directory names
     */
    List<String> list(String path);

    /**
     * Get a URL for accessing a file.
     *
     * @param path    the file path
     * @param expires URL expiration time in seconds
     * @return the URL
     */
    String getUrl(String path, int expires);
}
