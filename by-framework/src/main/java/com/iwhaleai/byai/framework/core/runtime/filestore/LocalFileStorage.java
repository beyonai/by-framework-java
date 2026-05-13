package com.iwhaleai.byai.framework.core.runtime.filestore;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Local filesystem implementation of FileStorage.
 */
@Slf4j
public class LocalFileStorage implements FileStorage {

    private final Path baseDir;

    public LocalFileStorage(String baseDir) {
        this.baseDir = Paths.get(baseDir).toAbsolutePath().normalize();
    }

    @Override
    public void initialize() {
        try {
            Files.createDirectories(baseDir);
            log.info("LocalFileStorage initialized at: {}", baseDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize LocalFileStorage at: " + baseDir, e);
        }
    }

    @Override
    public void shutdown() {
        // No-op for local storage
    }

    @Override
    public void write(String path, Object content, String encoding) {
        Path filePath = resolvePath(path);
        try {
            Files.createDirectories(filePath.getParent());
            if (content instanceof String) {
                Files.writeString(filePath, (String) content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } else if (content instanceof byte[]) {
                Files.write(filePath, (byte[]) content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            } else {
                Files.writeString(filePath, content.toString(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write file: " + path, e);
        }
    }

    @Override
    public Object read(String path, String encoding) {
        Path filePath = resolvePath(path);
        try {
            if (encoding != null && !encoding.isEmpty()) {
                return Files.readString(filePath);
            } else {
                return Files.readAllBytes(filePath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read file: " + path, e);
        }
    }

    @Override
    public void delete(String path) {
        Path targetPath = resolvePath(path);
        try {
            if (Files.isDirectory(targetPath)) {
                // Use Java NIO for directory deletion
                try (var stream = Files.walk(targetPath)) {
                    stream.sorted(Comparator.reverseOrder())
                            .forEach(p -> {
                                try {
                                    Files.delete(p);
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to delete: " + p, e);
                                }
                            });
                }
            } else {
                Files.deleteIfExists(targetPath);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete path: " + path, e);
        }
    }

    @Override
    public boolean exists(String path) {
        return Files.exists(resolvePath(path));
    }

    @Override
    public boolean isFile(String path) {
        return Files.isRegularFile(resolvePath(path));
    }

    @Override
    public boolean isDir(String path) {
        return Files.isDirectory(resolvePath(path));
    }

    @Override
    public List<String> list(String path) {
        Path dirPath = resolvePath(path.isEmpty() ? "." : path);
        try (var stream = Files.list(dirPath)) {
            return stream
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to list directory: " + path, e);
        }
    }

    @Override
    public String getUrl(String path, int expires) {
        return resolvePath(path).toUri().toString();
    }

    private Path resolvePath(String path) {
        // Normalize path separators and remove leading separators
        String normalized = path.replaceAll("[/\\\\]+", "/").replaceAll("^[/\\\\]+", "");
        Path resolved = baseDir.resolve(normalized).toAbsolutePath().normalize();
        // Security check: ensure path is within baseDir
        if (!resolved.startsWith(baseDir)) {
            throw new SecurityException("Path escape attempt detected: " + path);
        }
        return resolved;
    }
}
