package com.iwhaleai.byai.framework.util.http;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.common.exceptions.DiscoveryHttpClientError;
import com.iwhaleai.byai.framework.core.discovery.DiscoveryClient;
import com.iwhaleai.byai.framework.core.discovery.ServiceInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * HTTP client that integrates with Service Discovery.
 *
 * Resolves service names to physical addresses dynamically and handles load
 * balancing.
 * Supports automatically switching to a different node upon request failures.
 */
@Slf4j
public class DiscoveryHttpClient implements AutoCloseable {

    private final DiscoveryClient discoveryClient;
    private final ByHttpClient httpClient;
    private final boolean ownsHttpClient;
    private final RetryConfig retryConfig;
    private final long healthThresholdMs;

    public DiscoveryHttpClient(
            DiscoveryClient discoveryClient,
            ByHttpClient httpClient,
            RetryConfig retryConfig) {
        this(discoveryClient, httpClient, retryConfig, Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS);
    }

    public DiscoveryHttpClient(
            DiscoveryClient discoveryClient,
            ByHttpClient httpClient,
            RetryConfig retryConfig,
            long healthThresholdMs) {
        this.discoveryClient = discoveryClient;
        // We enforce RetryConfig.no_retry() on the underlying ByHttpClient if we create
        // it,
        // so that retries are fully controlled by DiscoveryHttpClient to enable
        // node-switching.
        this.ownsHttpClient = httpClient == null;
        if (httpClient == null) {
            this.httpClient = ByHttpClient.builder()
                    .baseUrl("")
                    .retryConfig(RetryConfig.noRetry())
                    .build();
        } else {
            this.httpClient = httpClient;
        }
        this.retryConfig = retryConfig != null ? retryConfig : RetryConfig.builder().build();
        this.healthThresholdMs = healthThresholdMs > 0 || healthThresholdMs == Constants.SD_NO_HEALTH_CHECK
                ? healthThresholdMs
                : Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS;
    }

    /**
     * Builder for DiscoveryHttpClient.
     */
    public static class Builder {
        private DiscoveryClient discoveryClient;
        private ByHttpClient httpClient;
        private RetryConfig retryConfig;
        private long healthThresholdMs = Constants.SD_DEFAULT_HEALTH_THRESHOLD_MS;

        public Builder discoveryClient(DiscoveryClient discoveryClient) {
            this.discoveryClient = discoveryClient;
            return this;
        }

        public Builder httpClient(ByHttpClient httpClient) {
            this.httpClient = httpClient;
            return this;
        }

        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        public Builder healthThresholdMs(long healthThresholdMs) {
            this.healthThresholdMs = healthThresholdMs;
            return this;
        }

        public DiscoveryHttpClient build() {
            return new DiscoveryHttpClient(discoveryClient, httpClient, retryConfig, healthThresholdMs);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Execute request with discovery-based node selection and retry.
     */
    private CompletableFuture<HttpResponse> requestWithDiscovery(
            String method,
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data,
            int retryCount,
            Set<String> excludeInstances) {
        final Set<String> excluded = excludeInstances != null ? new HashSet<>(excludeInstances) : new HashSet<>();

        return CompletableFuture.supplyAsync(() -> {
            // 1. Discover a healthy instance
            Optional<ServiceInstance> instanceOpt = discoveryClient.discover(serviceName, this.healthThresholdMs);
            if (instanceOpt.isEmpty()) {
                throw new DiscoveryHttpClientError(
                        "No available instances for service: " + serviceName);
            }

            ServiceInstance instance = instanceOpt.get();

            // 2. Construct the absolute URL
            String absoluteUrl = buildAbsoluteUrl(instance, path);
            int attempt = retryCount + 1;

            Exception lastError = null;

            // 3. Perform the request
            try {
                log.debug("[{}] {} -> {} (attempt {})",
                        method.toUpperCase(), serviceName, absoluteUrl, attempt);

                HttpResponse response = httpClient.request(
                        method, absoluteUrl, headers, params, json, data, 0).get(60, TimeUnit.SECONDS);

                // If success or not a retryable status code, return directly
                if (response.isSuccess()
                        || !retryConfig.getRetryOnStatusCodes().contains(response.getStatusCode())) {
                    return response;
                }

                log.warn("[{}] {} -> {}, switching node and retrying...",
                        method.toUpperCase(), absoluteUrl, response.getStatusCode());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lastError = e;
                log.warn("[{}] {} interrupted (attempt {}): {}",
                        method.toUpperCase(), absoluteUrl, attempt, e.getMessage());
            } catch (ExecutionException e) {
                lastError = e;
                log.warn("[{}] {} execution error (attempt {}): {}",
                        method.toUpperCase(), absoluteUrl, attempt, e.getMessage());
            } catch (Exception e) {
                lastError = e;
                log.warn("[{}] {} network error (attempt {}): {}",
                        method.toUpperCase(), absoluteUrl, attempt, e.getMessage());
            }

            // 4. Handle Retry
            if (attempt < retryConfig.getMaxAttempts()) {
                excluded.add(instance.getId());
                double delay = RetryConfig.calculateDelay(attempt, retryConfig);
                log.warn("Node-switching retry in {:.1f}s for service {}", delay, serviceName);

                try {
                    TimeUnit.MILLISECONDS.sleep((long) (delay * 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DiscoveryHttpClientError("Request interrupted during retry", e);
                }

                try {
                    return requestWithDiscovery(
                            method, serviceName, path, headers, params, json, data,
                            attempt, excluded).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DiscoveryHttpClientError("Request interrupted during retry", e);
                } catch (ExecutionException e) {
                    throw new DiscoveryHttpClientError("Request execution failed during retry", e);
                }
            }

            if (lastError != null) {
                throw new DiscoveryHttpClientError(
                        "Service request failed after " + retryConfig.getMaxAttempts() + " attempts: " + lastError,
                        lastError);
            }

            throw new DiscoveryHttpClientError(
                    "Service request failed after " + retryConfig.getMaxAttempts() + " attempts.");
        });
    }

    /**
     * Send GET request.
     */
    public CompletableFuture<HttpResponse> get(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params) {
        return requestWithDiscovery("GET", serviceName, path, headers, params, null, null, 0, null);
    }

    /**
     * Send POST request.
     */
    public CompletableFuture<HttpResponse> post(
            String serviceName,
            String path,
            Map<String, String> headers,
            Object json,
            Map<String, Object> data) {
        return requestWithDiscovery("POST", serviceName, path, headers, null, json, data, 0, null);
    }

    /**
     * Send PUT request.
     */
    public CompletableFuture<HttpResponse> put(
            String serviceName,
            String path,
            Map<String, String> headers,
            Object json,
            Map<String, Object> data) {
        return requestWithDiscovery("PUT", serviceName, path, headers, null, json, data, 0, null);
    }

    /**
     * Send PATCH request.
     */
    public CompletableFuture<HttpResponse> patch(
            String serviceName,
            String path,
            Map<String, String> headers,
            Object json,
            Map<String, Object> data) {
        return requestWithDiscovery("PATCH", serviceName, path, headers, null, json, data, 0, null);
    }

    /**
     * Send DELETE request.
     */
    public CompletableFuture<HttpResponse> delete(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params) {
        return requestWithDiscovery("DELETE", serviceName, path, headers, params, null, null, 0, null);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // File Download Methods (with service discovery)
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Download file from discovered service as InputStream.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param headers     Optional headers
     * @param params      Optional query parameters
     * @return InputStream containing the file content
     */
    public CompletableFuture<java.io.InputStream> download(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params) {
        return download("GET", serviceName, path, headers, params, null, null);
    }

    /**
     * Download file from discovered service as InputStream using the specified HTTP
     * method.
     */
    public CompletableFuture<java.io.InputStream> download(
            String method,
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return CompletableFuture.supplyAsync(() -> {
            Optional<ServiceInstance> instanceOpt = discoveryClient.discover(serviceName, this.healthThresholdMs);
            if (instanceOpt.isEmpty()) {
                throw new DiscoveryHttpClientError("No available instances for service: " + serviceName);
            }

            ServiceInstance instance = instanceOpt.get();
            String absoluteUrl = buildAbsoluteUrl(instance, path);

            try {
                return httpClient.download(method, absoluteUrl, headers, params, json, data).get();
            } catch (Exception e) {
                throw new DiscoveryHttpClientError("Download failed: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Download file from discovered service and save to local path.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param toPath      Local file path to save
     * @param headers     Optional headers
     * @param params      Optional query parameters
     * @return Path to the downloaded file
     */
    public CompletableFuture<java.nio.file.Path> downloadToFile(
            String serviceName,
            String path,
            java.nio.file.Path toPath,
            Map<String, String> headers,
            Map<String, Object> params) {
        return downloadToFile("GET", serviceName, path, toPath, headers, params, null, null);
    }

    /**
     * Download file from discovered service with the specified HTTP method and save
     * to local path.
     */
    public CompletableFuture<java.nio.file.Path> downloadToFile(
            String method,
            String serviceName,
            String path,
            java.nio.file.Path toPath,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return download(method, serviceName, path, headers, params, json, data).thenApply(inputStream -> {
            try (java.io.InputStream is = inputStream;
                    java.io.FileOutputStream fos = new java.io.FileOutputStream(toPath.toFile())) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                long totalBytes = 0;
                while ((bytesRead = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                    totalBytes += bytesRead;
                }
                log.info("Downloaded {} bytes to {}", totalBytes, toPath);
                return toPath;
            } catch (java.io.IOException e) {
                throw new DiscoveryHttpClientError("Failed to save file: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Download file from discovered service as byte array.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param headers     Optional headers
     * @param params      Optional query parameters
     * @return byte array containing the file content
     */
    public CompletableFuture<byte[]> downloadBytes(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params) {
        return downloadBytes("GET", serviceName, path, headers, params, null, null);
    }

    /**
     * Download file from discovered service as byte array using the specified HTTP
     * method.
     */
    public CompletableFuture<byte[]> downloadBytes(
            String method,
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return download(method, serviceName, path, headers, params, json, data).thenApply(inputStream -> {
            try {
                return inputStream.readAllBytes();
            } catch (java.io.IOException e) {
                throw new DiscoveryHttpClientError("Failed to read bytes: " + e.getMessage(), e);
            } finally {
                try {
                    inputStream.close();
                } catch (java.io.IOException ignored) {
                }
            }
        });
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // File Upload Methods (with service discovery)
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Upload file using multipart/form-data with service discovery.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param filePath    Path to the file to upload
     * @param headers     Optional headers
     * @param formFields  Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            java.nio.file.Path filePath,
            String fileField,
            Map<String, String> headers,
            Map<String, String> formFields) {
        java.util.List<ByHttpClient.MultipartPart> parts = new java.util.ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(ByHttpClient.MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        String field = fileField != null ? fileField : "file";
        parts.add(ByHttpClient.MultipartFilePart.fromPath(field, filePath));
        return upload(serviceName, path, headers, null, parts);
    }

    /**
     * Upload file using multipart/form-data with default field name "file" and
     * service discovery.
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            java.nio.file.Path filePath,
            Map<String, String> headers,
            Map<String, String> formFields) {
        return upload(serviceName, path, filePath, "file", headers, formFields);
    }

    /**
     * Upload multipart/form-data content with service discovery.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param headers     Optional headers
     * @param params      Optional query parameters
     * @param parts       Multipart parts, including text and file/stream parts
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params,
            java.util.List<ByHttpClient.MultipartPart> parts) {
        return uploadWithDiscovery(serviceName, path, headers, params, parts, 0, null);
    }

    /**
     * Upload multiple files using multipart/form-data with service discovery.
     *
     * @param serviceName Service name for discovery
     * @param path        URL path
     * @param filePaths   List of paths to the files to upload
     * @param headers     Optional headers
     * @param formFields  Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            java.util.List<java.nio.file.Path> filePaths,
            String fileField,
            Map<String, String> headers,
            Map<String, String> formFields) {
        java.util.List<ByHttpClient.MultipartPart> parts = new java.util.ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(ByHttpClient.MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        if (filePaths != null) {
            String field = fileField != null ? fileField : "file";
            for (java.nio.file.Path filePath : filePaths) {
                parts.add(ByHttpClient.MultipartFilePart.fromPath(field, filePath));
            }
        }
        return upload(serviceName, path, headers, null, parts);
    }

    /**
     * Upload multiple files using multipart/form-data with default field name
     * "file" and service discovery.
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            java.util.List<java.nio.file.Path> filePaths,
            Map<String, String> headers,
            Map<String, String> formFields) {
        return upload(serviceName, path, filePaths, "file", headers, formFields);
    }

    /**
     * Upload file from a stream using multipart/form-data with service discovery.
     *
     * @param serviceName    Service name for discovery
     * @param path           URL path
     * @param fileName       Name of the file
     * @param streamSupplier Supplier for the input stream
     * @param headers        Optional headers
     * @param formFields     Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            String fileName,
            String fileField,
            ByHttpClient.InputStreamSupplier streamSupplier,
            Map<String, String> headers,
            Map<String, String> formFields) {
        java.util.List<ByHttpClient.MultipartPart> parts = new java.util.ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(ByHttpClient.MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        String field = fileField != null ? fileField : "file";
        parts.add(ByHttpClient.MultipartFilePart.fromStream(field, fileName, null, streamSupplier));
        return upload(serviceName, path, headers, null, parts);
    }

    /**
     * Upload file from a stream using multipart/form-data with default field name
     * "file" and service discovery.
     */
    public CompletableFuture<HttpResponse> upload(
            String serviceName,
            String path,
            String fileName,
            ByHttpClient.InputStreamSupplier streamSupplier,
            Map<String, String> headers,
            Map<String, String> formFields) {
        return upload(serviceName, path, fileName, "file", streamSupplier, headers, formFields);
    }

    private CompletableFuture<HttpResponse> uploadWithDiscovery(
            String serviceName,
            String path,
            Map<String, String> headers,
            Map<String, Object> params,
            java.util.List<ByHttpClient.MultipartPart> parts,
            int retryCount,
            Set<String> excludeInstances) {
        final Set<String> excluded = excludeInstances != null ? new HashSet<>(excludeInstances) : new HashSet<>();

        return CompletableFuture.supplyAsync(() -> {
            Optional<ServiceInstance> instanceOpt = discoveryClient.discover(serviceName, this.healthThresholdMs);
            if (instanceOpt.isEmpty()) {
                throw new DiscoveryHttpClientError("No available instances for service: " + serviceName);
            }

            ServiceInstance instance = instanceOpt.get();
            String absoluteUrl = buildAbsoluteUrl(instance, path);
            int attempt = retryCount + 1;
            Exception lastError = null;

            try {
                log.debug("[POST] {} -> {} (multipart upload, attempt {})",
                        serviceName, absoluteUrl, attempt);

                HttpResponse response = httpClient.upload(absoluteUrl, headers, params, parts).get(60,
                        TimeUnit.SECONDS);

                if (response.isSuccess() || !retryConfig.getRetryOnStatusCodes().contains(response.getStatusCode())) {
                    return response;
                }

                log.warn("[POST] {} -> {}, switching node and retrying...", absoluteUrl, response.getStatusCode());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lastError = e;
                log.warn("[POST] {} interrupted (attempt {}): {}", absoluteUrl, attempt, e.getMessage());
            } catch (ExecutionException e) {
                lastError = e;
                log.warn("[POST] {} execution error (attempt {}): {}", absoluteUrl, attempt, e.getMessage());
            } catch (Exception e) {
                lastError = e;
                log.warn("[POST] {} network error (attempt {}): {}", absoluteUrl, attempt, e.getMessage());
            }

            if (attempt < retryConfig.getMaxAttempts()) {
                excluded.add(instance.getId());
                double delay = RetryConfig.calculateDelay(attempt, retryConfig);
                log.warn("Node-switching retry in {:.1f}s for service {}", delay, serviceName);

                try {
                    TimeUnit.MILLISECONDS.sleep((long) (delay * 1000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DiscoveryHttpClientError("Upload interrupted during retry", e);
                }

                try {
                    return uploadWithDiscovery(serviceName, path, headers, params, parts, attempt, excluded).get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new DiscoveryHttpClientError("Upload interrupted during retry", e);
                } catch (ExecutionException e) {
                    throw new DiscoveryHttpClientError("Upload execution failed during retry", e);
                }
            }

            if (lastError != null) {
                throw new DiscoveryHttpClientError(
                        "Service upload failed after " + retryConfig.getMaxAttempts() + " attempts: " + lastError,
                        lastError);
            }

            throw new DiscoveryHttpClientError(
                    "Service upload failed after " + retryConfig.getMaxAttempts() + " attempts.");
        });
    }

    private String buildAbsoluteUrl(ServiceInstance instance, String path) {
        String host = instance.getHost();
        int port = instance.getPort();
        String prefix = instance.getPathPrefix();

        String fullPath = "";
        if (prefix != null && !prefix.isEmpty()) {
            fullPath += prefix.startsWith("/") ? prefix : "/" + prefix;
        }

        if (fullPath.endsWith("/")) {
            fullPath = fullPath.substring(0, fullPath.length() - 1);
        }

        if (path != null && !path.isEmpty()) {
            fullPath += path.startsWith("/") ? path : "/" + path;
        }

        String protocol = instance.getProtocol();
        if (protocol == null || protocol.isEmpty()) {
            protocol = "http";
        }
        return String.format("%s://%s:%d%s", protocol, host, port, fullPath);
    }

    @Override
    public void close() throws Exception {
        if (ownsHttpClient) {
            httpClient.close();
        }
    }
}
