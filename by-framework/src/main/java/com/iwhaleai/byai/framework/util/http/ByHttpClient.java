package com.iwhaleai.byai.framework.util.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.iwhaleai.byai.framework.common.exceptions.HttpRequestError;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * HTTP client wrapper with automatic retry, timeout, and error handling.
 *
 * Features:
 * - Configurable retry with exponential backoff
 * - Automatic timeout handling
 * - Structured error responses
 * - Request/response logging
 * - Pluggable authentication (API Key, Bearer, Basic, OAuth2)
 */
@Slf4j
public class ByHttpClient implements AutoCloseable {

    private final String baseUrl;
    private final Map<String, String> defaultHeaders;
    private final Auth auth;
    private final int timeout;
    private final RetryConfig retryConfig;
    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;

    private CloseableHttpClient httpClient;

    public ByHttpClient(
            String baseUrl,
            Auth auth,
            int timeout,
            Map<String, String> headers,
            RetryConfig retryConfig) {
        this.baseUrl = baseUrl != null ? baseUrl.replaceAll("/$", "") : "";
        this.auth = auth != null ? auth : new NoAuth();
        this.timeout = timeout > 0 ? timeout : 30;
        this.defaultHeaders = headers != null ? new HashMap<>(headers) : new HashMap<>();
        this.retryConfig = retryConfig != null ? retryConfig : RetryConfig.builder().build();
        this.objectMapper = new ObjectMapper();
        this.executorService = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "by-http-client");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Builder for ByHttpClient.
     */
    public static class Builder {
        private String baseUrl = "";
        private Auth auth = new NoAuth();
        private int timeout = 30;
        private Map<String, String> headers = new HashMap<>();
        private RetryConfig retryConfig = RetryConfig.builder().build();

        public Builder baseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
            return this;
        }

        public Builder auth(Auth auth) {
            this.auth = auth;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder retryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return this;
        }

        public ByHttpClient build() {
            return new ByHttpClient(baseUrl, auth, timeout, headers, retryConfig);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    @FunctionalInterface
    public interface InputStreamSupplier {
        InputStream open() throws IOException;
    }

    public sealed interface MultipartPart permits MultipartTextPart, MultipartFilePart {
        String partName();
    }

    public record MultipartTextPart(
            String partName,
            String value,
            ContentType contentType) implements MultipartPart {
        public MultipartTextPart {
            partName = Objects.requireNonNull(partName, "partName cannot be null");
            value = Objects.requireNonNull(value, "value cannot be null");
            contentType = contentType != null
                    ? contentType
                    : ContentType.create("text/plain", StandardCharsets.UTF_8);
        }

        public static MultipartTextPart text(String partName, String value) {
            return new MultipartTextPart(partName, value, ContentType.create("text/plain", StandardCharsets.UTF_8));
        }

        public static MultipartTextPart text(String partName, String value, String contentType) {
            return new MultipartTextPart(partName, value, parseContentType(contentType, "text/plain"));
        }
    }

    public record MultipartFilePart(
            String partName,
            String fileName,
            ContentType contentType,
            Path filePath,
            InputStreamSupplier inputStreamSupplier) implements MultipartPart {
        public MultipartFilePart {
            partName = Objects.requireNonNull(partName, "partName cannot be null");
            boolean hasFilePath = filePath != null;
            boolean hasStreamSupplier = inputStreamSupplier != null;
            if (hasFilePath == hasStreamSupplier) {
                throw new IllegalArgumentException("Exactly one of filePath or inputStreamSupplier must be provided");
            }

            if (hasFilePath) {
                filePath = filePath.toAbsolutePath().normalize();
                fileName = fileName != null ? fileName : filePath.getFileName().toString();
                contentType = contentType != null ? contentType : detectContentType(filePath);
            } else {
                fileName = Objects.requireNonNull(fileName, "fileName cannot be null when streaming upload");
                contentType = contentType != null ? contentType : ContentType.DEFAULT_BINARY;
            }
        }

        public static MultipartFilePart fromPath(String partName, Path filePath) {
            return new MultipartFilePart(partName, null, null, filePath, null);
        }

        public static MultipartFilePart fromPath(String partName, Path filePath, String fileName, String contentType) {
            return new MultipartFilePart(partName, fileName, parseContentType(contentType), filePath, null);
        }

        public static MultipartFilePart fromStream(
                String partName,
                String fileName,
                String contentType,
                InputStreamSupplier inputStreamSupplier) {
            return new MultipartFilePart(partName, fileName, parseContentType(contentType), null, inputStreamSupplier);
        }
    }

    private record MultipartRequestContext(
            HttpUriRequest request,
            List<InputStream> openedStreams) implements AutoCloseable {
        private MultipartRequestContext {
            openedStreams = openedStreams != null ? openedStreams : Collections.emptyList();
        }

        @Override
        public void close() {
            for (InputStream inputStream : openedStreams) {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private synchronized void ensureClient() {
        if (httpClient == null) {
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(timeout * 1000)
                    .setSocketTimeout(timeout * 1000)
                    .setConnectionRequestTimeout(timeout * 1000)
                    .build();

            httpClient = HttpClients.custom()
                    .setDefaultRequestConfig(requestConfig)
                    .build();
        }
    }

    /**
     * Execute HTTP request with retry logic.
     */
    public CompletableFuture<HttpResponse> request(
            String method,
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data,
            int retryCount) {
        return CompletableFuture.supplyAsync(() -> {
            ensureClient();

            Map<String, String> requestHeaders = new HashMap<>(defaultHeaders);
            if (headers != null) {
                requestHeaders.putAll(headers);
            }

            int attempt = retryCount + 1;
            Exception lastError = null;

            while (attempt <= retryConfig.getMaxAttempts()) {
                try {
                    log.debug("[{}] {} (attempt {})", method.toUpperCase(), url, attempt);

                    HttpUriRequest request = buildRequest(method, url, requestHeaders, params, json, data);
                    auth.apply(request);

                    try (CloseableHttpResponse response = httpClient.execute(request)) {
                        HttpResponse httpResponse = parseResponse(response);

                        if (httpResponse.isSuccess()) {
                            log.debug("[{}] {} -> {}", method.toUpperCase(), url, httpResponse.getStatusCode());
                            return httpResponse;
                        }

                        if (retryConfig.getRetryOnStatusCodes().contains(httpResponse.getStatusCode())
                                && attempt < retryConfig.getMaxAttempts()) {
                            double delay = RetryConfig.calculateDelay(attempt, retryConfig);
                            log.warn("[{}] {} -> {}, retrying in {:.1f}s",
                                    method.toUpperCase(), url, httpResponse.getStatusCode(), delay);
                            TimeUnit.MILLISECONDS.sleep((long) (delay * 1000));
                            attempt++;
                            continue;
                        }

                        log.error("[{}] {} -> {}", method.toUpperCase(), url, httpResponse.getStatusCode());
                        return httpResponse;
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    lastError = e;
                    break;
                } catch (Exception e) {
                    lastError = e;
                    log.warn("[{}] {} error (attempt {}): {}",
                            method.toUpperCase(), url, attempt, e.getMessage());

                    if (attempt < retryConfig.getMaxAttempts()) {
                        double delay = RetryConfig.calculateDelay(attempt, retryConfig);
                        log.warn("Retrying in {:.1f}s after {}", delay, e.getClass().getSimpleName());
                        try {
                            TimeUnit.MILLISECONDS.sleep((long) (delay * 1000));
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        attempt++;
                    } else {
                        break;
                    }
                }
            }

            throw new HttpRequestError(
                    "Request failed after " + retryConfig.getMaxAttempts() + " attempts: " + lastError,
                    lastError);
        }, executorService);
    }

    private HttpUriRequest buildRequest(
            String method,
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) throws Exception {
        String absoluteUrl = url.startsWith("http") ? url : baseUrl + "/" + url.replaceAll("^/", "");

        RequestBuilder builder = RequestBuilder.create(method.toUpperCase())
                .setUri(absoluteUrl);

        // Add headers
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            builder.addHeader(entry.getKey(), entry.getValue());
        }

        // Add query params
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                builder.addParameter(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        // Add body (json or data)
        if (json != null) {
            String jsonStr = objectMapper.writeValueAsString(json);
            builder.setEntity(new StringEntity(jsonStr, ContentType.APPLICATION_JSON));
        } else if (data != null && !data.isEmpty()) {
            String dataStr = objectMapper.writeValueAsString(data);
            builder.setEntity(new StringEntity(dataStr, ContentType.APPLICATION_JSON));
        }

        return builder.build();
    }

    private HttpResponse parseResponse(CloseableHttpResponse response) throws IOException {
        StatusLine statusLine = response.getStatusLine();
        int statusCode = statusLine.getStatusCode();

        Map<String, String> headers = new HashMap<>();
        for (Header header : response.getAllHeaders()) {
            headers.put(header.getName(), header.getValue());
        }

        Object data = null;
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            String contentType = entity.getContentType() != null ? entity.getContentType().getValue() : "";
            String body = EntityUtils.toString(entity, StandardCharsets.UTF_8);

            if (contentType.contains("application/json")) {
                try {
                    data = objectMapper.readValue(body, Object.class);
                } catch (Exception e) {
                    data = body;
                }
            } else {
                data = body;
            }
        }

        return new HttpResponse(
                statusCode,
                headers,
                data,
                statusCode >= 200 && statusCode < 300);
    }

    /**
     * Send GET request.
     */
    public CompletableFuture<HttpResponse> get(String url, Map<String, String> headers, Map<String, Object> params) {
        return request("GET", url, headers, params, null, null, 0);
    }

    /**
     * Send POST request.
     */
    public CompletableFuture<HttpResponse> post(String url, Map<String, String> headers, Object json,
            Map<String, Object> data) {
        return request("POST", url, headers, null, json, data, 0);
    }

    /**
     * Send PUT request.
     */
    public CompletableFuture<HttpResponse> put(String url, Map<String, String> headers, Object json,
            Map<String, Object> data) {
        return request("PUT", url, headers, null, json, data, 0);
    }

    /**
     * Send PATCH request.
     */
    public CompletableFuture<HttpResponse> patch(String url, Map<String, String> headers, Object json,
            Map<String, Object> data) {
        return request("PATCH", url, headers, null, json, data, 0);
    }

    /**
     * Send DELETE request.
     */
    public CompletableFuture<HttpResponse> delete(String url, Map<String, String> headers, Map<String, Object> params) {
        return request("DELETE", url, headers, params, null, null, 0);
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // File Download Methods
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Download file as InputStream.
     * Caller is responsible for closing the stream.
     *
     * @return InputStream containing the file content
     */
    public CompletableFuture<InputStream> download(String url, Map<String, String> headers,
            Map<String, Object> params) {
        return download("GET", url, headers, params, null, null);
    }

    /**
     * Download file as InputStream using the specified HTTP method.
     * Caller is responsible for closing the stream.
     *
     * @return InputStream containing the file content
     */
    public CompletableFuture<InputStream> download(
            String method,
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return CompletableFuture.supplyAsync(() -> {
            ensureClient();

            Map<String, String> requestHeaders = new HashMap<>(defaultHeaders);
            if (headers != null) {
                requestHeaders.putAll(headers);
            }

            try {
                HttpUriRequest request = buildRequest(method, url, requestHeaders, params, json, data);
                auth.apply(request);

                log.debug("[{}] {} (download)", method.toUpperCase(), url);
                CloseableHttpResponse response = httpClient.execute(request);

                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode >= 200 && statusCode < 300) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        return new ResponseInputStream(entity.getContent(), response);
                    }
                    response.close();
                    throw new HttpRequestError("Empty response body", null, statusCode, null);
                } else {
                    // Consume entity to allow connection reuse
                    EntityUtils.consumeQuietly(response.getEntity());
                    response.close();
                    throw new HttpRequestError("Download failed with status: " + statusCode, null, statusCode, null);
                }
            } catch (HttpRequestError e) {
                throw e;
            } catch (Exception e) {
                throw new HttpRequestError("Download failed: " + e.getMessage(), e);
            }
        }, executorService);
    }

    /**
     * Download file and save to local path.
     *
     * @param url     Download URL
     * @param toPath  Local file path to save
     * @param headers Optional headers
     * @param params  Optional query parameters
     * @return Path to the downloaded file
     */
    public CompletableFuture<Path> downloadToFile(
            String url,
            Path toPath,
            Map<String, String> headers,
            Map<String, Object> params) {
        return downloadToFile("GET", url, toPath, headers, params, null, null);
    }

    /**
     * Download file with the specified HTTP method and save to local path.
     */
    public CompletableFuture<Path> downloadToFile(
            String method,
            String url,
            Path toPath,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return download(method, url, headers, params, json, data).thenApply(inputStream -> {
            try {
                Files.createDirectories(toPath.getParent());
                try (InputStream is = inputStream;
                        FileOutputStream fos = new FileOutputStream(toPath.toFile())) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    long totalBytes = 0;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        fos.write(buffer, 0, bytesRead);
                        totalBytes += bytesRead;
                    }
                    log.debug("Downloaded {} bytes to {}", totalBytes, toPath);
                    return toPath;
                }
            } catch (IOException e) {
                throw new HttpRequestError("Failed to save file: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Download file as byte array.
     * Note: For large files, use download() or downloadToFile() instead to avoid
     * memory issues.
     *
     * @return byte array containing the file content
     */
    public CompletableFuture<byte[]> downloadBytes(String url, Map<String, String> headers,
            Map<String, Object> params) {
        return downloadBytes("GET", url, headers, params, null, null);
    }

    /**
     * Download file as byte array using the specified HTTP method.
     */
    public CompletableFuture<byte[]> downloadBytes(
            String method,
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            Object json,
            Map<String, Object> data) {
        return download(method, url, headers, params, json, data).thenApply(inputStream -> {
            try {
                return inputStream.readAllBytes();
            } catch (IOException e) {
                throw new HttpRequestError("Failed to read bytes: " + e.getMessage(), e);
            } finally {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                }
            }
        });
    }

    private static final class ResponseInputStream extends FilterInputStream {
        private final CloseableHttpResponse response;

        private ResponseInputStream(InputStream inputStream, CloseableHttpResponse response) {
            super(inputStream);
            this.response = response;
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                response.close();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────────
    // File Upload Methods
    // ─────────────────────────────────────────────────────────────────────────────

    /**
     * Upload file using multipart/form-data.
     *
     * @param url        Upload URL
     * @param filePath   Path to the file to upload
     * @param headers    Optional headers
     * @param formFields Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            Path filePath,
            String fileField,
            Map<String, String> headers,
            Map<String, String> formFields) {
        List<MultipartPart> parts = new ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        String field = fileField != null ? fileField : "file";
        parts.add(MultipartFilePart.fromPath(field, filePath));
        return upload(url, headers, null, parts);
    }

    /**
     * Upload file using multipart/form-data with default field name "file".
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            Path filePath,
            Map<String, String> headers,
            Map<String, String> formFields) {
        return upload(url, filePath, "file", headers, formFields);
    }

    /**
     * Upload multiple files using multipart/form-data.
     *
     * @param url           Upload URL
     * @param filePaths     List of paths to the files to upload
     * @param fileField     The name of the file field (default: "file")
     * @param headers       Optional headers
     * @param formFields    Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            List<Path> filePaths,
            String fileField,
            Map<String, String> headers,
            Map<String, String> formFields
    ) {
        List<MultipartPart> parts = new ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        if (filePaths != null) {
            String field = fileField != null ? fileField : "file";
            for (Path filePath : filePaths) {
                parts.add(MultipartFilePart.fromPath(field, filePath));
            }
        }
        return upload(url, headers, null, parts);
    }

    /**
     * Upload multiple files using multipart/form-data with default field name "file".
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            List<Path> filePaths,
            Map<String, String> headers,
            Map<String, String> formFields
    ) {
        return upload(url, filePaths, "file", headers, formFields);
    }

    /**
     * Upload file from a stream using multipart/form-data.
     *
     * @param url            Upload URL
     * @param fileName       Name of the file
     * @param fileField      The name of the file field (default: "file")
     * @param streamSupplier Supplier for the input stream
     * @param headers        Optional headers
     * @param formFields     Optional form fields
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            String fileName,
            String fileField,
            InputStreamSupplier streamSupplier,
            Map<String, String> headers,
            Map<String, String> formFields
    ) {
        List<MultipartPart> parts = new ArrayList<>();
        if (formFields != null) {
            for (Map.Entry<String, String> entry : formFields.entrySet()) {
                parts.add(MultipartTextPart.text(entry.getKey(), entry.getValue()));
            }
        }
        String field = fileField != null ? fileField : "file";
        parts.add(MultipartFilePart.fromStream(field, fileName, null, streamSupplier));
        return upload(url, headers, null, parts);
    }

    /**
     * Upload file from a stream using multipart/form-data with default field name "file".
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            String fileName,
            InputStreamSupplier streamSupplier,
            Map<String, String> headers,
            Map<String, String> formFields
    ) {
        return upload(url, fileName, "file", streamSupplier, headers, formFields);
    }

    /**
     * Upload multipart/form-data content.
     *
     * @param url     Upload URL
     * @param headers Optional headers
     * @param params  Optional query parameters
     * @param parts   Multipart parts, including text and file/stream parts
     * @return HttpResponse
     */
    public CompletableFuture<HttpResponse> upload(
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            List<MultipartPart> parts) {
        return CompletableFuture.supplyAsync(() -> {
            ensureClient();

            Map<String, String> requestHeaders = new HashMap<>(defaultHeaders);
            if (headers != null) {
                requestHeaders.putAll(headers);
            }

            if (parts == null || parts.isEmpty()) {
                throw new IllegalArgumentException("Multipart parts cannot be empty");
            }

            MultipartRequestContext multipartRequest = null;
            try {
                multipartRequest = buildMultipartRequest("POST", url, requestHeaders, params, parts);
                auth.apply(multipartRequest.request());

                log.debug("[POST] {} (multipart upload parts: {})", url, parts.size());

                try (CloseableHttpResponse response = httpClient.execute(multipartRequest.request())) {
                    return parseResponse(response);
                }

            } catch (HttpRequestError e) {
                throw e;
            } catch (Exception e) {
                throw new HttpRequestError("Upload failed: " + e.getMessage(), e);
            } finally {
                if (multipartRequest != null) {
                    multipartRequest.close();
                }
            }
        }, executorService);
    }

    private MultipartRequestContext buildMultipartRequest(
            String method,
            String url,
            Map<String, String> headers,
            Map<String, Object> params,
            List<MultipartPart> parts) throws Exception {
        String absoluteUrl = url.startsWith("http") ? url : baseUrl + "/" + url.replaceAll("^/", "");
        MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create()
                .setMode(HttpMultipartMode.STRICT);
        List<InputStream> openedStreams = new ArrayList<>();

        for (MultipartPart part : parts) {
            if (part instanceof MultipartTextPart textPart) {
                entityBuilder.addPart(
                        textPart.partName(),
                        new StringBody(textPart.value(), textPart.contentType()));
                continue;
            }

            if (part instanceof MultipartFilePart filePart) {
                addMultipartFilePart(entityBuilder, openedStreams, filePart);
            }
        }

        RequestBuilder builder = RequestBuilder.create(method.toUpperCase())
                .setUri(absoluteUrl)
                .setEntity(entityBuilder.build());

        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                builder.addParameter(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }

        for (Map.Entry<String, String> entry : headers.entrySet()) {
            if (!"Content-Type".equalsIgnoreCase(entry.getKey())) {
                builder.addHeader(entry.getKey(), entry.getValue());
            }
        }

        return new MultipartRequestContext(builder.build(), openedStreams);
    }

    private void addMultipartFilePart(
            MultipartEntityBuilder entityBuilder,
            List<InputStream> openedStreams,
            MultipartFilePart filePart) throws IOException {
        if (filePart.filePath() != null) {
            validateUploadFile(filePart.filePath());
            entityBuilder.addPart(
                    filePart.partName(),
                    new FileBody(filePart.filePath().toFile(), filePart.contentType(), filePart.fileName()));
            return;
        }

        InputStream inputStream = filePart.inputStreamSupplier().open();
        if (inputStream == null) {
            throw new IllegalArgumentException("InputStreamSupplier returned null for part: " + filePart.partName());
        }
        openedStreams.add(inputStream);
        entityBuilder.addPart(
                filePart.partName(),
                new InputStreamBody(inputStream, filePart.contentType(), filePart.fileName()));
    }

    private void validateUploadFile(Path filePath) throws IOException {
        if (!Files.exists(filePath)) {
            throw new IOException("Upload file does not exist: " + filePath);
        }
        if (!Files.isRegularFile(filePath)) {
            throw new IOException("Upload file is not a regular file: " + filePath);
        }
        if (!Files.isReadable(filePath)) {
            throw new IOException("Upload file is not readable: " + filePath);
        }
    }

    private static ContentType detectContentType(Path filePath) {
        try {
            return parseContentType(Files.probeContentType(filePath));
        } catch (IOException e) {
            return ContentType.DEFAULT_BINARY;
        }
    }

    private static ContentType parseContentType(String contentType) {
        return parseContentType(contentType, ContentType.DEFAULT_BINARY.getMimeType());
    }

    private static ContentType parseContentType(String contentType, String defaultMimeType) {
        String resolved = contentType != null && !contentType.isBlank()
                ? contentType
                : defaultMimeType;
        return ContentType.create(resolved);
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        if (httpClient != null) {
            httpClient.close();
            httpClient = null;
        }
    }
}
