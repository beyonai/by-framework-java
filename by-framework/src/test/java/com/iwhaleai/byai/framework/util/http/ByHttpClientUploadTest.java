package com.iwhaleai.byai.framework.util.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ByHttpClientUploadTest {

    @TempDir
    Path tempDir;

    @Test
    void uploadMultipartSupportsCustomFileFieldNameAndFilename() throws Exception {
        AtomicReference<String> contentTypeRef = new AtomicReference<>();
        AtomicReference<String> bodyRef = new AtomicReference<>();

        try (TestUploadServer server = new TestUploadServer(contentTypeRef, bodyRef);
             ByHttpClient client = ByHttpClient.builder().baseUrl(server.baseUrl()).build()) {
            Path file = tempDir.resolve("avatar-source.txt");
            Files.writeString(file, "avatar-content", StandardCharsets.UTF_8);

            HttpResponse response = client.upload(
                    "/upload",
                    null,
                    null,
                    List.of(
                            ByHttpClient.MultipartTextPart.text("bizType", "avatar"),
                            ByHttpClient.MultipartFilePart.fromPath(
                                    "image",
                                    file,
                                    "avatar-custom.txt",
                                    "text/plain"
                            )
                    )
            ).get();

            assertTrue(response.isSuccess());
            assertTrue(contentTypeRef.get().startsWith("multipart/form-data;"));
            assertMultipartContains(bodyRef.get(), "name=\"bizType\"");
            assertMultipartContains(bodyRef.get(), "avatar");
            assertMultipartContains(bodyRef.get(), "name=\"image\"; filename=\"avatar-custom.txt\"");
            assertMultipartContains(bodyRef.get(), "avatar-content");
        }
    }

    @Test
    void uploadMultipartSupportsMultipleFiles() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();

        try (TestUploadServer server = new TestUploadServer(new AtomicReference<>(), bodyRef);
             ByHttpClient client = ByHttpClient.builder().baseUrl(server.baseUrl()).build()) {
            Path first = tempDir.resolve("first.txt");
            Path second = tempDir.resolve("second.txt");
            Files.writeString(first, "first-file-content", StandardCharsets.UTF_8);
            Files.writeString(second, "second-file-content", StandardCharsets.UTF_8);

            HttpResponse response = client.upload(
                    "/upload",
                    Map.of("X-Upload-Test", "multi"),
                    null,
                    List.of(
                            ByHttpClient.MultipartFilePart.fromPath("files", first),
                            ByHttpClient.MultipartFilePart.fromPath("files", second)
                    )
            ).get();

            assertTrue(response.isSuccess());
            assertMultipartContains(bodyRef.get(), "name=\"files\"; filename=\"first.txt\"");
            assertMultipartContains(bodyRef.get(), "first-file-content");
            assertMultipartContains(bodyRef.get(), "name=\"files\"; filename=\"second.txt\"");
            assertMultipartContains(bodyRef.get(), "second-file-content");
        }
    }

    @Test
    void uploadMultipartSupportsStreamParts() throws Exception {
        AtomicReference<String> bodyRef = new AtomicReference<>();

        try (TestUploadServer server = new TestUploadServer(new AtomicReference<>(), bodyRef);
             ByHttpClient client = ByHttpClient.builder().baseUrl(server.baseUrl()).build()) {
            HttpResponse response = client.upload(
                    "/upload",
                    null,
                    Map.of("source", "stream"),
                    List.of(
                            ByHttpClient.MultipartFilePart.fromStream(
                                    "streamFile",
                                    "stream.bin",
                                    "application/octet-stream",
                                    () -> new ByteArrayInputStream("stream-body".getBytes(StandardCharsets.UTF_8))
                            )
                    )
            ).get();

            assertTrue(response.isSuccess());
            assertMultipartContains(bodyRef.get(), "name=\"streamFile\"; filename=\"stream.bin\"");
            assertMultipartContains(bodyRef.get(), "stream-body");

            Object data = response.getData();
            assertNotNull(data);
            assertInstanceOf(Map.class, data);
            @SuppressWarnings("unchecked")
            Map<String, Object> dataMap = (Map<String, Object>) data;
            assertEquals("ok", dataMap.get("status"));
        }
    }

    private static void assertMultipartContains(String body, String expectedFragment) {
        assertNotNull(body);
        assertTrue(
                body.contains(expectedFragment),
                () -> "Expected multipart body to contain [" + expectedFragment + "], actual body:\n" + body
        );
    }

    private static final class TestUploadServer implements AutoCloseable {
        private final HttpServer server;

        private TestUploadServer(AtomicReference<String> contentTypeRef, AtomicReference<String> bodyRef) throws IOException {
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            this.server.createContext("/upload", exchange -> handleUpload(exchange, contentTypeRef, bodyRef));
            this.server.start();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        private static void handleUpload(
                HttpExchange exchange,
                AtomicReference<String> contentTypeRef,
                AtomicReference<String> bodyRef
        ) throws IOException {
            contentTypeRef.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            bodyRef.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.ISO_8859_1));

            byte[] responseBody = "{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBody.length);
            exchange.getResponseBody().write(responseBody);
            exchange.close();
        }
    }
}
