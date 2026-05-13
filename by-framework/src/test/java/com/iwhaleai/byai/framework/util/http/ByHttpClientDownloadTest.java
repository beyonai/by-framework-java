package com.iwhaleai.byai.framework.util.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ByHttpClientDownloadTest {

    @Test
    void downloadUsesGetByDefault() throws Exception {
        AtomicReference<String> methodRef = new AtomicReference<>();
        AtomicReference<String> bodyRef = new AtomicReference<>();

        try (TestDownloadServer server = new TestDownloadServer(methodRef, bodyRef);
             ByHttpClient client = ByHttpClient.builder().baseUrl(server.baseUrl()).build()) {
            byte[] bytes = client.downloadBytes("/download", null, Map.of("name", "default")).get();

            assertArrayEquals("download-get".getBytes(StandardCharsets.UTF_8), bytes);
            assertEquals("GET", methodRef.get());
            assertEquals("", bodyRef.get());
        }
    }

    @Test
    void downloadSupportsPostWithJsonBody() throws Exception {
        AtomicReference<String> methodRef = new AtomicReference<>();
        AtomicReference<String> bodyRef = new AtomicReference<>();
        AtomicReference<String> contentTypeRef = new AtomicReference<>();

        try (TestDownloadServer server = new TestDownloadServer(methodRef, bodyRef, contentTypeRef);
             ByHttpClient client = ByHttpClient.builder().baseUrl(server.baseUrl()).build()) {
            byte[] bytes = client.downloadBytes(
                    "POST",
                    "/download",
                    Map.of("X-Download-Mode", "post"),
                    Map.of("name", "post"),
                    Map.of("taskId", "task-123"),
                    null
            ).get();

            assertArrayEquals("download-post".getBytes(StandardCharsets.UTF_8), bytes);
            assertEquals("POST", methodRef.get());
            assertEquals("{\"taskId\":\"task-123\"}", bodyRef.get());
            assertEquals("application/json; charset=UTF-8", contentTypeRef.get());
        }
    }

    private static final class TestDownloadServer implements AutoCloseable {
        private final HttpServer server;

        private TestDownloadServer(
                AtomicReference<String> methodRef,
                AtomicReference<String> bodyRef
        ) throws IOException {
            this(methodRef, bodyRef, new AtomicReference<>());
        }

        private TestDownloadServer(
                AtomicReference<String> methodRef,
                AtomicReference<String> bodyRef,
                AtomicReference<String> contentTypeRef
        ) throws IOException {
            this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            this.server.createContext("/download", exchange -> handleRequest(exchange, methodRef, bodyRef, contentTypeRef));
            this.server.start();
        }

        private String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        private static void handleRequest(
                HttpExchange exchange,
                AtomicReference<String> methodRef,
                AtomicReference<String> bodyRef,
                AtomicReference<String> contentTypeRef
        ) throws IOException {
            methodRef.set(exchange.getRequestMethod());
            bodyRef.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            contentTypeRef.set(exchange.getRequestHeaders().getFirst("Content-Type"));

            String responseBody = "GET".equals(exchange.getRequestMethod()) ? "download-get" : "download-post";
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }
}
