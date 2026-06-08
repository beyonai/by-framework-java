package com.iwhaleai.byai.framework.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Minimal Langfuse ingestion client for GatewayClient client.dispatch spans.
 */
@Slf4j
public class LangfuseClientDispatchTracer implements ClientDispatchTracer {
    private static final String DEFAULT_BASE_URL = "https://cloud.langfuse.com";

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final String publicKey;
    private final String secretKey;
    private final String baseUrl;
    private final String ingestionUrl;
    private final boolean enabled;

    public LangfuseClientDispatchTracer() {
        this(System.getenv(), HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .build(), new ObjectMapper());
    }

    LangfuseClientDispatchTracer(
            Map<String, String> environment,
            HttpClient httpClient,
            ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.httpClient = httpClient;
        this.publicKey = clean(environment.get("LANGFUSE_PUBLIC_KEY"));
        this.secretKey = clean(environment.get("LANGFUSE_SECRET_KEY"));
        this.baseUrl = normalizeBaseUrl(firstNonBlank(
                clean(environment.get("LANGFUSE_BASE_URL")),
                clean(environment.get("LANGFUSE_HOST")),
                DEFAULT_BASE_URL));
        this.ingestionUrl = firstNonBlank(
                clean(environment.get("LANGFUSE_INGESTION_URL")),
                clean(environment.get("BY_FRAMEWORK_LANGFUSE_INGESTION_URL")),
                baseUrl + "/api/public/ingestion");
        String enabledValue = firstNonBlank(
                clean(environment.get("BY_FRAMEWORK_LANGFUSE_ENABLED")),
                clean(environment.get("LANGFUSE_ENABLED")),
                "true");
        this.enabled = !"false".equalsIgnoreCase(enabledValue)
                && !publicKey.isBlank()
                && !secretKey.isBlank();
    }

    @Override
    public ClientDispatchObservation start(ClientDispatchRequest request) {
        if (!enabled) {
            return null;
        }

        String now = Instant.now().toString();
        String traceIdHex = GatewayClient.stableTraceIdHex(request.traceId());
        String observationId = request.observationId();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("message_id", request.messageId());
        metadata.put("session_id", request.sessionId());
        metadata.put("trace_id", request.traceId());
        metadata.put("target_agent_type", request.targetAgentType());
        metadata.put("user_code", request.userCode());
        metadata.put("user_name", request.userName());
        metadata.put("header_metadata", request.metadata() != null ? request.metadata() : Map.of());

        Map<String, Object> traceBody = new HashMap<>();
        traceBody.put("id", traceIdHex);
        traceBody.put("name", "client.dispatch:" + request.targetAgentType());
        traceBody.put("sessionId", request.sessionId());
        traceBody.put("input", request.content());
        traceBody.put("metadata", metadata);

        Map<String, Object> spanBody = new HashMap<>();
        spanBody.put("id", observationId);
        spanBody.put("traceId", traceIdHex);
        spanBody.put("name", "client.dispatch:" + request.targetAgentType());
        spanBody.put("startTime", now);
        spanBody.put("input", request.content());
        spanBody.put("metadata", metadata);

        postBatch(List.of(
                event("trace-create", now, traceBody),
                event("span-create", now, spanBody)
        ));
        return new LangfuseObservation(traceIdHex, observationId);
    }

    private void postBatch(List<Map<String, Object>> events) {
        try {
            Map<String, Object> payload = Map.of("batch", events);
            String body = objectMapper.writeValueAsString(payload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(ingestionUrl))
                    .timeout(Duration.ofSeconds(10))
                    .version(HttpClient.Version.HTTP_1_1)
                    .header("Content-Type", "application/json")
                    .header("Authorization", basicAuthHeader())
                    .header("x-langfuse-sdk-name", "by-framework-java")
                    .header("x-langfuse-public-key", publicKey)
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                log.warn("Langfuse client.dispatch ingestion failed: status={}, body={}",
                        response.statusCode(), response.body());
            }
        } catch (Exception e) {
            log.warn("Langfuse client.dispatch ingestion skipped: endpoint={}, error={}",
                    ingestionUrl, e.getMessage());
        }
    }

    private Map<String, Object> event(String type, String timestamp, Map<String, Object> body) {
        Map<String, Object> event = new HashMap<>();
        event.put("id", UUID.randomUUID().toString());
        event.put("type", type);
        event.put("timestamp", timestamp);
        event.put("body", body);
        return event;
    }

    private String basicAuthHeader() {
        String credentials = publicKey + ":" + secretKey;
        return "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    private static String normalizeBaseUrl(String value) {
        String normalized = value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
        return normalized.isBlank() ? DEFAULT_BASE_URL : normalized;
    }

    private static String clean(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2
                && ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("'") && trimmed.endsWith("'")))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String firstNonBlank(String first, String second, String fallback) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return fallback;
    }

    private final class LangfuseObservation implements ClientDispatchObservation {
        private final String traceId;
        private final String observationId;

        private LangfuseObservation(String traceId, String observationId) {
            this.traceId = traceId;
            this.observationId = observationId;
        }

        @Override
        public String id() {
            return observationId;
        }

        @Override
        public void end(Object output, String error) {
            String now = Instant.now().toString();
            Map<String, Object> body = new HashMap<>();
            body.put("id", observationId);
            body.put("traceId", traceId);
            body.put("endTime", now);
            body.put("output", output);
            if (error != null && !error.isBlank()) {
                body.put("level", "ERROR");
                body.put("statusMessage", error);
            }
            postBatch(List.of(event("span-update", now, body)));
        }
    }
}
