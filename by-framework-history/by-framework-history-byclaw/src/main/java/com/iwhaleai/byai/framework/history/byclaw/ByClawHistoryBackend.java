package com.iwhaleai.byai.framework.history.byclaw;

import com.iwhaleai.byai.framework.common.RedisClient;
import com.iwhaleai.byai.framework.core.discovery.DiscoveryClient;
import com.iwhaleai.byai.framework.core.runtime.history.BaseHistoryBackend;
import com.iwhaleai.byai.framework.util.http.DiscoveryHttpClient;
import com.iwhaleai.byai.framework.util.http.HttpResponse;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * History backend that integrates with external ByAI service via HTTP API.
 * Uses DiscoveryHttpClient for service discovery and load balancing.
 */
@Slf4j
public class ByClawHistoryBackend extends BaseHistoryBackend {

    private static final String DEFAULT_SERVICE_NAME = System.getenv("BYAI_SERVICE_NAME");
    private static final String ACTUAL_DEFAULT_SERVICE_NAME = "byai-service";
    private static final String GET_MESSAGES_ENDPOINT = "/byaiService/open/api/inner/getMessages";

    private final String serviceName;
    private final DiscoveryHttpClient discoveryHttpClient;

    public ByClawHistoryBackend(DiscoveryHttpClient discoveryHttpClient, String serviceName) {
        this.discoveryHttpClient = discoveryHttpClient;
        this.serviceName = serviceName != null ? serviceName : 
                (DEFAULT_SERVICE_NAME != null ? DEFAULT_SERVICE_NAME : ACTUAL_DEFAULT_SERVICE_NAME);
    }

    public ByClawHistoryBackend(String serviceName) {
        this(DiscoveryHttpClient.builder()
                .discoveryClient(new DiscoveryClient(RedisClient.getInstance()))
                .build(), serviceName);
    }

    public ByClawHistoryBackend() {
        this((String) null);
    }

    @Override
    public List<Map<String, Object>> getHistory(String sessionId, int limit) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("sessionId", sessionId);
            payload.put("topK", limit);

            // Use DiscoveryHttpClient to perform the request
            HttpResponse response = discoveryHttpClient.post(
                    serviceName,
                    GET_MESSAGES_ENDPOINT,
                    Map.of("Content-Type", "application/json"),
                    payload,
                    null
            ).get(); // Synchronous wait for result

            if (response.isSuccess()) {
                return parseResponse(response.getData(), sessionId);
            } else {
                log.warn("Failed to get history from ByAI service: status={}, data={}",
                        response.getStatusCode(), response.getData());
                return Collections.emptyList();
            }
        } catch (Exception e) {
            log.error("Error fetching history from ByAI service for session: {}", sessionId, e);
            return Collections.emptyList();
        }
    }

    @Override
    public void saveMessage(String sessionId, String role, String content, Map<String, Object> metadata) {
        // This backend is read-only - it fetches from external service
        log.debug("ByClawHistoryBackend is read-only, ignoring saveMessage");
    }

    private List<Map<String, Object>> parseResponse(Object responseData, String sessionId) {
        try {
            if (!(responseData instanceof Map)) {
                log.warn("Unexpected response format from ByAI service for session: {}", sessionId);
                return Collections.emptyList();
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> response = (Map<String, Object>) responseData;
            List<Map<String, Object>> result = new ArrayList<>();

            // Try to extract messages from response
            Object data = response.get("data");
            if (data instanceof List<?> messages) {
                for (Object msg : messages) {
                    if (msg instanceof Map<?, ?> message) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> converted = convertMessage((Map<String, Object>) message);
                        if (converted != null) {
                            result.add(converted);
                        }
                    }
                }
            }

            return result;
        } catch (Exception e) {
            log.error("Error parsing ByAI service response for session: {}", sessionId, e);
            return Collections.emptyList();
        }
    }

    /**
     * Convert ByAI message format to standard format.
     * Maps usage=1 to "user", usage=2 to "assistant".
     */
    private Map<String, Object> convertMessage(Map<String, Object> message) {
        try {
            Map<String, Object> result = new HashMap<>();

            // Map usage to role
            Object usage = message.get("usage");
            if (usage instanceof Number usageValue) {
                switch (usageValue.intValue()) {
                    case 1:
                        result.put("role", "user");
                        break;
                    case 2:
                        result.put("role", "assistant");
                        break;
                    default:
                        result.put("role", "system");
                }
            } else {
                result.put("role", "assistant");
            }

            // Map messageContent to content
            Object content = message.get("messageContent");
            result.put("content", content != null ? content.toString() : "");

            // Preserve metadata
            result.put("metadata", message.getOrDefault("metadata", Collections.emptyMap()));

            return result;
        } catch (Exception e) {
            log.warn("Failed to convert message: {}", message, e);
            return null;
        }
    }
}
