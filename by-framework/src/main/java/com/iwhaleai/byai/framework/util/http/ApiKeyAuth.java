package com.iwhaleai.byai.framework.util.http;

import org.apache.http.HttpRequest;

/**
 * API key authentication (header or query param).
 */
public class ApiKeyAuth implements Auth {

    private final String key;
    private final String value;
    private final boolean inHeader;
    private final String prefix;

    public ApiKeyAuth(String key, String value, boolean inHeader, String prefix) {
        this.key = key;
        this.value = value;
        this.inHeader = inHeader;
        this.prefix = prefix != null ? prefix : "";
    }

    @Override
    public void apply(HttpRequest request) {
        if (inHeader) {
            String headerValue = prefix.isEmpty() ? value : prefix + " " + value;
            request.setHeader(key, headerValue);
        } else {
            // For query param, we would need to modify the URI
            // This is a simplified implementation
            throw new UnsupportedOperationException(
                "ApiKeyAuth with inHeader=false requires URI modification, not supported directly");
        }
    }
}
