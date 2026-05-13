package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Raised when service discovery fails to find a valid instance.
 */
public class DiscoveryHttpClientError extends HttpClientError {

    public DiscoveryHttpClientError(String message) {
        super(message);
    }

    public DiscoveryHttpClientError(String message, Throwable cause) {
        super(message, cause);
    }

    public DiscoveryHttpClientError(String message, Throwable cause, Integer statusCode, String responseBody) {
        super(message, cause, statusCode, responseBody);
    }
}
