package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Raised when an HTTP request fails after all retry attempts.
 */
public class HttpRequestError extends HttpClientError {

    public HttpRequestError(String message) {
        super(message);
    }

    public HttpRequestError(String message, Throwable cause) {
        super(message, cause);
    }

    public HttpRequestError(String message, Throwable cause, Integer statusCode, String responseBody) {
        super(message, cause, statusCode, responseBody);
    }
}
