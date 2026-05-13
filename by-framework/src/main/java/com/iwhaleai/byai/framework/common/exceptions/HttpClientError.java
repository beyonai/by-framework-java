package com.iwhaleai.byai.framework.common.exceptions;

/**
 * Base exception for HTTP client errors.
 */
public class HttpClientError extends FrameworkException {

    private final Integer statusCode;
    private final String responseBody;

    public HttpClientError(String message) {
        super(message);
        this.statusCode = null;
        this.responseBody = null;
    }

    public HttpClientError(String message, Throwable cause) {
        super(message, cause);
        this.statusCode = null;
        this.responseBody = null;
    }

    public HttpClientError(String message, Throwable cause, Integer statusCode, String responseBody) {
        super(message, cause);
        this.statusCode = statusCode;
        this.responseBody = responseBody;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public String getResponseBody() {
        return responseBody;
    }
}
