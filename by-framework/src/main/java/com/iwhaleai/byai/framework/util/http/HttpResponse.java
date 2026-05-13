package com.iwhaleai.byai.framework.util.http;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wrapper for HTTP response with typed data.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HttpResponse {

    private int statusCode;
    private java.util.Map<String, String> headers;
    private Object data;
    private boolean success;

    public boolean isSuccess() {
        return success;
    }
}
