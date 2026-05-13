package com.iwhaleai.byai.framework.util.http;

import org.apache.http.HttpRequest;

/**
 * Abstract base class for authentication strategies.
 */
public interface Auth {

    /**
     * Apply authentication to the outgoing request.
     */
    void apply(HttpRequest request);
}
