package com.iwhaleai.byai.framework.util.http;

import org.apache.http.HttpRequest;

/**
 * No authentication.
 */
public class NoAuth implements Auth {

    @Override
    public void apply(HttpRequest request) {
        // No-op
    }
}
