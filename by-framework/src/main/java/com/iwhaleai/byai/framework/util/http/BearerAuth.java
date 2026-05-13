package com.iwhaleai.byai.framework.util.http;

import org.apache.http.HttpRequest;

/**
 * Bearer token authentication (JWT, OAuth2 tokens).
 */
public class BearerAuth implements Auth {

    private final String token;

    public BearerAuth(String token) {
        this.token = token;
    }

    @Override
    public void apply(HttpRequest request) {
        request.setHeader("Authorization", "Bearer " + token);
    }
}
